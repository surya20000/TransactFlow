import argparse
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REQUIRED = ["transaction_id", "account_id", "device_id", "amount", "merchant", "timestamp", "city"]


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (clean_df, quarantine_df). Amount is read as str to survive bad rows."""
    df = df.copy()
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")

    null_mask = df[REQUIRED].isnull().any(axis=1) | (
        df[["account_id", "device_id", "merchant", "city"]]
          .apply(lambda s: s.str.strip().eq("")).any(axis=1)
    )
    bad_type = df["amount_num"].isna() & df["amount"].notna()
    neg_mask = df["amount_num"] <= 0
    dup_mask = df.duplicated(subset=["transaction_id"], keep="first")
    bad = null_mask | bad_type | neg_mask | dup_mask

    def reason(row):
        if dup_mask.loc[row.name]: return "duplicate"
        if bad_type.loc[row.name]: return "bad_amount_type"
        if neg_mask.loc[row.name]: return "negative_amount"
        return "null_field"

    quar = df[bad].copy()
    if len(quar):
        quar["quarantine_reason"] = quar.apply(reason, axis=1)

    clean_df = df[~bad].copy()
    clean_df["amount"]    = clean_df["amount_num"]
    clean_df["timestamp"] = pd.to_datetime(clean_df["timestamp"])
    clean_df["date"]      = clean_df["timestamp"].dt.date.astype(str)
    clean_df.drop(columns=["amount_num"], inplace=True)

    return clean_df, quar


def account_features(clean_df: pd.DataFrame) -> pd.DataFrame:
    df = clean_df.sort_values(["account_id", "timestamp"])

    # rolling 24h txn count — requires timestamp as index
    df = df.set_index("timestamp")
    txn_count_24h = (
        df.groupby("account_id")["transaction_id"]
          .rolling("24h", closed="both")
          .count()
          .rename("txn_count_24h")
          .reset_index()
    )
    df = df.reset_index().merge(txn_count_24h, on=["account_id", "timestamp"])

    acc_stats = (
        df.groupby("account_id")["amount"]
          .agg(avg_amount="mean", std_amount="std")
          .reset_index()
    )
    df = df.merge(acc_stats, on="account_id")
    df["amount_spike_flag"] = (df["amount"] > 3 * df["avg_amount"]).astype(int)

    return (
        df.groupby(["account_id", "date"])
          .agg(
              txn_count          = ("transaction_id",   "count"),
              total_amount       = ("amount",            "sum"),
              avg_amount         = ("amount",            "mean"),
              max_txn_count_24h  = ("txn_count_24h",    "max"),
              spike_count        = ("amount_spike_flag", "sum"),
              distinct_merchants = ("merchant",          "nunique"),
              distinct_cities    = ("city",              "nunique"),
          )
          .reset_index()
    )


def device_features(clean_df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        clean_df.groupby(["device_id", "date"])
                .agg(
                    distinct_accounts = ("account_id",     "nunique"),
                    txn_count         = ("transaction_id", "count"),
                    total_amount      = ("amount",         "sum"),
                )
                .reset_index()
    )
    agg["multi_account_flag"] = (agg["distinct_accounts"] > 1).astype(int)
    return agg


def run(input_path: str, output_path: str, quarantine_path: str) -> None:
    logger.info("Reading %s", input_path)
    raw = pd.read_csv(input_path, dtype=str)
    logger.info("Raw rows: %d", len(raw))

    clean_df, quar = clean(raw)
    logger.info("Clean: %d  |  Quarantined: %d", len(clean_df), len(quar))

    if len(quar):
        os.makedirs(quarantine_path, exist_ok=True)
        quar.to_parquet(f"{quarantine_path}/quarantine.parquet", index=False)

    acc = account_features(clean_df)
    dev = device_features(clean_df)

    Path(output_path).mkdir(parents=True, exist_ok=True)
    acc.to_parquet(f"{output_path}/account_features.parquet",    index=False)
    dev.to_parquet(f"{output_path}/device_features.parquet",     index=False)
    clean_df.to_parquet(f"{output_path}/clean_transactions.parquet", index=False)

    logger.info("Done → %s", output_path)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input",      required=True)
    p.add_argument("--output",     required=True)
    p.add_argument("--quarantine", required=True)
    args = p.parse_args()
    run(args.input, args.output, args.quarantine)
