"""
Load Parquet feature files into Postgres without needing Airflow.

Usage:
    python scripts/load_postgres.py \
        --features data/features/2024-01-01 \
        --transactions data/features/2024-01-01/clean_transactions.parquet

Environment variables (or .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
"""

import argparse
import logging
import os
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def db_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'transactflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'transactflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'transactflow')}"
    )
    return create_engine(url, pool_pre_ping=True)


def load_transactions(engine, path: str) -> int:
    logger.info("Loading transactions from %s", path)
    df = pd.read_parquet(path)
    # pandas to_sql with if_exists='append' is fine for bulk load;
    # the UNIQUE constraint on transaction_id will skip any re-loaded dupes
    df.to_sql(
        "transactions",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5_000,
    )
    logger.info("Inserted %d transaction rows", len(df))
    return len(df)


def upsert_account_features(engine, path: str) -> int:
    df = pd.read_parquet(path)
    logger.info("Upserting %d account feature rows", len(df))
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO account_features
                    (account_id, date, txn_count, total_amount, avg_amount,
                     max_txn_count_24h, spike_count, distinct_merchants, distinct_cities)
                VALUES
                    (:account_id, :date, :txn_count, :total_amount, :avg_amount,
                     :max_txn_count_24h, :spike_count, :distinct_merchants, :distinct_cities)
                ON CONFLICT (account_id, date) DO UPDATE SET
                    txn_count          = EXCLUDED.txn_count,
                    total_amount       = EXCLUDED.total_amount,
                    avg_amount         = EXCLUDED.avg_amount,
                    max_txn_count_24h  = EXCLUDED.max_txn_count_24h,
                    spike_count        = EXCLUDED.spike_count,
                    distinct_merchants = EXCLUDED.distinct_merchants,
                    distinct_cities    = EXCLUDED.distinct_cities,
                    updated_at         = now()
            """), row.to_dict())
    return len(df)


def upsert_device_features(engine, path: str) -> int:
    df = pd.read_parquet(path)
    logger.info("Upserting %d device feature rows", len(df))
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO device_features
                    (device_id, date, distinct_accounts, txn_count, total_amount, multi_account_flag)
                VALUES
                    (:device_id, :date, :distinct_accounts, :txn_count, :total_amount, :multi_account_flag)
                ON CONFLICT (device_id, date) DO UPDATE SET
                    distinct_accounts  = EXCLUDED.distinct_accounts,
                    txn_count          = EXCLUDED.txn_count,
                    total_amount       = EXCLUDED.total_amount,
                    multi_account_flag = EXCLUDED.multi_account_flag,
                    updated_at         = now()
            """), row.to_dict())
    return len(df)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--features",     required=True,  help="Path to feature output dir (e.g. data/features/2024-01-01)")
    p.add_argument("--transactions",  default=None,   help="Path to clean_transactions.parquet (optional)")
    p.add_argument("--skip-schema",   action="store_true", help="Skip running schema.sql (if already applied)")
    args = p.parse_args()

    engine = db_engine()

    # apply schema first unless told to skip
    if not args.skip_schema:
        schema_path = Path(__file__).parent.parent / "sql" / "schema.sql"
        with schema_path.open() as f:
            ddl = f.read()
        with engine.begin() as conn:
            conn.execute(text(ddl))
        logger.info("Schema applied")

    t0 = time.perf_counter()

    feat_dir = Path(args.features)

    txn_path = args.transactions or str(feat_dir / "clean_transactions.parquet")
    if Path(txn_path).exists():
        load_transactions(engine, txn_path)
    else:
        logger.warning("No transactions file at %s — skipping", txn_path)

    acc_path = feat_dir / "account_features.parquet"
    if acc_path.exists():
        upsert_account_features(engine, str(acc_path))
    else:
        logger.warning("No account_features.parquet in %s", feat_dir)

    dev_path = feat_dir / "device_features.parquet"
    if dev_path.exists():
        upsert_device_features(engine, str(dev_path))
    else:
        logger.warning("No device_features.parquet in %s", feat_dir)

    elapsed = time.perf_counter() - t0
    logger.info("Load complete in %.1fs", elapsed)


if __name__ == "__main__":
    main()
