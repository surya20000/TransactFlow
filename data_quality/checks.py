import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


@dataclass
class DQReport:
    total_rows:      int = 0
    null_rows:       int = 0
    duplicate_rows:  int = 0
    negative_amount: int = 0
    bad_type_rows:   int = 0
    quarantined:     int = 0
    passed:          bool = True
    failures:        list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"  Total rows       : {self.total_rows:,}",
            f"  Null fields      : {self.null_rows:,}",
            f"  Duplicates       : {self.duplicate_rows:,}",
            f"  Negative amounts : {self.negative_amount:,}",
            f"  Bad types        : {self.bad_type_rows:,}",
            f"  Quarantined      : {self.quarantined:,}",
            f"  Status           : {'PASSED' if self.passed else 'FAILED'}",
        ]
        if self.failures:
            lines += [f"    - {f}" for f in self.failures]
        return "\n".join(lines)


REQUIRED_COLUMNS = ["transaction_id", "account_id", "device_id", "amount", "merchant", "timestamp", "city"]

# Thresholds — adjust these based on expected data quality per environment
NULL_THRESHOLD      = 0.01   # 1%
DUPLICATE_THRESHOLD = 0.005  # 0.5%
NEGATIVE_THRESHOLD  = 0.005  # 0.5%
ROW_DROP_THRESHOLD  = 0.10   # 10% drop vs previous run signals upstream issue


def run_checks(
    df: pd.DataFrame,
    quarantine_path: str,
    previous_row_count: Optional[int] = None,
) -> DQReport:
    """Run DQ checks and write rejected rows to quarantine_path. Raises DataQualityError on failure."""
    report = DQReport(total_rows=len(df))
    quarantine_frames: list[pd.DataFrame] = []

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        msg = f"Missing required columns: {missing_cols}"
        report.failures.append(msg)
        report.passed = False
        raise DataQualityError(msg)

    # null / empty strings in any required field
    null_mask = df[REQUIRED_COLUMNS].isnull().any(axis=1) | (
        df[["account_id", "device_id", "merchant", "city"]]
        .apply(lambda s: s.str.strip() == "")
        .any(axis=1)
    )
    report.null_rows = int(null_mask.sum())
    if report.null_rows / report.total_rows > NULL_THRESHOLD:
        msg = f"Null rate {report.null_rows / report.total_rows:.2%} exceeds {NULL_THRESHOLD:.2%}"
        report.failures.append(msg)
        report.passed = False
        logger.error(msg)
    quar = df[null_mask].copy()
    quar["quarantine_reason"] = "null_field"
    quarantine_frames.append(quar)

    dup_mask = df.duplicated(subset=["transaction_id"], keep="first")
    report.duplicate_rows = int(dup_mask.sum())
    if report.duplicate_rows / report.total_rows > DUPLICATE_THRESHOLD:
        msg = f"Duplicate rate {report.duplicate_rows / report.total_rows:.2%} exceeds {DUPLICATE_THRESHOLD:.2%}"
        report.failures.append(msg)
        report.passed = False
        logger.error(msg)
    quar = df[dup_mask].copy()
    quar["quarantine_reason"] = "duplicate"
    quarantine_frames.append(quar)

    numeric_amount = pd.to_numeric(df["amount"], errors="coerce")

    neg_mask = numeric_amount <= 0
    report.negative_amount = int(neg_mask.sum())
    if report.negative_amount / report.total_rows > NEGATIVE_THRESHOLD:
        msg = f"Negative amount rate {report.negative_amount / report.total_rows:.2%} exceeds {NEGATIVE_THRESHOLD:.2%}"
        report.failures.append(msg)
        report.passed = False
        logger.error(msg)
    quar = df[neg_mask].copy()
    quar["quarantine_reason"] = "negative_or_zero_amount"
    quarantine_frames.append(quar)

    type_mask = numeric_amount.isna() & df["amount"].notna() & (df["amount"] != "")
    report.bad_type_rows = int(type_mask.sum())
    quar = df[type_mask].copy()
    quar["quarantine_reason"] = "bad_amount_type"
    quarantine_frames.append(quar)

    if previous_row_count and previous_row_count > 0:
        drop_rate = (previous_row_count - report.total_rows) / previous_row_count
        if drop_rate > ROW_DROP_THRESHOLD:
            msg = f"Row count dropped {drop_rate:.2%} (prev={previous_row_count:,}, now={report.total_rows:,})"
            report.failures.append(msg)
            report.passed = False
            logger.error(msg)

    if quarantine_frames:
        combined = pd.concat(quarantine_frames, ignore_index=True).drop_duplicates()
        report.quarantined = len(combined)
        os.makedirs(quarantine_path, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        out = os.path.join(quarantine_path, f"quarantine_{ts}.csv")
        combined.to_csv(out, index=False)
        logger.info("Quarantined %d rows → %s", report.quarantined, out)

    logger.info("DQ report:\n%s", report.summary())

    if not report.passed:
        raise DataQualityError(f"Data quality checks failed: {report.failures}")

    return report


if __name__ == "__main__":
    import argparse
    import sys

    p = argparse.ArgumentParser()
    p.add_argument("--input",      required=True)
    p.add_argument("--quarantine", required=True)
    p.add_argument("--prev-count", type=int, default=None)
    args = p.parse_args()

    df = pd.read_csv(args.input, dtype=str)
    try:
        run_checks(df, args.quarantine, args.prev_count)
        sys.exit(0)
    except DataQualityError as e:
        logger.critical("DQ FAILED: %s", e)
        sys.exit(1)
