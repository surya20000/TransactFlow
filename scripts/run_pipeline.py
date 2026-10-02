"""
End-to-end pipeline runner — no Airflow required.
Generates data, runs DQ checks, transforms, and loads into Postgres.

Usage:
    python scripts/run_pipeline.py [--rows 1000000] [--date 2024-01-01] [--skip-load]
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent


def run(cmd: list[str], **kwargs):
    logger.info("$ %s", " ".join(cmd))
    result = subprocess.run(cmd, check=True, **kwargs)
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows",      type=int, default=1_000_000)
    p.add_argument("--date",      default="2024-01-01", help="Logical date partition (YYYY-MM-DD)")
    p.add_argument("--skip-gen",  action="store_true", help="Skip data generation if CSV already exists")
    p.add_argument("--skip-load", action="store_true", help="Skip Postgres load step")
    args = p.parse_args()

    raw_csv   = ROOT / "data" / "transactions_raw.csv"
    feat_dir  = ROOT / "data" / "features" / args.date
    quar_dir  = ROOT / "data" / "quarantine"

    t_start = time.perf_counter()

    # 1. Generate
    if not args.skip_gen or not raw_csv.exists():
        run([sys.executable, str(ROOT / "generator" / "generate_transactions.py"),
             "--rows", str(args.rows), "--output", str(raw_csv)])
    else:
        logger.info("Skipping generation — %s already exists", raw_csv)

    # 2. DQ checks
    run([sys.executable, str(ROOT / "data_quality" / "checks.py"),
         "--input", str(raw_csv),
         "--quarantine", str(quar_dir)])

    # 3. Transform
    run([sys.executable, str(ROOT / "spark" / "jobs" / "pandas_transform.py"),
         "--input",      str(raw_csv),
         "--output",     str(feat_dir),
         "--quarantine", str(quar_dir)])

    # 4. Load to Postgres (optional)
    if not args.skip_load:
        run([sys.executable, str(ROOT / "scripts" / "load_postgres.py"),
             "--features", str(feat_dir)])

    elapsed = time.perf_counter() - t_start
    logger.info("Pipeline finished in %.1fs", elapsed)


if __name__ == "__main__":
    main()
