import random
import csv
import uuid
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

N_ACCOUNTS  = 10_000
N_DEVICES   = 7_000   # fewer devices than accounts so some accounts share one
N_MERCHANTS = 500
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
]

ACCOUNTS  = [f"ACC{str(i).zfill(6)}" for i in range(N_ACCOUNTS)]
DEVICES   = [f"DEV{str(i).zfill(6)}" for i in range(N_DEVICES)]
MERCHANTS = [f"MER{str(i).zfill(5)}" for i in range(N_MERCHANTS)]

# ~5% of accounts share one of 50 "hot" devices to simulate device-sharing fraud rings
ACCOUNT_DEVICE_MAP: dict[str, str] = {}
for acc in ACCOUNTS:
    if random.random() < 0.05:
        ACCOUNT_DEVICE_MAP[acc] = random.choice(DEVICES[:50])
    else:
        ACCOUNT_DEVICE_MAP[acc] = random.choice(DEVICES)

START_TS = datetime(2024, 1, 1)
END_TS   = datetime(2024, 12, 31, 23, 59, 59)


def random_ts() -> str:
    delta = END_TS - START_TS
    secs  = random.randint(0, int(delta.total_seconds()))
    return (START_TS + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")


def clean_row() -> dict:
    acc = random.choice(ACCOUNTS)
    amount = round(np.random.lognormal(mean=3.5, sigma=1.5), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id":     acc,
        "device_id":      ACCOUNT_DEVICE_MAP[acc],
        "amount":         amount,
        "merchant":       random.choice(MERCHANTS),
        "timestamp":      random_ts(),
        "city":           random.choice(CITIES),
    }


def inject_bad_data(rows: list[dict], total: int) -> list[dict]:
    bad_rows = []

    # nulls: 0.3%
    for _ in range(int(total * 0.003)):
        r = clean_row()
        r[random.choice(["account_id", "device_id", "amount", "merchant", "city"])] = ""
        bad_rows.append(r)

    # duplicates: 0.2% — copy existing rows verbatim so transaction_id repeats
    for _ in range(int(total * 0.002)):
        bad_rows.append(dict(random.choice(rows[:1000])))

    # negative amounts: 0.15%
    for _ in range(int(total * 0.0015)):
        r = clean_row()
        r["amount"] = -round(random.uniform(1, 500), 2)
        bad_rows.append(r)

    # wrong type: 0.1% — amount is a non-numeric string
    for _ in range(int(total * 0.001)):
        r = clean_row()
        r["amount"] = "not_a_number"
        bad_rows.append(r)

    logger.info(
        "Injected bad data — nulls:%d  duplicates:%d  negatives:%d  type-errors:%d",
        int(total * 0.003), int(total * 0.002), int(total * 0.0015), int(total * 0.001),
    )
    return bad_rows


FIELDNAMES = ["transaction_id", "account_id", "device_id", "amount", "merchant", "timestamp", "city"]


def generate(n_rows: int, output_path: str) -> None:
    logger.info("Generating %d clean rows …", n_rows)
    rows: list[dict] = [clean_row() for _ in range(n_rows)]

    all_rows = rows + inject_bad_data(rows, n_rows)
    random.shuffle(all_rows)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("Wrote %d total rows to %s", len(all_rows), output_path)
    logger.info(
        "Accounts: %d  |  Devices: %d  |  Hot-device accounts: ~%d",
        N_ACCOUNTS, N_DEVICES, int(N_ACCOUNTS * 0.05),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--rows",   type=int, default=1_000_000)
    parser.add_argument("--output", type=str, default="data/transactions_raw.csv")
    args = parser.parse_args()
    generate(args.rows, args.output)
