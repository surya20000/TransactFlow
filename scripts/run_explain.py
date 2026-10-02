"""
Measure EXPLAIN ANALYZE query times before and after adding an index on account_id.
Prints a clean comparison table.

Requires a running Postgres with data loaded (docker compose up -d postgres).

Usage:
    python scripts/run_explain.py
"""

import os
import re
import time

try:
    import psycopg2
except ImportError:
    raise SystemExit("psycopg2 not found. Run inside Docker or pip install psycopg2-binary.")


DSN = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "user":     os.getenv("POSTGRES_USER",     "transactflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "transactflow"),
    "dbname":   os.getenv("POSTGRES_DB",       "transactflow"),
}

SAMPLE_ACCOUNT = "ACC000001"

QUERY = f"SELECT * FROM transactions WHERE account_id = '{SAMPLE_ACCOUNT}'"

EXPLAIN = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {QUERY}"


def connect():
    return psycopg2.connect(**DSN)


def run_explain(conn) -> tuple[str, float]:
    """Returns (plan_text, actual_time_ms)."""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(EXPLAIN)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
    plan = "\n".join(r[0] for r in rows)
    # also extract the reported actual time from the plan text
    match = re.search(r"actual time=[\d.]+\.\.([\d.]+)", plan)
    reported_ms = float(match.group(1)) if match else elapsed
    return plan, reported_ms


def drop_index(conn):
    with conn.cursor() as cur:
        cur.execute("DROP INDEX IF EXISTS idx_transactions_account_id")
    conn.commit()


def create_index(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions (account_id)")
    conn.commit()


def row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transactions")
        return cur.fetchone()[0]


def main():
    conn = connect()
    n = row_count(conn)
    print(f"\nTable: transactions  |  Rows: {n:,}")
    print(f"Query:  {QUERY}\n")

    # warm the buffer cache
    with conn.cursor() as cur:
        cur.execute(QUERY)
        cur.fetchall()

    drop_index(conn)
    print("Without index (seq scan):")
    plan_before, ms_before = run_explain(conn)
    print(plan_before[:600])
    print(f"  → reported actual time: {ms_before:.1f} ms\n")

    create_index(conn)
    # give planner a moment to see the new index
    with conn.cursor() as cur:
        cur.execute("ANALYZE transactions")
    conn.commit()

    print("With index (idx_transactions_account_id):")
    plan_after, ms_after = run_explain(conn)
    print(plan_after[:600])
    print(f"  → reported actual time: {ms_after:.1f} ms\n")

    speedup = ms_before / ms_after if ms_after > 0 else float("inf")
    print("=" * 50)
    print(f"  Without index : {ms_before:>8.1f} ms")
    print(f"  With index    : {ms_after:>8.1f} ms")
    print(f"  Speedup       : {speedup:>8.1f}×")
    print("=" * 50)

    conn.close()


if __name__ == "__main__":
    main()
