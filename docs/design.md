# Design Notes

## OLTP vs OLAP — why two table designs?

The `transactions` table stores raw events exactly as they arrived — one row per transaction,
with a UUID primary key and a `CHECK (amount > 0)` constraint. This is the source of truth.
If you need to answer "show me every transaction for account ACC000001 on 15 June", you query here.

The `account_features` and `device_features` tables are pre-aggregated daily summaries.
Instead of re-computing rolling counts and averages on every API call, the pipeline
runs once per day and writes the results. The API then does a single-row lookup by
`(account_id, date)` — no joins, no aggregation at read time.

This split is sometimes called a **Lambda architecture lite**: raw events on one side,
pre-computed features on the other. In practice, many risk systems work exactly this way
because feature computation is expensive and latency requirements for the serving API are tight.

---

## Idempotency

Every step in the pipeline is safe to re-run:

| Step | How it's idempotent |
|------|---------------------|
| S3 upload | Key includes the logical date — overwriting the same key is harmless |
| Parquet write | `mode("overwrite")` with date partition |
| Postgres load | `INSERT … ON CONFLICT DO UPDATE` (upsert) |
| Airflow DAG | `catchup=False` — won't backfill unintentionally |

This matters in batch pipelines because tasks fail and get retried all the time.
If re-running a task corrupts data, you have a much bigger problem on your hands.

---

## Window functions used

### 24-hour rolling transaction count

```sql
COUNT(transaction_id) OVER (
    PARTITION BY account_id
    ORDER BY unix_ts
    RANGEFETWEEN -86400 AND 0   -- 86400 seconds = 24 hours
)
```

This is a **range window** (not rows window) — it looks back exactly 24 hours from each
transaction's timestamp, not just N preceding rows. This gives an accurate velocity signal
regardless of how unevenly spaced the transactions are.

### Amount spike flag

```python
amount_spike_flag = 1 if amount > 3 * avg_amount else 0
```

`avg_amount` is computed over an **unbounded partition window** (all rows for the account).
Flagging a transaction at 3× the account's own average is a simple but interpretable
anomaly signal — easy to explain to non-technical stakeholders.

---

## Why a pandas fallback?

PySpark is the production path, but it requires a JVM and Spark installation. The pandas
version produces the same Parquet schema and is what runs in local development and tests.
Both entry points live in `spark/jobs/` so the directory structure stays consistent.

---

## Risk score design

The score is rule-based (not ML) for two reasons:
1. It's explainable — you can point to the exact factor that drove the score
2. It doesn't need training data, which we don't have (synthetic dataset)

Each factor has a hard cap so no single signal can max out the score alone:

| Factor | Cap | Signal |
|--------|-----|--------|
| Spike rate | 0.35 | Amount spikes relative to own history |
| 24h velocity | 0.30 | Too many transactions in a short window |
| Device sharing | 0.25 | Same device used by multiple accounts |
| Geo spread | 0.10 | Transactions across many cities |

The caps sum to 1.0. In a real system you'd replace this with a trained gradient boosting
model, but the schema and API contract would stay the same.
