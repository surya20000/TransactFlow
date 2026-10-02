# TransactFlow

Batch transaction data pipeline with risk feature serving.

**Stack**: Python · PySpark/pandas · Airflow · PostgreSQL · FastAPI · Docker · LocalStack S3

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      TransactFlow Pipeline                   │
│                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌───────────────────┐ │
│  │  Generator  │──▶│ LocalStack  │──▶│ PySpark / pandas  │ │
│  │  (1M rows)  │   │   S3        │   │ clean + features  │ │
│  └─────────────┘   └─────────────┘   └────────┬──────────┘ │
│                                                │  Parquet   │
│                                       ┌────────▼──────────┐ │
│                                       │    PostgreSQL      │ │
│                                       │  OLTP: txns       │ │
│                                       │  OLAP: features   │ │
│                                       └────────┬──────────┘ │
│                                                │            │
│                                       ┌────────▼──────────┐ │
│                                       │  FastAPI Risk API  │ │
│                                       │  /risk/{account}  │ │
│                                       │  /device/{device} │ │
│                                       └───────────────────┘ │
│                                                             │
│  Airflow DAG (daily):                                       │
│  generate → ingest_s3 → validate → transform → load        │
└─────────────────────────────────────────────────────────────┘
```

| Layer | Tech | What it does |
|-------|------|--------------|
| Generator | Python + NumPy | 1M synthetic rows with injected bad data |
| Object store | LocalStack S3 | Local S3 emulator — swap endpoint for real AWS |
| Transform | PySpark (pandas fallback) | Cleaning + window-function feature engineering |
| Orchestration | Airflow 2.9 | Daily DAG, retries, idempotent tasks |
| Data quality | Custom pandas checks | Null / dup / negative / type / row-drop guards |
| Warehouse | PostgreSQL 15 | OLTP transactions + OLAP feature aggregates |
| API | FastAPI | Risk score + device linkage endpoints |

---

## Quick Start

### Option A — full pipeline locally (no Docker)

```bash
# install deps
pip install -r requirements-dev.txt

# 1. generate, validate, transform, load
python scripts/run_pipeline.py --rows 1000000 --date 2024-01-01

# 2. start API
uvicorn api.main:app --reload --port 8000

# 3. call it
curl http://localhost:8000/health
curl http://localhost:8000/risk/ACC000001
curl http://localhost:8000/device/DEV000001
```

> `--skip-load` skips the Postgres step if you don't have it running.

### Option B — full stack with Docker

```bash
docker compose up -d

# open Airflow UI: http://localhost:8080  (admin / admin)
# trigger the "transactflow_pipeline" DAG

# API: http://localhost:8000/docs
```

## Project Structure

```
TransactFlow/
├── generator/
│   └── generate_transactions.py    # 1M row CSV + bad-data injection
├── spark/jobs/
│   ├── transform_features.py       # PySpark cleaning + window features
│   └── pandas_transform.py         # same logic, no Spark dependency
├── data_quality/
│   └── checks.py                   # DQ guards, quarantine writer
├── pipeline/dags/
│   └── transactflow_dag.py         # Airflow DAG (6 tasks)
├── sql/
│   ├── schema.sql                  # DDL (idempotent IF NOT EXISTS)
│   └── analytical_queries.sql      # 5 queries + EXPLAIN comparison
├── api/
│   ├── main.py                     # FastAPI app
│   └── tests/
│       ├── test_api.py             # 15 API tests
│       └── test_transforms.py      # 19 transform + DQ tests
├── scripts/
│   ├── run_pipeline.py             # end-to-end local runner
│   ├── load_postgres.py            # standalone Parquet → Postgres loader
│   └── run_explain.py              # EXPLAIN benchmark
├── infra/
│   ├── Dockerfile.api
│   └── requirements.api.txt
├── docs/
│   └── design.md                   # OLTP vs OLAP, idempotency, window functions
├── conftest.py                     # pytest root config
├── docker-compose.yml
├── requirements.txt                # runtime deps
├── requirements-dev.txt            # + test/lint deps
├── pyproject.toml                  # ruff + pytest config
└── .env.example
```

---

## Components

### Data Generator

- **1,000,000 clean rows** + 7,500 injected bad rows (0.74%)
- Bad data: nulls (0.3%), duplicates (0.2%), negatives (0.15%), bad types (0.1%)
- **Device sharing**: 5% of accounts share one of 50 "hot" devices to simulate fraud rings
- Amounts are log-normal distributed (realistic spend pattern)

### Airflow DAG

| Task | What it does |
|------|-------------|
| `generate_data` | Runs the generator |
| `ingest_to_s3` | Uploads CSV to LocalStack S3 (keyed by `{{ ds }}`) |
| `validate_data` | DQ checks — fails the DAG if thresholds are breached |
| `spark_transform` | `spark-submit` the PySpark job |
| `pandas_transform_fallback` | Pandas alternative if Spark is unavailable |
| `create_schema` | Runs idempotent DDL via Postgres operator |
| `load_to_postgres` | Upserts features (`ON CONFLICT DO UPDATE`) |

All tasks use 3 retries with exponential backoff. Every S3 path and Postgres write is idempotent — re-running a day is safe.

### Transformation (PySpark + pandas)

- Cast and reject bad rows → quarantine Parquet
- **24h rolling txn count** via `rangeBetween(-86400, 0)` window (seconds-based range, not row-based)
- **Amount spike flag**: `amount > 3 × per-account average`
- **Device multi-account flag**: `distinct_accounts > 1` on same device per day
- Output: Parquet partitioned by `date`

### Data Quality

| Check | Threshold | On fail |
|-------|-----------|---------|
| Required columns present | must exist | raise immediately |
| Null / empty fields | < 1% | quarantine + fail DAG |
| Duplicate `transaction_id` | < 0.5% | quarantine + fail DAG |
| Negative / zero amounts | < 0.5% | quarantine + fail DAG |
| Non-numeric amount | any | quarantine |
| Row-count drop vs previous run | < 10% | fail DAG |

### SQL (PostgreSQL)

Two table designs intentionally:
- **OLTP** `transactions`: normalised, UUID PK, CHECK constraints — the source of truth
- **OLAP** `account_features` / `device_features`: pre-aggregated daily, composite PK `(id, date)` — fast API lookups, no joins at read time

See [`docs/design.md`](docs/design.md) for a full explanation of why.

**Analytical queries** (`sql/analytical_queries.sql`):
1. Top-10 accounts by spend
2. 7-day spike trend with `RANK` and `LAG`
3. Device-sharing network (`HAVING`, subquery)
4. Month-over-month volume (`DATE_TRUNC`, `LAG`)
5. Per-account risk bracket (`NTILE` quartiles)

### API

| Endpoint | Returns |
|----------|---------|
| `GET /health` | DB connectivity status |
| `GET /risk/{account_id}` | Risk score [0–1] + contributing factors |
| `GET /device/{device_id}` | Linked accounts for a device |
| `GET /docs` | Interactive Swagger UI |

Risk score factors: spike rate (0.35) + 24h velocity (0.30) + device sharing (0.25) + geo spread (0.10). See [`docs/design.md`](docs/design.md).

---

## Measured Numbers

| Metric | Value |
|--------|-------|
| Rows generated | 1,007,500 in **10.7 s** |
| CSV size | 98.6 MB |
| Transform time (pandas) | **8.4 s** |
| Rows quarantined | 7,500 (0.74%) — nulls 3k, dups 2k, negatives 1.5k, bad-type 1k |
| Clean transactions (Parquet) | 1,000,000 rows · 51.4 MB |
| Account feature rows | 875,299 |
| Device feature rows | 726,114 |
| Shared-device fraud signal | 121,651 entries across 2,803 devices |
| SQL index speedup | 312 ms → 1.8 ms (~174×) |
| Test suite | **34/34 in 1.25 s** |

*Measured on Python 3.13.7, Windows 10, 4-core / 16 GB. SQL EXPLAIN via PostgreSQL 15 in Docker.*

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest -v          # 34 tests, ~1.3s, no Postgres or Spark needed
ruff check .       # lint
```

Tests use `MagicMock` to inject DB results — no live services required.

---

## Environment Variables

Copy `.env.example` to `.env` and adjust for your environment.

| Variable | Default |
|----------|---------|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_USER` | `transactflow` |
| `POSTGRES_PASSWORD` | `transactflow` |
| `POSTGRES_DB` | `transactflow` |
| `AWS_ACCESS_KEY_ID` | `test` |
| `AWS_SECRET_ACCESS_KEY` | `test` |
| `S3_ENDPOINT_URL` | `http://localstack:4566` |
| `TRANSACTFLOW_S3_BUCKET` | `transactflow-data` |

---

## Scaling to 100M rows/day

| Concern | Change |
|---------|--------|
| Generator | Chunked writes directly to S3 |
| Spark | EMR / Dataproc cluster; enable AQE + dynamic partition pruning |
| Postgres | Partition `transactions` by month; move OLAP to Redshift / BigQuery |
| Airflow | Switch to CeleryExecutor or KubernetesExecutor |
| API | Redis cache for hot accounts (TTL ≈ 5 min); PgBouncer connection pooling |
| DQ | Great Expectations or Soda for declarative, scalable checks |
