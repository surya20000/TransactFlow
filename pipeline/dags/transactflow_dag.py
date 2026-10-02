from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

DEFAULT_ARGS = {
    "owner":                     "transactflow",
    "depends_on_past":           False,
    "email_on_failure":          False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

S3_BUCKET     = Variable.get("TRANSACTFLOW_S3_BUCKET", default_var="transactflow-data")
S3_ENDPOINT   = Variable.get("S3_ENDPOINT_URL",        default_var="http://localstack:4566")
POSTGRES_CONN = "transactflow_postgres"
RAW_CSV       = "/opt/airflow/data/transactions_raw.csv"
QUARANTINE_DIR = "/opt/airflow/data/quarantine"

# S3 paths keyed by logical_date → reruns overwrite the same partition (idempotent)
def s3_raw(date: str)  -> str: return f"s3a://{S3_BUCKET}/raw/{date}/transactions.csv"
def s3_out(date: str)  -> str: return f"s3a://{S3_BUCKET}/features/{date}"
def s3_quar(date: str) -> str: return f"s3a://{S3_BUCKET}/quarantine/{date}"


def ingest_to_s3(**ctx) -> None:
    import boto3, botocore

    date     = ctx["ds"]
    dest_key = f"raw/{date}/transactions.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url          = S3_ENDPOINT,
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID",     "test"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )

    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except botocore.exceptions.ClientError:
        s3.create_bucket(Bucket=S3_BUCKET)

    s3.upload_file(RAW_CSV, S3_BUCKET, dest_key)
    print(f"Uploaded {RAW_CSV} → s3://{S3_BUCKET}/{dest_key}")


def validate_data(**ctx) -> None:
    import sys
    sys.path.insert(0, "/opt/airflow")
    import pandas as pd
    from data_quality.checks import run_checks

    df = pd.read_csv(RAW_CSV, dtype=str)
    run_checks(df, quarantine_path=QUARANTINE_DIR)
    print(f"DQ passed for {ctx['ds']}")


def load_features_to_postgres(**ctx) -> None:
    """Upsert daily feature Parquet files into Postgres. Safe to rerun."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    import pandas as pd
    from sqlalchemy import create_engine, text

    date     = ctx["ds"]
    feat_dir = f"/opt/airflow/data/features/{date}"

    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER','transactflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD','transactflow')}@"
        f"postgres:5432/"
        f"{os.getenv('POSTGRES_DB','transactflow')}"
    )
    engine = create_engine(db_url)

    acc_path = f"{feat_dir}/account_features"
    try:
        acc_df = pd.read_parquet(acc_path)
        with engine.begin() as conn:
            for _, row in acc_df.iterrows():
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
                        distinct_cities    = EXCLUDED.distinct_cities
                """), row.to_dict())
        print(f"Loaded {len(acc_df)} account feature rows")
    except FileNotFoundError:
        print(f"No account features at {acc_path}, skipping")

    dev_path = f"{feat_dir}/device_features"
    try:
        dev_df = pd.read_parquet(dev_path)
        with engine.begin() as conn:
            for _, row in dev_df.iterrows():
                conn.execute(text("""
                    INSERT INTO device_features
                        (device_id, date, distinct_accounts, txn_count,
                         total_amount, multi_account_flag)
                    VALUES
                        (:device_id, :date, :distinct_accounts, :txn_count,
                         :total_amount, :multi_account_flag)
                    ON CONFLICT (device_id, date) DO UPDATE SET
                        distinct_accounts  = EXCLUDED.distinct_accounts,
                        txn_count          = EXCLUDED.txn_count,
                        total_amount       = EXCLUDED.total_amount,
                        multi_account_flag = EXCLUDED.multi_account_flag
                """), row.to_dict())
        print(f"Loaded {len(dev_df)} device feature rows")
    except FileNotFoundError:
        print(f"No device features at {dev_path}, skipping")


with DAG(
    dag_id          = "transactflow_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "Batch transaction risk-feature pipeline",
    schedule        = "@daily",
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    tags            = ["transactflow", "batch", "risk"],
    max_active_runs = 1,
) as dag:

    t_generate = BashOperator(
        task_id      = "generate_data",
        bash_command = (
            "cd /opt/airflow && "
            "python generator/generate_transactions.py "
            "--rows 1000000 --output data/transactions_raw.csv"
        ),
    )

    t_ingest = PythonOperator(
        task_id         = "ingest_to_s3",
        python_callable = ingest_to_s3,
    )

    t_validate = PythonOperator(
        task_id         = "validate_data",
        python_callable = validate_data,
    )

    # primary transform: PySpark via spark-submit
    t_transform = BashOperator(
        task_id      = "spark_transform",
        bash_command = (
            "spark-submit "
            "--master local[*] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/airflow/spark/jobs/transform_features.py "
            "--input {{ var.value.get('TRANSACTFLOW_S3_BUCKET', 'transactflow-data') and "
            "  's3a://' + var.value.get('TRANSACTFLOW_S3_BUCKET', 'transactflow-data') + '/raw/{{ ds }}/transactions.csv' }} "
            "--output /opt/airflow/data/features/{{ ds }} "
            "--quarantine /opt/airflow/data/quarantine"
        ),
        env = {
            "AWS_ACCESS_KEY_ID":     "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_DEFAULT_REGION":    "us-east-1",
        },
    )

    # pandas fallback — runs if Spark is unavailable
    t_transform_pandas = BashOperator(
        task_id      = "pandas_transform_fallback",
        bash_command = (
            "cd /opt/airflow && "
            "python spark/jobs/pandas_transform.py "
            "--input data/transactions_raw.csv "
            "--output data/features/{{ ds }} "
            "--quarantine data/quarantine"
        ),
        trigger_rule = "none_failed_min_one_success",
    )

    t_create_schema = PostgresOperator(
        task_id          = "create_schema",
        postgres_conn_id = POSTGRES_CONN,
        sql              = "sql/schema.sql",
    )

    t_load = PythonOperator(
        task_id         = "load_to_postgres",
        python_callable = load_features_to_postgres,
    )

    t_generate >> t_ingest >> t_validate >> t_transform >> t_create_schema >> t_load
    t_validate >> t_transform_pandas >> t_create_schema
