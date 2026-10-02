"""
spark-submit spark/jobs/transform_features.py \
    --input  s3a://transactflow-data/raw/2024-01-01/transactions.csv \
    --output s3a://transactflow-data/features/2024-01-01 \
    --quarantine s3a://transactflow-data/quarantine/2024-01-01
"""

import argparse
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id",     StringType(), True),
    StructField("device_id",      StringType(), True),
    StructField("amount",         StringType(), True),  # string so bad rows don't get dropped at read time
    StructField("merchant",       StringType(), True),
    StructField("timestamp",      StringType(), True),
    StructField("city",           StringType(), True),
])


def create_spark(app_name: str = "TransactFlow") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint",           "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key",         "test")
        .config("spark.hadoop.fs.s3a.secret.key",         "test")
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def clean(df):
    df = df.withColumn("amount_numeric", F.col("amount").cast(DoubleType()))

    required = ["transaction_id", "account_id", "device_id", "merchant", "city"]
    null_cond = F.lit(False)
    for c in required:
        null_cond = null_cond | F.col(c).isNull() | (F.col(c) == "")

    bad = (
        null_cond
        | F.col("amount_numeric").isNull()
        | (F.col("amount_numeric") <= 0)
        | F.col("timestamp").isNull()
    )

    # keep first occurrence of each transaction_id
    dedup_w = Window.partitionBy("transaction_id").orderBy("timestamp")
    df = df.withColumn("_row_num", F.row_number().over(dedup_w))
    dup_bad = F.col("_row_num") > 1

    quarantine = (
        df.filter(bad | dup_bad)
          .withColumn("quarantine_reason",
                      F.when(dup_bad, "duplicate")
                       .when(F.col("amount_numeric").isNull(), "bad_amount_type")
                       .when(F.col("amount_numeric") <= 0, "negative_amount")
                       .otherwise("null_field"))
          .drop("_row_num", "amount_numeric")
    )

    clean_df = (
        df.filter(~bad & ~dup_bad)
          .withColumn("amount",    F.col("amount_numeric"))
          .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("date",      F.to_date("timestamp"))
          .drop("_row_num", "amount_numeric")
    )

    return clean_df, quarantine


def account_features(clean_df):
    df = clean_df.withColumn("unix_ts", F.unix_timestamp("timestamp"))

    # 24-hour range window expressed in seconds
    w_24h = Window.partitionBy("account_id").orderBy("unix_ts").rangeBetween(-86400, 0)
    w_acc = Window.partitionBy("account_id")

    df = (
        df.withColumn("txn_count_24h",    F.count("transaction_id").over(w_24h))
          .withColumn("avg_amount",        F.avg("amount").over(w_acc))
          .withColumn("std_amount",        F.stddev("amount").over(w_acc))
          .withColumn("amount_spike_flag",
                      F.when(F.col("amount") > 3 * F.col("avg_amount"), 1).otherwise(0))
    )

    return (
        df.groupBy("account_id", "date")
          .agg(
              F.count("transaction_id").alias("txn_count"),
              F.sum("amount").alias("total_amount"),
              F.avg("amount").alias("avg_amount"),
              F.max("txn_count_24h").alias("max_txn_count_24h"),
              F.sum("amount_spike_flag").alias("spike_count"),
              F.countDistinct("merchant").alias("distinct_merchants"),
              F.countDistinct("city").alias("distinct_cities"),
          )
    )


def device_features(clean_df):
    return (
        clean_df.groupBy("device_id", "date")
                .agg(
                    F.countDistinct("account_id").alias("distinct_accounts"),
                    F.count("transaction_id").alias("txn_count"),
                    F.sum("amount").alias("total_amount"),
                )
                .withColumn(
                    "multi_account_flag",
                    F.when(F.col("distinct_accounts") > 1, 1).otherwise(0),
                )
    )


def run(input_path: str, output_path: str, quarantine_path: str) -> None:
    spark = create_spark()

    logger.info("Reading %s", input_path)
    raw = spark.read.csv(input_path, header=True, schema=RAW_SCHEMA)
    logger.info("Raw rows: %d", raw.count())

    clean_df, quar_df = clean(raw)
    logger.info("Clean: %d  |  Quarantined: %d", clean_df.count(), quar_df.count())

    quar_df.write.mode("overwrite").partitionBy("quarantine_reason").parquet(f"{quarantine_path}/quarantine")

    account_features(clean_df).write.mode("overwrite").partitionBy("date").parquet(f"{output_path}/account_features")
    device_features(clean_df).write.mode("overwrite").partitionBy("date").parquet(f"{output_path}/device_features")
    clean_df.write.mode("overwrite").partitionBy("date").parquet(f"{output_path}/clean_transactions")

    logger.info("Done → %s", output_path)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input",      required=True)
    p.add_argument("--output",     required=True)
    p.add_argument("--quarantine", required=True)
    args = p.parse_args()
    run(args.input, args.output, args.quarantine)
