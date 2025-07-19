#!/usr/bin/env python3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Load Bronze Email CSVs")
        # ── Iceberg: make "local" point at the same warehouse as spark_catalog ──
        .config("spark.sql.catalog.local",          "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",     "hadoop")
        .config("spark.sql.catalog.local.warehouse","/opt/iceberg/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse")
        .getOrCreate()
    )

def align_schema_and_cast(df, spark, table_name):
    table_schema = spark.table(table_name).schema
    table_cols = [field.name for field in table_schema]
    df_cols = df.columns

    common_cols = [c for c in table_cols if c in df_cols]

    for field in table_schema:
        col_name = field.name
        if col_name in common_cols:
            logger.info(f"Casting column '{col_name}' to {field.dataType}")
            df = df.withColumn(col_name, col(col_name).cast(field.dataType))

    return df.select(common_cols)

def load_csv_to_iceberg(spark, csv_path, table_name, timestamp_cols=None):
    logger.info(f"Loading {csv_path} into {table_name}")
    df = spark.read.option("header", "true").csv(csv_path)

    if timestamp_cols:
        for ts_col in timestamp_cols:
            if ts_col in df.columns:
                logger.info(f"Converting column {ts_col} to timestamp")
                df = df.withColumn(ts_col, to_timestamp(col(ts_col)))

    df_aligned = align_schema_and_cast(df, spark, table_name)
    df_aligned.writeTo(table_name).append()
    logger.info(f"Loaded {df_aligned.count()} rows into {table_name}")

def main():
    spark = create_spark_session()

    base_path = "/opt/spark-apps/data/bronze"


    # טען טבלה טבלה
    load_csv_to_iceberg(
        spark,
        f"{base_path}/bronze_email_send_stream.csv",
        "local.bronze.email_send_stream",
        timestamp_cols=["event_time", "ingestion_time"]
    )
    load_csv_to_iceberg(
        spark,
        f"{base_path}/bronze_email_interactions.csv",
        "local.bronze.email_interactions",
        timestamp_cols=["interaction_time", "ingestion_time"]
    )
    load_csv_to_iceberg(
        spark,
        f"{base_path}/bronze_email_delivery_servers.csv",
        "local.bronze.email_delivery_servers",
        timestamp_cols=["event_time", "ingestion_time"]
    )
    load_csv_to_iceberg(
        spark,
        f"{base_path}/bronze_form_submissions.csv",
        "local.bronze.form_submissions",
        timestamp_cols=["event_time", "ingestion_time"]
    )
    load_csv_to_iceberg(
        spark,
        f"{base_path}/bronze_subscribers.csv",
        "local.bronze.subscribers",
        timestamp_cols=["date_added"]
    )

    spark.stop()
    logger.info("All CSV files loaded successfully!")

if __name__ == "__main__":
    main()
