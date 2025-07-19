#!/usr/bin/env python3
"""
ETL pipeline: Load from Bronze, transform and write to Silver using MERGE SQL,
without using processing_status column.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Bronze to Silver ETL with MERGE SQL")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse")
        .getOrCreate()
    )

def calculate_data_quality_score(df, checks):
    df = df.withColumn("data_quality_score", lit(100))
    for check_name, check_expr, penalty in checks:
        df = df.withColumn(
            "data_quality_score",
            when(check_expr, col("data_quality_score")).otherwise(col("data_quality_score") - penalty)
        )
        df = df.withColumn(f"dq_check_{check_name}", check_expr)
    return df

def process_email_send_stream_to_silver(spark, process_date):
    logger.info(f"Processing email_send_stream for date: {process_date}")

    bronze_df = spark.sql(f"""
        SELECT * FROM local.bronze.email_send_stream
        WHERE date(event_time) = '{process_date}'
    """)

    quality_checks = [
        ("valid_send_id", col("send_id").isNotNull(), 20),
        ("valid_account_id", col("account_id").isNotNull(), 20),
        ("valid_recipient_count", col("recipient_count") > 0, 15),
        ("positive_message_size", col("message_size_kb") > 0, 15),
        ("valid_event_time", col("event_time").isNotNull(), 10)
    ]

    bronze_df = calculate_data_quality_score(bronze_df, quality_checks)

    silver_df = bronze_df.select(
        col("send_id"),
        col("account_id"),
        col("sender_email"),
        col("message_subject").alias("subject"),
        lit(None).cast("string").alias("server_id"),
        lit(None).cast("string").alias("server_ip"),
        lit(None).cast("string").alias("status"),
        col("event_time"),
        current_timestamp().alias("ingestion_time"),
        col("data_quality_score"),
        col("dq_check_valid_send_id"),
        col("dq_check_valid_account_id"),
        col("dq_check_valid_recipient_count"),
        col("dq_check_positive_message_size"),
        col("dq_check_valid_event_time"),
    )

    silver_df.createOrReplaceTempView("email_send_stream_updates")

    spark.sql("""
        MERGE INTO local.silver.campaign_base AS target
        USING email_send_stream_updates AS source
        ON target.send_id = source.send_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = silver_df.count()
    logger.info(f"Processed {count} records to local.silver.campaign_base")
    return count

def process_email_interactions_to_silver(spark, process_date):
    logger.info(f"Processing email_interactions for date: {process_date}")

    bronze_df = spark.sql(f"""
        SELECT * FROM local.bronze.email_interactions
        WHERE date(interaction_time) = '{process_date}'
    """)

    quality_checks = [
        ("valid_interaction_id", col("interaction_id").isNotNull(), 20),
        ("valid_send_id", col("send_id").isNotNull(), 20),
        ("valid_interaction_type", col("interaction_type").isNotNull(), 20),
        ("valid_interaction_time", col("interaction_time").isNotNull(), 15)
    ]

    bronze_df = calculate_data_quality_score(bronze_df, quality_checks)

    silver_df = bronze_df.select(
        col("interaction_id"),
        col("send_id"),
        col("recipient_email"),
        col("interaction_type"),
        col("interaction_time").alias("event_time"),
        lit(None).cast("int").alias("response_delay_sec"),
        col("details"),
        current_timestamp().alias("ingestion_time"),
        col("data_quality_score"),
        col("dq_check_valid_interaction_id"),
        col("dq_check_valid_send_id"),
        col("dq_check_valid_interaction_type"),
        col("dq_check_valid_interaction_time"),
    )

    silver_df.createOrReplaceTempView("email_interactions_updates")

    spark.sql("""
        MERGE INTO local.silver.email_interactions_cleaned AS target
        USING email_interactions_updates AS source
        ON target.interaction_id = source.interaction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = silver_df.count()
    logger.info(f"Processed {count} records to local.silver.email_interactions_cleaned")
    return count

def process_subscribers_to_silver(spark, process_date):
    logger.info(f"Processing subscribers for date: {process_date}")

    bronze_df = spark.sql(f"""
        SELECT * FROM local.bronze.subscribers
        WHERE date(date_added) <= '{process_date}'
    """)

    silver_df = bronze_df.select(
        col("subscriber_id"),
        col("account_id"),
        col("email"),
        col("subscription_status"),
        lit(0).cast("int").alias("total_interactions"),
        lit(None).cast("string").alias("last_event_type"),
        lit(None).cast("timestamp").alias("first_event_time"),
        lit(None).cast("timestamp").alias("last_event_time"),
        current_timestamp().alias("ingestion_time"),
    )

    silver_df.createOrReplaceTempView("subscribers_updates")

    spark.sql("""
        MERGE INTO local.silver.subscriber_base AS target
        USING subscribers_updates AS source
        ON target.subscriber_id = source.subscriber_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = silver_df.count()
    logger.info(f"Processed {count} records to local.silver.subscriber_base")
    return count

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    process_date = "2025-07-19"

    try:
        logger.info(f"Starting Bronze to Silver ETL for date {process_date}")

        email_send_count = process_email_send_stream_to_silver(spark, process_date)
        email_interactions_count = process_email_interactions_to_silver(spark, process_date)
        subscribers_count = process_subscribers_to_silver(spark, process_date)

        logger.info(f"ETL completed:")
        logger.info(f"Email Send Stream processed: {email_send_count} records")
        logger.info(f"Email Interactions processed: {email_interactions_count} records")
        logger.info(f"Subscribers processed: {subscribers_count} records")

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
