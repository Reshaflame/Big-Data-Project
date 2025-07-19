#!/usr/bin/env python3
"""
Initialize Iceberg Tables for Email Data Pipeline
Creates all tables in Bronze, Silver, and Gold layers
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg configuration (local warehouse)"""
    return (
        SparkSession.builder
        .appName("Initialize Email Iceberg Tables")
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


# ---------------------- BRONZE ---------------------- #
def create_bronze_tables(spark):
    logger.info("Creating Bronze layer tables...")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.bronze")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.email_send_stream (
            send_id STRING,
            account_id STRING,
            sender_email STRING,
            reply_to_email STRING,
            recipient_count INT,
            message_size_kb INT,
            message_subject STRING,
            message_body STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.email_interactions (
            interaction_id STRING,
            send_id STRING,
            recipient_email STRING,
            interaction_type STRING,
            interaction_time TIMESTAMP,
            details STRING,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(interaction_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.email_delivery_servers (
            server_id STRING,
            ip_address STRING,
            profile_id STRING,
            current_score DOUBLE,
            blacklist_status ARRAY<STRING>,
            status STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.form_submissions (
            submission_id STRING,
            landing_id STRING,
            event_time TIMESTAMP,
            visitor_ip STRING,
            utm_source STRING,
            utm_medium STRING,
            utm_campaign STRING,
            time_to_submit_sec INT,
            form_data STRING,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.subscribers (
            subscriber_id STRING,
            account_id STRING,
            email STRING,
            subscription_status STRING,
            date_added TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(date_added))
    """)

    logger.info("✅ Bronze layer tables created successfully")


# ---------------------- SILVER ---------------------- #
def create_silver_tables(spark):
    logger.info("Creating Silver layer tables...")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.campaign_base (
            send_id STRING,
            account_id STRING,
            sender_email STRING,
            subject STRING,
            server_id STRING,
            server_ip STRING,
            status STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.email_interactions_cleaned (
            interaction_id STRING,
            send_id STRING,
            recipient_email STRING,
            interaction_type STRING,
            event_time TIMESTAMP,
            response_delay_sec INT,
            details STRING,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.subscriber_base (
            subscriber_id STRING,
            account_id STRING,
            email STRING,
            subscription_status STRING,
            total_interactions INT,
            last_event_type STRING,
            first_event_time TIMESTAMP,
            last_event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.form_submissions_cleaned (
            submission_id STRING,
            landing_id STRING,
            visitor_ip STRING,
            utm_source STRING,
            utm_medium STRING,
            utm_campaign STRING,
            time_to_submit_sec INT,
            name STRING,
            email STRING,
            company STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.server_status_enriched (
            server_id STRING,
            ip_address STRING,
            current_score DOUBLE,
            status STRING,
            blacklist_status STRING,
            risk_level STRING,
            blacklist_count INT,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP
        ) USING iceberg
    """)

    logger.info("✅ Silver layer tables created successfully")


# ---------------------- GOLD ---------------------- #
def create_gold_tables(spark):
    logger.info("Creating Gold layer tables...")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.gold")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.campaign_content_insights (
            send_id STRING,
            account_id STRING,
            subject STRING,
            subject_length INT,
            body_length INT,
            template_name STRING,
            element_count INT,
            has_cta_button BOOLEAN,
            is_text_only BOOLEAN,
            total_interactions INT,
            open_rate DOUBLE,
            click_rate DOUBLE,
            content_style STRING,
            recommendation STRING,
            evaluation_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.subscriber_engagement_score (
            subscriber_id STRING,
            account_id STRING,
            total_interactions INT,
            open_rate DOUBLE,
            click_rate DOUBLE,
            spam_rate DOUBLE,
            unsubscribe_rate DOUBLE,
            avg_response_delay_sec DOUBLE,
            days_since_last_event INT,
            engagement_score DOUBLE,
            evaluation_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.subscriber_churn_risk_prediction (
            subscriber_id STRING,
            account_id STRING,
            churn_risk_score DOUBLE,
            churn_risk_level STRING,
            days_since_last_event INT,
            total_interactions INT,
            unsubscribe_rate DOUBLE,
            spam_rate DOUBLE,
            evaluation_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.server_profile_recommendation (
            server_id STRING,
            ip_address STRING,
            current_score DOUBLE,
            status STRING,
            blacklist_count INT,
            recommended_action STRING,
            recommendation_reason STRING,
            evaluation_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.account_score_summary (
            account_id STRING,
            total_campaigns_sent INT,
            total_interactions INT,
            open_rate DOUBLE,
            spam_rate DOUBLE,
            total_form_submissions INT,
            landing_conversion_rate DOUBLE,
            avg_server_score DOUBLE,
            account_health STRING,
            improvement_suggestion STRING,
            evaluation_time TIMESTAMP
        ) USING iceberg
    """)

    logger.info("✅ Gold layer tables created successfully")


def main():
    spark = create_spark_session()
    try:
        create_bronze_tables(spark)
        create_silver_tables(spark)
        create_gold_tables(spark)

        logger.info("\n=== Created Databases ===")
        spark.sql("SHOW DATABASES IN local").show()

        for db in ["bronze", "silver", "gold"]:
            logger.info(f"\nTables in {db} database:")
            spark.sql(f"SHOW TABLES IN local.{db}").show()

        logger.info("🎉 All Email Iceberg tables initialized successfully!")

    except Exception as e:
        logger.error(f"❌ Error creating tables: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
