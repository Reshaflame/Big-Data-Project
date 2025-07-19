#!/usr/bin/env python3
"""
Email Send Stream to Bronze
Consumes data from Kafka topic and writes to Bronze Iceberg table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

print("🔥 Script started")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Kafka and Iceberg configuration"""
    
    return SparkSession.builder \
        .appName("Email Send Stream to Bronze") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
        .getOrCreate()

def process_email_send_stream(spark):
    """Process live email-send events, echo to console + write to Bronze"""
    print("✅ Streaming query launched")
    logger.info("Starting email-send stream processing…")

    send_schema = StructType([
        StructField("send_id",          StringType(), True),
        StructField("account_id",       StringType(), True),
        StructField("sender_email",     StringType(), True),
        StructField("reply_to_email",   StringType(), True),
        StructField("recipient_count",  IntegerType(), True),
        StructField("message_size_kb",  IntegerType(), True),
        StructField("message_subject",  StringType(), True),
        StructField("message_body",     StringType(), True),
        StructField("event_time",       StringType(), True),
        StructField("ingestion_time",   StringType(), True),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "kafka:29092")
           .option("subscribe", "email_send_stream")
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .select(from_json(col("value").cast("string"), send_schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", to_timestamp("event_time"))
              .withColumn("ingestion_time", to_timestamp("ingestion_time")))

    # ① console sink – so you can eyeball rows in the Airflow log
    (parsed.writeStream
           .format("console")
           .outputMode("append")
           .option("truncate", False)
           .start())

    # ② Iceberg sink – real Bronze landing
    return (parsed.writeStream
            .format("iceberg")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .option("path", "local.bronze.email_send_stream")
            .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/bronze_email_send_stream")
            .start())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        query = process_email_send_stream(spark)
        logger.info(f"Email-send streaming query started: {query.id}")
        print(f"Query active: {query.isActive}")
        print(f"Query status: {query.status}")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in streaming job: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
