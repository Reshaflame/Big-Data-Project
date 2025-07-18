#!/usr/bin/env python
# -----------------------------------------------------------------------------------
#  Email-send Kafka stream  ➜  Iceberg bronze          (with row-level validation)
# -----------------------------------------------------------------------------------
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC           = "email_send_stream"
CHECKPOINT_DIR  = "/home/iceberg/warehouse/checkpoints/email_send_stream"
GOOD_TABLE      = "bronze.email_send_stream"
BAD_TABLE       = "quarantine.email_send_stream"

# 1a. ──────────────────────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("ingest_email_send_stream")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1b ── create schemas / tables if missing ────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.quarantine")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.email_send_stream (
    send_id          STRING,
    account_id       STRING,
    sender_email     STRING,
    reply_to_email   STRING,
    recipient_count  INT,
    message_size_kb  INT,
    message_subject  STRING,
    message_body     STRING,
    event_time       TIMESTAMP,
    ingestion_time   TIMESTAMP,
    ingest_ts        TIMESTAMP
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.quarantine.email_send_stream (
    send_id          STRING,
    account_id       STRING,
    sender_email     STRING,
    reply_to_email   STRING,
    recipient_count  INT,
    message_size_kb  INT,
    message_subject  STRING,
    message_body     STRING,
    event_time       TIMESTAMP,
    ingestion_time   TIMESTAMP,
    ingest_ts        TIMESTAMP,
    error_flag       STRING
)
USING iceberg
TBLPROPERTIES ('kind' = 'quarantine')
""")

# 2 ──────────────────────────────────────────────────────────────────────────────
schema = T.StructType([
    T.StructField("send_id",          T.StringType()),
    T.StructField("account_id",       T.StringType()),
    T.StructField("sender_email",     T.StringType()),
    T.StructField("reply_to_email",   T.StringType()),
    T.StructField("recipient_count",  T.IntegerType()),
    T.StructField("message_size_kb",  T.IntegerType()),
    T.StructField("message_subject",  T.StringType()),
    T.StructField("message_body",     T.StringType()),
    T.StructField("event_time",       T.TimestampType()),
    T.StructField("ingestion_time",   T.TimestampType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 3 ──────────────────────────────────────────────────────────────────────────────
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
       .select(F.from_json("json_str", schema).alias("data"))
       .select("data.*")
       .withColumn("ingest_ts", F.current_timestamp())
)

# 4 ── validation predicate -----------------------------------------------------
valid_expr = (
    F.col("send_id").isNotNull() & F.col("account_id").isNotNull() &
    F.col("sender_email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$") &
    F.col("reply_to_email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$") &
    F.col("recipient_count").between(1, 100_000) &
    F.col("message_size_kb").between(1, 10_240) &
    (F.col("event_time") <= F.col("ingestion_time") + F.expr("INTERVAL 48 HOURS"))
)

good_df = parsed.filter(valid_expr)

bad_df  = (
    parsed.filter(~valid_expr)
          .withColumn("error_flag", F.lit("ROW_FAILED_VALIDATION"))
)

# 5 ──────────────────────────────────────────────────────────────────────────────
good_q = (
    good_df.writeStream
        .format("iceberg")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/good")
        .option("path", GOOD_TABLE)          # or .table(GOOD_TABLE)
        .outputMode("append")
        .start()
)

bad_q = (
    bad_df.writeStream
        .format("iceberg")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bad")
        .option("path", BAD_TABLE)
        .outputMode("append")
        .start()
)

# keep the driver alive until you Ctrl-C or a query fails
good_q.awaitTermination()
bad_q.awaitTermination()
