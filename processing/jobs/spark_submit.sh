#!/bin/sh
spark-submit \
  --packages \
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,\
org.apache.kafka:kafka-clients:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.610" \
  /opt/jobs/ingest_email_stream.py
