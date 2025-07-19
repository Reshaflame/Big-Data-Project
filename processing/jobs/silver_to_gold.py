#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.functions import countDistinct

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver to Gold ETL") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse") \
        .getOrCreate()

def build_gold_campaign_content_insights(spark, evaluation_date):
    logger.info("Building GOLD_CAMPAIGN_CONTENT_INSIGHTS")
    campaigns = spark.table("local.silver.campaign_base")
    interactions = spark.table("local.silver.email_interactions_cleaned")
    
    agg = interactions.groupBy("send_id").agg(
        count("*").alias("total_interactions"),
        (sum(when(col("interaction_type") == "open", 1).otherwise(0)) / count("*")).alias("open_rate"),
        (sum(when(col("interaction_type") == "click", 1).otherwise(0)) / count("*")).alias("click_rate")
    )
    gold = (
        campaigns.join(agg, "send_id", "left")
        .withColumn("subject_length", length(col("subject")))
        .withColumn("body_length", lit(None))  
        .withColumn("template_name", lit(None))
        .withColumn("element_count", lit(None))
        .withColumn("has_cta_button", lit(False))
        .withColumn("is_text_only", lit(True))
        .withColumn("content_style", lit(None))
        .withColumn("recommendation", lit(None))
        .withColumn("evaluation_time", current_timestamp())
        .select(
            "send_id", "account_id", "subject", "subject_length", "body_length", "template_name",
            "element_count", "has_cta_button", "is_text_only", "total_interactions",
            "open_rate", "click_rate", "content_style", "recommendation", "evaluation_time"
        )
    )
    gold.write.mode("overwrite").format("iceberg").saveAsTable("local.gold.campaign_content_insights")

def build_gold_subscriber_engagement_score(spark, evaluation_date):
    logger.info("Building GOLD_SUBSCRIBER_ENGAGEMENT_SCORE")
    subs = spark.table("local.silver.subscriber_base")
    inter = spark.table("local.silver.email_interactions_cleaned")
    agg = (
        inter.groupBy("recipient_email")
        .agg(
            count("*").alias("total_interactions"),
            (sum(when(col("interaction_type") == "open", 1).otherwise(0)) / count("*")).alias("open_rate"),
            (sum(when(col("interaction_type") == "click", 1).otherwise(0)) / count("*")).alias("click_rate"),
            (sum(when(col("interaction_type") == "spam", 1).otherwise(0)) / count("*")).alias("spam_rate"),
            (sum(when(col("interaction_type") == "unsubscribe", 1).otherwise(0)) / count("*")).alias("unsubscribe_rate"),
            avg("response_delay_sec").alias("avg_response_delay_sec"),
            max("event_time").alias("last_event_time")
        )
    )
    fact = (
        subs.join(agg, subs.email == agg.recipient_email, "left")
        .withColumn("evaluation_time", current_timestamp())
        .withColumn("days_since_last_event", datediff(current_date(), col("last_event_time")))
        .withColumn(
            "engagement_score",
            col("open_rate") * 0.4 + col("click_rate") * 0.4 - col("spam_rate") * 0.1 - col("unsubscribe_rate") * 0.1
        )
        .select(
            "subscriber_id", "account_id", "total_interactions", "open_rate", "click_rate", "spam_rate",
            "unsubscribe_rate", "avg_response_delay_sec", "days_since_last_event", "engagement_score", "evaluation_time"
        )
    )
    fact.write.mode("overwrite").format("iceberg").saveAsTable("local.gold.subscriber_engagement_score")

def build_gold_subscriber_churn_risk_prediction(spark, evaluation_date):
    logger.info("Building GOLD_SUBSCRIBER_CHURN_RISK_PREDICTION")
    eng = spark.table("local.gold.subscriber_engagement_score")
    
    fact = (
        eng.withColumn(
            "churn_risk_score",
            1.0 - col("engagement_score")
        )
        .withColumn(
            "churn_risk_level",
            when(col("churn_risk_score") > 0.7, "High")
            .when(col("churn_risk_score") > 0.4, "Medium")
            .otherwise("Low")
        )
        .select(
            "subscriber_id", "account_id", "churn_risk_score", "churn_risk_level",
            "days_since_last_event", "total_interactions", "unsubscribe_rate", "spam_rate", "evaluation_time"
        )
    )
    fact.write.mode("overwrite").format("iceberg").saveAsTable("local.gold.subscriber_churn_risk_prediction")

def build_gold_server_profile_recommendation(spark, evaluation_date):
    logger.info("Building GOLD_SERVER_PROFILE_RECOMMENDATION")
    servers = spark.table("local.silver.server_status_enriched")
    
    fact = (
        servers
        .withColumn(
            "recommended_action",
            when(col("blacklist_count") > 1, "Investigate Blacklist")
            .when(col("current_score") < 50, "Review Server Health")
            .otherwise("No Action")
        )
        .withColumn(
            "recommendation_reason",
            when(col("blacklist_count") > 1, "Multiple blacklists detected")
            .when(col("current_score") < 50, "Low server score")
            .otherwise("All metrics normal")
        )
        .withColumn("evaluation_time", current_timestamp())
        .select(
            "server_id", "ip_address", "current_score", "status", "blacklist_count",
            "recommended_action", "recommendation_reason", "evaluation_time"
        )
    )
    fact.write.mode("overwrite").format("iceberg").saveAsTable("local.gold.server_profile_recommendation")

def build_gold_account_score_summary(spark, evaluation_date):
    logger.info("Building GOLD_ACCOUNT_SCORE_SUMMARY")
    campaigns = spark.table("local.silver.campaign_base")
    inter = spark.table("local.silver.email_interactions_cleaned")
    forms = spark.table("local.silver.form_submissions_cleaned")
    servers = spark.table("local.silver.server_status_enriched")
    
    agg = (
        campaigns.groupBy("account_id")
        .agg(
            countDistinct("send_id").alias("total_campaigns_sent")
        )
        .join(
            inter.groupBy("account_id")
            .agg(
                count("*").alias("total_interactions"),
                (sum(when(col("interaction_type") == "open", 1).otherwise(0)) / count("*")).alias("open_rate"),
                (sum(when(col("interaction_type") == "spam", 1).otherwise(0)) / count("*")).alias("spam_rate")
            ),
            "account_id", "left"
        )
        .join(
            forms.groupBy("account_id")
            .agg(count("*").alias("total_form_submissions")),
            "account_id", "left"
        )
        .join(
            servers.groupBy("account_id").agg(avg("current_score").alias("avg_server_score")),
            "account_id", "left"
        )
        .withColumn("landing_conversion_rate", lit(None))  # להעשיר לפי קשר קמפיין/דף נחיתה
        .withColumn("account_health", when(col("spam_rate") < 0.05, "Good").otherwise("Risk"))
        .withColumn("improvement_suggestion", when(col("spam_rate") >= 0.05, "Reduce spam reports").otherwise(lit(None)))
        .withColumn("evaluation_time", current_timestamp())
        .select(
            "account_id", "total_campaigns_sent", "total_interactions", "open_rate", "spam_rate",
            "total_form_submissions", "landing_conversion_rate", "avg_server_score",
            "account_health", "improvement_suggestion", "evaluation_time"
        )
    )
    agg.write.mode("overwrite").format("iceberg").saveAsTable("local.gold.account_score_summary")

def main():
    spark = create_spark_session()
    evaluation_date = datetime.now().strftime("%Y-%m-%d")
    try:
        logger.info(f"Starting Silver to Gold ETL for evaluation_date: {evaluation_date}")
        build_gold_campaign_content_insights(spark, evaluation_date)
        build_gold_subscriber_engagement_score(spark, evaluation_date)
        build_gold_subscriber_churn_risk_prediction(spark, evaluation_date)
        build_gold_server_profile_recommendation(spark, evaluation_date)
        build_gold_account_score_summary(spark, evaluation_date)
        logger.info("Silver to Gold ETL completed successfully!")
    except Exception as e:
        logger.error(f"Error in Silver to Gold ETL: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
