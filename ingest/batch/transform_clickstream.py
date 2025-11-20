from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as f_min, max as f_max, count as f_count,
    sum as f_sum, when, to_timestamp
)

spark = SparkSession.builder.appName("ClickstreamTransforms").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read bronze events
bronze_path = "data_lake/clickstream_raw/events"
df = spark.read.parquet(bronze_path)

# Ensure timestamp column is proper timestamp type
df = df.withColumn("event_ts", to_timestamp(col("ts")))

# -----------------------------
# 2. Session-level metrics
# -----------------------------
session_metrics = (
    df.groupBy("session_id")
      .agg(
          f_min("event_ts").alias("session_start"),
          f_max("event_ts").alias("session_end"),
          f_count("*").alias("page_views"),
          f_sum(col("clicked")).alias("clicks"),
          f_min("user_id").alias("user_id"),
          f_min("device").alias("device"),
          f_min("campaign").alias("campaign"),
          f_min("referrer").alias("referrer"),
      )
      .withColumn(
          "session_duration_sec",
          (col("session_end").cast("long") - col("session_start").cast("long"))
      )
      .withColumn(
          "session_converted",
          when(col("clicks") > 0, 1).otherwise(0)
      )
)

session_out = "data_lake/clickstream_refined/session_metrics"
(
    session_metrics
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(session_out)
)

# -----------------------------
# 3. User-level metrics
# -----------------------------
user_metrics = (
    session_metrics.groupBy("user_id")
      .agg(
          f_count("*").alias("total_sessions"),
          f_sum("page_views").alias("total_page_views"),
          f_sum("clicks").alias("total_clicks"),
          f_sum("session_converted").alias("converted_sessions"),
          f_max("session_end").alias("last_seen_ts"),
      )
      .withColumn(
          "ctr",
          when(col("total_page_views") > 0,
               col("total_clicks") / col("total_page_views")
          ).otherwise(0.0)
      )
)

user_out = "data_lake/clickstream_refined/user_metrics"
(
    user_metrics
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(user_out)
)

print("Wrote session_metrics and user_metrics tables.")
spark.stop()
