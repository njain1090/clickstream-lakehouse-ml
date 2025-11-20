from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import col

# -------------------------
# Spark session with S3/MinIO config
# -------------------------
spark = SparkSession.builder \
    .appName("ClickstreamIngestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Define schema for raw clickstream events
# -------------------------
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("clicked", IntegerType(), True)
])

# -------------------------
# Read streaming JSON lines from landing zone
# -------------------------
input_path = "landing/raw_events"

df_raw = spark.readStream \
    .schema(schema) \
    .json(input_path)

# -------------------------
# Write raw data to MinIO (S3)
# -------------------------
output_path = "data_lake/clickstream_raw/events/"
checkpoint_path = "data_lake/clickstream_raw/checkpoints/raw/"

query = df_raw.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

query.awaitTermination()
