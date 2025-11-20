# clickstream-lakehouse-ml
End-to-end clickstream lakehouse with Spark, Delta, MLflow, etc.

# Clickstream Lakehouse & CTR Prediction (Spark + MLflow)

End-to-end **data engineering + ML** project that simulates website clickstream traffic, ingests it with **Spark Structured Streaming**, builds a local **lakehouse-style** data lake (bronze/silver layers), and trains a **CTR (click-through / conversion) prediction model** tracked with **MLflow**.

All of this runs **locally** on WSL2 + Docker with **zero cloud cost**, but follows patterns that can be lifted to Kafka/S3/Databricks in production.

---

## Architecture

```text
                +---------------------------+
                |  data_gen/generate_events |
                |  (Python clickstream sim) |
                +-------------+-------------+
                              |
                              v
                     landing/raw_events      (JSONL files)
                              |
                              v
         +--------------------+---------------------+
         |  Spark Structured Streaming (Bronze)     |
         |  ingest/spark_streaming_job/main.py      |
         +--------------------+---------------------+
                              |
                              v
           data_lake/clickstream_raw/events         (Parquet)

                              |
                              v
         +--------------------+---------------------+
         |  Spark Batch Transforms (Silver)         |
         |  ingest/batch/transform_clickstream.py   |
         +--------------------+---------------------+
                       |                    |
                       v                    v
  data_lake/clickstream_refined/session_metrics   (session-level)
  data_lake/clickstream_refined/user_metrics      (user-level)

                              |
                              v
         +--------------------+---------------------+
         |  ML Training (CTR Prediction)           |
         |  ml/training/train_ctr_model.py         |
         +--------------------+---------------------+
                              |
                     metrics & params --> MLflow UI
                       model artifact --> artifacts/ctr_model

