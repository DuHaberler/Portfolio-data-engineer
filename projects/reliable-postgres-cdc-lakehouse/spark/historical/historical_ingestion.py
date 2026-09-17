import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

topic = os.environ["KAFKA_TOPIC"]
raw_path = os.environ["RAW_PATH"]
checkpoint_path = os.environ["CHECKPOINT_PATH"]

spark = (
    SparkSession.builder
    .appName(f"raw-ingestion-{topic}")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

raw_df = (
    df.select(
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").alias("key_raw"),
        col("value").alias("value_raw"),
        current_timestamp().alias("ingested_at")
    ).withColumn("ingestion_date", to_date(col("ingested_at")))
)
query = (
    raw_df.writeStream
    .format("parquet")
    .outputMode("append")
    .partitionBy("ingestion_date")
    .option("path", raw_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
