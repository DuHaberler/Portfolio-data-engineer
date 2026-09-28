import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test_raw_schema")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.parquet("s3a://lakehouse/raw/landing/orders")

df.printSchema()

df.select(
    "topic",
    "kafka_partition",
    "offset",
    "kafka_timestamp_utc",
    "ingested_at_utc",
    "ingestion_date_utc"
).show(truncate=False)

spark.stop()