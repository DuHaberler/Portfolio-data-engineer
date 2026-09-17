import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("test-raw-uniqueness")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.parquet("s3a://lakehouse/raw/orders")

duplicates = (
    df.groupBy(
        "topic",
        "kafka_partition",
        "offset"
    )
    .count()
    .filter(col("count") > 1)
)

duplicates.show(truncate=False)

spark.stop()