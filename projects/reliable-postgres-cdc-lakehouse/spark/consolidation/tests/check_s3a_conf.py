import os
from pyspark.sql import SparkSession
from common.minio_auth import get_minio_credential

access_key, secret_key = get_minio_credential()

spark = (
    SparkSession.builder
    .appName("check-s3a-conf")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .getOrCreate()
)
conf = spark.sparkContext._jsc.hadoopConfiguration()

print("DIAG access_key:", repr(conf.get("fs.s3a.access.key")))
print("DIAG secret_igual_ao_arquivo:", conf.get("fs.s3a.secret.key") == secret_key)
print("DIAG credentials_provider:", conf.get("fs.s3a.aws.credentials.provider"))
print("DIAG bucket_access_key:", conf.get("fs.s3a.bucket.lakehouse.access.key"))
print("DIAG bucket_provider:", conf.get("fs.s3a.bucket.lakehouse.aws.credentials.provider"))
print("DIAG aws_env:", [k for k in os.environ if k.startswith("AWS_")])

spark.stop()