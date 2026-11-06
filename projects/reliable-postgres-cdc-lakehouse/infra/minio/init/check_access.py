import os
from minio import Minio
from minio.error import S3Error

with open(os.environ["MINIO_SECRETS_FILE"], encoding="utf-8") as file:
    values = dict(line.strip().split("=", 1) for line in file if "=" in line)

access_key = values["MINIO_SPARK_ACCESS_KEY"]
print("access_key:", repr(access_key))

client = Minio(
    os.environ["MINIO_ENDPOINT"],
    access_key=access_key,
    secret_key=values["MINIO_SPARK_SECRET_KEY"],
    secure=False,
    region=os.environ["MINIO_REGION"],
)

try:
    client.stat_object("lakehouse", "_control/compaction/orders/2026-09-17.json")
    print("Controle: ALLOWED")
except S3Error as e:
    print("Controle:", e.code)