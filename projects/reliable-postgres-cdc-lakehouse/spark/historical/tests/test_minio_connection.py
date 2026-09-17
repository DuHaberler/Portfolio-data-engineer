### COMANDOS PARA TESTE DE CONEXÃO E CONSUMO MINIO, ANTES DE CRIAR O CONTAINER SPARK ###

# DESCOBRIR A REDE MINIO: docker inspect portfolio-minio --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}'
# RODAR O SPARK TEMPORARIAMENTE NA REDE DESCOBERTA: 
"""
docker run --rm `
    --network <NOME_DA_REDE> `
    --env-file .env `
    -v "${PWD}\spark\historical:/opt/spark/work-dir:ro" `
    portfolio-spark:4.1.3 `
    /opt/spark/bin/spark-submit `
    --packages org.apache.hadoop:hadoop-aws:3.4.2 `
    /opt/spark/work-dir/test_minio_connection.py
"""
##__FIM DO COMENTARIO__##


import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-minio-connection")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.createDataFrame(
    [("spark-minio-ok",)],
    ["message"]
)

path = "s3a://lakehouse/tests/spark-minio"

df.write.mode("overwrite").parquet(path)

spark.read.parquet(path).show(truncate=False)

spark.stop()