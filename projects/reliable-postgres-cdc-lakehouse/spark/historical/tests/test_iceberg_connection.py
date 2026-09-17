import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-iceberg-connection") # cria a aplicação Spark e dá um nome para ela.

    #Iceberg
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") # habilita funcionalidades específicas do Iceberg dentro do Spark SQL.
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")                      # diz para o spark que existe um catálogo chamado lakehouse e ele será gerenciado pelo Iceberg.
    .config("spark.sql.catalog.lakehouse.type", "hadoop")                                                # define qual implementação de catálogo Iceberg será usada. Nesse caso, HadoopCatalog
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")                        # diz onde esse catálogo vai armazenar suas tabelas.

    #MinIO/s3a
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)