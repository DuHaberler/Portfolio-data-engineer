### COMANDOS PARA TESTE DE CONEXÃO E CONSUMO SPARK, ANTES DE CRIAR O CONTAINER SPARK ###

# DESCOBRIR A REDE KAFKA: docker inspect portfolio-kafka --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}'
# RODAR O SPARK TEMPORARIAMENTE NA REDE DESCOBERTA: 
"""
docker run --rm `
    --network <NOME_DA_REDE> `
    -v "${PWD}\spark\historical:/opt/spark/work-dir:ro" `
    portfolio-spark:4.1.3 `
    /opt/spark/bin/spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 `
    /opt/spark/work-dir/test_kafka_connection.py
"""

##__FIM DO COMENTARIO__##

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-kafka-connection")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "cdc_debezium.public.orders")
    .option("startingOffsets", "earliest")
    .load()
)

df.printSchema()

query = (
    df.select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        "key",
        "value"
    )
    .writeStream
    .format("console")
    .option("truncate", "false")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

spark.stop()


