import os
import json
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

def get_vars():

    DATASET = os.environ["DATASET"]
    CONTROL_PATH = os.environ["CONTROL_PATH"]

    PARTITION_DATE_ENV = os.getenv("PARTITION_DATE")
    
    if PARTITION_DATE_ENV:
        partition_date = datetime.strptime(
            PARTITION_DATE_ENV,
            "%Y-%m-%d"
        ).date()

    else:
        partition_date = (
            datetime.now(timezone.utc).date()
            - timedelta(days=1)
        )

    spark = (
            SparkSession.builder
            .appName(f"cdc-parser-{DATASET}")
            .config("spark.sql.session.timeZone","UTC")
            .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
            .config("spark.hadoop.fs.s3a.path.style.access","true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
            .getOrCreate()
        )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    control_file_path = f"{CONTROL_PATH}/{partition_date}.json"
    control_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(control_file_path)
    fs = control_path_hadoop.getFileSystem(hadoop_conf)
    
    return (spark, control_path_hadoop, fs, partition_date)


def check_partition_run(control_path_hadoop, fs, spark, partition_date):
    if fs.exists(control_path_hadoop):
        input_stream = fs.open(control_path_hadoop)

        try:
            content = bytes(
                input_stream.readAllBytes()
            ).decode("utf-8")
        finally:
            input_stream.close()

        control = json.loads(content)

        status_json = control.get("status")

        if status_json == "COMPLETED":
            published_path = control.get("published_path")
        else:
            print(f"Partition {partition_date} compaction not completed.\n Unable to process.")
            spark.stop()
            raise SystemExit(0)

        if published_path is None:
            print(f"No data: partition {partition_date}.\n Unable to process.")
            spark.stop()
            raise SystemExit(0)
    else:
        print(f"Control file for partition {partition_date} does not exist.\n Unable to process.")
        spark.stop()
        raise SystemExit(0)

    return published_path

def check_parquet_files(fs, path, spark):
    if not fs.exists(path):
        print(f"Publication path {path.toString()} does not exist.\n Unable to process.")
        spark.stop()
        raise SystemExit(0)
    
    else:
        objects = fs.listStatus(path)

        parquet_files = []

        for object in objects:
            if object.isFile() and object.getPath().getName().endswith(".parquet"):
                parquet_files.append(object)

        if len(parquet_files) == 0:
            print(f"No Parquet files found in {path.toString()}.\n Unable to process.")
            spark.stop()
            raise SystemExit(0)

def decode_df(df_raw):

    df_decoded = (
        df_raw
        .filter(
            col("value_raw").isNotNull()
        )
        .select(
            "topic",
            "kafka_partition",
            "offset",
            "kafka_timestamp_utc",
            col("key_raw").cast("string").alias("key_json"),
            col("value_raw").cast("string").alias("value_json")
        )
    )

    return df_decoded     

def get_debezium_schema():

    order_schema = StructType([
        StructField("id_order", StringType(), True),
        StructField("id_client", StringType(), True),
        StructField("status_order", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    source_schema = StructType([
        StructField("lsn", LongType(), True),
        StructField("txId", LongType(), True),
        StructField("sequence", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ])

    payload_schema = StructType([
        StructField("before", order_schema, True),
        StructField("after", order_schema, True),
        StructField("source", source_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ])

    return StructType([
        StructField("payload", payload_schema, True)
    ])


def parse_cdc(df_decoded, debezium_schema):

    parsed_df = (
        df_decoded
        .withColumn(
            "debezium",
            from_json(
                col("value_json"),
                debezium_schema
            )
        )
        .select(
            "topic",
            "kafka_partition",
            "offset",
            "kafka_timestamp_utc",

            col("debezium.payload.op")
            .alias("cdc_operation"),

            col("debezium.payload.source.lsn")
            .alias("source_lsn"),

            col("debezium.payload.source.txId")
            .alias("source_tx_id"),

            col("debezium.payload.source.sequence")
            .alias("source_sequence"),

            col("debezium.payload.source.ts_ms")
            .alias("source_ts_ms"),

            col("debezium.payload.ts_ms")
            .alias("cdc_ts_ms"),

            col("debezium.payload.before")
            .alias("before"),

            col("debezium.payload.after")
            .alias("after"),
        )
    )

    return parsed_df

def main():

    (spark, control_path_hadoop, fs, partition_date) = get_vars()

    published_path = check_partition_run(control_path_hadoop, fs, spark, partition_date)
    published_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(published_path)
    
    check_parquet_files(fs, published_path_hadoop, spark)

    df_raw = spark.read.parquet(published_path)
    df_decoded = decode_df(df_raw)

    debezium_schema = get_debezium_schema()  

    parsed_df = parse_cdc(df_decoded, debezium_schema)

    parsed_df.printSchema()
    parsed_df.show(
        10,
        truncate=False
    )



if __name__ == "__main__":
    main()