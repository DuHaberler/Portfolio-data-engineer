#---Bibliotecas-------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
import os
import math
import json
from pyspark.sql import SparkSession
from datetime import datetime, timezone, timedelta

from pyspark.sql.functions import (
    xxhash64, count, min as spark_min, max as spark_max, sum as spark_sum
)

#---Classe------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
class ControlFile:
    def __init__(self, fs, control_path_hadoop, dataset, partition_date, run_id, started_at):
        self.fs = fs
        self.control_path_hadoop = control_path_hadoop

        self.control = {
            "dataset": dataset,
            "partition_date": str(partition_date),
            "run_id": run_id,
            "started_at": started_at
        }
    

    def start(self):
        self.control.update({
            "status": "RUNNING",
            "published_path": None, 
            "finished_at": None
        })
        self.write_control_file()


    def exec_infos(self, total_size_bytes, output_bytes, input_parquet_files, output_parquet_files, input_rows, output_rows):
        self.control.update({
            "input_bytes": total_size_bytes,
            "output_bytes": output_bytes,

            "input_files": len(input_parquet_files),
            "output_files": len(output_parquet_files),

            "input_rows": input_rows,
            "output_rows": output_rows,
        })
    

    def no_data(self):
        self.control.update({
            "status": "COMPLETED",
            "input_files": 0,
            "output_files": 0,
            "input_bytes": 0,
            "output_bytes": 0,
            "input_rows": 0,
            "output_rows": 0,
            "finished_at": datetime.now(timezone.utc).isoformat()
        })
        self.write_control_file()
    

    def operation(self, operation):
        self.control.update({
            "operation": operation,
        })
        self.write_control_file()
    

    def failed_validation(self):
        self.control.update({
            "status": "FAILED",
            "validation":{
                "method": "kafka_identity_fingerprint",
                "identity_columns": [
                    "topic",
                    "kafka_partition",
                    "offset"
                ],
                "result":"FAILED"
            }
        })
        self.write_control_file()
    

    def passed_validation(self, run_path):
        self.control.update({
            "status": "COMPLETED",
            "published_path": run_path,
            "validation":{
                "method": "kafka_identity_fingerprint",
                "identity_columns": [
                    "topic",
                    "kafka_partition",
                    "offset"
                ],
                "result": "PASSED"
            },

            "finished_at": datetime.now(timezone.utc).isoformat()
        })
        self.write_control_file()
    

    def execution_failed(self, error):
        self.control.update({
            "status": "FAILED",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(error)
        })
        self.write_control_file()


    def write_control_file(self):
        json_content = json.dumps(
            self.control,
            indent=2
        )

        output_stream = self.fs.create(
            self.control_path_hadoop,
            True
        )

        try:
            output_stream.write(
                bytearray(json_content.encode("utf-8"))
            )
        finally:
            output_stream.close()


#---Funções-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
def get_vars():
    DATASET = os.environ["DATASET"]

    LANDING_PATH = os.environ["LANDING_PATH"]
    COMPACTED_PATH = os.environ["COMPACTED_PATH"]
    CONTROL_PATH = os.environ["CONTROL_PATH"]

    TARGET_FILE_SIZE_MB = 128
    SMALL_FILE_THRESHOLD_MB = 96 #75% TARGET
    MIN_FILES_TO_COMPACT = 5
    FRAGMENTATION_THRESHOLD = 4

    RUNNING_STALE_AFTER_MINUTES = int(
        os.getenv("RUNNING_STALE_AFTER_MINUTES", "30")
    )

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

    partition_path = (
        f"{LANDING_PATH}/ingestion_date_utc={partition_date}"
    )

    spark = (
        SparkSession.builder
        .appName(f"compaction-{DATASET}")
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .getOrCreate()
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    started_at = datetime.now(timezone.utc).isoformat()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(partition_path)
    fs = path.getFileSystem(hadoop_conf)

    control_file_path = f"{CONTROL_PATH}/{partition_date}.json"
    control_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(control_file_path)
    fs.mkdirs(control_path_hadoop.getParent())

    control = ControlFile(fs, control_path_hadoop, DATASET, partition_date, run_id, started_at)
     
    return (COMPACTED_PATH, TARGET_FILE_SIZE_MB, SMALL_FILE_THRESHOLD_MB, MIN_FILES_TO_COMPACT, 
    FRAGMENTATION_THRESHOLD, RUNNING_STALE_AFTER_MINUTES, partition_date, partition_path, 
    spark, run_id, control_path_hadoop, hadoop_conf, path, fs, control)


def check_partition_path(spark, path, fs, control, partition_date):
    if not fs.exists(path):
        control.no_data()
        
        print(
            f"Partition {partition_date} does not exist.\n"
            f"Marked as COMPLETED with no data"
        )

        spark.stop()
        raise SystemExit(0)


def check_partition_status(fs, control_path_hadoop, RUNNING_STALE_AFTER_MINUTES, spark, partition_date):
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
            print(
                f"Partition {partition_date} already completed.\n"
                "Nothing to process."
            )
        
            spark.stop()
            raise SystemExit(0)
        
        elif status_json == "FAILED":
            print(
                f"Previous run for {partition_date} failed.\n"
                f"Starting a new attempt."
            )
        
        elif status_json == "RUNNING":
            started_at = datetime.fromisoformat(
                control["started_at"]
            )

            running_age = datetime.now(timezone.utc) - started_at

            stale_after = timedelta(
                minutes=RUNNING_STALE_AFTER_MINUTES
            )

            if running_age < stale_after:
                print(
                    f"Partition {partition_date} is already running.\n"
                    f"Age: {running_age}"
                )
                spark.stop()
                raise SystemExit(0)

            print(
                f"Previous RUNNING state is stale\n"
                f"(age: {running_age})\n"
                f"Starting a new attempt:"
            )
        else:
            raise RuntimeError(
                f"Unknow control status: {status_json}"
            )


def get_parquet_files(fs, path):
    statuses = fs.listStatus(path)

    parquet_files = []

    for status in statuses:
        if status.isFile() and status.getPath().getName().endswith(".parquet"):
            parquet_files.append(status)
            
    return parquet_files


def calculate_compaction(parquet_files, SMALL_FILE_THRESHOLD_MB, TARGET_FILE_SIZE_MB, MIN_FILES_TO_COMPACT, FRAGMENTATION_THRESHOLD):
    total_size_bytes = sum(
        status.getLen()
        for status in parquet_files
    )

    small_file_threshold_bytes = (
        SMALL_FILE_THRESHOLD_MB * 1024 * 1024
    )

    small_file_count = sum(
        1
        for status in parquet_files
        if status.getLen() < small_file_threshold_bytes
    )

    target_size_file_bytes = TARGET_FILE_SIZE_MB * 1024 * 1024

    expected_files = max(
        1,
        math.ceil(total_size_bytes / target_size_file_bytes)
    )

    fragmentation_ratio = (
        len(parquet_files) / expected_files
    )

    should_compact = (
        small_file_count >= MIN_FILES_TO_COMPACT
        and fragmentation_ratio >= FRAGMENTATION_THRESHOLD
    )

    return (should_compact, expected_files, total_size_bytes)

def create_compacted_path(COMPACTED_PATH, fs, partition_date, run_id, spark):
    run_path = (
        f"{COMPACTED_PATH}/"
        f"ingestion_date_utc={partition_date}/"
        f"run-{run_id}"
    )

    run_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(run_path)
    fs.mkdirs(run_path_hadoop)

    return (run_path_hadoop, run_path)


def perform_operation(operation, parquet_files, spark, run_path_hadoop, hadoop_conf, partition_path, expected_files, run_path, fs):
    if operation == "NO_NEED":
        for status in parquet_files:
            source_path = status.getPath()

            destination_path = spark._jvm.org.apache.hadoop.fs.Path(
                run_path_hadoop,
                source_path.getName()
            )

            spark._jvm.org.apache.hadoop.fs.FileUtil.copy(
                fs,
                source_path,
                fs,
                destination_path,
                False, #deleteSource
                hadoop_conf
            )
    elif operation == "DO_COMPACTION":
        input_df = spark.read.parquet(partition_path)

        (
            input_df
            .coalesce(expected_files)
            .write
            .mode("overwrite")
            .parquet(run_path)
        )

def calculate_fingerprint(df):
    fingerprint_rows = (
        df
        .select(
            "topic",
            "kafka_partition",
            "offset"
        )
        .withColumn(
            "identity_hash",
            xxhash64(
                "topic",
                "kafka_partition",
                "offset"
            ).cast("decimal(38,0)")
        )
        .groupBy(
            "topic",
            "kafka_partition"
        )
        .agg(
            count("*").alias("row_count"),
            spark_min("offset").alias("min_offset"),
            spark_max("offset").alias("max_offset"),
            spark_sum("identity_hash").alias("hash_sum")
        )
        .collect()
    )

    return {
        (row["topic"], row["kafka_partition"]): {
            "row_count": row["row_count"],
            "min_offset": row["min_offset"],
            "max_offset": row["max_offset"],
            "hash_sum": row["hash_sum"],
        }
        for row in fingerprint_rows
    }


def get_fingerprints(run_path, fs, spark, partition_path):
    run_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(run_path)
    output_statuses = fs.listStatus(run_path_hadoop)

    output_parquet_files = [
        status
        for status in output_statuses
        if status.isFile()
        and status.getPath().getName().endswith(".parquet")
    ]

    output_bytes = sum(
        file.getLen()
        for file in output_parquet_files
    )

    input_df = spark.read.parquet(partition_path)
    output_df = spark.read.parquet(run_path)

    input_fingerprint = calculate_fingerprint(input_df)
    output_fingerprint = calculate_fingerprint(output_df)

    input_rows = sum(
        values["row_count"]
        for values in input_fingerprint.values()
    )

    output_rows = sum(
        values["row_count"]
        for values in output_fingerprint.values()
    )

    return (input_fingerprint, output_fingerprint, output_bytes, output_parquet_files, input_rows, output_rows)

#---Main--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
def main():
    (COMPACTED_PATH, TARGET_FILE_SIZE_MB, SMALL_FILE_THRESHOLD_MB, MIN_FILES_TO_COMPACT, 
    FRAGMENTATION_THRESHOLD, RUNNING_STALE_AFTER_MINUTES, partition_date, partition_path, 
    spark, run_id, control_path_hadoop, hadoop_conf, path, fs, control) = get_vars()

    check_partition_status(fs, control_path_hadoop, RUNNING_STALE_AFTER_MINUTES, spark, partition_date)

    try:
        control.start()
        check_partition_path(spark, path, fs, control, partition_date)

        input_parquet_files = get_parquet_files(fs, path)

        if len(input_parquet_files) == 0:
            control.no_data()
            
            print(
                f"No data found for partition {partition_date}\n."
                "Marked as COMPLETED."
            )

            spark.stop()
            raise SystemExit(0)

        (should_compact, expected_files, total_size_bytes) = calculate_compaction(input_parquet_files, 
        SMALL_FILE_THRESHOLD_MB, TARGET_FILE_SIZE_MB, MIN_FILES_TO_COMPACT, FRAGMENTATION_THRESHOLD)

        if should_compact:
            operation = "DO_COMPACTION"
        else:
            operation = "NO_NEED"

        control.operation(operation)

        (run_path_hadoop, run_path) = create_compacted_path(COMPACTED_PATH, fs, partition_date, run_id, spark)

        perform_operation(operation, input_parquet_files, spark, run_path_hadoop, hadoop_conf, partition_path, expected_files, run_path, fs)

        (input_fingerprint, output_fingerprint, output_bytes, output_parquet_files, input_rows, output_rows) = get_fingerprints(run_path, fs, spark, partition_path)

        control.exec_infos(total_size_bytes, output_bytes, input_parquet_files, output_parquet_files, input_rows, output_rows)

        if input_fingerprint != output_fingerprint:
            control.failed_validation()

            raise RuntimeError(
                f"Identity fingerprint mismatch between input and output."
            )

        control.passed_validation(run_path)
        
    except Exception as error:
        control.execution_failed(error)
        raise

if __name__ == "__main__":
    main()