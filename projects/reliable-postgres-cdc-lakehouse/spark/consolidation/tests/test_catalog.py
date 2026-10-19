import os
from pyspark.sql import SparkSession


def main():

    spark = (
        SparkSession.builder
        .appName("test-polaris-catalog")
        .config(
            "spark.sql.catalog.lakehouse.credential",
            f"root:{os.environ['POLARIS_ROOT_CLIENT_SECRET']}"
        )
        .getOrCreate()
    )

    try:
        spark.sql("SHOW NAMESPACES IN lakehouse").show(truncate=False)
    finally:
        spark.stop()

    return 0

if __name__=="__main__":
    main()