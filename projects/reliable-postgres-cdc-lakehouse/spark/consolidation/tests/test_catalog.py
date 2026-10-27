from pyspark.sql import SparkSession
from common.polaris_auth import get_polaris_credential


def main():

    spark = (
        SparkSession.builder
        .appName("test-polaris-catalog")
        .config(
            "spark.sql.catalog.lakehouse.credential",
            get_polaris_credential()
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