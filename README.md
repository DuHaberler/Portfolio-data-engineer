# Data Engineering Portfolio

Hands-on data engineering projects by Eduardo Haberler, Data Engineer with 8 years in the financial sector and AWS Certified Data Engineer – Associate.

My focus is reliability: pipelines that not only move data, but can prove it arrived complete, can be reprocessed safely and recover from failures.

## Projects

### Reliable PostgreSQL CDC Lakehouse

A pipeline that captures changes from a PostgreSQL order system (orders, payments and refunds) and delivers them to a lakehouse for analytics.

How the data flows:

1. Debezium reads every change from PostgreSQL and sends it to Kafka.
2. Spark Structured Streaming reads from Kafka and stores the raw events as Parquet files in MinIO.
3. Once a day, the raw data is compacted and checked before it is published. A run only becomes visible if its row count, offsets and hash match what was in Kafka.
4. Next step: an analytical layer with Apache Iceberg tables and access control through Apache Polaris.

Current status:

- Foundation and CDC capture: done
- Raw layer: done, final review in progress
- Analytical layer: in progress
- Orchestration and failure testing: planned

Stack: PostgreSQL, Debezium, Kafka, Spark, MinIO, Parquet, Apache Iceberg, Apache Polaris, Docker Compose.

Details, design decisions and how to run it: see the project folder, projects/reliable-postgres-cdc-lakehouse.

## Contact

LinkedIn: https://www.linkedin.com/in/eduardo-haberler-a97b8916a
