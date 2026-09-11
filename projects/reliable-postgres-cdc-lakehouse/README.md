# Reliable PostgreSQL CDC Lakehouse

A local, open-source data engineering project focused on building a reliable Change Data Capture (CDC) pipeline from PostgreSQL to a lakehouse architecture.

The project explores CDC, event streaming, durable raw storage, reprocessing, idempotency, analytical consolidation and failure recovery.

## Architecture

### Current architecture:

PostgreSQL -> Debezium -> Kafka Connect -> Kafka -> Spark Structured Streaming -> Parquet -> MinIO

### Planned analytical flow:

Raw parquet -> Spark -> Iceberg -> Analytical layer

## Technology Stack

- PostgreSQL 17
- Debezium 3.5
- Apache Kafka 4.x
- Kafka Connect
- Apache Spark 4.1
- MinIO
- Apache Parquet
- Apache Iceberg (planned for analytical tables)
- Docker Compose
- Airflow (planned for orchestration)


## CDC Flow

PostgreSQL logical replication exposes changesd from:

- 'orders'
- 'payments'
- 'refunds'

Debezium consumes PostgreSQL WAL using

- logical replication
- publication
- replication slot
- 'pgoutput'

Each source table is published to an independent Kafka topic.

Examples:

- 'cdc_debezium.public.orders'
- 'cdc_debezium.public.payments'
- 'cdc_debezium.public.refunds'

## Raw Layer

The raw layer is designed as an append-only historical event store.

Each Kafka topic will be ingested  independently by a Spark Structured Streaming aplication.

Raw Schema:

 -------------------------------------------------------------
|Column            | Type             | Description           |
|-------------------------------------------------------------|        
|topic             | STRING           | Kafka Topic           |    
|kafka_partition   | INT              | Kafka partition       |    
|offset            | LONG             | Kafka Offset          |    
|kafka_timestamp   | TIMESTAMP        | Kafka record ts       |    
|key_raw           | BINARY           | Original Kafka key    |     
|value_raw         | BINARY           | Original Kafka value  |     
|ingested_at       | TIMESTAMP        | Lake ingestion ts     |
 -------------------------------------------------------------

The original kafka key and value are intentionally preserved without parsing the Debezium payload at ingested time.

The tuple (topic, kafka_partition, offset) represents the techinical identity of a Kafka record and can be used downstream for deduplication and replay control.

## Raw Storage Strategy

Raw CDC events are stored as Parquet files in MinIO.

The raw ingestion path intentionally dows not use Iceberg.

The main reasons are:

- raw data is append-only;
- the layer represents events rather than current entity states;
- minimal transformation is desired;
- reducing metadata and commit overhead in the continuous streaming path;
- Iceberg capabilities such as MERGE and table snapshots provide greater value in processed and analytical layers.

Spark Structured Streaming checkpoints are stored separately in MinIO.

## Reliable Principles

The architecture is being designed around:

- durable CDC positions;
- Kafka Offsets;
- replayability;
- idempotent downstream processing;
- persistent streaming checkpoints;
- immutable raw events;
- separation between compute and storage;
- deterministic reprocessing.

## Project Status

### M0 - Foundation
Completed.

- Repository structure
- Docker Compose foundation
- PostgreSQL
- Kafka
- MinIO
- Spark base image

### M1 - CDC
Completed.

- PostgreSQL logical replication
- Debezium connector
- Kafka Connect
- Initial snapshot
- INSERT / UPDATE / DELETE validation
- Tombstone validation

### M2 - Historical Raw Layer
In progress.

Completed:

- Spark <-> Kafka integration
- Spark <-> MinIO integration
- raw schema definition
- raw storage strategy

Next:

- 'Kafka -> Spark -> Parquet' streaming job
- persistent checkpoint
- partition strategy
- failure/restart validation

### M3 Analytical Consolidation
Planned.

### M4 - Orchestration and Reprocessing
Planned.

### M5 - Reliability and Failure Scenarios
Planned.

### M6 - Documentation and Portfolio Representation
Planned.





