# Reliable PostgreSQL CDC Lakehouse

A local, open-source data engineering project focused on building a reliable Change Data Capture (CDC) pipeline from PostgreSQL to a lakehouse architecture.

The project explores CDC, event streaming, durable raw storage, reprocessing, idempotency, analytical consolidation and failure recovery.

## Architecture

### Current architecture:

PostgreSQL 
->  Debezium 
->  Kafka Connect 
->  Kafka 
->  Spark Structured Streaming 
->  Raw Landing (Parquet / MinIO) 
->  Raw Publication
->  Identity Fingerprint Validation
->  Raw Compacted (Parquet / MinIO)

Spark Structured Streaming checkpoints and Raw Publication control state are persisted separately in MinIO.

### Planned analytical flow:

Raw Compacted 
->  Spark 
->  Iceberg 
->  Analytical layer

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

PostgreSQL logical replication exposes changes from:

- 'orders'
- 'payments'
- 'refunds'

Debezium consumes PostgreSQL WAL using:

- logical replication
- publication
- replication slot
- 'pgoutput'

Each source table is published to an independent Kafka topic.

Examples:

- 'cdc_debezium.public.orders'
- 'cdc_debezium.public.payments'
- 'cdc_debezium.public.refunds'

Each Kafka topic will be ingested independently by a Spark Structured Streaming aplication.

## Raw Layer

The raw preserves CDC events as an append-only historical event store.

It is divided into two physical areas:

- Raw Landing;
- Raw Compacted.

### Raw Landing

Raw Landing is the direct output of Spark Structured Streaming.

Path pattern: "raw/landing/<dataset>/ingestion_date_utc=<YYYY-MM-DD>/"

Raw Schema:

 --------------------------------------------------------------------
|Column             | Type             | Description                 |
|--------------------------------------------------------------------|        
|topic              | STRING           | Kafka Topic                 |    
|kafka_partition    | INT              | Kafka partition             |    
|offset             | LONG             | Kafka Offset                |    
|kafka_timestamp_utc| TIMESTAMP        | Kafka record TS in UTC      |    
|key_raw            | BINARY           | Original Kafka key          |     
|value_raw          | BINARY           | Original Kafka value        |     
|ingested_at_utc    | TIMESTAMP        | Lake ingestion TS in UTC    |
|ingested_date_utc  | DATE             | UTC ingestion partition date|
 --------------------------------------------------------------------

The original kafka key and value are intentionally preserved without parsing the Debezium payload during raw ingestion.

The tuple (topic, kafka_partition, offset) represents the techinical identity of a Kafka record and can be used downstream for deduplication and replay control.

## Raw Publication

Raw Landing partitions are published through a daily D-1 process.

A specific partition can also be selected explicitly for backfill or reprocessing.

The publication job evaluates the physical organization of the target partition and performs one of two operations:

- "DO_COMPACTION": rewrite the partition into fewer Parquet files.
- "NO_NEED": preserve the current Parquet file organization.

Each attempt writes to a versioned path identified by a unique "run_id".

Before publication, the generated output is validated using the Kafka record identity.

## Raw Compacted

Raw Compacted contains validated raw runs intended for downstram processing.

Path pattern: "raw/compacted/<dataset>/ingestion_date_utc=<YYYY-MM-DD>/run-<>run_id/"

A physical run is not automatically considered published because its files exist.

Only a successfully validated run referenced by "published_path" in the execution control state is considered published.

## Raw Storage Strategy

Raw CDC events are stored as Parquet files in MinIO.

The continuous Raw Landing ingestion path intentionally dows not use Iceberg.

The main reasons are:

- raw data is append-only;
- the layer represents events rather than current entity states;
- minimal transformation is desired;
- metadata and commit overhead are reduced in the continuous streaming path;
- Iceberg capabilities such as MERGE and table snapshots provide greater value in processed and analytical layers.

Spark Structured Streaming checkpoints are stored separately in MinIO.

Raw Publication execution state is also persisted separately from the datasets.

## Validation and Publication

Raw Publication validates that Kafka event identities are preserved between Raw Landing and the generated output.

Kafka record identity is defined by:

(topic, kafka_partition, offset)

The validation compares aggregated information including:

- row count;
- minimum offset;
- maximum offset;
- aggregated xxhash64 identity hash.

A run can only become the published version after successful validation.

Execution state is persisted as:

- RUNNING
- COMPLETED
- FAILED

Failed executions can be retried and abandoned RUNNING executions can be treated as stale after the configured timeout.

## Reliable Principles

The architecture is being designed around:

- durable CDC positions;
- Kafka Offsets;
- replayability;
- idempotent downstream processing;
- persistent streaming checkpoints;
- immutable Raw Landing events;
- versioned publication attempts;
- validation before publication;
- separation between compute and storage;
- deterministic reprocessing;
- isolate failure domains per CDC topic.

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
- Independent Spark application per CDC topic
- Raw Schema definition
- Continuous Kafka -> Spark -> Parquet Ingestion
- Persistent streaming checkpoints
- UTC ingestion partitioning
- Failure and restart validation
- Immutable Raw Landing storage
- Daily D-1 Raw Publication
- "DO-COMPACTION" / "NO_NEED" processing
- Versioned publication runs
- Kafka identity fingerprint validation
- Persistent publication control state
- Explicit partition processing for backfill and processing

### M3 Analytical Consolidation
Planned.

### M4 - Orchestration and Reprocessing
Planned.

Expected Scope:

- Airflow orchestration
- Scheduled Raw Publication
- Retry policies
- Backfill orchestration

### M5 - Reliability and Failure Scenarios
Planned.

### M6 - Documentation and Portfolio Representation
In progress.

- Architecture Overview
- Architecture Decision Records
- Operational runbooks
- Project README





