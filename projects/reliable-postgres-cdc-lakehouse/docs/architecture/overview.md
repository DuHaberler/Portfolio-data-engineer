# Architecture Overview

## Data Flow

PostgreSQL -(WAL)-> Debezium PostgreSQL Connector -> Kafka Connect -> Kafka Topic -> Spark Structured Streaming -> Raw Parquet -> MinIO

Future:

Raw Layer -> Processing / Consolidation -> Iceberg Tables -> Analytical Consumers

## Component Responsabilities

### PostgreSQL

Transactional source system.

Responsabilities:
- persist business state;
- generate WAL;
- expose logical replication.

CDC Tables:

- orders
- payments
- refunds


### Debezium

CDC capture layer

Responsabilities:
- consume PostgreSQL logical replication;
- convert database changes into CDC events;
- maintain source position;
- publish events through Kafka Connect.

### Kafka

Transport and durable event buffer.

Responsabilities:
- decouple source capture from downstream processing;
- retain CDC events;
- preserve ordering within partitions;
- support replay through offsets.

### Spark Structured Streaming

Raw ingestion compute layer.

Responsabilities:

- consume Kafka topics;
- preserve Kafka metadata;
- preserve original key/value bytes;
- add ingestion metadata;
- persist raw events.

### MinIO

Object storage layer

Responsabilities:

- persist raw Parquet files;
- persist Spark checkpoints;
- provide an s3-compatible storage interface

### Iceberg

Table format for processed and analytical datasets

Responsabilities:

- transactional table commits;
- snapshot management;
- schema evolution;
- MERGE operations;
- analytical state management.