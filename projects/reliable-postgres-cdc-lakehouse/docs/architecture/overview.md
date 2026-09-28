# Architecture Overview

## Data Flow

PostgreSQL
    |
    | Logical Replication / WAL
    V
Debezium PostgreSQL Connector
    |
    | Kafka Connect Runtime
    V
Kafka Topics
    |
    V
Spark Structured Streaming
    |
    +--> Streaming Checkpoints
    |               |
    |               V
    |             MinIO
    V
Raw Landing (Parquet / MinIO)
    |
    | Daily D-1 Publication
    V
Raw Publication Job
    |
    +--> DO_COMPACTION
    |
    +--> NO_NEED
    |
    V
Identity Fingerprint Validation
    |
    V
Raw Compacted (Parquet / MinIO)
    |
    V
Analytical / Data Science / Future Processing - Layers


## Component Responsabilities

### PostgreSQL

Transactional source system.

Responsabilities:
- persist business state;
- generate WAL;
- expose logical replication.

CDC Tables:

- "orders"
- "payments"
- "refunds"

### Debezium PostgreSQL Connector

CDC capture component between PostgreSQL and Kafka.

Responsabilities:

- consume PostgreSQL logical replication;
- decode changes from WAL;
- converst database changes into CDC events;
- preserve source metadata required for change tracking;
- send CDC events through Kafka Connect.

### Kafka Connect

Runtime responsible for executing the Debezium connector.

Responsabilities:
- run and manage the Debezium PostgreSQL Connector;
- manage connector tasks;
- persist connector offsets;
- recover CDC consumption after restarts;
- publish CDC events to Kafka topics.

### Kafka

Transport and durable event buffer.

Responsabilities:
- decouple source capture from downstream processing;
- retain CDC events according to configured retention;
- preserve ordering within partitions;
- support replay through offsets while events are retained;
- provide the event stream consumed by Spark Structured Streaming.

### Spark Structured Streaming

Continuous ingestion layer between Kafka and the raw historical storage

Responsabilities:

- consume CDC events from Kafka.
- preserve Kafka metadata required for traceability;
- persist events continuously into Raw Landing;
- maintain streaming checkpoints in MinIO;
- recover ingestion progress after start;
- isolate ingestion processing by dataset/topic.


### Raw Landing

Immutable historical entry point for CDC events.

Storage:

- Parquet;
- MinIO;
- partitioned by ingestion_date.

Path pattern:

"raw/landing/<dataset>/ingestion_date_utc=<YYYY-MM-DD>/"

Responsabilities:

- persist the raw CDC events stream;
- preserve Kafka events identity;
- retain historical events for reprocessing;
- act as source of truth for raw reprocessing;
- provide input for the Raw Publication Job.

Kafka event identity:

- "topic"
- "kafka_partititon"
- "offset"

Raw Landing is not intended to be consumed directly by downstream analytical processing.

### Raw Publication Job

Daily process responsible for producing a validated raw representation from Raw Landing.

Default processing window:

- D-1 UTC

Possible operations:

- "DO_COMPACTION": rewrite the partition into fewer Parquet files when the compaction policy is met.
- "NO_NEED": preserve the existing physical organization when compaction is not required.

Responsabilities:

- read the target Raw Landing partition;
- evaluate file fragmentation;
- determine whether compaction is required;
- create a versioned execution path;
- execute "DO_COMPACTION" or "NO_NEED";
- submit the generated output to validation;
- persist execution state and metrics.

Execution states:

- "RUNNING"
- "COMPLETED"
- "FAILED"

A "FAILED" execution may be retried.

A "RUNNING" execution older than the configured stale timeout may be treated as abandoned and retried.

If the target partition does not exist or contains no Parquet files, the execution is completed with no published output.

### Identity Fingerprint Validation

Validation mechanism executed before a raw run can be published.

Identity:

- "topic"
- "kafka_partititon"
- "offset"

Validation compares aggregated information between input and output:

- row count;
- minimum offset;
- maximum offset;
- aggregated xxhash64 identity hash.

Responsabilities:

- verify that Kafka event identities are preserved;
- detect differences between input and output;
- prevent publication when validation fails.

Only a run that successfully passes validation can become the published version.

### Raw Compacted

Validated raw representation used as the stable input for downstream processing.

Storage:

- Parquet;
- MinIO;
- partitioned by "ingestion_date_utc";
- versioned by "run_id".

Path pattern:

"raw/compacted/<dataset>/ingestion_date_utc=<YYYY-MM-DD>/run-<run_id>"

Responsabilities:


- provide validated raw data forr downstream processing;
- reduce file fragmentation when compaction is required, preventing the small file problem;
- preserve the event semantics of Raw Landing;
- isolate execution attempts through versioned paths;
- support retries without overwriting previous runs.

The physical existence of a "run-*" path does not mean that the run is published.

The published version is the validated run referenced by "published_path" in the execution control state.

### MinIO

S3-compatible object storage used by the local lakehouse environment.

Responsabilities:

- store Raw Landing Parquet files;
- store Raw Compacted Parquet files;
- persist Spark Structured Streaming checkpoints;
- persist Raw Publication Job control state;
- provide S3-compatible storage access through S3A.


### Analytical / Data Science / Future Processing - Layers

Downstream processing built from validated Raw Compacted datasets.

Responsabilities:

- consume published raw datasets;
- apply business and analytical transformations;
- build consolidated data models;
- prepare datasets for analytical consumptions;
- support Data Science and BI workloads.

The detailed design of these layers is deferred to later project milestones.