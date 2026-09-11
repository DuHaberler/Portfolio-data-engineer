# ADR 02 - Store Raw CDC Events as Parquet

Status: accepted

## Context

The raw ingestion layer receives CDC events continuously from Kafka.

The primary responsability of this layer is to preserve the original events with minimal transformation and allow downstream replay and reprocessing.

Two alternatives were considered:

1. Store raw events as Iceberg tables.
2. Store raw events directly as Parquet files.

## Decision

The raw layer will use append-only Parquet files stored in MinIO.

Iceberg will not be used in the raw streaming ingestion path.

Kafka metadata will be persisted together with the original record:

- topic
- partition
- offset
- timestamp
- key
- value

Spark Structured Streaming checkpoints will be stored separately.

## Rationale

The raw layer represents an immutable event history rather than the current state of business entities.

Capabilities such as MERGE, snapshot-based table state, and transactional updates provide limited value in this layer.

Using Parquet directly:

- simplifies the conntinuous ingestion path;
- avoids frequent Iceberg commits and snapshots;
- reduces metadata management;
- preserves the original events;
- keeps Iceberg complexity for layers where table-state semantics are needed.

## Consequences

### Positive

- simpler ingestion atchitecture;
- fewer components in the critical streaming path;
- no Iceberg snapshot proliferation caused by frequent microbatches;
- raw events remain format-independent from downstream business logic.

### Negative

- no native Iceberg time treavel in the raw layer;
- no transactional table abstraction;
- schema and file organization must be managed by the ingestion pipeline;
- compaction and small-file management require separate strategies.

## Reconsideration

This decision may be revisited if the raw layer later requires:

- transactional table semantics;
- extensive schema evolution;
- direct analytical consuption;
- snapshot-based recovery or auditing.