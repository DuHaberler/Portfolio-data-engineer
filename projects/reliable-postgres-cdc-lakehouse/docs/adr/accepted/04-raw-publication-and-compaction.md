# ADR O4 - Raw Publication and Compaction

Statuc: accepted

## Context

Raw CDC events are continuously written by Spark Structured Streaming to:

- "raw/landing/orders"
- "raw/landing/payments"
- "raw/landing/refunds"

The Raw Landing layer is append-only and partitioned by "ingestion_date_utc".

Countinuous streaming ingestion may generate multiple small parquet files over time. Dowstream processing should not depend directly on this physical file layout and should consume only validated raw datasets.

The following alternatives were considered:

1. Downstream consumers read directly from Raw Landing
2. Periodically overwrite each Raw Landing partition with compacted files
3. Create a separate Raw Compacted layer with versioned publication runs
4. Introduce a table format such as Iceberg directly in the raw layer

## Decision

Create a separate Raw Publication process between Raw Landing and downstream processing.
The job will process one partition per execution, using D-1 UTC by default.
The target partition may also be explicitly provided through "PARTITION_DATE" for testing, backfill or reprocessing.

Each execution will choose one of these operations:

- "DO COMPACTION":  will rewrite the partition into fewer Parquet files when the configured fragmentation policy is met.
- "NO_NEED":        will copy the existing Parquet files without rewriting their content when compaction is no required.

Each execution will write to a versioned path:

"raw/compacted/<dataset>/ingestion_date_utc=<date>/run-<run_id>/"

The generated run must pass an identity fingerprint validation before it can be considered published.

The Kafka event identity is defined by:

- "topic";
- "kafka_partition";
- "offset".

The published version will be identified through "published_path" in the execution control state.

## Rationale

Separating Raw Landing from Raw Compacted preserves the immutable ingestion history while allowing downstream consumers to use a more stable physical representation.

Raw Landing remains the source of truth for raw reprocessing.
The publication layer allows physical optimization without modifying or overwriting the original streaming output.
Using versioned "run_id" paths allows retries and new execution attempts without replacing previous physical outputs.

The "DO_COMPACTION" operation reduces file fragmentation when the configured policy determines that a rewrite is justified.
The "NO_NEED" operation avoids unnecessary Spark rewrites when the existing file organization is already acceptable.

Validation is required before publication so that the existence of output files alone does not make a run valid for dowstream consumption.

The fingerprint validation compares aggregated Kafka identities using:

- row count
- minimum offset
- maximum offset
- aggregated xxhash64

This provides an integrity check without requiring a full payload comparison between input and output.

## Consequences

### Positive

- Raw Landing remains immutable
- Downstream processing does not depend directly on streaming file fragmentation
- Small files can be reduced without modifying the original raw history
- Unnecessary rewrites are avoided through the "NO_NEED" operation
- Execution attempts are isolated through versioned "run_id" paths
- Failed attempts do not overwrite previous physical runs
- Publication requires explicit validation
- Kafka event identity is preserved and verified vefore publication
- Historical partitions can be explicitly reprocessed through "PARTITION_DATE"
- Raw publication state can be tracked independently from the physical existence of files

### Negative

- The same data may exist in both Raw Landing and Raw Compacted, increasing storage usage.
- Additional Spark processing is required for publication and validation
- Failed or superseded execution path may remain as orphan objects until a retention policy is introduced
- Fingerprint validation is probabilistic and dows not compare rhe complete CDC payload
- The current control mechanism does not provide an atomic distributed lock between concurrent executions
- Compaction thresholds may require adjustment as real workload characteristics become know

## Reconsideration

This decision should be revisited if:

- Raw Landing file fragmentation becomes negligible and the publication layer no longer provides sufficient value;
- Storage suplication between Raw Landing and Raw Compacted becomes significant;
- Downstream workloads require transactional table semantics;
- Iceberg or another table format becomes responsible for compaction and publication management;
- The number or size of the partitions makes the current fingerprint validation too expensive;
- Stronger integrity guarantees require exact comparison instead of aggregated fingerprints;
- Concurrent executions require distributed locking or transactional publication;
- Orchestration or storage lifecycle requirements significantly changes the current publication model.