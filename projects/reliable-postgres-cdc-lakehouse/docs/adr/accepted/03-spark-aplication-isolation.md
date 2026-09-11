# ADR 03 - Run independent Spark Application per CDC Topic

Status: accepted

## Context

- 'cdc_debezium.public.orders'
- 'cdc_debezium.public.payments'
- 'cdc_debezium.public.refunds'

Each topic has its own ingestion flow, checkpoint, failure domain and raw storage destination.

The following alternatives were considered:

1. A single Spark application consuming all topics
2. Grouping related topics into fewer Spark applications
3. One independent Spark application per topic
4. Introducing a Spark Standalone cluster with master and worker nodes

## Decision

Run one independent Spark application per Kafka topic:

- 'spark-orders'
- 'spark-payments'
- 'spark-refunds'

All applications will reuse the same Spark Docker image and the same parameterized ingestion code.

A Spark Standalone cluster will not be introduceat this stage.

## Rationale

Independent applications provide clear isolation between ingestion pipelines while keeping the implementation simples.

Each application can have its own:

- Kafka topic
- checkpoint location
- raw storage path
- logs
- restart lifecycle
- resource configuration

Using the same parameterized code avoiuds duplicating implementation acress the three pipelines.

A Spark Standalone cluster was considered unnecessary for the current project scale because it would introduce additional scheduling and operational complexity without solving an immediate requirement.

## Consequences

### Positive

- Independent failure domains
- Independent checkpoints and recovery
- Easier troubleshooting and log analysis
- Individual pipelines can be restarted without affecting the others
- Same ingestion code can bu reused through configuration
- Future resource allocation can be adjusted per pipeline
- No additional Spark cluster-management components are required

### Negative

- Multiple Spark processes consume more baseline resources than a single application
- More containers must be operated and monitored
- Shared logic must remain properly parameterized to avoid configuration drift
- Cross-topic processing would require a separate application or downstream layer

## Reconsideration

This decision shoud be revisited if:

- the number of source topics grows significantly;
- resources utilization becomes inefficient;
- centralized Spark resource scheduling becomes necessary;
- workloads require dynamic executor allocation or distributed cluster execution;
- the deployment target moves to Kubernetes or a managed Spark platform.