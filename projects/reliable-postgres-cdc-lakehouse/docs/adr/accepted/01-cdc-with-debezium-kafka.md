# ADR 01 - Use Debezium and Kafka for PostgreSQL CDC

Status: accepted

## Context

The project requires capturing changes from the 'orders', 'payments' and 'refunds' tables with low latency while preserving event order and supporting replay.

The following alternatives were considered:

1. Periodic polling using 'updated_at'
2. Outbox pattern with polling
3. PostgreSQL logical replication with Debezium and Kafka

## Decision

Use PostgreSQL logical replication with Debezium, Kafka Connect and Kafka.

Debezium consumes changes from the PostgreSQL WAL through a logical replication slot and publishes CDC events to Kafka topics.

## Rationale

This approach was selected because it provides:

- change capture directly from the PostgreSQL WAL;
- lower source-system query overhead compared with frequent polling;
- low-latency event propagation;
- durable source-position tracking through a replication slot;
- event retention and replay through Kafka offsets;
- decoupling between change capture and downstream consumers;
- ordering guarantees within each Kafka partition.

## Consequences

### Positive

- Reduced read pressure on transactional tables
- Replay capability through Kafka offsets
- Decoupled producers and consumers
- Support for multiple downstream events
- Preservation of database change events
- Lower latency than periodic polling approaches

### Negative

- Additional operational components: Kafka, Kafka Connect and Debezium
- PostgreSQL replication slots require monitoring to avoids excessive WAL retention
- Kafka and Kafka Connect require persistence and operational monitoring
- Downstream consumers must account for retries, replay and idempotency
- Ordering is guaranteed only within a Kafka partition

## Reconsideration

This decision should be revisited if:

- the use case only requires periodic snapshots;
- near-real-time propagation is no longer required;
- operational simplicity becomes more important than replayability and low latency;
- the data volume and business requirements no longer justify a CDC architecture.