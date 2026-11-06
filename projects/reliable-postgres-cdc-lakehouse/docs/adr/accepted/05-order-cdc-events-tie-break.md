# ADR 05 - Order CDC events by source LSN with Kafka offset tie-break

Status: accepted

## Context

ADR 01 guarantees ordering only within a Kafka partition, and the pipeline is at-least-once.
orders_current must deterministically select the latest event per id_order, including on replay and reprocessing.

The following alternatives were considered:

1. Debezium ts_ms
2. Kafka offset only
3. PostgreSQL source LSN (source.lsn)

## Decision

Consolidate orders_current per id_order using max_by with the composite ordering key (source_lsn, offset).

Delete events (op = 'd') are kept in orders_current using the 'before' image. Tombstones are filtered out before parsing.

## Rationale

- source_lsn reflects the commit order in the PostgreSQL WAL, independent of Kafka delivery;
- ts_ms has millisecond resolution and can collide or drift;
- offset alone does not survive reprocessing across different publications;
- without a tie-break, max_by is non-deterministic when the LSN ties (e.g., redelivered events);
- the Debezium message key is the primary key, so each id_order stays in a single partition and offsets are comparable.

## Consequences

### Positive

- Deterministic orders_current across reruns
- Redelivered duplicates resolved without failing the pipeline
- Uses technical metadata already preserved by the parser

### Negative

- Depends on a PostgreSQL-specific field
- Correctness of the tie-break depends on the partitioning premise
- Deletes remain as rows, so consumers must filter cdc_operation = 'd'

## Validation

tests/test_orders_current.py: LSN prevails over offset; offset breaks LSN ties; 20 shuffled and repartitioned runs return the same result.
The failure of the previous implementation (LSN only) was observed but not recorded.

## Reconsideration

This decision should be revisited if:

- the topic is repartitioned or the message key changes;
- the source database changes;
- the same entity is produced by multiple sources.