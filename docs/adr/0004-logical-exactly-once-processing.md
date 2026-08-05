# ADR 0004: Logical exactly-once market processing

- Status: Accepted
- Date: 2026-08-05

## Context

Kafka delivery is at least once. A process can fail after PostgreSQL commits but
before the corresponding Kafka offset commits, and object storage cannot join a
PostgreSQL transaction. Treating either external commit as atomic would hide the
failure mode rather than solve it.

## Decision

- Each partition has a PostgreSQL checkpoint containing its next expected
  offset. The market-price row, archive-outbox row, and checkpoint update commit
  in one database transaction.
- A redelivered offset below the database checkpoint is a successful no-op. An
  offset above it is a gap and stops the consumer instead of silently losing
  data. Kafka offsets commit only after the database transaction succeeds.
- The market-price logical key is `(provider, symbol, trading_date)`. Reuse with
  identical content is a no-op; conflicting content is preserved in the DLQ.
- Invalid events use a deterministic DLQ key of `topic:partition:offset`, retain
  the original bytes, and advance the source checkpoint only after the DLQ
  broker acknowledges the write.
- Parquet archival uses a transactional outbox. Workers claim rows with leases,
  write deterministic S3 object keys, and mark completion afterward. A crash
  after the object write safely overwrites the same key on retry.

## Consequences

The query model and archive converge to one logical record despite redelivery,
consumer restarts, and archiver retries. The system does not claim distributed
exactly-once delivery: duplicate DLQ publications remain possible, but their
keys are deterministic. The first archive version writes one Parquet object per
event for simple failure semantics; a measured compaction stage is deferred to
M5 to address the small-file tradeoff.
