# ADR 0001: System boundaries and storage ownership

- Status: Accepted
- Date: 2026-08-04

## Context

The platform needs transactional product behavior, replayable market-data
ingestion, and analytical history. Using one database for all three concerns
would simplify the first demo but obscure failure boundaries and make immutable
history harder to reproduce.

## Decision

- PostgreSQL is the system of record for watchlists, research notes, paper
  accounts, ledger entries, positions, idempotency keys, and consumer
  checkpoints.
- Kafka transports versioned market events. It is not the permanent system of
  record and topic retention is finite.
- Parquet objects in S3-compatible storage form the immutable analytical archive.
- The API never writes directly to Kafka for accounting operations. Paper trades
  are committed transactionally in PostgreSQL.
- The dashboard talks only to the API, never directly to infrastructure.

## Consequences

Consumers must be idempotent because message delivery and process restarts can
repeat work. Product availability can be reasoned about independently from
market-data ingestion. Local development requires several containers, so the
project provides health checks and a single Compose entry point.
