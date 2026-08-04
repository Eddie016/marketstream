# ADR 0002: Versioned snapshots before live data

- Status: Accepted
- Date: 2026-08-04

## Context

Live APIs change, enforce quotas, and make tests nondeterministic. A portfolio
project also needs reviewers to reproduce a demo without obtaining paid market
data.

## Decision

The initial ingestion path downloads end-of-day OHLCV into a versioned snapshot.
A committed manifest records provider, requested symbols, date interval, schema
version, file checksums, and retrieval metadata. CI uses a small redistributable
fixture. Kafka receives events through a deterministic replay command rather
than directly from the provider.

## Consequences

Development and failure tests are repeatable and do not consume API quota.
Market freshness is intentionally delayed. A later incremental importer can be
added without changing the canonical event contract.
