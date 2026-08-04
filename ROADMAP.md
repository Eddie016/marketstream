# Roadmap

Every milestone is complete only when its acceptance checks are automated or
captured as reproducible evidence. Planned features are not described as shipped
features in the resume or project README.

## M0 — Foundation

- [x] Independent repository boundary and product scope
- [x] Architecture decision records and initial data ownership model
- [x] FastAPI service with separate liveness and readiness contracts
- [x] Docker Compose definitions for PostgreSQL, Kafka, MinIO, and API
- [x] Formatting, linting, type-checking, tests, and GitHub Actions
- [ ] Full Compose smoke test on a running Docker engine
- [ ] GitHub remote and branch-protection settings

## M1 — Reproducible data

- [ ] Versioned source manifest with SHA-256 checksums
- [ ] Historical OHLCV downloader with validation and rate-limit handling
- [ ] Committed deterministic CI fixture
- [ ] Protobuf market-event schema with explicit schema version
- [ ] Replay CLI supporting fixed ordering, speed, pause, and resume

Acceptance: the same manifest produces the same ordered event identifiers on
two clean runs; CI succeeds without network access.

## M2 — Reliable stream processing

- [ ] Kafka topics partitioned by ticker
- [ ] Idempotent consumer keyed by source, ticker, and trading date
- [ ] Transactional checkpoint and query-model update
- [ ] Parquet archive partitioned for analytical reads
- [ ] Bounded retry and dead-letter topic

Acceptance: replaying a snapshot twice does not change logical row counts;
terminating a consumer mid-batch and restarting it yields the same final state.

## M3 — Research API

- [ ] Watchlists and research notes
- [ ] Paper accounts, orders, executions, positions, and cash ledger
- [ ] Portfolio snapshots, return, volatility, drawdown, and SPY comparison
- [ ] Company event timeline and SEC filing links
- [ ] Idempotency protection for mutation requests

Acceptance: an end-to-end API test creates a watchlist, records a paper trade,
values the portfolio, and preserves accounting invariants under duplicate calls.

## M4 — Demonstrable product

- [ ] Overview, portfolio, research, and system-status screens
- [ ] Loading, empty, stale-data, and failure states
- [ ] Accessible responsive UI
- [ ] One-command seeded demo

Acceptance: a new user can complete the primary workflow from a clean clone
without manually editing the database or Kafka topics.

## M5 — Engineering evidence

- [ ] Structured logs, correlation IDs, Prometheus metrics, Grafana dashboard
- [ ] Unit, integration, end-to-end, restart, and failure-injection tests
- [ ] Throughput, latency, consumer-lag, and recovery benchmark
- [ ] Architecture case study and 90-second demo recording
- [ ] Tagged `v0.1.0-demo` release

## M6 — Real-use iteration

- [ ] Maintain a personal paper portfolio and research journal for 8–12 weeks
- [ ] Convert observed friction into dated GitHub issues
- [ ] Ship at least four user-driven improvements with changelog entries
- [ ] Publish an honest retrospective covering decisions and limitations
