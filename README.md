# MarketStream

MarketStream is a reproducible personal equity-research platform. It replays a
versioned historical market-data snapshot through Kafka, builds idempotent
query models and Parquet archives, and exposes watchlist and paper-portfolio
workflows through an API and web dashboard.

The project has two goals:

1. Demonstrate production-minded backend and data-platform engineering.
2. Become a research journal that improves through sustained personal use.

MarketStream is an educational research tool. It does not provide investment
advice, predict future returns, or place real trades.

## Status

Milestone 0 is in progress. The repository currently provides the service
contract, architecture decisions, local infrastructure, API health endpoint,
and CI quality gates. See [ROADMAP.md](ROADMAP.md) for acceptance criteria.

## Architecture

```text
versioned OHLCV snapshot
          |
          v
deterministic replay producer ---> Kafka ---> idempotent consumer
                                                |          |
                                                v          v
                                           PostgreSQL   Parquet/MinIO
                                                |
                                                v
                                         FastAPI backend
                                                |
                                                v
                                         research dashboard
```

PostgreSQL owns transactional product state such as watchlists, research notes,
paper orders, positions, and replay checkpoints. Parquet in S3-compatible object
storage is the immutable analytical archive. Kafka decouples deterministic
ingestion from storage and makes failure/recovery behavior observable.

## Quick start

Prerequisites: Docker with Compose v2 and `make`.

```bash
cp .env.example .env
make up
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
```

OpenAPI documentation is available at <http://localhost:8000/docs>.

Stop the stack without deleting data:

```bash
make down
```

## Local development

Python 3.11+ is supported.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pip-tools==7.5.0
make install-dev
make check
```

## Repository layout

```text
src/marketstream/     FastAPI application and domain code
tests/                unit and API contract tests
docs/adr/             architecture decision records
infra/                local infrastructure configuration
.github/workflows/    continuous integration
```

## Reproducibility policy

- Source data is identified by provider, symbol set, date range, schema version,
  and SHA-256 checksums in a committed manifest.
- CI uses a small committed fixture and never depends on a live market-data API.
- Replay ordering and event identifiers are deterministic.
- Performance numbers are published only with the dataset, command, hardware,
  and configuration used to measure them.

## License

Code in this independent repository is released under the MIT License. Market
data remains subject to the terms of its original provider and is not
redistributed by this repository.
