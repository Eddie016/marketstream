# M2 local acceptance evidence

- Date: 2026-08-05
- Environment: Docker Desktop, PostgreSQL 17.6, Kafka 4.3.1, MinIO
- Dataset: 8,813 Plotly snapshot events plus 6 committed synthetic fixture events

## Restart recovery

The consumer was forcibly restarted after 3,133 of 8,819 unique events had
committed. It recovered from the persisted PostgreSQL checkpoints and converged
to 8,819 unique rows. Final Kafka lag was zero on all non-empty partitions:

```text
partition 1: next_offset=5039, lag=0
partition 2: next_offset=2518, lag=0
partition 3: next_offset=1262, lag=0
```

This run also recovered from an observed failure after the first PostgreSQL
transaction committed but before its Kafka offset committed. Offset 0 was
redelivered and treated as a no-op before processing continued.

## Duplicate replay

The complete 8,813-event snapshot was published a second time without a replay
checkpoint. Kafka accepted all 8,813 additional records. After consumer lag
returned to zero, both `market_prices` and `archive_outbox` remained at 8,819
rows, with 8,819 distinct event IDs.

## Archive and DLQ

MinIO contained 8,819 readable Parquet objects under deterministic paths such
as:

```text
market-prices/provider=plotly-github/symbol=AAPL/year=2013/
date=2013-02-08/3c7949458f443dc494e032ca142226b14f6a53e66f9b4c543e22377a032e22b0.parquet
```

A non-Protobuf payload injected at source partition 1, offset 10075 produced a
DLQ record keyed `market-prices-v1:1:10075`. The DLQ retained the original
payload, the source checkpoint advanced to 10076, and the logical price count
remained 8,819.

Run the read-only convergence verifier against a started stack with:

```bash
make verify-m2
```
