# Third-party data

## Plotly example datasets

- Repository: <https://github.com/plotly/datasets>
- File: `all_stocks_5yr.csv`
- Repository license: MIT
- Used for: reproducible historical OHLCV bootstrap

MarketStream downloads the source file at setup time, selects the symbols and
date interval declared in the request, canonicalizes the rows, and records
SHA-256 checksums. The source CSV is not copied into this repository.

The public baseline intentionally ends in 2018. It demonstrates ingestion and
replay; it is not suitable for current investment decisions. A separately
configured provider will be required for ongoing personal use.
