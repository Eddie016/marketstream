from decimal import Decimal
from pathlib import Path

import pytest

from marketstream.market_data.validation import (
    MarketDataValidationError,
    canonical_csv_bytes,
    parse_provider_csv,
    read_canonical_csv,
    sha256_bytes,
    sha256_file,
)

PROVIDER_CSV = b"""Date,Open,High,Low,Close,Volume
2024-01-03,184.22,185.88,183.43,184.25,58414500
2024-01-02,185.64,188.44,183.89,185.64,82488700
"""


def test_provider_rows_are_validated_sorted_and_canonicalized(tmp_path: Path) -> None:
    rows = parse_provider_csv(PROVIDER_CSV, "aapl", "usd")

    assert [row.trading_date.isoformat() for row in rows] == [
        "2024-01-02",
        "2024-01-03",
    ]
    assert rows[0].symbol == "AAPL"
    assert rows[0].open == Decimal("185.64")

    payload = canonical_csv_bytes(rows)
    path = tmp_path / "AAPL.csv"
    path.write_bytes(payload)

    assert read_canonical_csv(path) == rows
    assert sha256_file(path) == sha256_bytes(payload)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "no CSV header"),
        (b"Date,Open\n2024-01-02,1\n", "missing"),
        (
            b"Date,Open,High,Low,Close,Volume\n2024-01-02,10,9,8,10,1\n",
            "daily range",
        ),
        (
            b"Date,Open,High,Low,Close,Volume\n2024-01-02,10,9,11,10,1\n",
            "low must not exceed high",
        ),
        (
            b"Date,Open,High,Low,Close,Volume\n2024-01-02,0,9,0,8,1\n",
            "prices must be positive",
        ),
        (
            b"Date,Open,High,Low,Close,Volume\n2024-01-02,10,11,9,10,-1\n",
            "non-negative",
        ),
        (
            b"Date,Open,High,Low,Close,Volume\n2024-01-02,nope,11,9,10,1\n",
            "invalid provider row",
        ),
    ],
)
def test_invalid_provider_data_is_rejected(payload: bytes, message: str) -> None:
    with pytest.raises(MarketDataValidationError, match=message):
        parse_provider_csv(payload, "AAPL", "USD")


def test_duplicate_dates_are_rejected() -> None:
    payload = PROVIDER_CSV + b"2024-01-02,185,188,183,186,1\n"

    with pytest.raises(MarketDataValidationError, match="duplicate"):
        parse_provider_csv(payload, "AAPL", "USD")


def test_multi_symbol_provider_file_is_filtered() -> None:
    payload = b"""date,open,high,low,close,volume,Name
2024-01-02,10,12,9,11,100,AAPL
2024-01-02,20,22,19,21,200,SPY
"""

    rows = parse_provider_csv(payload, "SPY", "USD")

    assert len(rows) == 1
    assert rows[0].symbol == "SPY"
    assert rows[0].close == Decimal("21")


def test_noncanonical_file_header_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("date,close\n2024-01-02,1\n", encoding="utf-8")

    with pytest.raises(MarketDataValidationError, match="non-canonical"):
        read_canonical_csv(path)
