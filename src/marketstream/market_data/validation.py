import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

CANONICAL_COLUMNS = (
    "symbol",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "currency",
)


class MarketDataValidationError(ValueError):
    """Raised when provider data violates the canonical OHLCV contract."""


@dataclass(frozen=True, slots=True)
class MarketRow:
    symbol: str
    trading_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    currency: str

    def as_csv_row(self) -> dict[str, str | int]:
        return {
            "symbol": self.symbol,
            "trading_date": self.trading_date.isoformat(),
            "open": canonical_decimal(self.open),
            "high": canonical_decimal(self.high),
            "low": canonical_decimal(self.low),
            "close": canonical_decimal(self.close),
            "volume": self.volume,
            "currency": self.currency,
        }


def canonical_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def parse_provider_csv(payload: bytes, symbol: str, currency: str) -> list[MarketRow]:
    try:
        decoded = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise MarketDataValidationError("provider response is not UTF-8") from error

    reader = csv.DictReader(io.StringIO(decoded))
    if reader.fieldnames is None:
        raise MarketDataValidationError("provider response has no CSV header")
    field_map = {name.strip().lower(): name for name in reader.fieldnames}
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(field_map):
        missing = ", ".join(sorted(required - set(field_map)))
        raise MarketDataValidationError(f"provider response is missing: {missing}")

    raw_rows = list(reader)
    if "name" in field_map:
        raw_rows = [
            raw
            for raw in raw_rows
            if raw[field_map["name"]].strip().upper() == symbol.upper()
        ]
    rows = [
        _parse_row(raw, field_map, symbol.upper(), currency.upper(), line_number)
        for line_number, raw in enumerate(raw_rows, start=2)
    ]
    return validate_rows(rows, symbol)


def read_canonical_csv(path: Path) -> list[MarketRow]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != CANONICAL_COLUMNS:
            raise MarketDataValidationError(f"{path} has a non-canonical header")
        rows = [
            _parse_canonical_row(raw, line_number)
            for line_number, raw in enumerate(reader, start=2)
        ]
    expected_symbol = rows[0].symbol if rows else path.stem
    return validate_rows(rows, expected_symbol)


def canonical_csv_bytes(rows: list[MarketRow]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CANONICAL_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(row.as_csv_row() for row in rows)
    return output.getvalue().encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_rows(rows: list[MarketRow], expected_symbol: str) -> list[MarketRow]:
    if not rows:
        raise MarketDataValidationError(f"no rows returned for {expected_symbol}")
    ordered = sorted(rows, key=lambda row: row.trading_date)
    seen_dates: set[date] = set()
    for row in ordered:
        if row.symbol != expected_symbol.upper():
            raise MarketDataValidationError("a file contains multiple symbols")
        if row.trading_date in seen_dates:
            raise MarketDataValidationError(
                f"duplicate {row.symbol} date: {row.trading_date}"
            )
        seen_dates.add(row.trading_date)
        if min(row.open, row.high, row.low, row.close) <= 0:
            raise MarketDataValidationError("prices must be positive")
        if row.low > row.high:
            raise MarketDataValidationError("low must not exceed high")
        if row.low > min(row.open, row.close) or row.high < max(row.open, row.close):
            raise MarketDataValidationError("OHLC values violate the daily range")
        if row.volume < 0:
            raise MarketDataValidationError("volume must be non-negative")
    return ordered


def _parse_row(
    raw: dict[str, str],
    field_map: dict[str, str],
    symbol: str,
    currency: str,
    line_number: int,
) -> MarketRow:
    try:
        return MarketRow(
            symbol=symbol,
            trading_date=date.fromisoformat(raw[field_map["date"]]),
            open=Decimal(raw[field_map["open"]]),
            high=Decimal(raw[field_map["high"]]),
            low=Decimal(raw[field_map["low"]]),
            close=Decimal(raw[field_map["close"]]),
            volume=int(raw[field_map["volume"]]),
            currency=currency,
        )
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise MarketDataValidationError(
            f"invalid provider row {line_number}"
        ) from error


def _parse_canonical_row(raw: dict[str, str], line_number: int) -> MarketRow:
    try:
        return MarketRow(
            symbol=raw["symbol"],
            trading_date=date.fromisoformat(raw["trading_date"]),
            open=Decimal(raw["open"]),
            high=Decimal(raw["high"]),
            low=Decimal(raw["low"]),
            close=Decimal(raw["close"]),
            volume=int(raw["volume"]),
            currency=raw["currency"],
        )
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise MarketDataValidationError(
            f"invalid canonical row {line_number}"
        ) from error
