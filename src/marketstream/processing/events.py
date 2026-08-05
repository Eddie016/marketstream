import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from google.protobuf.message import DecodeError

from marketstream.proto.market_event_pb2 import MarketEvent


class InvalidMarketEvent(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ValidatedMarketEvent:
    event_id: str
    schema_version: int
    provider: str
    symbol: str
    trading_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    currency: str
    snapshot_id: str


def decode_event(payload: bytes) -> ValidatedMarketEvent:
    message = MarketEvent()
    try:
        message.ParseFromString(payload)
    except DecodeError as error:
        raise InvalidMarketEvent("payload is not valid MarketEvent protobuf") from error
    try:
        values = ValidatedMarketEvent(
            event_id=message.event_id,
            schema_version=message.schema_version,
            provider=message.provider,
            symbol=message.symbol,
            trading_date=date.fromisoformat(message.trading_date),
            open=Decimal(message.open),
            high=Decimal(message.high),
            low=Decimal(message.low),
            close=Decimal(message.close),
            volume=message.volume,
            currency=message.currency,
            snapshot_id=message.snapshot_id,
        )
    except (InvalidOperation, ValueError) as error:
        raise InvalidMarketEvent("event contains an invalid date or decimal") from error
    required = (
        values.event_id,
        values.provider,
        values.symbol,
        values.currency,
        values.snapshot_id,
    )
    if not all(required) or values.schema_version < 1:
        raise InvalidMarketEvent("event is missing required identity fields")
    identity = (
        f"{values.schema_version}|{values.provider}|{values.symbol}|"
        f"{values.trading_date.isoformat()}"
    )
    expected_event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    if values.event_id != expected_event_id:
        raise InvalidMarketEvent("event_id does not match the logical event identity")
    if values.currency != values.currency.upper() or len(values.currency) != 3:
        raise InvalidMarketEvent("currency must be a three-letter uppercase code")
    if values.symbol != values.symbol.upper():
        raise InvalidMarketEvent("symbol must be uppercase")
    if (
        values.volume < 0
        or min(values.open, values.high, values.low, values.close) <= 0
    ):
        raise InvalidMarketEvent("OHLC prices must be positive and volume non-negative")
    if values.low > min(values.open, values.close) or values.high < max(
        values.open, values.close
    ):
        raise InvalidMarketEvent("OHLC bounds are inconsistent")
    return values
