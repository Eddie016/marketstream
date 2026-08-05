from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MarketEvent(_message.Message):
    __slots__ = ("event_id", "schema_version", "provider", "symbol", "trading_date", "open", "high", "low", "close", "volume", "currency", "snapshot_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    TRADING_DATE_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    schema_version: int
    provider: str
    symbol: str
    trading_date: str
    open: str
    high: str
    low: str
    close: str
    volume: int
    currency: str
    snapshot_id: str
    def __init__(self, event_id: _Optional[str] = ..., schema_version: _Optional[int] = ..., provider: _Optional[str] = ..., symbol: _Optional[str] = ..., trading_date: _Optional[str] = ..., open: _Optional[str] = ..., high: _Optional[str] = ..., low: _Optional[str] = ..., close: _Optional[str] = ..., volume: _Optional[int] = ..., currency: _Optional[str] = ..., snapshot_id: _Optional[str] = ...) -> None: ...
