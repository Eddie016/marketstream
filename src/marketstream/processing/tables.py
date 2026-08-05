from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MarketPrice(Base):
    __tablename__ = "market_prices"

    provider: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(16), primary_key=True)
    trading_date: Mapped[date] = mapped_column(Date, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True)
    schema_version: Mapped[int] = mapped_column(Integer)
    open: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    high: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    low: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    close: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    volume: Mapped[int] = mapped_column(BigInteger)
    currency: Mapped[str] = mapped_column(String(3))
    snapshot_id: Mapped[str] = mapped_column(String(128))
    source_topic: Mapped[str] = mapped_column(String(249))
    source_partition: Mapped[int] = mapped_column(Integer)
    source_offset: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ConsumerCheckpoint(Base):
    __tablename__ = "consumer_checkpoints"

    consumer_group: Mapped[str] = mapped_column(String(255), primary_key=True)
    topic: Mapped[str] = mapped_column(String(249), primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    next_offset: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ArchiveOutbox(Base):
    __tablename__ = "archive_outbox"

    event_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("market_prices.event_id", ondelete="CASCADE"),
        primary_key=True,
    )
    object_key: Mapped[str] = mapped_column(String(512), unique=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    archived_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
