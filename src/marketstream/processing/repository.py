from dataclasses import asdict, astuple
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from marketstream.processing.events import ValidatedMarketEvent
from marketstream.processing.tables import (
    ArchiveOutbox,
    ConsumerCheckpoint,
    MarketPrice,
)


class OffsetGapError(RuntimeError):
    pass


class ConflictingEventError(RuntimeError):
    pass


def archive_object_key(event: ValidatedMarketEvent) -> str:
    return (
        f"market-prices/provider={event.provider}/symbol={event.symbol}/"
        f"year={event.trading_date.year}/date={event.trading_date.isoformat()}/"
        f"{event.event_id}.parquet"
    )


def apply_event(
    session: Session,
    *,
    consumer_group: str,
    topic: str,
    partition: int,
    offset: int,
    event: ValidatedMarketEvent,
) -> bool:
    """Atomically update the query model, outbox, and source checkpoint."""

    now = datetime.now(UTC)
    identity = {
        "consumer_group": consumer_group,
        "topic": topic,
        "partition": partition,
    }
    checkpoint = session.get(ConsumerCheckpoint, tuple(identity.values()))
    if checkpoint is None:
        checkpoint = ConsumerCheckpoint(**identity, next_offset=0, updated_at=now)
        session.add(checkpoint)
        session.flush()
    else:
        session.execute(
            select(ConsumerCheckpoint)
            .where(
                ConsumerCheckpoint.consumer_group == consumer_group,
                ConsumerCheckpoint.topic == topic,
                ConsumerCheckpoint.partition == partition,
            )
            .with_for_update()
        ).scalar_one()

    if offset < checkpoint.next_offset:
        return False
    if offset > checkpoint.next_offset:
        raise OffsetGapError(
            f"expected offset {checkpoint.next_offset}, received {offset}"
        )

    key = (event.provider, event.symbol, event.trading_date)
    existing = session.get(MarketPrice, key)
    inserted = existing is None
    if existing is None:
        price = MarketPrice(
            **asdict(event),
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            created_at=now,
        )
        session.add(price)
        # The outbox FK references a non-primary unique key. Flush the parent first;
        # both writes remain inside the same transaction.
        session.flush()
        session.add(
            ArchiveOutbox(
                event_id=event.event_id,
                object_key=archive_object_key(event),
                status="pending",
                attempt_count=0,
                created_at=now,
            )
        )
    else:
        stored = (
            existing.event_id,
            existing.schema_version,
            existing.provider,
            existing.symbol,
            existing.trading_date,
            existing.open,
            existing.high,
            existing.low,
            existing.close,
            existing.volume,
            existing.currency,
            existing.snapshot_id,
        )
        if stored != astuple(event):
            raise ConflictingEventError(
                "logical price key already contains different event data"
            )
    checkpoint.next_offset = offset + 1
    checkpoint.updated_at = now
    return inserted


def advance_invalid_offset(
    session: Session,
    *,
    consumer_group: str,
    topic: str,
    partition: int,
    offset: int,
) -> bool:
    now = datetime.now(UTC)
    key = (consumer_group, topic, partition)
    checkpoint = session.get(ConsumerCheckpoint, key)
    if checkpoint is None:
        checkpoint = ConsumerCheckpoint(
            consumer_group=consumer_group,
            topic=topic,
            partition=partition,
            next_offset=0,
            updated_at=now,
        )
        session.add(checkpoint)
        session.flush()
    else:
        session.execute(
            select(ConsumerCheckpoint)
            .where(
                ConsumerCheckpoint.consumer_group == consumer_group,
                ConsumerCheckpoint.topic == topic,
                ConsumerCheckpoint.partition == partition,
            )
            .with_for_update()
        ).scalar_one()
    if offset < checkpoint.next_offset:
        return False
    if offset > checkpoint.next_offset:
        raise OffsetGapError(
            f"expected offset {checkpoint.next_offset}, received {offset}"
        )
    checkpoint.next_offset = offset + 1
    checkpoint.updated_at = now
    return True
