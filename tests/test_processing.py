import io
from datetime import date
from decimal import Decimal
from typing import Any

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from marketstream.market_data.replay import event_id
from marketstream.processing import bootstrap
from marketstream.processing.archive import price_to_parquet
from marketstream.processing.archiver import (
    archive_one,
    run_archiver,
)
from marketstream.processing.archiver import (
    main as archiver_main,
)
from marketstream.processing.archiver import (
    parse_args as parse_archiver_args,
)
from marketstream.processing.consumer import (
    SourceRecord,
    process_record,
    run_consumer,
)
from marketstream.processing.consumer import (
    main as consumer_main,
)
from marketstream.processing.consumer import (
    parse_args as parse_consumer_args,
)
from marketstream.processing.events import InvalidMarketEvent, decode_event
from marketstream.processing.repository import OffsetGapError
from marketstream.processing.tables import (
    ArchiveOutbox,
    Base,
    ConsumerCheckpoint,
    MarketPrice,
)
from marketstream.proto.market_event_pb2 import MarketEvent


def payload(
    *,
    close: str = "102.00",
    symbol: str = "AAPL",
    trading_date: str = "2024-01-02",
) -> bytes:
    message = MarketEvent(
        event_id=event_id(1, "fixture", symbol, trading_date),
        schema_version=1,
        provider="fixture",
        symbol=symbol,
        trading_date=trading_date,
        open="100.00",
        high="103.00",
        low="99.00",
        close=close,
        volume=1000,
        currency="USD",
        snapshot_id="fixture-v1",
    )
    return message.SerializeToString(deterministic=True)


@pytest.fixture
def engine() -> Any:
    database = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(database)
    return database


def source(value: bytes, offset: int = 0) -> SourceRecord:
    return SourceRecord("prices", 0, offset, b"AAPL", value)


def test_duplicate_delivery_and_replay_converge(engine: Any) -> None:
    dead_letters: list[tuple[bytes, bytes]] = []

    def publish(key: bytes, value: bytes) -> None:
        dead_letters.append((key, value))

    assert process_record(
        engine, source(payload()), consumer_group="query-v1", dlq_publish=publish
    )
    assert not process_record(
        engine, source(payload()), consumer_group="query-v1", dlq_publish=publish
    )
    assert not process_record(
        engine,
        source(payload(), offset=1),
        consumer_group="query-v1",
        dlq_publish=publish,
    )

    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(MarketPrice)) == 1
        assert session.scalar(select(func.count()).select_from(ArchiveOutbox)) == 1
        checkpoint = session.get(ConsumerCheckpoint, ("query-v1", "prices", 0))
        assert checkpoint is not None
        assert checkpoint.next_offset == 2
    assert dead_letters == []


def test_gap_does_not_advance_transaction(engine: Any) -> None:
    with pytest.raises(OffsetGapError, match="expected offset 0, received 2"):
        process_record(
            engine,
            source(payload(), offset=2),
            consumer_group="query-v1",
            dlq_publish=lambda _key, _value: None,
        )
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(MarketPrice)) == 0
        assert session.scalar(select(func.count()).select_from(ConsumerCheckpoint)) == 0


def test_invalid_event_is_preserved_in_dlq_and_checkpointed(engine: Any) -> None:
    dead_letters: list[tuple[bytes, bytes]] = []
    assert not process_record(
        engine,
        source(b"not-protobuf"),
        consumer_group="query-v1",
        dlq_publish=lambda key, value: dead_letters.append((key, value)),
    )
    assert dead_letters[0][0] == b"prices:0:0"
    assert b'"value_base64":"bm90LXByb3RvYnVm"' in dead_letters[0][1]
    with Session(engine) as session:
        checkpoint = session.get(ConsumerCheckpoint, ("query-v1", "prices", 0))
        assert checkpoint is not None
        assert checkpoint.next_offset == 1

    # A database-committed/Kafka-uncommitted redelivery keeps the checkpoint stable.
    assert not process_record(
        engine,
        source(b"not-protobuf"),
        consumer_group="query-v1",
        dlq_publish=lambda _key, _value: None,
    )


def test_invalid_event_offset_gap_is_rejected(engine: Any) -> None:
    with pytest.raises(OffsetGapError, match="expected offset 0, received 1"):
        process_record(
            engine,
            source(b"broken", offset=1),
            consumer_group="query-v1",
            dlq_publish=lambda _key, _value: None,
        )


def test_conflicting_logical_price_goes_to_dlq(engine: Any) -> None:
    dead_letters: list[tuple[bytes, bytes]] = []

    def publish(key: bytes, value: bytes) -> None:
        dead_letters.append((key, value))

    process_record(
        engine, source(payload()), consumer_group="query-v1", dlq_publish=publish
    )
    assert not process_record(
        engine,
        source(payload(close="101.00"), offset=1),
        consumer_group="query-v1",
        dlq_publish=publish,
    )
    assert b"ConflictingEventError" in dead_letters[0][1]
    with Session(engine) as session:
        price = session.get(MarketPrice, ("fixture", "AAPL", date(2024, 1, 2)))
        assert price is not None
        assert price.close == Decimal("102.00000000")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda item: setattr(item, "event_id", "wrong"), "event_id"),
        (lambda item: setattr(item, "low", "104"), "bounds"),
        (lambda item: setattr(item, "currency", "usd"), "currency"),
    ],
)
def test_decode_rejects_invalid_contract(mutate: Any, message: str) -> None:
    item = MarketEvent()
    item.ParseFromString(payload())
    mutate(item)
    with pytest.raises(InvalidMarketEvent, match=message):
        decode_event(item.SerializeToString())


def test_parquet_contains_typed_price_row(engine: Any) -> None:
    process_record(
        engine,
        source(payload()),
        consumer_group="query-v1",
        dlq_publish=lambda _key, _value: None,
    )
    with Session(engine) as session:
        price = session.execute(select(MarketPrice)).scalar_one()
        table = pq.read_table(io.BytesIO(price_to_parquet(price)))
    assert table.num_rows == 1
    assert table.column("symbol").to_pylist() == ["AAPL"]
    assert table.column("trading_date").to_pylist() == [date(2024, 1, 2)]


class RecordingS3:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.objects: list[dict[str, Any]] = []

    def put_object(self, **request: Any) -> None:
        if self.error is not None:
            raise self.error
        self.objects.append(request)


def test_archiver_retries_with_deterministic_key(engine: Any) -> None:
    process_record(
        engine,
        source(payload()),
        consumer_group="query-v1",
        dlq_publish=lambda _key, _value: None,
    )
    failing = RecordingS3(RuntimeError("storage unavailable"))
    with pytest.raises(RuntimeError, match="storage unavailable"):
        archive_one(engine, failing, "archive", max_attempts=2)

    with Session(engine) as session:
        outbox = session.execute(select(ArchiveOutbox)).scalar_one()
        assert outbox.status == "pending"
        assert outbox.attempt_count == 1
        expected_key = outbox.object_key

    healthy = RecordingS3()
    assert archive_one(engine, healthy, "archive", max_attempts=2)
    assert healthy.objects[0]["Key"] == expected_key
    with Session(engine) as session:
        outbox = session.execute(select(ArchiveOutbox)).scalar_one()
        assert outbox.status == "archived"
        assert outbox.attempt_count == 2
    assert not archive_one(engine, healthy, "archive", max_attempts=2)


def test_archiver_marks_terminal_failure(engine: Any) -> None:
    process_record(
        engine,
        source(payload()),
        consumer_group="query-v1",
        dlq_publish=lambda _key, _value: None,
    )
    with pytest.raises(RuntimeError):
        archive_one(
            engine, RecordingS3(RuntimeError("offline")), "archive", max_attempts=1
        )
    with Session(engine) as session:
        outbox = session.execute(select(ArchiveOutbox)).scalar_one()
        assert outbox.status == "failed"


class FakeFuture:
    def get(self, timeout: int) -> None:
        assert timeout == 30


class FakeProducer:
    instance: "FakeProducer"

    def __init__(self, **options: Any) -> None:
        assert options["acks"] == "all"
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.closed = False
        FakeProducer.instance = self

    def send(self, topic: str, *, key: bytes, value: bytes) -> FakeFuture:
        self.sent.append((topic, key, value))
        return FakeFuture()

    def flush(self, timeout: int) -> None:
        assert timeout == 30

    def close(self, timeout: int) -> None:
        assert timeout == 30
        self.closed = True


class FakeMessage:
    topic = "prices"
    partition = 0
    offset = 0
    key = b"AAPL"
    value = payload()


class FakeConsumer:
    instance: "FakeConsumer"

    def __init__(self, topic: str, **options: Any) -> None:
        assert topic == "prices"
        assert options["enable_auto_commit"] is False
        self.messages = [FakeMessage()]
        self.commits: list[Any] = []
        self.closed = False
        FakeConsumer.instance = self

    def __iter__(self) -> Any:
        return iter(self.messages)

    def commit(self, offsets: Any) -> None:
        self.commits.append(offsets)

    def close(self) -> None:
        self.closed = True


def test_consumer_commits_kafka_only_after_database_transaction(engine: Any) -> None:
    run_consumer(
        engine=engine,
        bootstrap_servers="unused:9092",
        topic="prices",
        dlq_topic="dead-letters",
        consumer_group="query-v1",
        max_attempts=2,
        consumer_factory=FakeConsumer,
        producer_factory=FakeProducer,
    )
    assert len(FakeConsumer.instance.commits) == 1
    committed = next(iter(FakeConsumer.instance.commits[0].values()))
    assert committed.offset == 1
    assert committed.leader_epoch == -1
    assert FakeConsumer.instance.closed
    assert FakeProducer.instance.closed
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(MarketPrice)) == 1


def test_consumer_publishes_invalid_payload_to_dlq(engine: Any) -> None:
    class InvalidMessage(FakeMessage):
        value = b"broken"

    class InvalidConsumer(FakeConsumer):
        def __init__(self, topic: str, **options: Any) -> None:
            super().__init__(topic, **options)
            self.messages = [InvalidMessage()]

    run_consumer(
        engine=engine,
        bootstrap_servers="unused:9092",
        topic="prices",
        dlq_topic="dead-letters",
        consumer_group="query-v1",
        consumer_factory=InvalidConsumer,
        producer_factory=FakeProducer,
    )
    assert FakeProducer.instance.sent[0][0] == "dead-letters"
    assert FakeProducer.instance.sent[0][1] == b"prices:0:0"


def test_run_archiver_drains_until_empty(engine: Any) -> None:
    process_record(
        engine,
        source(payload()),
        consumer_group="query-v1",
        dlq_publish=lambda _key, _value: None,
    )
    storage = RecordingS3()
    run_archiver(engine, storage, "archive", max_attempts=2)
    assert len(storage.objects) == 1


def test_worker_argument_validation() -> None:
    assert parse_consumer_args([]).topic == "market-prices-v1"
    assert parse_archiver_args(["--max-attempts", "3"]).max_attempts == 3
    with pytest.raises(ValueError, match="at least one"):
        consumer_main(["--max-attempts", "0"])
    with pytest.raises(ValueError, match="at least one"):
        archiver_main(["--max-attempts", "0"])


def test_bootstrap_creates_missing_archive_bucket(monkeypatch: Any) -> None:
    class FakeS3:
        def __init__(self) -> None:
            self.created: list[str] = []

        def head_bucket(self, *, Bucket: str) -> None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "missing"}}, "HeadBucket"
            )

        def create_bucket(self, *, Bucket: str) -> None:
            self.created.append(Bucket)

    storage = FakeS3()
    monkeypatch.setattr(bootstrap.boto3, "client", lambda *_args, **_kwargs: storage)
    bootstrap.ensure_archive_bucket()
    assert storage.created == ["marketstream"]


def test_bootstrap_does_not_mask_s3_auth_failure(monkeypatch: Any) -> None:
    class ForbiddenS3:
        def head_bucket(self, *, Bucket: str) -> None:
            raise ClientError(
                {"Error": {"Code": "403", "Message": "forbidden"}}, "HeadBucket"
            )

    monkeypatch.setattr(
        bootstrap.boto3, "client", lambda *_args, **_kwargs: ForbiddenS3()
    )
    with pytest.raises(ClientError):
        bootstrap.ensure_archive_bucket()
