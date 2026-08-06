import argparse
import base64
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from marketstream.config import get_settings
from marketstream.database import get_engine
from marketstream.processing.events import InvalidMarketEvent, decode_event
from marketstream.processing.repository import (
    ConflictingEventError,
    advance_invalid_offset,
    apply_event,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


def dlq_payload(record: SourceRecord, error: Exception) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "source": {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
            },
            "key_base64": base64.b64encode(record.key or b"").decode("ascii"),
            "value_base64": base64.b64encode(record.value).decode("ascii"),
            "error_type": type(error).__name__,
            "error": str(error),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def process_record(
    engine: Engine,
    record: SourceRecord,
    *,
    consumer_group: str,
    dlq_publish: Callable[[bytes, bytes], None],
) -> bool:
    """Process one record; return whether it inserted a new logical price."""

    try:
        event = decode_event(record.value)
        with Session(engine) as session, session.begin():
            return apply_event(
                session,
                consumer_group=consumer_group,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                event=event,
            )
    except (InvalidMarketEvent, ConflictingEventError) as error:
        key = f"{record.topic}:{record.partition}:{record.offset}".encode("ascii")
        dlq_publish(key, dlq_payload(record, error))
        with Session(engine) as session, session.begin():
            advance_invalid_offset(
                session,
                consumer_group=consumer_group,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
        return False


def run_consumer(
    *,
    engine: Engine,
    bootstrap_servers: str,
    topic: str,
    dlq_topic: str,
    consumer_group: str,
    max_attempts: int = 5,
    consumer_factory: Any = KafkaConsumer,
    producer_factory: Any = KafkaProducer,
) -> None:
    consumer = consumer_factory(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        key_deserializer=None,
        value_deserializer=None,
    )
    producer = producer_factory(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=max_attempts,
    )

    def publish_dlq(key: bytes, payload: bytes) -> None:
        producer.send(dlq_topic, key=key, value=payload).get(timeout=30)

    try:
        for message in consumer:
            record = SourceRecord(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                key=message.key,
                value=message.value,
            )
            for attempt in range(1, max_attempts + 1):
                try:
                    inserted = process_record(
                        engine,
                        record,
                        consumer_group=consumer_group,
                        dlq_publish=publish_dlq,
                    )
                    partition = TopicPartition(message.topic, message.partition)
                    consumer.commit(
                        {partition: OffsetAndMetadata(message.offset + 1, "", -1)}
                    )
                    LOGGER.info(
                        "processed market event",
                        extra={
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "inserted": inserted,
                        },
                    )
                    break
                except Exception:
                    if attempt == max_attempts:
                        raise
                    time.sleep(min(2 ** (attempt - 1), 8))
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        consumer.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Run the reliable market consumer")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_market_topic)
    parser.add_argument("--dlq-topic", default=settings.kafka_dlq_topic)
    parser.add_argument("--consumer-group", default=settings.kafka_consumer_group)
    parser.add_argument("--max-attempts", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.max_attempts < 1:
        raise ValueError("max-attempts must be at least one")
    logging.basicConfig(level=get_settings().log_level)
    run_consumer(
        engine=get_engine(),
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        dlq_topic=args.dlq_topic,
        consumer_group=args.consumer_group,
        max_attempts=args.max_attempts,
    )


if __name__ == "__main__":
    main()
