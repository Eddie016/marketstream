"""Read-only acceptance verifier for a running M2 Compose stack."""

import argparse
import io
import json
import time

import boto3
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from marketstream.config import Settings, get_settings
from marketstream.processing.tables import (
    ArchiveOutbox,
    ConsumerCheckpoint,
    MarketPrice,
)


def verify_once(
    settings: Settings,
    *,
    expected_logical_prices: int | None,
    expected_source_events: int | None,
    expected_dlq_events: int | None,
) -> dict[str, object]:
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with Session(engine) as session:
        logical_prices = session.scalar(select(func.count()).select_from(MarketPrice))
        unique_events = session.scalar(
            select(func.count(func.distinct(MarketPrice.event_id)))
        )
        outbox_statuses = dict(
            session.execute(
                select(ArchiveOutbox.status, func.count()).group_by(
                    ArchiveOutbox.status
                )
            ).all()
        )
        checkpoints = {
            item.partition: item.next_offset
            for item in session.scalars(
                select(ConsumerCheckpoint).where(
                    ConsumerCheckpoint.consumer_group == settings.kafka_consumer_group,
                    ConsumerCheckpoint.topic == settings.kafka_market_topic,
                )
            ).all()
        }

    consumer = KafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    partitions = consumer.partitions_for_topic(settings.kafka_market_topic) or set()
    topic_partitions = [
        TopicPartition(settings.kafka_market_topic, partition)
        for partition in sorted(partitions)
    ]
    end_offsets = consumer.end_offsets(topic_partitions)
    dlq_partitions = consumer.partitions_for_topic(settings.kafka_dlq_topic) or set()
    dlq_offsets = consumer.end_offsets(
        [
            TopicPartition(settings.kafka_dlq_topic, partition)
            for partition in sorted(dlq_partitions)
        ]
    )
    consumer.close()

    source_events = sum(end_offsets.values())
    for topic_partition, end_offset in end_offsets.items():
        if end_offset:
            assert checkpoints.get(topic_partition.partition) == end_offset
    assert logical_prices == unique_events
    assert outbox_statuses == {"archived": logical_prices}
    if expected_logical_prices is not None:
        assert logical_prices == expected_logical_prices
    if expected_source_events is not None:
        assert source_events == expected_source_events
    if expected_dlq_events is not None:
        assert sum(dlq_offsets.values()) == expected_dlq_events

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
    )
    objects = [
        item
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=settings.s3_bucket, Prefix="market-prices/"
        )
        for item in page.get("Contents", [])
    ]
    assert len(objects) == logical_prices
    assert objects
    sample_key = objects[0]["Key"]
    sample = s3.get_object(Bucket=settings.s3_bucket, Key=sample_key)
    sample_table = pq.read_table(io.BytesIO(sample["Body"].read()))
    assert sample_table.num_rows == 1

    return {
        "status": "passed",
        "source_events": source_events,
        "logical_prices": logical_prices,
        "archived_objects": len(objects),
        "dlq_events": sum(dlq_offsets.values()),
        "sample_parquet": sample_key,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-seconds", type=float, default=0)
    parser.add_argument("--expected-logical-prices", type=int)
    parser.add_argument("--expected-source-events", type=int)
    parser.add_argument("--expected-dlq-events", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            report = verify_once(
                get_settings(),
                expected_logical_prices=args.expected_logical_prices,
                expected_source_events=args.expected_source_events,
                expected_dlq_events=args.expected_dlq_events,
            )
            print(json.dumps(report, sort_keys=True))
            return
        except (AssertionError, StopIteration):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


if __name__ == "__main__":
    main()
