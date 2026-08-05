"""Publish deterministic valid, duplicate, and invalid M2 acceptance records."""

from kafka import KafkaProducer

from marketstream.config import get_settings
from marketstream.market_data.replay import event_id
from marketstream.proto.market_event_pb2 import MarketEvent


def main() -> None:
    settings = get_settings()
    trading_date = "2026-08-05"
    event = MarketEvent(
        event_id=event_id(1, "ci-fixture", "AAPL", trading_date),
        schema_version=1,
        provider="ci-fixture",
        symbol="AAPL",
        trading_date=trading_date,
        open="200.00",
        high="205.00",
        low="199.00",
        close="203.00",
        volume=1000,
        currency="USD",
        snapshot_id="ci-m2-v1",
    )
    payload = event.SerializeToString(deterministic=True)
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        retries=5,
    )
    for value in (payload, payload, b"invalid-market-event"):
        producer.send(settings.kafka_market_topic, key=b"AAPL", value=value).get(
            timeout=30
        )
    producer.close(timeout=30)


if __name__ == "__main__":
    main()
