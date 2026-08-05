import json
from pathlib import Path
from typing import Any, ClassVar

import pytest

from marketstream.market_data.replay import build_replay_plan, main, publish

FIXTURE_MANIFEST = Path("tests/fixtures/market_data/manifest.json")


class FakeFuture:
    def get(self, timeout: int) -> None:
        assert timeout == 30


class FakeProducer:
    instances: ClassVar[list["FakeProducer"]] = []

    def __init__(self, **configuration: Any) -> None:
        self.configuration = configuration
        self.sent: list[tuple[str, str, bytes]] = []
        self.flush_count = 0
        self.closed = False
        self.instances.append(self)

    def send(self, topic: str, *, key: str, value: Any) -> FakeFuture:
        serialized_key = self.configuration["key_serializer"](key)
        serialized_value = self.configuration["value_serializer"](value)
        self.sent.append((topic, serialized_key.decode("ascii"), serialized_value))
        return FakeFuture()

    def flush(self, timeout: int) -> None:
        assert timeout == 30
        self.flush_count += 1

    def close(self, timeout: int) -> None:
        assert timeout == 30
        self.closed = True


def test_replay_plan_has_stable_order_and_digest() -> None:
    first = build_replay_plan(FIXTURE_MANIFEST)
    second = build_replay_plan(FIXTURE_MANIFEST)

    assert len(first.events) == 6
    assert first.ordered_event_sha256 == second.ordered_event_sha256
    assert [(event.trading_date, event.symbol) for event in first.events] == [
        ("2024-01-02", "AAPL"),
        ("2024-01-02", "SPY"),
        ("2024-01-03", "AAPL"),
        ("2024-01-03", "SPY"),
        ("2024-01-04", "AAPL"),
        ("2024-01-04", "SPY"),
    ]
    assert first.events[0].snapshot_id == "synthetic-fixture-v1-99b42515455a"
    assert len(first.events[0].event_id) == 64


def test_publish_writes_checkpoint_and_resumes(tmp_path: Path) -> None:
    FakeProducer.instances.clear()
    plan = build_replay_plan(FIXTURE_MANIFEST)
    checkpoint = tmp_path / "checkpoint.json"

    sent = publish(
        plan,
        FIXTURE_MANIFEST,
        "unused:9092",
        "market-prices-v1",
        0,
        checkpoint,
        producer_factory=FakeProducer,
    )

    assert sent == 6
    assert FakeProducer.instances[-1].closed
    assert FakeProducer.instances[-1].flush_count == 1
    checkpoint_payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert checkpoint_payload["last_event_id"] == plan.events[-1].event_id

    sent_after_resume = publish(
        plan,
        FIXTURE_MANIFEST,
        "unused:9092",
        "market-prices-v1",
        0,
        checkpoint,
        producer_factory=FakeProducer,
    )
    assert sent_after_resume == 0


def test_checkpoint_from_another_manifest_is_rejected(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(
        json.dumps({"manifest": "/other/manifest.json", "last_event_id": "x"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="another manifest"):
        publish(
            build_replay_plan(FIXTURE_MANIFEST),
            FIXTURE_MANIFEST,
            "unused:9092",
            "topic",
            0,
            checkpoint,
            producer_factory=FakeProducer,
        )


def test_checkpoint_with_unknown_event_is_rejected(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(
        json.dumps(
            {
                "manifest": str(FIXTURE_MANIFEST.resolve()),
                "last_event_id": "0" * 64,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not exist"):
        publish(
            build_replay_plan(FIXTURE_MANIFEST),
            FIXTURE_MANIFEST,
            "unused:9092",
            "topic",
            0,
            checkpoint,
            producer_factory=FakeProducer,
        )


def test_replay_plan_cli(capsys: pytest.CaptureFixture[str]) -> None:
    main(["plan", str(FIXTURE_MANIFEST)])

    output = json.loads(capsys.readouterr().out)
    assert output["events"] == 6
    assert len(output["ordered_event_sha256"]) == 64
