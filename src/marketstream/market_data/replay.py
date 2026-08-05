import argparse
import hashlib
import json
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kafka import KafkaProducer

from marketstream.market_data.download import verify_snapshot
from marketstream.market_data.models import SnapshotManifest
from marketstream.market_data.validation import MarketRow, read_canonical_csv
from marketstream.proto.market_event_pb2 import MarketEvent


@dataclass(frozen=True, slots=True)
class ReplayPlan:
    events: tuple[MarketEvent, ...]
    ordered_event_sha256: str


def event_id(
    schema_version: int,
    provider: str,
    symbol: str,
    trading_date: str,
) -> str:
    identity = f"{schema_version}|{provider}|{symbol}|{trading_date}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def build_event(manifest: SnapshotManifest, row: MarketRow) -> MarketEvent:
    trading_date = row.trading_date.isoformat()
    return MarketEvent(
        event_id=event_id(
            manifest.schema_version,
            manifest.provider,
            row.symbol,
            trading_date,
        ),
        schema_version=manifest.schema_version,
        provider=manifest.provider,
        symbol=row.symbol,
        trading_date=trading_date,
        open=format(row.open.normalize(), "f"),
        high=format(row.high.normalize(), "f"),
        low=format(row.low.normalize(), "f"),
        close=format(row.close.normalize(), "f"),
        volume=row.volume,
        currency=row.currency,
        snapshot_id=manifest.snapshot_id,
    )


def build_replay_plan(manifest_path: Path) -> ReplayPlan:
    manifest = verify_snapshot(manifest_path)
    snapshot_dir = manifest_path.parent
    rows = [
        row
        for item in manifest.files
        for row in read_canonical_csv(snapshot_dir / item.path)
    ]
    rows.sort(key=lambda row: (row.trading_date, row.symbol))
    events = tuple(build_event(manifest, row) for row in rows)
    ordered_digest = hashlib.sha256()
    for event in events:
        ordered_digest.update(event.event_id.encode("ascii"))
        ordered_digest.update(b"\n")
    return ReplayPlan(events=events, ordered_event_sha256=ordered_digest.hexdigest())


def publish(
    plan: ReplayPlan,
    manifest_path: Path,
    bootstrap_servers: str,
    topic: str,
    events_per_second: float,
    checkpoint_path: Path | None,
    *,
    producer_factory: Any = KafkaProducer,
) -> int:
    start_index = _resume_index(plan.events, manifest_path, checkpoint_path)
    producer = producer_factory(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=10,
        key_serializer=lambda value: value.encode("ascii"),
        value_serializer=lambda value: value.SerializeToString(deterministic=True),
    )
    delay = 0.0 if events_per_second == 0 else 1.0 / events_per_second
    sent = 0
    try:
        for event in plan.events[start_index:]:
            producer.send(topic, key=event.symbol, value=event).get(timeout=30)
            sent += 1
            if checkpoint_path is not None:
                _write_checkpoint(checkpoint_path, manifest_path, event.event_id)
            if delay:
                time.sleep(delay)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
    return sent


def _resume_index(
    events: Iterable[MarketEvent],
    manifest_path: Path,
    checkpoint_path: Path | None,
) -> int:
    if checkpoint_path is None or not checkpoint_path.exists():
        return 0
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    expected_manifest = str(manifest_path.resolve())
    if checkpoint.get("manifest") != expected_manifest:
        raise ValueError("checkpoint belongs to another manifest")
    last_event_id = checkpoint.get("last_event_id")
    for index, event in enumerate(events):
        if event.event_id == last_event_id:
            return index + 1
    raise ValueError("checkpoint event does not exist in this replay plan")


def _write_checkpoint(path: Path, manifest_path: Path, last_event_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(
            {
                "manifest": str(manifest_path.resolve()),
                "last_event_id": last_event_id,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan or publish deterministic replay")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "publish"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("manifest", type=Path)
        if command == "publish":
            subparser.add_argument("--bootstrap-servers", default="localhost:29092")
            subparser.add_argument("--topic", default="market-prices-v1")
            subparser.add_argument("--events-per-second", type=float, default=20.0)
            subparser.add_argument("--checkpoint", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    plan = build_replay_plan(args.manifest)
    if args.command == "plan":
        print(
            json.dumps(
                {
                    "events": len(plan.events),
                    "ordered_event_sha256": plan.ordered_event_sha256,
                },
                sort_keys=True,
            )
        )
        return
    if args.events_per_second < 0:
        raise ValueError("events-per-second must be non-negative")
    sent = publish(
        plan,
        args.manifest,
        args.bootstrap_servers,
        args.topic,
        args.events_per_second,
        args.checkpoint,
    )
    print(json.dumps({"sent": sent}, sort_keys=True))
