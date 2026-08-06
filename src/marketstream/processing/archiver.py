import argparse
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from sqlalchemy import Engine, or_, select
from sqlalchemy.orm import Session

from marketstream.config import get_settings
from marketstream.database import get_engine
from marketstream.processing.archive import price_to_parquet
from marketstream.processing.tables import ArchiveOutbox, MarketPrice

LOGGER = logging.getLogger(__name__)


def archive_one(
    engine: Engine,
    s3_client: Any,
    bucket: str,
    *,
    max_attempts: int,
    stale_after: timedelta = timedelta(minutes=5),
) -> bool:
    now = datetime.now(UTC)
    with Session(engine) as session, session.begin():
        outbox = session.execute(
            select(ArchiveOutbox)
            .where(
                ArchiveOutbox.attempt_count < max_attempts,
                or_(
                    ArchiveOutbox.status == "pending",
                    (
                        (ArchiveOutbox.status == "processing")
                        & (ArchiveOutbox.claimed_at < now - stale_after)
                    ),
                ),
            )
            .order_by(ArchiveOutbox.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        ).scalar_one_or_none()
        if outbox is None:
            return False
        outbox.status = "processing"
        outbox.claimed_at = now
        outbox.attempt_count += 1
        event_id = outbox.event_id
        object_key = outbox.object_key

    try:
        with Session(engine) as session:
            price = session.execute(
                select(MarketPrice).where(MarketPrice.event_id == event_id)
            ).scalar_one()
            payload = price_to_parquet(price)
        s3_client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=payload,
            ContentType="application/vnd.apache.parquet",
            Metadata={"event-id": event_id},
        )
    except Exception as error:
        with Session(engine) as session, session.begin():
            failed = session.get(ArchiveOutbox, event_id)
            if failed is not None:
                failed.status = (
                    "failed" if failed.attempt_count >= max_attempts else "pending"
                )
                failed.last_error = str(error)[:2000]
                failed.claimed_at = None
        raise

    with Session(engine) as session, session.begin():
        completed = session.get(ArchiveOutbox, event_id)
        if completed is not None:
            completed.status = "archived"
            completed.archived_at = datetime.now(UTC)
            completed.claimed_at = None
            completed.last_error = None
    return True


def run_archiver(
    engine: Engine,
    s3_client: Any,
    bucket: str,
    max_attempts: int,
    *,
    watch: bool = False,
) -> None:
    while True:
        archived = archive_one(engine, s3_client, bucket, max_attempts=max_attempts)
        if archived:
            LOGGER.info("archived market event")
        elif watch:
            time.sleep(2)
        else:
            return


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain the Parquet archive outbox")
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--watch", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = get_settings()
    max_attempts = (
        args.max_attempts
        if args.max_attempts is not None
        else settings.archive_max_attempts
    )
    if max_attempts < 1:
        raise ValueError("max-attempts must be at least one")
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
    )
    run_archiver(
        get_engine(), client, settings.s3_bucket, max_attempts, watch=args.watch
    )


if __name__ == "__main__":
    main()
