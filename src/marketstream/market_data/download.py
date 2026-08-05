import argparse
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import certifi

from marketstream.market_data.models import (
    SnapshotFile,
    SnapshotManifest,
    SourceRequest,
)
from marketstream.market_data.validation import (
    canonical_csv_bytes,
    parse_provider_csv,
    read_canonical_csv,
    sha256_bytes,
    sha256_file,
)

USER_AGENT = "MarketStream/0.1 educational-research-platform"


class DownloadError(RuntimeError):
    """Raised when a remote snapshot cannot be fetched after bounded retries."""


def load_source_request(path: Path) -> tuple[SourceRequest, bytes]:
    payload = path.read_bytes()
    request = SourceRequest.model_validate_json(payload)
    canonical = request.model_dump_json(indent=2).encode("utf-8") + b"\n"
    return request, canonical


def provider_url(request: SourceRequest, symbol: str) -> str:
    del symbol
    if request.provider == "plotly-github":
        return (
            "https://raw.githubusercontent.com/plotly/datasets/"
            "master/all_stocks_5yr.csv"
        )
    raise DownloadError(f"unsupported provider: {request.provider}")


def fetch(url: str, attempts: int = 3, timeout_seconds: float = 30.0) -> bytes:
    last_error: Exception | None = None
    tls_context = ssl.create_default_context(cafile=certifi.where())
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout_seconds,
                context=tls_context,
            ) as response:
                if response.status != 200:
                    raise DownloadError(f"provider returned HTTP {response.status}")
                payload: bytes = response.read()
                return payload
        except (OSError, urllib.error.URLError, DownloadError) as error:
            last_error = error
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))
    raise DownloadError(
        f"download failed after {attempts} attempts: {url}"
    ) from last_error


def build_snapshot(
    request_path: Path,
    output_root: Path,
    *,
    expected_manifest_path: Path | None = None,
    fetcher: Any = fetch,
) -> SnapshotManifest:
    request, canonical_request = load_source_request(request_path)
    request_sha256 = sha256_bytes(canonical_request)
    snapshot_id = f"{request.dataset_id}-{request_sha256[:12]}"
    snapshot_dir = output_root / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    files: list[SnapshotFile] = []
    payload_cache: dict[str, bytes] = {}
    for symbol in request.symbols:
        url = provider_url(request, symbol)
        if url not in payload_cache:
            payload_cache[url] = fetcher(url)
        payload = payload_cache[url]
        rows = parse_provider_csv(payload, symbol, request.currency)
        rows = [
            row
            for row in rows
            if request.start_date <= row.trading_date <= request.end_date
        ]
        if not rows:
            raise DownloadError(f"no {symbol} rows in the requested date interval")
        canonical_payload = canonical_csv_bytes(rows)
        relative_path = f"{symbol}.csv"
        _atomic_write(snapshot_dir / relative_path, canonical_payload)
        files.append(
            SnapshotFile(
                symbol=symbol,
                path=relative_path,
                sha256=sha256_bytes(canonical_payload),
                row_count=len(rows),
                first_date=rows[0].trading_date,
                last_date=rows[-1].trading_date,
            )
        )

    manifest = SnapshotManifest(
        schema_version=request.schema_version,
        snapshot_id=snapshot_id,
        request_sha256=request_sha256,
        dataset_id=request.dataset_id,
        provider=request.provider,
        currency=request.currency,
        requested_start_date=request.start_date,
        requested_end_date=request.end_date,
        generated_at=datetime.now(UTC),
        files=files,
    )
    _atomic_write(
        snapshot_dir / "manifest.json",
        manifest.model_dump_json(indent=2).encode("utf-8") + b"\n",
    )
    if expected_manifest_path is not None:
        expected = SnapshotManifest.model_validate_json(
            expected_manifest_path.read_bytes()
        )
        if manifest.model_dump(exclude={"generated_at"}) != expected.model_dump(
            exclude={"generated_at"}
        ):
            raise ValueError("downloaded snapshot differs from expected manifest")
    return manifest


def verify_snapshot(
    manifest_path: Path,
    data_dir: Path | None = None,
) -> SnapshotManifest:
    manifest = SnapshotManifest.model_validate_json(manifest_path.read_bytes())
    snapshot_dir = data_dir or manifest_path.parent
    failures: list[str] = []
    for item in manifest.files:
        path = (snapshot_dir / item.path).resolve()
        if not path.is_relative_to(snapshot_dir.resolve()):
            failures.append(f"unsafe path {item.path}")
            continue
        if not path.is_file():
            failures.append(f"missing {item.path}")
            continue
        actual = sha256_file(path)
        if actual != item.sha256:
            failures.append(f"checksum mismatch for {item.path}")
            continue
        rows = read_canonical_csv(path)
        if len(rows) != item.row_count:
            failures.append(f"row count mismatch for {item.path}")
        elif rows[0].symbol != item.symbol:
            failures.append(f"symbol mismatch for {item.path}")
        elif rows[0].trading_date != item.first_date:
            failures.append(f"first date mismatch for {item.path}")
        elif rows[-1].trading_date != item.last_date:
            failures.append(f"last date mismatch for {item.path}")
    if failures:
        raise ValueError("; ".join(failures))
    return manifest


def _atomic_write(path: Path, payload: bytes) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and verify market snapshots")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("request", type=Path)
    download_parser.add_argument("--output-root", type=Path, default=Path("data/raw"))
    download_parser.add_argument("--expected-manifest", type=Path)

    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("manifest", type=Path)
    verify_parser.add_argument("--data-dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "download":
        manifest = build_snapshot(
            args.request,
            args.output_root,
            expected_manifest_path=args.expected_manifest,
        )
        print(
            json.dumps(
                {"snapshot_id": manifest.snapshot_id, "files": len(manifest.files)}
            )
        )
        return
    manifest = verify_snapshot(args.manifest, args.data_dir)
    print(json.dumps({"snapshot_id": manifest.snapshot_id, "status": "verified"}))
