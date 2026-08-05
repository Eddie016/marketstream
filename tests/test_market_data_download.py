import json
import urllib.error
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from marketstream.market_data.download import (
    DownloadError,
    build_snapshot,
    fetch,
    load_source_request,
    main,
    provider_url,
    verify_snapshot,
)
from marketstream.market_data.models import SourceRequest

PROVIDER_CSV = b"""Date,Open,High,Low,Close,Volume
2024-01-02,10,12,9,11,100
2024-01-03,11,13,10,12,200
"""


def write_request(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset_id": "test-eod-v1",
                "provider": "plotly-github",
                "symbols": ["spy", "aapl"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "currency": "usd",
            }
        ),
        encoding="utf-8",
    )


def test_build_and_verify_snapshot_is_deterministic(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path)

    manifest = build_snapshot(
        request_path,
        tmp_path / "data",
        fetcher=lambda _: PROVIDER_CSV,
    )
    manifest_path = tmp_path / "data" / manifest.snapshot_id / "manifest.json"

    assert [item.symbol for item in manifest.files] == ["AAPL", "SPY"]
    assert all(item.row_count == 2 for item in manifest.files)
    assert verify_snapshot(manifest_path) == manifest

    first_checksums = [item.sha256 for item in manifest.files]
    rebuilt = build_snapshot(
        request_path,
        tmp_path / "data",
        fetcher=lambda _: PROVIDER_CSV,
    )
    assert [item.sha256 for item in rebuilt.files] == first_checksums
    assert rebuilt.snapshot_id == manifest.snapshot_id

    matched = build_snapshot(
        request_path,
        tmp_path / "data",
        expected_manifest_path=manifest_path,
        fetcher=lambda _: PROVIDER_CSV,
    )
    assert matched.snapshot_id == manifest.snapshot_id

    expected_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_payload["currency"] = "EUR"
    mismatched_manifest = tmp_path / "mismatched.json"
    mismatched_manifest.write_text(json.dumps(expected_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="differs from expected"):
        build_snapshot(
            request_path,
            tmp_path / "data",
            expected_manifest_path=mismatched_manifest,
            fetcher=lambda _: PROVIDER_CSV,
        )


def test_verify_detects_missing_and_modified_files(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path)
    manifest = build_snapshot(request_path, tmp_path, fetcher=lambda _: PROVIDER_CSV)
    manifest_path = tmp_path / manifest.snapshot_id / "manifest.json"
    data_path = tmp_path / manifest.snapshot_id / manifest.files[0].path
    data_path.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_snapshot(manifest_path)

    data_path.unlink()
    with pytest.raises(ValueError, match="missing"):
        verify_snapshot(manifest_path)


def test_request_normalization_and_validation(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    write_request(request_path)

    request, canonical = load_source_request(request_path)

    assert request.symbols == ["AAPL", "SPY"]
    assert request.currency == "USD"
    assert canonical.endswith(b"\n")
    assert provider_url(request, "AAPL").endswith("all_stocks_5yr.csv")

    with pytest.raises(ValidationError, match="symbols must be unique"):
        SourceRequest.model_validate(
            {
                **request.model_dump(),
                "symbols": ["AAPL", "aapl"],
            }
        )
    with pytest.raises(ValidationError, match="start_date"):
        SourceRequest.model_validate(
            {
                **request.model_dump(),
                "start_date": "2025-01-01",
                "end_date": "2024-01-01",
            }
        )


def test_unsupported_provider_is_rejected() -> None:
    request = SourceRequest(
        schema_version=1,
        dataset_id="test-v1",
        provider="unknown",
        symbols=["SPY"],
        start_date="2024-01-01",  # type: ignore[arg-type]
        end_date="2024-01-02",  # type: ignore[arg-type]
        currency="USD",
    )

    with pytest.raises(DownloadError, match="unsupported"):
        provider_url(request, "SPY")


def test_cli_download_and_verify(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture_manifest = Path("tests/fixtures/market_data/manifest.json")

    main(
        [
            "verify",
            str(fixture_manifest),
            "--data-dir",
            str(fixture_manifest.parent),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "verified"


def test_fetch_retries_then_returns_bytes(monkeypatch: Any) -> None:
    class Response:
        status = 200

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self) -> bytes:
            return PROVIDER_CSV

    outcomes: list[object] = [urllib.error.URLError("temporary"), Response()]

    def fake_urlopen(*_: object, **__: object) -> Response:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, Response)
        return outcome

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("time.sleep", lambda _: None)

    assert fetch("https://example.invalid") == PROVIDER_CSV


def test_fetch_reports_bounded_failure(monkeypatch: Any) -> None:
    def always_fail(*_: object, **__: object) -> None:
        raise urllib.error.URLError("offline")

    monkeypatch.setattr("urllib.request.urlopen", always_fail)

    with pytest.raises(DownloadError, match="after 1 attempts"):
        fetch("https://example.invalid", attempts=1)
