from typing import Any

from marketstream import database
from marketstream.config import Settings, get_settings


def test_default_settings_are_loadable() -> None:
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.environment == "local"
    assert settings.s3_secret_key.get_secret_value() == "marketstream-local-only"


def test_database_health_succeeds_for_reachable_database(monkeypatch: Any) -> None:
    database.get_engine.cache_clear()
    monkeypatch.setattr(
        database,
        "get_settings",
        lambda: Settings(database_url="sqlite+pysqlite:///:memory:"),
    )

    assert list(database.database_health()) == [True]
    database.get_engine.cache_clear()


def test_database_health_converts_connection_failure_to_false(
    monkeypatch: Any,
) -> None:
    class BrokenEngine:
        def connect(self) -> None:
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(database, "get_engine", lambda: BrokenEngine())

    assert list(database.database_health()) == [False]
