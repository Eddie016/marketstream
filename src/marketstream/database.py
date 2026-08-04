from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy import Engine, create_engine, text

from marketstream.config import get_settings


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def database_health() -> Iterator[bool]:
    """Yield database readiness as a dependency that can be replaced in tests."""

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))
        yield True
    except Exception:  # readiness must convert infrastructure failures to 503
        yield False
