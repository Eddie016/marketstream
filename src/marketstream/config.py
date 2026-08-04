from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from MARKETSTREAM_* environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="MARKETSTREAM_",
        extra="ignore",
    )

    environment: Literal["local", "test", "production"] = "local"
    log_level: str = "INFO"
    database_url: str = (
        "postgresql+psycopg://marketstream:marketstream@localhost:5432/marketstream"
    )
    kafka_bootstrap_servers: str = "localhost:29092"
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "marketstream"
    s3_secret_key: SecretStr = SecretStr("marketstream-local-only")
    s3_bucket: str = "marketstream"


@lru_cache
def get_settings() -> Settings:
    return Settings()
