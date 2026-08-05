from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(ge=1)
    dataset_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]+$")
    provider: str
    symbols: list[str] = Field(min_length=1)
    start_date: date
    end_date: date
    currency: str = Field(min_length=3, max_length=3)

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, symbols: list[str]) -> list[str]:
        normalized = [symbol.strip().upper() for symbol in symbols]
        if len(set(normalized)) != len(normalized):
            raise ValueError("symbols must be unique")
        return sorted(normalized)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, currency: str) -> str:
        return currency.upper()

    @model_validator(mode="after")
    def validate_interval(self) -> "SourceRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        return self


class SnapshotFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(gt=0)
    first_date: date
    last_date: date


class SnapshotManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int
    snapshot_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_id: str
    provider: str
    currency: str
    requested_start_date: date
    requested_end_date: date
    generated_at: datetime
    files: list[SnapshotFile]

    @field_validator("files")
    @classmethod
    def validate_unique_files(cls, files: list[SnapshotFile]) -> list[SnapshotFile]:
        symbols = [item.symbol for item in files]
        paths = [item.path for item in files]
        if len(set(symbols)) != len(symbols) or len(set(paths)) != len(paths):
            raise ValueError("manifest files must have unique symbols and paths")
        return sorted(files, key=lambda item: item.symbol)
