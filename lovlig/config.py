"""Pipeline configuration with environment variable support."""

import os
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_hash_workers() -> int:
    """Return a sensible default for hash worker threads."""
    cpu_count = os.cpu_count() or 1
    return max(1, min(32, cpu_count))


class Settings(BaseSettings):
    """Pipeline configuration loaded from environment variables.

    Loads from environment (LOVDATA_*), .env file, or defaults.
    """

    model_config = SettingsConfigDict(
        env_prefix="LOVDATA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API settings
    api_url: str = "https://api.lovdata.no"
    api_timeout: int = 30
    dataset_filter: str | None = "gjeldende"

    # Directories
    raw_data_dir: Path = Path("data/raw")
    extracted_data_dir: Path = Path("data/extracted")
    state_file: Path = Path("data/state.json")

    # Performance
    max_hash_workers: int = Field(default_factory=_default_hash_workers)
    max_download_concurrency: int = 4

    @field_validator("dataset_filter", mode="before")
    @classmethod
    def parse_null_filter(cls, v: str | None) -> str | None:
        """Convert 'null' string to None."""
        if isinstance(v, str) and v.lower() in ("null", "none", ""):
            return None
        return v

    @field_validator("raw_data_dir", "extracted_data_dir", "state_file", mode="after")
    @classmethod
    def create_dirs(cls, v: Path) -> Path:
        """Create directories if they don't exist."""
        if v.name.endswith(".json"):  # state_file
            v.parent.mkdir(parents=True, exist_ok=True)
        else:  # directories
            v.mkdir(parents=True, exist_ok=True)
        return v.resolve()
