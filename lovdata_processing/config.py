"""Pipeline configuration management."""

import os
from pathlib import Path

from pydantic import BaseModel, Field


def _default_hash_workers() -> int:
    """Return a sensible default for hash worker threads."""
    cpu_count = os.cpu_count() or 1
    return max(1, min(32, cpu_count))


class PipelineConfig(BaseModel):
    """Configuration for the lovdata processing pipeline."""

    # API settings
    api_url: str = "https://api.lovdata.no"
    dataset_filter: str | None = "gjeldende"  # Filter datasets by name (None = all datasets)

    # Directory settings
    raw_data_dir: Path = Path("data/raw")
    extracted_data_dir: Path = Path("data/extracted")

    # State management
    state_file: Path = Path("state.json")

    # Performance tuning
    max_hash_workers: int = Field(default_factory=_default_hash_workers, ge=1, le=64)
    max_download_concurrency: int = Field(default=4, ge=1, le=16)
