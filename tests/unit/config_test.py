"""Unit tests for configuration management."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from lovlig.config import Settings


class TestSettings:
    """Test configuration."""

    def test_defaults(self, tmp_path):
        """Test default settings."""
        os.chdir(tmp_path)
        settings = Settings()

        assert settings.api_url == "https://api.lovdata.no"
        assert settings.api_timeout == 30
        assert settings.dataset_filter == "gjeldende"
        assert settings.max_hash_workers >= 1
        assert settings.max_download_concurrency == 4

    def test_null_filter(self, tmp_path):
        """Test dataset_filter null conversion."""
        os.chdir(tmp_path)

        assert Settings(dataset_filter="null").dataset_filter is None
        assert Settings(dataset_filter="none").dataset_filter is None
        assert Settings(dataset_filter="").dataset_filter is None
        assert Settings(dataset_filter="gjeldende").dataset_filter == "gjeldende"

    def test_dirs_created(self, tmp_path):
        """Test directories are auto-created."""
        os.chdir(tmp_path)

        raw = tmp_path / "raw"
        extracted = tmp_path / "extracted"
        state = tmp_path / "state" / "state.json"

        Settings(raw_data_dir=raw, extracted_data_dir=extracted, state_file=state)

        assert raw.exists()
        assert extracted.exists()
        assert state.parent.exists()

    def test_env_loading(self, tmp_path, monkeypatch):
        """Test loading from environment."""
        os.chdir(tmp_path)

        monkeypatch.setenv("LOVDATA_API_TIMEOUT", "60")
        monkeypatch.setenv("LOVDATA_DATASET_FILTER", "custom")
        monkeypatch.setenv("LOVDATA_MAX_HASH_WORKERS", "8")

        settings = Settings()

        assert settings.api_timeout == 60
        assert settings.dataset_filter == "custom"
        assert settings.max_hash_workers == 8

    def test_programmatic_override(self, tmp_path, monkeypatch):
        """Test programmatic override of env."""
        os.chdir(tmp_path)

        monkeypatch.setenv("LOVDATA_API_TIMEOUT", "60")

        settings = Settings(api_timeout=90)
        assert settings.api_timeout == 90
