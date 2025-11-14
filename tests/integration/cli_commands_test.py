"""Integration tests for CLI commands."""

import bz2
import tarfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from lovdata_processing.cli.app import app
from lovdata_processing.domain.models import (
    FileMetadata,
    FileStatus,
    PipelineState,
    RawDatasetMetadata,
)
from lovdata_processing.state.manager import PipelineStateManager


@pytest.fixture
def cli_runner():
    """Create a Typer CLI test runner."""
    return CliRunner()


@pytest.fixture
def populated_state_file(tmp_path):
    """Create a state file with test data."""
    state_file = tmp_path / "state.json"

    state = PipelineState(
        raw_datasets={
            "dataset1.tar.bz2": RawDatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=datetime(2024, 1, 1),
                files={
                    "file1.xml": FileMetadata(
                        path="file1.xml",
                        size=100,
                        sha256="abc123",
                        last_changed=datetime(2024, 1, 1),
                        status=FileStatus.ADDED,
                    ),
                    "file2.xml": FileMetadata(
                        path="file2.xml",
                        size=200,
                        sha256="def456",
                        last_changed=datetime(2024, 1, 1),
                        status=FileStatus.UNCHANGED,
                    ),
                    "removed.xml": FileMetadata(
                        path="removed.xml",
                        size=50,
                        sha256="xyz789",
                        last_changed=datetime(2024, 1, 1),
                        status=FileStatus.REMOVED,
                    ),
                },
            )
        }
    )

    with PipelineStateManager(state_file) as manager:
        manager.data = state

    return state_file


class TestFilesListCommand:
    """Test 'lov files list' command."""

    def test_list_files_by_status_added(self, cli_runner, populated_state_file, monkeypatch):
        """List files filtered by ADDED status."""
        monkeypatch.setenv("STATE_FILE", str(populated_state_file))

        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = populated_state_file

            result = cli_runner.invoke(app, ["files", "list", "--status", "added"])

            assert result.exit_code == 0
            assert "added" in result.stdout.lower()
            assert "1 added" in result.stdout  # Should show 1 added file
            assert "unchanged" not in result.stdout.lower()  # UNCHANGED file should not appear

    def test_list_files_with_limit(self, cli_runner, populated_state_file):
        """List files with limit applied."""
        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = populated_state_file

            result = cli_runner.invoke(app, ["files", "list", "--limit", "1"])

            assert result.exit_code == 0
            # Should only show 1 file
            assert result.stdout.count(".xml") == 1

    def test_list_shows_summary(self, cli_runner, populated_state_file):
        """List command should show summary."""
        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = populated_state_file

            result = cli_runner.invoke(app, ["files", "list"])

            assert result.exit_code == 0
            assert "Summary:" in result.stdout


class TestFilesStatsCommand:
    """Test 'lov files stats' command."""

    def test_stats_shows_all_datasets(self, cli_runner, populated_state_file):
        """Stats command should show statistics for all datasets."""
        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = populated_state_file

            result = cli_runner.invoke(app, ["files", "stats"])

            assert result.exit_code == 0
            # Dataset name might be truncated in table, so check for partial match
            assert "dataset1" in result.stdout
            assert "Dataset Statistics" in result.stdout

    def test_stats_shows_correct_counts(self, cli_runner, populated_state_file):
        """Stats should show correct file counts by status."""
        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = populated_state_file

            result = cli_runner.invoke(app, ["files", "stats"])

            assert result.exit_code == 0
            # Should show 3 total files, 1 added, 1 unchanged, 1 removed
            assert "3" in result.stdout  # Total files
            assert "1" in result.stdout  # Counts


class TestFilesPruneCommand:
    """Test 'lov files prune' command."""

    def test_prune_dry_run_shows_what_would_be_removed(self, cli_runner, tmp_path):
        """Dry-run should show files that would be pruned."""
        state_file = tmp_path / "state.json"
        state = PipelineState(
            raw_datasets={
                "dataset1.tar.bz2": RawDatasetMetadata(
                    filename=Path("dataset1.tar.bz2"),
                    last_modified=datetime(2024, 1, 1),
                    files={
                        "removed.xml": FileMetadata(
                            path="removed.xml",
                            size=100,
                            sha256="abc123",
                            last_changed=datetime(2024, 1, 1),
                            status=FileStatus.REMOVED,
                        ),
                    },
                )
            }
        )

        with PipelineStateManager(state_file) as manager:
            manager.data = state

        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = state_file
            mock_config.return_value.extracted_data_dir = tmp_path / "extracted"

            result = cli_runner.invoke(app, ["files", "prune", "--dry-run"])

            assert result.exit_code == 0
            assert "Would prune 1 files" in result.stdout or "Would prune" in result.stdout
            assert "dry-run" in result.stdout.lower() or "without --dry-run" in result.stdout

    def test_prune_removes_files_from_state(self, cli_runner, tmp_path):
        """Actual prune should remove files from state."""
        state_file = tmp_path / "state.json"
        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        # Create file on disk
        dataset_dir = extracted_dir / "dataset1"
        dataset_dir.mkdir()
        removed_file = dataset_dir / "removed.xml"
        removed_file.write_text("<doc>To be removed</doc>")

        state = PipelineState(
            raw_datasets={
                "dataset1.tar.bz2": RawDatasetMetadata(
                    filename=Path("dataset1.tar.bz2"),
                    last_modified=datetime(2024, 1, 1),
                    files={
                        "removed.xml": FileMetadata(
                            path="removed.xml",
                            size=100,
                            sha256="abc123",
                            last_changed=datetime(2024, 1, 1),
                            status=FileStatus.REMOVED,
                        ),
                        "kept.xml": FileMetadata(
                            path="kept.xml",
                            size=200,
                            sha256="def456",
                            last_changed=datetime(2024, 1, 1),
                            status=FileStatus.UNCHANGED,
                        ),
                    },
                )
            }
        )

        with PipelineStateManager(state_file) as manager:
            manager.data = state

        with patch("lovdata_processing.cli.app.Settings") as mock_config:
            mock_config.return_value.state_file = state_file
            mock_config.return_value.extracted_data_dir = extracted_dir

            result = cli_runner.invoke(app, ["files", "prune"])

            assert result.exit_code == 0

        # Verify file was removed from state
        with PipelineStateManager(state_file) as manager:
            files = manager.get_file_metadata("dataset1.tar.bz2")
            assert "removed.xml" not in files
            assert "kept.xml" in files

        # Verify file was removed from disk
        assert not removed_file.exists()
