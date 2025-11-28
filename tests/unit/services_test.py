"""Unit tests for business logic services."""

from datetime import datetime
from pathlib import Path

import pytest

from lovlig.domain.models import (
    FileMetadata,
    FileStatus,
    State,
    DatasetMetadata,
)
from lovlig.domain.services import (
    DatasetUpdateService,
    FileManagementService,
    FileQueryService,
)


class TestDatasetUpdateService:
    """Test dataset update logic."""

    def test_returns_new_datasets(self):
        """New datasets should be flagged for update."""
        service = DatasetUpdateService()
        current = {
            "dataset1.tar.bz2": DatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=datetime(2024, 1, 1),
                files={},
            )
        }
        previous = {}

        to_update = service.get_datasets_to_update(current, previous)

        assert "dataset1.tar.bz2" in to_update
        assert len(to_update) == 1

    def test_returns_datasets_with_changed_timestamps(self):
        """Datasets with updated timestamps should be flagged."""
        service = DatasetUpdateService()
        current = {
            "dataset1.tar.bz2": DatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=datetime(2024, 1, 2),  # Changed
                files={},
            )
        }
        previous = {
            "dataset1.tar.bz2": DatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=datetime(2024, 1, 1),
                files={},
            )
        }

        to_update = service.get_datasets_to_update(current, previous)

        assert "dataset1.tar.bz2" in to_update

    def test_skips_unchanged_datasets(self):
        """Datasets with same timestamps should not be updated."""
        service = DatasetUpdateService()
        timestamp = datetime(2024, 1, 1)
        current = {
            "dataset1.tar.bz2": DatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=timestamp,
                files={},
            )
        }
        previous = {
            "dataset1.tar.bz2": DatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=timestamp,
                files={},
            )
        }

        to_update = service.get_datasets_to_update(current, previous)

        assert len(to_update) == 0


class TestFileQueryService:
    """Test file querying and statistics."""

    def test_filter_by_status_added(self, sample_pipeline_state):
        """Filter files by ADDED status."""
        service = FileQueryService()

        results = service.get_files_by_filter(sample_pipeline_state, status="added")

        assert len(results) == 1
        assert results[0]["path"] == "file1.xml"
        assert results[0]["status"] == "added"

    def test_filter_by_status_removed(self, sample_pipeline_state):
        """Filter files by REMOVED status."""
        service = FileQueryService()

        results = service.get_files_by_filter(sample_pipeline_state, status="removed")

        assert len(results) == 1
        assert results[0]["path"] == "file3.xml"

    def test_filter_by_status_changed(self, sample_pipeline_state):
        """'changed' meta-status includes ADDED and MODIFIED."""
        service = FileQueryService()

        results = service.get_files_by_filter(sample_pipeline_state, status="changed")

        assert len(results) == 2  # file1 (added) + fileA (modified)
        statuses = {r["status"] for r in results}
        assert statuses == {"added", "modified"}

    def test_filter_by_dataset_name(self, sample_pipeline_state):
        """Filter files by dataset name."""
        service = FileQueryService()

        results = service.get_files_by_filter(sample_pipeline_state, dataset="dataset1")

        assert len(results) == 3
        assert all(r["dataset"] == "dataset1.tar.bz2" for r in results)

    def test_limit_results(self, sample_pipeline_state):
        """Limit number of results returned."""
        service = FileQueryService()

        results = service.get_files_by_filter(sample_pipeline_state, limit=2)

        assert len(results) == 2

    def test_get_dataset_statistics(self, sample_pipeline_state):
        """Calculate correct statistics per dataset."""
        service = FileQueryService()

        stats = service.get_dataset_statistics(sample_pipeline_state)

        assert "dataset1.tar.bz2" in stats
        dataset1_stats = stats["dataset1.tar.bz2"]
        assert dataset1_stats["total"] == 3
        assert dataset1_stats["added"] == 1
        assert dataset1_stats["unchanged"] == 1
        assert dataset1_stats["removed"] == 1
        assert dataset1_stats["modified"] == 0

    def test_statistics_exclude_removed_from_size(self, sample_pipeline_state):
        """Total size should exclude REMOVED files."""
        service = FileQueryService()

        stats = service.get_dataset_statistics(sample_pipeline_state)

        dataset1_stats = stats["dataset1.tar.bz2"]
        # file1 (100) + file2 (200), file3 (150) is REMOVED
        assert dataset1_stats["total_size"] == 300


class TestFileManagementService:
    """Test file management operations."""

    def test_prune_identifies_removed_files(self, sample_pipeline_state, tmp_path):
        """Prune should identify files marked as REMOVED."""
        service = FileManagementService()

        result = service.prune_removed_files(
            sample_pipeline_state, tmp_path, dry_run=True
        )

        assert result["total_removed_files"] == 1
        assert "dataset1.tar.bz2" in result["datasets_pruned"]

    def test_prune_dry_run_doesnt_delete(self, sample_pipeline_state, tmp_path):
        """Dry-run should not modify state or delete files."""
        service = FileManagementService()
        original_files = len(sample_pipeline_state.raw_datasets["dataset1.tar.bz2"].files)

        service.prune_removed_files(sample_pipeline_state, tmp_path, dry_run=True)

        # State unchanged
        assert (
            len(sample_pipeline_state.raw_datasets["dataset1.tar.bz2"].files)
            == original_files
        )

    def test_prune_removes_from_state(self, sample_pipeline_state, tmp_path):
        """Actual prune should remove files from state."""
        service = FileManagementService()

        service.prune_removed_files(sample_pipeline_state, tmp_path, dry_run=False)

        files = sample_pipeline_state.raw_datasets["dataset1.tar.bz2"].files
        assert "file3.xml" not in files
        assert "file1.xml" in files  # Non-removed files remain

    def test_prune_deletes_from_disk(self, sample_pipeline_state, tmp_path):
        """Actual prune should delete files from disk."""
        service = FileManagementService()

        # Create the file on disk
        dataset_dir = tmp_path / "dataset1"
        dataset_dir.mkdir()
        removed_file = dataset_dir / "file3.xml"
        removed_file.write_text("content")

        result = service.prune_removed_files(sample_pipeline_state, tmp_path, dry_run=False)

        assert result["total_deleted_files"] == 1
        assert not removed_file.exists()
