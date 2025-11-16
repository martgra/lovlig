"""End-to-end tests for the full pipeline.

These tests use real tar archives and test the complete workflow
without mocks, ensuring all components work together correctly.
"""

import bz2
import io
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from lovdata_processing.domain.models import FileMetadata, FileStatus, DatasetMetadata
from lovdata_processing.operations.extract import extract_tar_bz2
from lovdata_processing.state.manager import StateManager


def create_tar_bz2_with_files(archive_path: Path, files: dict[str, bytes]) -> None:
    """Create a tar.bz2 archive with the given files.

    Args:
        archive_path: Path where the archive will be created
        files: Dictionary mapping file paths to their content
    """
    with bz2.open(archive_path, "wb") as bz2_file:
        with tarfile.open(fileobj=bz2_file, mode="w") as tar:
            for file_path, content in files.items():
                tarinfo = tarfile.TarInfo(name=file_path)
                tarinfo.size = len(content)
                tarinfo.mtime = datetime(2024, 1, 1).timestamp()
                tar.addfile(tarinfo, fileobj=io.BytesIO(content))


@pytest.fixture
def real_archive_set(tmp_path):
    """Create a realistic set of archives for E2E testing."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Archive 1: Initial dataset
    archive1_path = raw_dir / "legal-docs-v1.tar.bz2"
    create_tar_bz2_with_files(
        archive1_path,
        {
            "doc_0.xml": b'<?xml version="1.0"?>\n<document id="0">Content 0</document>',
            "doc_1.xml": b'<?xml version="1.0"?>\n<document id="1">Content 1</document>',
            "doc_2.xml": b'<?xml version="1.0"?>\n<document id="2">Content 2</document>',
        },
    )

    # Archive 2: Updated dataset with changes
    archive2_path = raw_dir / "legal-docs-v2.tar.bz2"
    create_tar_bz2_with_files(
        archive2_path,
        {
            "doc_0.xml": b'<?xml version="1.0"?>\n<document id="0">Content 0</document>',  # unchanged
            "doc_1.xml": b'<?xml version="1.0"?>\n<document id="1">MODIFIED Content 1</document>',  # modified
            "doc_3.xml": b'<?xml version="1.0"?>\n<document id="3">NEW Content 3</document>',  # added
            # doc_2.xml removed
        },
    )

    return {
        "raw_dir": raw_dir,
        "archive1": archive1_path,
        "archive2": archive2_path,
    }


class TestFullPipelineWorkflow:
    """Test the complete end-to-end pipeline workflow."""

    def test_initial_extraction_creates_files(self, real_archive_set, tmp_path):
        """Test initial extraction creates files correctly."""
        extract_dir = tmp_path / "extracted"
        dataset_dir = extract_dir / "dataset1"

        # Extract the archive
        result = extract_tar_bz2(
            archive_path=real_archive_set["archive1"],
            extract_dir=dataset_dir,
            previous_files={},
            dataset_version=datetime(2024, 1, 1),
        )

        # Verify extraction
        assert (dataset_dir / "doc_0.xml").exists()
        assert (dataset_dir / "doc_1.xml").exists()
        assert (dataset_dir / "doc_2.xml").exists()

        # Verify result structure
        current_files, changeset = result
        assert len(current_files) == 3
        assert len(changeset.new_files) == 3

    def test_extraction_with_state_persistence(self, real_archive_set, tmp_path):
        """Test that extraction results can be persisted to state."""
        extract_dir = tmp_path / "extracted"
        state_file = tmp_path / "state.json"
        dataset_key = "legal-docs-v1.tar.bz2"
        dataset_dir = extract_dir / "legal-docs-v1"

        # Extract archive
        current_files, _ = extract_tar_bz2(
            archive_path=real_archive_set["archive1"],
            extract_dir=dataset_dir,
            previous_files={},
            dataset_version=datetime(2024, 1, 1),
        )

        # Save to state
        with StateManager(state_file) as manager:
            dataset_metadata = DatasetMetadata(
                filename=Path(dataset_key),
                last_modified=datetime(2024, 1, 1),
                files=current_files,
            )
            manager.data.raw_datasets[dataset_key] = dataset_metadata

        # Verify state persisted
        assert state_file.exists()

        # Load state in new session
        with StateManager(state_file) as manager:
            assert dataset_key in manager.data.raw_datasets
            loaded_files = manager.data.raw_datasets[dataset_key].files
            assert len(loaded_files) == 3
            assert all(f.status == FileStatus.ADDED for f in loaded_files.values())

    def test_reprocessing_detects_changes(self, real_archive_set, tmp_path):
        """Test that reprocessing the same dataset detects changes."""
        extract_dir = tmp_path / "extracted"
        state_file = tmp_path / "state.json"
        dataset_key = "legal-docs.tar.bz2"
        dataset_dir = extract_dir / "legal-docs"

        # First extraction
        current_files_v1, _ = extract_tar_bz2(
            archive_path=real_archive_set["archive1"],
            extract_dir=dataset_dir,
            previous_files={},
            dataset_version=datetime(2024, 1, 1),
        )

        # Second extraction with modified archive
        current_files_v2, changeset_v2 = extract_tar_bz2(
            archive_path=real_archive_set["archive2"],
            extract_dir=dataset_dir,
            previous_files=current_files_v1,
            dataset_version=datetime(2024, 1, 15),
        )

        # Verify changes detected
        assert len(changeset_v2.new_files) == 1  # doc_3.xml
        assert "doc_3.xml" in changeset_v2.new_files

        assert len(changeset_v2.modified_files) == 1  # doc_1.xml
        assert "doc_1.xml" in changeset_v2.modified_files

        assert len(changeset_v2.unchanged_files) == 1  # doc_0.xml
        assert "doc_0.xml" in changeset_v2.unchanged_files

        assert len(changeset_v2.removed_files) == 1  # doc_2.xml
        assert "doc_2.xml" in changeset_v2.removed_files

    def test_nested_file_structure_preserved(self, tmp_path):
        """Test that nested directory structures are preserved."""
        archive_path = tmp_path / "nested.tar.bz2"
        create_tar_bz2_with_files(
            archive_path,
            {
                "subdir/nested.xml": b"<doc>Nested content</doc>",
                "subdir/deeper/file.xml": b"<doc>Deeper content</doc>",
            },
        )

        extract_dir = tmp_path / "extracted"

        extract_tar_bz2(
            archive_path=archive_path,
            extract_dir=extract_dir,
            previous_files={},
            dataset_version=datetime(2024, 1, 1),
        )

        # Verify nested structure
        assert (extract_dir / "subdir" / "nested.xml").exists()
        assert (extract_dir / "subdir" / "deeper" / "file.xml").exists()
        assert (extract_dir / "subdir" / "nested.xml").read_text() == "<doc>Nested content</doc>"

    def test_full_cycle_extract_persist_reload(self, real_archive_set, tmp_path):
        """Test complete cycle: extract → persist to state → reload from state → re-extract."""
        extract_dir = tmp_path / "extracted"
        state_file = tmp_path / "state.json"
        dataset_key = "legal-docs.tar.bz2"
        dataset_dir = extract_dir / "legal-docs"

        # Cycle 1: Extract and persist
        current_files, _ = extract_tar_bz2(
            archive_path=real_archive_set["archive1"],
            extract_dir=dataset_dir,
            previous_files={},
            dataset_version=datetime(2024, 1, 1),
        )

        with StateManager(state_file) as manager:
            dataset_metadata = DatasetMetadata(
                filename=Path(dataset_key),
                last_modified=datetime(2024, 1, 1),
                files=current_files,
            )
            manager.data.raw_datasets[dataset_key] = dataset_metadata

        # Cycle 2: Reload and re-extract (unchanged)
        with StateManager(state_file) as manager:
            previous_files = manager.data.raw_datasets[dataset_key].files

        current_files_v2, changeset_v2 = extract_tar_bz2(
            archive_path=real_archive_set["archive1"],
            extract_dir=dataset_dir,
            previous_files=previous_files,
            dataset_version=datetime(2024, 1, 1),
        )

        # All files should be unchanged
        assert len(changeset_v2.unchanged_files) == 3
        assert len(changeset_v2.new_files) == 0
        assert len(changeset_v2.modified_files) == 0
        assert len(changeset_v2.removed_files) == 0
