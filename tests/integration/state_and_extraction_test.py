"""Integration tests for state management + archive extraction."""

import bz2
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from lovdata_processing.config import Settings
from lovdata_processing.domain.models import FileStatus, RawDatasetMetadata
from lovdata_processing.domain.services import ArchiveProcessingService
from lovdata_processing.acquisition.extract import extract_tar_bz2_incremental
from lovdata_processing.orchestrators import ExtractionOrchestrator
from lovdata_processing.state.manager import PipelineStateManager


class TestStateAndExtraction:
    """Test full extraction flow with state updates."""

    def test_full_extraction_flow_with_state_updates(self, tmp_path):
        """Extract archive, compute hashes, update state - full cycle."""
        # Setup
        state_file = tmp_path / "state.json"
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        # Create test archive
        archive_content = tmp_path / "content"
        archive_content.mkdir()
        (archive_content / "doc1.xml").write_text("<doc>Document 1</doc>")
        (archive_content / "doc2.xml").write_text("<doc>Document 2</doc>")

        archive_path = raw_dir / "test-dataset.tar.bz2"
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                for xml_file in archive_content.glob("*.xml"):
                    tar.add(xml_file, arcname=xml_file.name)

        # Process with state manager
        with PipelineStateManager(state_file) as state:
            # Initialize dataset
            state.update_dataset_metadata(
                "test-dataset.tar.bz2",
                RawDatasetMetadata(
                    filename=Path("test-dataset.tar.bz2"),
                    last_modified=datetime(2024, 1, 1),
                    files={},
                ),
            )

            # Extract
            current_files, changeset = extract_tar_bz2_incremental(
                archive_path=archive_path,
                extract_dir=extract_dir / "test-dataset",
                dataset_version=datetime(2024, 1, 1),
                previous_files={},
            )

            # Update state
            state.update_file_metadata("test-dataset.tar.bz2", current_files)

        # Verify state was persisted
        assert state_file.exists()

        # Reload and verify
        with PipelineStateManager(state_file) as state:
            files = state.get_file_metadata("test-dataset.tar.bz2")
            assert len(files) == 2
            assert "doc1.xml" in files
            assert "doc2.xml" in files
            assert files["doc1.xml"].status == FileStatus.ADDED

    def test_re_extract_same_archive_all_unchanged(self, tmp_path):
        """Re-extracting same archive should mark all files UNCHANGED."""
        # Setup
        state_file = tmp_path / "state.json"
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        # Create archive
        archive_content = tmp_path / "content"
        archive_content.mkdir()
        (archive_content / "same.xml").write_text("<doc>Same content</doc>")

        archive_path = raw_dir / "test.tar.bz2"
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_content / "same.xml", arcname="same.xml")

        # First extraction
        with PipelineStateManager(state_file) as state:
            state.update_dataset_metadata(
                "test.tar.bz2",
                RawDatasetMetadata(
                    filename=Path("test.tar.bz2"),
                    last_modified=datetime(2024, 1, 1),
                    files={},
                ),
            )

            files1, _ = extract_tar_bz2_incremental(
                archive_path=archive_path,
                extract_dir=extract_dir / "test",
                dataset_version=datetime(2024, 1, 1),
                previous_files={},
            )
            state.update_file_metadata("test.tar.bz2", files1)

        # Second extraction
        with PipelineStateManager(state_file) as state:
            previous = state.get_file_metadata("test.tar.bz2")

            files2, changeset = extract_tar_bz2_incremental(
                archive_path=archive_path,
                extract_dir=extract_dir / "test",
                dataset_version=datetime(2024, 1, 2),
                previous_files=previous,
            )

            assert files2["same.xml"].status == FileStatus.UNCHANGED
            assert len(changeset.unchanged_files) == 1
            assert len(changeset.new_files) == 0
            assert len(changeset.modified_files) == 0

    def test_extract_modified_archive_detects_changes(self, tmp_path):
        """Extracting modified archive should detect MODIFIED files."""
        # Setup
        state_file = tmp_path / "state.json"
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        archive_content = tmp_path / "content"
        archive_content.mkdir()
        archive_path = raw_dir / "test.tar.bz2"

        # First version
        (archive_content / "file.xml").write_text("<doc>Version 1</doc>")
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_content / "file.xml", arcname="file.xml")

        # First extraction
        with PipelineStateManager(state_file) as state:
            state.update_dataset_metadata(
                "test.tar.bz2",
                RawDatasetMetadata(
                    filename=Path("test.tar.bz2"),
                    last_modified=datetime(2024, 1, 1),
                    files={},
                ),
            )

            files1, _ = extract_tar_bz2_incremental(
                archive_path=archive_path,
                extract_dir=extract_dir / "test",
                dataset_version=datetime(2024, 1, 1),
                previous_files={},
            )
            state.update_file_metadata("test.tar.bz2", files1)

        # Modify file
        (archive_content / "file.xml").write_text("<doc>Version 2 - CHANGED!</doc>")
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_content / "file.xml", arcname="file.xml")

        # Second extraction
        with PipelineStateManager(state_file) as state:
            previous = state.get_file_metadata("test.tar.bz2")

            files2, changeset = extract_tar_bz2_incremental(
                archive_path=archive_path,
                extract_dir=extract_dir / "test",
                dataset_version=datetime(2024, 1, 2),
                previous_files=previous,
            )

            assert files2["file.xml"].status == FileStatus.MODIFIED
            assert "file.xml" in changeset.modified_files

    def test_directory_naming_no_tar_suffix(self, tmp_path, sample_tar_archive):
        """REGRESSION TEST: Extracted directory should NOT have .tar suffix."""
        # This is the key test that would have caught the .tar bug
        state_file = tmp_path / "state.json"
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        extract_dir = tmp_path / "extracted"

        # Create config with test paths
        config = Settings(
            raw_data_dir=raw_dir,
            extracted_data_dir=extract_dir,
            state_file=state_file,
        )

        # Copy archive to raw dir
        archive_in_raw = raw_dir / "my-dataset.tar.bz2"
        archive_in_raw.write_bytes(sample_tar_archive.read_bytes())

        datasets = {
            "my-dataset.tar.bz2": RawDatasetMetadata(
                filename=Path("my-dataset.tar.bz2"),
                last_modified=datetime(2024, 1, 1),
                files={},
            )
        }

        with PipelineStateManager(state_file) as state:
            # Initialize dataset in state
            for key, metadata in datasets.items():
                state.update_dataset_metadata(key, metadata)

            # Process archives (this uses the directory naming logic)
            orchestrator = ExtractionOrchestrator(config)
            results = orchestrator.process_archives(
                state=state,
                datasets=datasets,
                reporter=None,
            )

        # Check directory was created correctly
        expected_dir = extract_dir / "my-dataset"  # NOT my-dataset.tar
        assert expected_dir.exists(), f"Expected {expected_dir} to exist"
        assert not (extract_dir / "my-dataset.tar").exists(), "Should NOT create .tar directory"

        # Check files were extracted to correct location
        assert (expected_dir / "file1.xml").exists()
