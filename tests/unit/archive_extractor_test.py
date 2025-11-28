"""Unit tests for archive extraction and directory naming."""

import bz2
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from lovlig.domain.models import FileMetadata, FileStatus
from lovlig.operations.extract import (
    compute_file_hash,
    extract_tar_bz2_incremental,
)


class TestDirectoryNaming:
    """Test that extracted directories are named correctly (regression test for .tar bug)."""

    def test_directory_name_without_tar_suffix(self, tmp_path):
        """Extracted directory should be 'dataset-name' not 'dataset-name.tar'."""
        # Create archive
        archive_dir = tmp_path / "content"
        archive_dir.mkdir()
        (archive_dir / "test.xml").write_text("<doc>Test</doc>")

        archive_path = tmp_path / "my-dataset.tar.bz2"
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "test.xml", arcname="test.xml")

        extract_dir = tmp_path / "extracted"

        # Extract
        extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 1),
            previous_files={},
        )

        # Verify directory structure
        assert extract_dir.exists()
        assert (extract_dir / "test.xml").exists()
        # Should NOT create my-dataset.tar directory
        assert not (extract_dir.parent / "my-dataset.tar").exists()


class TestFileStatusDetection:
    """Test file status detection logic."""

    def test_new_files_marked_as_added(self, tmp_path):
        """New files should be marked with ADDED status."""
        archive_path = tmp_path / "test.tar.bz2"
        archive_dir = tmp_path / "content"
        archive_dir.mkdir()
        (archive_dir / "new.xml").write_text("<doc>New file</doc>")

        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "new.xml", arcname="new.xml")

        extract_dir = tmp_path / "extracted"

        current_files, changeset = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 1),
            previous_files={},
        )

        assert current_files["new.xml"].status == FileStatus.ADDED
        assert "new.xml" in changeset.new_files

    def test_unchanged_files_marked_correctly(self, tmp_path):
        """Files with same hash should be marked UNCHANGED."""
        archive_path = tmp_path / "test.tar.bz2"
        archive_dir = tmp_path / "content"
        archive_dir.mkdir()
        content = "<doc>Same content</doc>"
        (archive_dir / "same.xml").write_text(content)

        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "same.xml", arcname="same.xml")

        extract_dir = tmp_path / "extracted"

        # First extraction
        first_files, _ = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 1),
            previous_files={},
        )

        # Second extraction with same content
        current_files, changeset = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 2),
            previous_files=first_files,
        )

        assert current_files["same.xml"].status == FileStatus.UNCHANGED
        assert "same.xml" in changeset.unchanged_files

    def test_modified_files_detected(self, tmp_path):
        """Files with different hash should be marked MODIFIED."""
        archive_dir = tmp_path / "content"
        archive_dir.mkdir()

        # Create first version
        (archive_dir / "modified.xml").write_text("<doc>Version 1</doc>")
        archive_path = tmp_path / "test.tar.bz2"

        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "modified.xml", arcname="modified.xml")

        extract_dir = tmp_path / "extracted"

        # First extraction
        first_files, _ = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 1),
            previous_files={},
        )

        # Modify content
        (archive_dir / "modified.xml").write_text("<doc>Version 2 - changed!</doc>")
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "modified.xml", arcname="modified.xml")

        # Second extraction
        current_files, changeset = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 2),
            previous_files=first_files,
        )

        assert current_files["modified.xml"].status == FileStatus.MODIFIED
        assert "modified.xml" in changeset.modified_files

    def test_removed_files_kept_in_state(self, tmp_path):
        """Removed files should be kept in state with REMOVED status."""
        archive_dir = tmp_path / "content"
        archive_dir.mkdir()

        # Create first archive with file
        (archive_dir / "will_be_removed.xml").write_text("<doc>Temporary</doc>")
        archive_path = tmp_path / "test.tar.bz2"

        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                tar.add(archive_dir / "will_be_removed.xml", arcname="will_be_removed.xml")

        extract_dir = tmp_path / "extracted"

        # First extraction
        first_files, _ = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 1),
            previous_files={},
        )

        # Create second archive without the file
        (archive_dir / "will_be_removed.xml").unlink()
        with bz2.open(archive_path, "wb") as bz2_file:
            with tarfile.open(fileobj=bz2_file, mode="w") as tar:
                pass  # Empty archive

        # Second extraction
        current_files, changeset = extract_tar_bz2_incremental(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=datetime(2024, 1, 2),
            previous_files=first_files,
        )

        assert "will_be_removed.xml" in current_files
        assert current_files["will_be_removed.xml"].status == FileStatus.REMOVED
        assert "will_be_removed.xml" in changeset.removed_files


class TestHashComputation:
    """Test hash computation consistency."""

    def test_hash_is_consistent(self, tmp_path):
        """Same content should produce same hash."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        content = "Test content for hashing"

        file1.write_text(content)
        file2.write_text(content)

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)

        assert hash1 == hash2
        assert len(hash1) == 32  # xxh128 is 32 hex characters

    def test_different_content_different_hash(self, tmp_path):
        """Different content should produce different hashes."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"

        file1.write_text("Content A")
        file2.write_text("Content B")

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)

        assert hash1 != hash2
