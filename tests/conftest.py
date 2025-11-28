"""Configure tests."""

import bz2
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from lovlig.domain.models import (
    FileMetadata,
    FileStatus,
    State,
    DatasetMetadata,
)


@pytest.fixture
def tmp_state_file(tmp_path):
    """Create a temporary state file path."""
    return tmp_path / "state.json"


@pytest.fixture
def sample_tar_archive(tmp_path):
    """Create a small test tar.bz2 archive with known content."""
    archive_dir = tmp_path / "archive_content"
    archive_dir.mkdir()

    # Create test files with known content
    (archive_dir / "file1.xml").write_text("<doc>File 1 content</doc>")
    (archive_dir / "file2.xml").write_text("<doc>File 2 content</doc>")
    (archive_dir / "file3.xml").write_text("<doc>File 3 content</doc>")

    # Create tar.bz2 archive
    archive_path = tmp_path / "test-dataset.tar.bz2"
    with bz2.open(archive_path, "wb") as bz2_file:
        with tarfile.open(fileobj=bz2_file, mode="w") as tar:
            for xml_file in archive_dir.glob("*.xml"):
                tar.add(xml_file, arcname=xml_file.name)

    return archive_path


@pytest.fixture
def sample_datasets():
    """Create sample dataset metadata."""
    return {
        "test-dataset.tar.bz2": DatasetMetadata(
            filename=Path("test-dataset.tar.bz2"),
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            files={},
        ),
        "another-dataset.tar.bz2": DatasetMetadata(
            filename=Path("another-dataset.tar.bz2"),
            last_modified=datetime(2024, 1, 2, 12, 0, 0),
            files={},
        ),
    }


@pytest.fixture
def sample_pipeline_state():
    """Create a sample pipeline state with files."""
    return State(
        raw_datasets={
            "dataset1.tar.bz2": DatasetMetadata(
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
                    "file3.xml": FileMetadata(
                        path="file3.xml",
                        size=150,
                        sha256="ghi789",
                        last_changed=datetime(2024, 1, 1),
                        status=FileStatus.REMOVED,
                    ),
                },
            ),
            "dataset2.tar.bz2": DatasetMetadata(
                filename=Path("dataset2.tar.bz2"),
                last_modified=datetime(2024, 1, 2),
                files={
                    "fileA.xml": FileMetadata(
                        path="fileA.xml",
                        size=300,
                        sha256="jkl012",
                        last_changed=datetime(2024, 1, 2),
                        status=FileStatus.MODIFIED,
                    ),
                },
            ),
        }
    )
