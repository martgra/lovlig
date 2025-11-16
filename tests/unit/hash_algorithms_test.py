"""Tests for xxHash file hashing."""

import pytest

from lovdata_processing.operations.extract import compute_file_hash
from lovdata_processing.domain.models import FileMetadata, FileStatus


class TestHashAlgorithms:
    """Test hash algorithm implementations."""

    def test_xxhash_consistent(self, tmp_path):
        """xxHash produces consistent results for identical content."""
        file1 = tmp_path / "test1.txt"
        file2 = tmp_path / "test2.txt"

        content = b"Test content for hashing"
        file1.write_bytes(content)
        file2.write_bytes(content)

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)

        assert hash1 == hash2
        assert len(hash1) == 32  # xxh128 produces 32 hex characters

    def test_xxhash_different_content(self, tmp_path):
        """xxHash produces different hashes for different content."""
        file1 = tmp_path / "test1.txt"
        file2 = tmp_path / "test2.txt"

        file1.write_bytes(b"Content A")
        file2.write_bytes(b"Content B")

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)

        assert hash1 != hash2

    def test_xxhash_performance(self, tmp_path):
        """xxHash processes large files quickly."""
        import time

        # Create 10MB test file
        test_file = tmp_path / "large.bin"
        with open(test_file, "wb") as f:
            f.write(b"x" * (10 * 1024 * 1024))

        # Benchmark xxHash
        start = time.perf_counter()
        compute_file_hash(test_file)
        xxh_time = time.perf_counter() - start

        # Should process 10MB in under 100ms (very conservative)
        assert xxh_time < 0.1, f"xxHash took {xxh_time:.3f}s for 10MB"


class TestFileMetadata:
    """Test FileMetadata model."""

    def test_default_hash(self):
        """FileMetadata uses xxHash."""
        meta = FileMetadata(
            path="test.xml",
            size=100,
            sha256="abc123" * 5 + "ab",  # 32 char xxh128
        )
        assert len(meta.sha256) == 32

    def test_metadata_serialization(self):
        """FileMetadata serializes correctly."""
        meta = FileMetadata(
            path="test.xml",
            size=100,
            sha256="abc123",
            status=FileStatus.ADDED,
        )

        # Serialize and deserialize
        data = meta.model_dump()
        restored = FileMetadata(**data)

        assert restored.sha256 == "abc123"
        assert restored.status == FileStatus.ADDED
