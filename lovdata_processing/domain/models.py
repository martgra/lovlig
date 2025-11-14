"""Domain models for the pipeline."""

from datetime import datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class FileStatus(str, Enum):
    """Status of a file in the current dataset state."""

    ADDED = "added"  # File was added in the last update
    MODIFIED = "modified"  # File content changed in the last update
    UNCHANGED = "unchanged"  # File exists but unchanged since last update
    REMOVED = "removed"  # File was removed in the last update


class FileMetadata(BaseModel):
    """Individual file metadata within an archive."""

    path: str  # Relative path within archive
    size: int  # File size in bytes
    sha256: str  # SHA256 hash of file contents
    last_changed: datetime | None = None  # Dataset API version when this file was added/modified
    status: FileStatus = FileStatus.UNCHANGED  # Current lifecycle status


class RawDatasetMetadata(BaseModel):
    """Dataset metadata."""

    filename: Path
    last_modified: datetime
    files: dict[str, FileMetadata] = Field(default_factory=dict)


class PipelineState(BaseModel):
    """Complete pipeline state."""

    raw_datasets: dict[str, RawDatasetMetadata] = Field(default_factory=dict)


class ArchiveChangeSet(BaseModel):
    """Represents changes between current and previous archive state."""

    new_files: list[str] = Field(default_factory=list)
    modified_files: list[str] = Field(default_factory=list)
    removed_files: list[str] = Field(default_factory=list)
    unchanged_files: list[str] = Field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return True if any files were added, modified, or removed."""
        return bool(self.new_files or self.modified_files or self.removed_files)

    def __repr__(self) -> str:
        """Return string representation of changeset."""
        return (
            f"ArchiveChangeSet("
            f"new={len(self.new_files)}, "
            f"modified={len(self.modified_files)}, "
            f"removed={len(self.removed_files)}, "
            f"unchanged={len(self.unchanged_files)})"
        )
