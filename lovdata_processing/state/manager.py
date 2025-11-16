"""State persistence for tracking pipeline data."""

from logging import Logger
from pathlib import Path
from typing import Any

import orjson
from atomicwrites import atomic_write

from lovdata_processing.domain.models import FileMetadata, PipelineState, RawDatasetMetadata

logger = Logger(__file__)


class PipelineStateManager:
    """Context manager for tracking file state using hashes.

    Maintains a JSON file mapping file paths to their hash values,
    with atomic writes to prevent corruption.

    Example:
        with FileState("state.json") as state:
            if not state.has_file("path/to/file") or state.get_hash("path/to/file") != new_hash:
                # Process file
                state.update_state("path/to/file", new_hash)
    """

    def __init__(self, path: str | Path):
        """Initialize FileState manager.

        Args:
            path: Path to the state JSON file
        """
        self.path = Path(path)
        self.data: PipelineState = PipelineState()
        self._loaded = False

    def __enter__(self) -> "PipelineStateManager":
        """Enter context manager, loading existing state if available."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists():
            try:
                content = self.path.read_bytes()
                json_data = orjson.loads(content)
                sanitized = self._sanitize_raw_state(json_data)
                self.data = PipelineState.model_validate(sanitized)
            except orjson.JSONDecodeError as e:
                logger.error(f"Failed to parse state file {self.path}: {e}")
                raise
            except OSError as e:
                logger.error(f"Failed to read state file {self.path}: {e}")
                raise
        else:
            logger.debug(f"No existing state file at {self.path}, starting fresh")

        self._loaded = True
        return self

    def get_file_metadata(self, dataset_key: str) -> dict[str, FileMetadata]:
        """Get file metadata for a dataset.

        Args:
            dataset_key: Key identifying the dataset

        Returns:
            Dictionary of file metadata, empty if dataset not found
        """
        if dataset_key in self.data.raw_datasets:
            return self.data.raw_datasets[dataset_key].files
        return {}

    def update_file_metadata(self, dataset_key: str, files: dict[str, FileMetadata]) -> None:
        """Update file metadata for a dataset.

        Args:
            dataset_key: Key identifying the dataset
            files: New file metadata to store
        """
        if dataset_key in self.data.raw_datasets:
            self.data.raw_datasets[dataset_key].files = files
        else:
            logger.warning(f"Dataset {dataset_key} not in state, cannot update file metadata")

    def update_dataset_metadata(self, dataset_key: str, metadata: RawDatasetMetadata) -> None:
        """Update or add dataset metadata.

        Args:
            dataset_key: Key identifying the dataset
            metadata: Dataset metadata to store
        """
        if dataset_key in self.data.raw_datasets:
            # Preserve existing file metadata
            existing_files = self.data.raw_datasets[dataset_key].files
            self.data.raw_datasets[dataset_key] = metadata
            self.data.raw_datasets[dataset_key].files = existing_files
        else:
            self.data.raw_datasets[dataset_key] = metadata

    def __exit__(self, exc_type, _exc_value, _traceback) -> bool:
        """Exit context manager, saving state if no exceptions occurred.

        Args:
            exc_type: Exception type if raised
            exc_value: Exception value if raised
            traceback: Exception traceback if raised

        Returns:
            False to propagate any exceptions
        """
        if exc_type is None:
            try:
                payload = orjson.dumps(
                    self.data.model_dump(mode="json"),
                    option=orjson.OPT_INDENT_2,
                )
                with atomic_write(self.path, mode="wb", overwrite=True) as f:
                    f.write(payload)
                    f.write(b"\n")  # Add trailing newline
            except OSError as e:
                logger.error(f"Failed to write state file {self.path}: {e}")
                raise

        return False  # Don't suppress exceptions

    @classmethod
    def _sanitize_raw_state(_cls, payload: Any) -> dict[str, Any]:
        """Ensure schema-compatibility of state data."""
        if not isinstance(payload, dict):
            return {"raw_datasets": {}}

        raw_datasets = payload.get("raw_datasets")
        if not isinstance(raw_datasets, dict):
            raw_datasets = {}

        # Validate structure but don't modify data
        sanitized_payload = dict(payload)
        sanitized_payload["raw_datasets"] = raw_datasets
        return sanitized_payload
