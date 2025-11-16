"""Business logic services for the pipeline."""

from datetime import datetime
from pathlib import Path

from lovdata_processing.domain.models import (
    ArchiveChangeSet,
    DatasetMetadata,
    FileMetadata,
    FileStatus,
    State,
)
from lovdata_processing.domain.types import ExtractionProgressHook


class DatasetUpdateService:
    """Service for determining which datasets need updating."""

    @staticmethod
    def get_datasets_to_update(
        current_datasets: dict[str, DatasetMetadata],
        previous_datasets: dict[str, DatasetMetadata],
    ) -> dict[str, DatasetMetadata]:
        """Determine which datasets need updating based on last_modified.

        Args:
            current_datasets: Current dataset metadata from API
            previous_datasets: Previously stored dataset metadata

        Returns:
            Dictionary of datasets that need to be updated
        """
        datasets_to_update = {}

        for dataset_key, dataset in current_datasets.items():
            if (
                dataset_key in previous_datasets
                and dataset.last_modified != previous_datasets[dataset_key].last_modified
            ) or dataset_key not in previous_datasets:
                datasets_to_update[dataset_key] = dataset

        return datasets_to_update


class ArchiveProcessingService:
    """Service for orchestrating archive extraction and change detection."""

    def __init__(self, extractor_func):
        """Initialize the service with an extraction function.

        Args:
            extractor_func: Function that performs incremental extraction.
                Should have signature: (archive_path, extract_dir,
                previous_files, progress_hook) -> (current_files, changeset)
        """
        self.extractor_func = extractor_func

    def process_archive(
        self,
        archive_path: Path,
        extract_dir: Path,
        dataset_version: datetime,
        previous_files: dict[str, FileMetadata],
        progress_hook: ExtractionProgressHook | None = None,
    ) -> tuple[dict[str, FileMetadata], ArchiveChangeSet]:
        """Extract archive and detect changes.

        Args:
            archive_path: Path to the archive file
            extract_dir: Directory to extract files to
            dataset_version: Dataset's last_modified timestamp from API
            previous_files: Previous file metadata for comparison
            progress_hook: Optional callback for progress tracking

        Returns:
            Tuple of (current_files, changeset)
        """
        current_files, changeset = self.extractor_func(
            archive_path=archive_path,
            extract_dir=extract_dir,
            dataset_version=dataset_version,
            previous_files=previous_files,
            progress_hook=progress_hook,
        )

        return current_files, changeset


class FileQueryService:
    """Service for querying and analyzing files in pipeline state."""

    @staticmethod
    def get_files_by_filter(
        state: State,
        status: str | None = None,
        dataset: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Query files from state with optional filters.

        Args:
            state: State to query
            status: Filter by status (added, modified, unchanged, removed, or changed)
            dataset: Filter by dataset name (partial match)
            limit: Maximum number of results to return

        Returns:
            List of file dictionaries with dataset, path, status, last_changed, size
        """
        results = []

        for dataset_key, dataset_metadata in state.raw_datasets.items():
            if dataset and dataset not in dataset_key:
                continue

            for file_path, file_meta in dataset_metadata.files.items():
                # Filter by status
                if status:
                    if status == "changed":
                        if file_meta.status not in [FileStatus.ADDED, FileStatus.MODIFIED]:
                            continue
                    elif file_meta.status.value != status:
                        continue

                results.append(
                    {
                        "dataset": dataset_key,
                        "path": file_path,
                        "status": file_meta.status.value,
                        "last_changed": file_meta.last_changed,
                        "size": file_meta.size,
                    }
                )

        # Apply limit
        if limit:
            results = results[:limit]

        return results

    @staticmethod
    def get_dataset_statistics(state: State, dataset: str | None = None) -> dict[str, dict]:
        """Calculate statistics for datasets in state.

        Args:
            state: State to analyze
            dataset: Optional dataset name filter (partial match)

        Returns:
            Dictionary mapping dataset keys to statistics dict with:
                - total: Total file count
                - added/modified/unchanged/removed: Counts by status
                - total_size: Total size of non-removed files
        """
        dataset_stats = {}

        for dataset_key, dataset_metadata in state.raw_datasets.items():
            if dataset and dataset not in dataset_key:
                continue

            stats = {
                "total": len(dataset_metadata.files),
                "added": 0,
                "modified": 0,
                "unchanged": 0,
                "removed": 0,
                "total_size": 0,
            }

            for file_meta in dataset_metadata.files.values():
                stats[file_meta.status.value] += 1
                if file_meta.status != FileStatus.REMOVED:
                    stats["total_size"] += file_meta.size

            dataset_stats[dataset_key] = stats

        return dataset_stats


class FileManagementService:
    """Service for managing files in pipeline state and on disk."""

    @staticmethod
    def prune_removed_files(state: State, extract_root_dir: Path, dry_run: bool = False) -> dict:
        """Remove files marked as REMOVED from state and disk.

        Args:
            state: State to modify
            extract_root_dir: Root directory containing extracted datasets
            dry_run: If True, only report what would be done without making changes

        Returns:
            Dictionary with:
                - total_removed_files: Number of files removed from state
                - total_deleted_files: Number of files actually deleted from disk
                - datasets_pruned: List of dataset keys that had files pruned
        """
        total_removed_files = 0
        total_deleted_files = 0
        datasets_pruned = []

        for dataset_key, dataset_metadata in list(state.raw_datasets.items()):
            removed_files = [
                path
                for path, meta in dataset_metadata.files.items()
                if meta.status == FileStatus.REMOVED
            ]

            if removed_files:
                total_removed_files += len(removed_files)
                datasets_pruned.append(dataset_key)

                if not dry_run:
                    # Remove from state
                    for path in removed_files:
                        del dataset_metadata.files[path]

                    # Remove from disk (remove .tar.bz2 extension)
                    dataset_name = Path(dataset_metadata.filename).stem.removesuffix(".tar")
                    dataset_extract_dir = extract_root_dir / dataset_name
                    for path in removed_files:
                        file_path = dataset_extract_dir / path
                        if file_path.exists():
                            file_path.unlink()
                            total_deleted_files += 1

        return {
            "total_removed_files": total_removed_files,
            "total_deleted_files": total_deleted_files,
            "datasets_pruned": datasets_pruned,
        }
