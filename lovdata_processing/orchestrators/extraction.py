"""Archive extraction orchestrator.

Coordinates the extraction of datasets and file-level change detection.
"""

from functools import partial
from pathlib import Path

from lovdata_processing.config import Settings
from lovdata_processing.domain.models import DatasetMetadata
from lovdata_processing.domain.services import ArchiveProcessingService
from lovdata_processing.operations.extract import extract_tar_bz2_incremental
from lovdata_processing.state.manager import StateManager
from lovdata_processing.ui import Reporter


class Extraction:
    """Orchestrates archive extraction with change detection.

    This orchestrator coordinates the extraction of tar.bz2 archives,
    detecting file-level changes and updating state.
    """

    def __init__(self, config: Settings | None = None):
        """Initialize the extraction orchestrator.

        Args:
            config: Pipeline configuration. If None, creates new Settings() from environment.
        """
        self.config = config if config is not None else Settings()

        # Create extractor with configured hash workers
        extractor_func = partial(
            extract_tar_bz2_incremental,
            max_hash_workers=self.config.max_hash_workers,
        )
        self.archive_service = ArchiveProcessingService(extractor_func)

    def process_archives(
        self,
        state: StateManager,
        datasets: dict[str, DatasetMetadata],
        reporter: Reporter | None = None,
    ) -> dict[str, dict]:
        """Process archives and detect file-level changes.

        Args:
            state: State manager
            datasets: Dictionary of datasets to process (key = dataset filename)
            reporter: Optional reporter for progress and results

        Returns:
            Dictionary mapping dataset keys to their results:
                - changeset: The detected changes
                - success: Whether processing succeeded
                - error: Error message if failed
        """
        results = {}
        raw_dir = self.config.raw_data_dir
        extracted_dir = self.config.extracted_data_dir

        for dataset_key, dataset_metadata in datasets.items():
            archive_path = raw_dir / dataset_metadata.filename
            # Remove .tar.bz2 extension (stem only removes .bz2, leaving .tar)
            dataset_name = Path(dataset_metadata.filename).stem.removesuffix(".tar")
            dataset_extract_dir = extracted_dir / dataset_name

            if not archive_path.exists():
                if reporter:
                    reporter.report_archive_not_found(archive_path)
                results[dataset_key] = {
                    "success": False,
                    "error": f"Archive not found: {archive_path}",
                }
                continue

            # Extract with progress reporting
            try:
                result = self._extract_dataset(
                    dataset_key=dataset_key,
                    dataset_metadata=dataset_metadata,
                    archive_path=archive_path,
                    extract_dir=dataset_extract_dir,
                    state=state,
                    reporter=reporter,
                )
                results[dataset_key] = result
            except Exception as e:
                if reporter:
                    reporter.report_error(f"Failed to extract {dataset_key}: {e}")
                results[dataset_key] = {
                    "success": False,
                    "error": str(e),
                }

        return results

    def _extract_dataset(
        self,
        dataset_key: str,
        dataset_metadata: DatasetMetadata,
        archive_path: Path,
        extract_dir: Path,
        state: StateManager,
        reporter: Reporter | None,
    ) -> dict:
        """Extract a single dataset with progress reporting.

        Args:
            dataset_key: Dataset identifier
            dataset_metadata: Dataset metadata
            archive_path: Path to archive file
            extract_dir: Directory to extract to
            state: State manager
            reporter: Optional progress reporter

        Returns:
            Result dictionary with changeset and success status
        """
        # Setup progress context
        ctx = reporter.extraction_context() if reporter else None
        if ctx:
            ctx.__enter__()

        try:
            if reporter:
                reporter.start_extraction(dataset_key)

            # Get previous file metadata from state
            previous_files = state.get_file_metadata(dataset_key)

            # Process archive using service
            current_files, changeset = self.archive_service.process_archive(
                archive_path=archive_path,
                extract_dir=extract_dir,
                dataset_version=dataset_metadata.last_modified,
                previous_files=previous_files,
                progress_hook=reporter.create_extraction_progress_hook() if reporter else None,
            )

            # Update state with new file metadata
            state.update_file_metadata(dataset_key, current_files)

            if reporter:
                reporter.complete_extraction()

            return {"changeset": changeset, "success": True}
        finally:
            if ctx:
                ctx.__exit__(None, None, None)
            # Report results after progress bar is closed
            if reporter:
                reporter.report_changeset(dataset_key, changeset)
