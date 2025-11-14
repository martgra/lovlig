"""Pipeline orchestration."""

import asyncio
from functools import partial
from pathlib import Path

from lovdata_processing.config import PipelineConfig
from lovdata_processing.domain.services import ArchiveProcessingService, DatasetUpdateService
from lovdata_processing.load.archive_extractor import extract_tar_bz2_incremental
from lovdata_processing.load.data import download_datasets, get_dataset_metadata
from lovdata_processing.state.manager import PipelineStateManager
from lovdata_processing.ui import PipelineReporter


def process_archives(
    state: PipelineStateManager,
    datasets: dict,
    raw_dir: Path,
    extracted_dir: Path,
    reporter: PipelineReporter | None = None,
    config: PipelineConfig | None = None,
):
    """Process archives and detect file-level changes.

    Args:
        state: Pipeline state manager
        datasets: Dictionary of datasets to process
        raw_dir: Directory containing raw archive files
        extracted_dir: Directory for extracted files
        reporter: Optional reporter for progress and results (defaults to no output)
        config: Pipeline configuration for settings like hash worker count

    Returns:
        Dictionary mapping dataset keys to their changesets
    """
    results = {}

    config = config or PipelineConfig()

    # Create service for archive processing with configured hash workers
    extractor = partial(
        extract_tar_bz2_incremental,
        max_hash_workers=config.max_hash_workers,
    )
    archive_service = ArchiveProcessingService(extractor)

    for dataset_key, dataset_metadata in datasets.items():
        archive_path = raw_dir / dataset_metadata.filename
        dataset_extract_dir = extracted_dir / dataset_metadata.filename.stem

        if archive_path.exists():
            # Each dataset gets its own extraction context
            ctx = reporter.extraction_context() if reporter else None

            if ctx:
                ctx.__enter__()

            try:
                if reporter:
                    reporter.start_extraction(dataset_key)

                # Get previous file metadata from state
                previous_files = state.get_file_metadata(dataset_key)

                # Process archive using service
                current_files, changeset = archive_service.process_archive(
                    archive_path=archive_path,
                    extract_dir=dataset_extract_dir,
                    dataset_version=dataset_metadata.last_modified,
                    previous_files=previous_files,
                    progress_hook=reporter.create_extraction_progress_hook() if reporter else None,
                )

                # Update state with new file metadata
                state.update_file_metadata(dataset_key, current_files)

                if reporter:
                    reporter.complete_extraction()

                results[dataset_key] = {"changeset": changeset, "success": True}
            finally:
                if ctx:
                    ctx.__exit__(None, None, None)

            # Report results after progress bar is closed
            if reporter:
                reporter.report_changeset(dataset_key, changeset)
        else:
            if reporter:
                reporter.report_archive_not_found(archive_path)
            results[dataset_key] = {
                "success": False,
                "error": f"Archive not found: {archive_path}",
            }

    return results


def run_pipeline(
    config: PipelineConfig | None = None,
    reporter: PipelineReporter | None = None,
    force_download: bool = False,
):
    """Run the complete data pipeline.

    Args:
        config: Pipeline configuration. Defaults to PipelineConfig() with default values.
        reporter: Optional reporter for progress and results. Defaults to RichReporter.
        force_download: Redownload all datasets regardless of timestamps.
    """
    from lovdata_processing.ui import RichReporter

    if config is None:
        config = PipelineConfig()

    if reporter is None:
        reporter = RichReporter()

    # Type narrowing - at this point reporter is always PipelineReporter
    assert reporter is not None

    raw_dir = config.raw_data_dir
    extracted_dir = config.extracted_data_dir

    # Create update service
    update_service = DatasetUpdateService()

    with PipelineStateManager(config.state_file) as state:
        # Step 1: Fetch current metadata from API
        datasets = get_dataset_metadata(config.api_url, config.dataset_filter)

        # Step 2: Determine which datasets need updates (based on timestamp)
        if force_download:
            datasets_to_update = datasets
        else:
            datasets_to_update = update_service.get_datasets_to_update(
                datasets, state.data.raw_datasets
            )

        # Step 3: Download updated datasets
        if datasets_to_update:
            reporter.report_datasets_to_update(len(datasets_to_update))

            # Create progress hooks for downloads
            progress_hooks = {}
            for _, dataset_metadata in datasets_to_update.items():
                filename = str(dataset_metadata.filename)
                progress_hooks[filename] = reporter.create_download_progress_hook(filename)

            # Download with progress tracking
            with reporter.download_context():
                asyncio.run(
                    download_datasets(
                        datasets_to_update,
                        raw_dir,
                        progress_hooks,
                        config.api_url,
                        config.max_download_concurrency,
                    )
                )
        else:
            reporter.report_datasets_to_update(0)

        # Step 4: Remove datasets from state that no longer exist in API
        current_dataset_keys = set(datasets.keys())
        state_dataset_keys = set(state.data.raw_datasets.keys())
        removed_datasets = state_dataset_keys - current_dataset_keys
        for removed_key in removed_datasets:
            del state.data.raw_datasets[removed_key]
            if reporter:
                reporter.report_warning(f"Dataset {removed_key} removed from API")

        # Step 5: Extract archives and detect file-level changes
        if datasets_to_update:
            extraction_results = process_archives(
                state, datasets_to_update, raw_dir, extracted_dir, reporter, config
            )

            # Step 6: Only update metadata for successfully extracted datasets
            for dataset_key in datasets_to_update:
                if extraction_results.get(dataset_key, {}).get("success"):
                    state.update_dataset_metadata(dataset_key, datasets[dataset_key])

        # Step 7: Update metadata for datasets that didn't need extraction
        for dataset_key in datasets:
            if dataset_key not in datasets_to_update:
                state.update_dataset_metadata(dataset_key, datasets[dataset_key])
