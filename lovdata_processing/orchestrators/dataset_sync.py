"""Dataset synchronization orchestrator.

Coordinates the complete end-to-end dataset synchronization workflow.
"""

import asyncio

from lovdata_processing.acquisition.download import download_datasets, get_dataset_metadata
from lovdata_processing.config import Settings
from lovdata_processing.domain.services import DatasetUpdateService
from lovdata_processing.orchestrators.extraction import ExtractionOrchestrator
from lovdata_processing.state.manager import PipelineStateManager
from lovdata_processing.ui import PipelineReporter


class DatasetSyncOrchestrator:
    """Orchestrates the complete dataset synchronization workflow.

    This orchestrator coordinates the entire pipeline:
    1. Fetch dataset metadata from API
    2. Determine which datasets need updates
    3. Download updated datasets
    4. Extract archives and detect changes
    5. Update state
    """

    def __init__(self, config: Settings | None = None):
        """Initialize the dataset sync orchestrator.

        Args:
            config: Pipeline configuration. If None, creates new Settings() from environment.
        """
        self.config = config if config is not None else Settings()
        self.update_service = DatasetUpdateService()
        self.extraction_orchestrator = ExtractionOrchestrator(self.config)

    def sync_datasets(
        self,
        reporter: PipelineReporter | None = None,
        force_download: bool = False,
    ) -> None:
        """Run the complete dataset synchronization workflow.

        Args:
            reporter: Optional reporter for progress. Defaults to PipelineReporter().
            force_download: Redownload all datasets regardless of timestamps.
        """
        if reporter is None:
            reporter = PipelineReporter()

        with PipelineStateManager(self.config.state_file) as state:
            # Step 1: Fetch current metadata from API
            datasets = get_dataset_metadata(self.config.api_url, self.config.dataset_filter)

            # Step 2: Determine which datasets need updates
            if force_download:
                datasets_to_update = datasets
            else:
                datasets_to_update = self.update_service.get_datasets_to_update(
                    datasets, state.data.raw_datasets
                )

            # Step 3: Download updated datasets
            if datasets_to_update:
                self._download_datasets(datasets_to_update, reporter)
            else:
                reporter.report_datasets_to_update(0)

            # Step 4: Remove datasets from state that no longer exist in API
            self._cleanup_removed_datasets(datasets, state, reporter)

            # Step 5: Ensure all current datasets exist in state
            self._ensure_datasets_in_state(datasets, state)

            # Step 6: Extract archives and detect file-level changes
            if datasets_to_update:
                extraction_results = self.extraction_orchestrator.process_archives(
                    state, datasets_to_update, reporter
                )

                # Step 7: Update metadata for successfully extracted datasets
                for dataset_key in datasets_to_update:
                    if extraction_results.get(dataset_key, {}).get("success"):
                        state.update_dataset_metadata(dataset_key, datasets[dataset_key])

    def _download_datasets(
        self,
        datasets_to_update: dict,
        reporter: PipelineReporter,
    ) -> None:
        """Download datasets with progress tracking.

        Args:
            datasets_to_update: Datasets that need to be downloaded
            reporter: Progress reporter
        """
        reporter.report_datasets_to_update(len(datasets_to_update))

        with reporter.download_context():
            # Create progress hooks for downloads
            progress_hooks = {}
            for _, dataset_metadata in datasets_to_update.items():
                filename = str(dataset_metadata.filename)
                progress_hooks[filename] = reporter.create_download_progress_hook(filename)

            asyncio.run(
                download_datasets(
                    datasets_to_update,
                    self.config.raw_data_dir,
                    progress_hooks,
                    self.config.api_url,
                    self.config.max_download_concurrency,
                )
            )

    def _cleanup_removed_datasets(
        self,
        current_datasets: dict,
        state: PipelineStateManager,
        reporter: PipelineReporter,
    ) -> None:
        """Remove datasets from state that no longer exist in API.

        Args:
            current_datasets: Current datasets from API
            state: State manager
            reporter: Progress reporter
        """
        current_dataset_keys = set(current_datasets.keys())
        state_dataset_keys = set(state.data.raw_datasets.keys())
        removed_datasets = state_dataset_keys - current_dataset_keys

        for removed_key in removed_datasets:
            del state.data.raw_datasets[removed_key]
            reporter.report_warning(f"Dataset {removed_key} removed from API")

    def _ensure_datasets_in_state(
        self,
        datasets: dict,
        state: PipelineStateManager,
    ) -> None:
        """Ensure all current datasets exist in state.

        Args:
            datasets: Current datasets from API
            state: State manager
        """
        for dataset_key, dataset_metadata in datasets.items():
            if dataset_key not in state.data.raw_datasets:
                state.update_dataset_metadata(dataset_key, dataset_metadata)
