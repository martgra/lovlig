"""Lovdata Processing SDK.

A Python library for downloading, extracting, and tracking changes in Lovdata datasets.

Quick Start (High-Level API):
    >>> from lovdata_processing import sync_datasets
    >>> sync_datasets()  # Downloads and extracts all datasets

Quick Start (SDK API):
    >>> from lovdata_processing import DatasetSyncOrchestrator, Settings
    >>> config = Settings(dataset_filter="gjeldende")
    >>> orchestrator = DatasetSyncOrchestrator(config)
    >>> orchestrator.sync_datasets()

Configuration:
    >>> from lovdata_processing import Settings
    >>> import os
    >>> os.environ["LOVDATA_API_TIMEOUT"] = "60"
    >>> config = Settings()  # Loads from environment

    >>> # Or configure programmatically
    >>> config = Settings(
    ...     api_url="https://api.lovdata.no",
    ...     raw_data_dir="data/raw",
    ...     extracted_data_dir="data/extracted"
    ... )

Public API:
    High-level functions:
        - sync_datasets: Run complete dataset synchronization
        - extract_archives: Extract archives with change detection

    Orchestrators:
        - DatasetSyncOrchestrator: Full pipeline orchestration
        - ExtractionOrchestrator: Archive extraction orchestration

    Configuration:
        - Settings: Configuration model

    Domain Models:
        - FileMetadata: File metadata with hash and status
        - FileStatus: File status enum (ADDED, MODIFIED, UNCHANGED, REMOVED)
        - RawDatasetMetadata: Dataset metadata
        - PipelineState: Complete pipeline state
        - ArchiveChangeSet: Archive changes summary

    State Management:
        - PipelineStateManager: State persistence manager

    Reporters (for custom UIs):
        - PipelineReporter: Progress reporter (use silent=True for headless mode)
"""

# Configuration
from lovdata_processing.config import Settings

# Domain models
from lovdata_processing.domain import (
    ArchiveChangeSet,
    FileMetadata,
    FileStatus,
    PipelineState,
    RawDatasetMetadata,
)

# Orchestrators
from lovdata_processing.orchestrators import DatasetSyncOrchestrator, ExtractionOrchestrator

# State management
from lovdata_processing.state.manager import PipelineStateManager

# UI Reporters
from lovdata_processing.ui import PipelineReporter

__all__ = [
    # High-level functions
    "sync_datasets",
    "extract_archives",
    # Orchestrators
    "DatasetSyncOrchestrator",
    "ExtractionOrchestrator",
    # Configuration
    "Settings",
    # Domain models
    "FileMetadata",
    "FileStatus",
    "RawDatasetMetadata",
    "PipelineState",
    "ArchiveChangeSet",
    # State management
    "PipelineStateManager",
    # Reporters
    "PipelineReporter",
]

# Version
__version__ = "0.1.0"


# High-level convenience functions
def sync_datasets(
    config: Settings | None = None,
    reporter: PipelineReporter | None = None,
    force_download: bool = False,
) -> None:
    """Run complete dataset synchronization (high-level convenience function).

    This is the simplest way to use the library - it handles everything:
    fetching dataset metadata, downloading updates, extracting archives,
    and tracking changes.

    Args:
        config: Pipeline configuration. If None, uses global config.
        reporter: Progress reporter. If None, uses PipelineReporter().
        force_download: Redownload all datasets regardless of timestamps.

    Example:
        >>> from lovdata_processing import sync_datasets
        >>> sync_datasets()  # Uses default config and Rich terminal output

        >>> from lovdata_processing import sync_datasets, Settings
        >>> config = Settings(dataset_filter="gjeldende")
        >>> sync_datasets(config=config, force_download=True)
    """
    orchestrator = DatasetSyncOrchestrator(config)
    orchestrator.sync_datasets(reporter=reporter, force_download=force_download)


def extract_archives(
    datasets: dict,
    config: Settings | None = None,
    reporter: PipelineReporter | None = None,
) -> dict:
    """Extract archives with change detection (high-level convenience function).

    Args:
        datasets: Dictionary mapping dataset keys to RawDatasetMetadata
        config: Pipeline configuration. If None, uses global config.
        reporter: Progress reporter. If None, uses PipelineReporter().

    Returns:
        Dictionary mapping dataset keys to extraction results

    Example:
        >>> from lovdata_processing import extract_archives, Settings
        >>> from lovdata_processing.state.manager import PipelineStateManager
        >>> config = Settings()
        >>> with PipelineStateManager(config.state_file) as state:
        ...     results = extract_archives(datasets, config)
    """
    orchestrator = ExtractionOrchestrator(config)
    config_obj = config if config is not None else Settings()

    with PipelineStateManager(config_obj.state_file) as state:
        return orchestrator.process_archives(state, datasets, reporter)
