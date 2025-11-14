"""Example: Using lovdata_processing as an SDK.

This example demonstrates how to use lovdata_processing programmatically
as a Python library (SDK) rather than via the CLI.
"""

import os
from pathlib import Path

from lovdata_processing import (
    DatasetSyncOrchestrator,
    PipelineReporter,
    Settings,
    sync_datasets,
)


def example_simple_usage():
    """Simplest usage - use defaults and sync everything."""
    print("=" * 60)
    print("Example 1: Simple Usage")
    print("=" * 60)

    # Just call sync_datasets() - it uses default config and Rich output
    sync_datasets()


def example_with_environment_config():
    """Load configuration from environment variables."""
    print("\n" + "=" * 60)
    print("Example 2: Environment Configuration")
    print("=" * 60)

    # Set environment variables
    os.environ["LOVDATA_API_TIMEOUT"] = "60"
    os.environ["LOVDATA_DATASET_FILTER"] = "gjeldende"
    os.environ["LOVDATA_MAX_DOWNLOAD_CONCURRENCY"] = "8"

    # Load from environment
    settings = Settings()
    print(f"Loaded config: timeout={settings.api_timeout}, filter={settings.dataset_filter}")

    # Sync with environment config
    sync_datasets(config=settings)


def example_with_custom_config():
    """Use programmatic configuration."""
    print("\n" + "=" * 60)
    print("Example 3: Programmatic Configuration")
    print("=" * 60)

    # Create custom config programmatically
    settings = Settings(
        api_url="https://api.lovdata.no",
        dataset_filter="gjeldende",  # Only "gjeldende" datasets
        raw_data_dir=Path("my_data/raw"),
        extracted_data_dir=Path("my_data/extracted"),
        state_file=Path("my_data/state.json"),
        max_download_concurrency=5,
    )

    # Sync with custom config
    sync_datasets(config=settings, force_download=False)


def example_headless_mode():
    """Use silent reporter for headless/server mode."""
    print("\n" + "=" * 60)
    print("Example 4: Headless Mode (No Terminal Output)")
    print("=" * 60)

    settings = Settings(
        dataset_filter="gjeldende",
        raw_data_dir=Path("data/raw"),
        extracted_data_dir=Path("data/extracted"),
    )

    # Use silent mode for no output (good for cron jobs, servers)
    reporter = PipelineReporter(silent=True)
    sync_datasets(config=settings, reporter=reporter)
    print("✓ Sync completed silently")


def example_orchestrator_api():
    """Use the orchestrator API directly for more control."""
    print("\n" + "=" * 60)
    print("Example 5: Orchestrator API (More Control)")
    print("=" * 60)

    # Create configuration
    settings = Settings(
        dataset_filter="gjeldende",
        raw_data_dir=Path("data/raw"),
        extracted_data_dir=Path("data/extracted"),
    )

    # Create orchestrator
    orchestrator = DatasetSyncOrchestrator(settings)

    # Create reporter (or use silent=True for no output)
    reporter = PipelineReporter()

    # Run synchronization
    orchestrator.sync_datasets(reporter=reporter, force_download=False)


def example_monitoring():
    """Monitor what changed in the last sync."""
    print("\n" + "=" * 60)
    print("Example 6: Monitoring Changes")
    print("=" * 60)

    from lovdata_processing import PipelineStateManager

    settings = Settings()

    # Read state to see what changed
    with PipelineStateManager(settings.state_file) as state:
        for dataset_key, dataset in state.data.raw_datasets.items():
            print(f"\nDataset: {dataset_key}")
            print(f"  Last modified: {dataset.last_modified}")
            print(f"  Total files: {len(dataset.files)}")

            # Count by status
            added = sum(1 for f in dataset.files.values() if f.status.value == "added")
            modified = sum(1 for f in dataset.files.values() if f.status.value == "modified")
            unchanged = sum(1 for f in dataset.files.values() if f.status.value == "unchanged")
            removed = sum(1 for f in dataset.files.values() if f.status.value == "removed")

            print(
                f"  Added: {added}, Modified: {modified}, "
                f"Unchanged: {unchanged}, Removed: {removed}"
            )


def example_query_files():
    """Query files from state using service layer."""
    print("\n" + "=" * 60)
    print("Example 7: Query Files by Status")
    print("=" * 60)

    from lovdata_processing import FileStatus, PipelineStateManager
    from lovdata_processing.domain.services import FileQueryService

    settings = Settings()
    query_service = FileQueryService()

    with PipelineStateManager(settings.state_file) as state:
        # Get all added files
        added_files = query_service.get_files_by_status(state.data, FileStatus.ADDED, limit=10)

        print("\nRecently added files (first 10):")
        for file_info in added_files:
            print(f"  - {file_info['dataset']}/{file_info['path']}")
            print(f"    Size: {file_info['size']} bytes")

        # Get statistics
        stats = query_service.get_dataset_statistics(state.data)
        print("\nDataset Statistics:")
        for dataset_name, dataset_stats in stats.items():
            print(f"\n  {dataset_name}:")
            print(f"    Total files: {dataset_stats['total_files']}")
            print(f"    Total size: {dataset_stats['total_size'] / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Lovdata Processing SDK Examples")
    print("=" * 60)
    print("\nThese examples show different ways to use lovdata_processing")
    print("as a Python library (SDK) in your own code.\n")

    # Uncomment the examples you want to run:

    # example_simple_usage()
    # example_with_custom_config()
    # example_headless_mode()
    # example_orchestrator_api()
    # example_monitoring()
    # example_query_files()

    print("\nTo run an example, uncomment it in the __main__ section.")
