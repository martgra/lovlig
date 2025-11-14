"""Data acquisition layer.

This module provides functionality for downloading and extracting Lovdata datasets.

Public API:
    Download operations:
        - get_dataset_metadata: Fetch dataset list from API
        - download_datasets: Async batch download

    Extract operations:
        - extract_tar_bz2: Full extraction
        - extract_tar_bz2_incremental: Incremental extraction with change detection
        - compute_file_hash: Fast file hashing with xxHash
"""

from lovdata_processing.acquisition.download import download_datasets, get_dataset_metadata
from lovdata_processing.acquisition.extract import (
    compute_file_hash,
    extract_tar_bz2,
    extract_tar_bz2_incremental,
)

__all__ = [
    # Download operations
    "get_dataset_metadata",
    "download_datasets",
    # Extract operations
    "extract_tar_bz2",
    "extract_tar_bz2_incremental",
    "compute_file_hash",
]
