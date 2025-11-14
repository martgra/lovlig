"""Tests for pipeline with reporter abstraction."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lovdata_processing.config import PipelineConfig
from lovdata_processing.domain.models import RawDatasetMetadata
from lovdata_processing.pipeline import process_archives, run_pipeline
from lovdata_processing.ui import SilentReporter


@pytest.fixture
def mock_state():
    """Create a mock PipelineStateManager."""
    state = MagicMock()
    state.compare_and_extract_archive = MagicMock()
    return state


@pytest.fixture
def sample_datasets():
    """Create sample datasets for testing."""
    return {
        "dataset1": RawDatasetMetadata(
            filename=Path("dataset1.tar.bz2"),
            last_modified=datetime(2024, 1, 1),
            files={},
        )
    }


def test_process_archives_with_silent_reporter(mock_state, sample_datasets, tmp_path):
    """Test process_archives runs successfully with SilentReporter."""
    # Setup
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    extracted_dir = tmp_path / "extracted"
    extracted_dir.mkdir()

    # Create archive file
    archive_path = raw_dir / "dataset1.tar.bz2"
    archive_path.write_text("dummy archive")

    # Mock the state methods
    mock_changeset = MagicMock()
    mock_changeset.has_changes = False
    mock_changeset.unchanged_files = []
    mock_state.get_file_metadata.return_value = {}

    # Mock the extractor to return fake data
    with patch("lovdata_processing.pipeline.extract_tar_bz2_incremental") as mock_extractor:
        mock_extractor.return_value = ({}, mock_changeset)

        reporter = SilentReporter()
        config = PipelineConfig()

        # Execute
        results = process_archives(
            mock_state,
            sample_datasets,
            raw_dir,
            extracted_dir,
            reporter,
            config,
        )

        # Verify
        assert "dataset1" in results
        assert results["dataset1"]["success"] is True
        # Verify get_file_metadata was called
        mock_state.get_file_metadata.assert_called_once()
        # Verify update_file_metadata was called
        mock_state.update_file_metadata.assert_called_once()
        # Verify extractor was called
        mock_extractor.assert_called_once()


def test_process_archives_without_reporter(mock_state, sample_datasets, tmp_path):
    """Test process_archives runs successfully without a reporter."""
    # Setup
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    extracted_dir = tmp_path / "extracted"
    extracted_dir.mkdir()

    # Create archive file
    archive_path = raw_dir / "dataset1.tar.bz2"
    archive_path.write_text("dummy archive")

    # Mock the state methods
    mock_changeset = MagicMock()
    mock_changeset.has_changes = False
    mock_state.get_file_metadata.return_value = {}

    # Mock the extractor to return fake data
    with patch("lovdata_processing.pipeline.extract_tar_bz2_incremental") as mock_extractor:
        mock_extractor.return_value = ({}, mock_changeset)

        # Execute without reporter
        results = process_archives(
            mock_state,
            sample_datasets,
            raw_dir,
            extracted_dir,
            reporter=None,
            config=PipelineConfig(),
        )

        # Verify
        assert "dataset1" in results
        assert results["dataset1"]["success"] is True
        # Verify get_file_metadata was called
        mock_state.get_file_metadata.assert_called_once()
        # Verify update_file_metadata was called
        mock_state.update_file_metadata.assert_called_once()
        # Verify extractor was called
        mock_extractor.assert_called_once()


def test_process_archives_missing_file(mock_state, sample_datasets, tmp_path):
    """Test process_archives handles missing archive files."""
    # Setup
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    extracted_dir = tmp_path / "extracted"
    extracted_dir.mkdir()

    # Don't create the archive file

    reporter = SilentReporter()
    config = PipelineConfig()

    # Execute
    results = process_archives(
        mock_state,
        sample_datasets,
        raw_dir,
        extracted_dir,
        reporter,
        config,
    )

    # Verify
    assert "dataset1" in results
    assert results["dataset1"]["success"] is False
    assert "Archive not found" in results["dataset1"]["error"]
    mock_state.get_file_metadata.assert_not_called()
    mock_state.update_file_metadata.assert_not_called()


@patch("lovdata_processing.pipeline.get_dataset_metadata")
@patch("lovdata_processing.pipeline.PipelineStateManager")
@patch("lovdata_processing.pipeline.download_datasets")
def test_run_pipeline_with_silent_reporter(
    mock_download, mock_state_manager_class, mock_get_metadata, tmp_path
):
    """Test run_pipeline with SilentReporter for headless execution."""
    # Setup
    mock_datasets = {
        "dataset1": RawDatasetMetadata(
            filename=Path("dataset1.tar.bz2"),
            last_modified=datetime(2024, 1, 1),
            files={},
        )
    }
    mock_get_metadata.return_value = mock_datasets

    # Setup mock state manager
    mock_state = MagicMock()
    mock_state.get_datasets_to_update.return_value = {}
    mock_state.data.raw_datasets = {}
    mock_state_manager_class.return_value.__enter__.return_value = mock_state
    mock_state_manager_class.return_value.__exit__.return_value = None

    reporter = SilentReporter()
    config = PipelineConfig()

    # Execute
    run_pipeline(config=config, reporter=reporter)

    # Verify no errors occurred
    mock_get_metadata.assert_called_once()
    # Update service is used instead of state method
    assert mock_state.data.raw_datasets is not None


def test_silent_reporter_no_output(capsys):
    """Test that SilentReporter produces no output."""
    reporter = SilentReporter()

    # Call all reporter methods
    reporter.report_datasets_to_update(5)
    reporter.report_warning("test warning")
    reporter.report_archive_not_found(Path("/some/path"))

    mock_changeset = MagicMock()
    mock_changeset.has_changes = True
    mock_changeset.new_files = ["file1.txt"]
    mock_changeset.modified_files = []
    mock_changeset.removed_files = []
    mock_changeset.unchanged_files = []

    reporter.report_changeset("dataset1", mock_changeset)

    # Verify no output
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_silent_reporter_context_managers():
    """Test that SilentReporter context managers work correctly."""
    reporter = SilentReporter()

    # Test download context
    with reporter.download_context() as ctx:
        assert ctx is not None

    # Test extraction context
    with reporter.extraction_context() as ctx:
        assert ctx is not None

    # Test progress hooks return callable functions
    download_hook = reporter.create_download_progress_hook("file.txt")
    assert callable(download_hook)
    download_hook(100, 1000)  # Should not raise

    extraction_hook = reporter.create_extraction_progress_hook()
    assert callable(extraction_hook)
    extraction_hook("file.txt", 50, 100)  # Should not raise
