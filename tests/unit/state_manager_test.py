"""Tests for pipeline state management."""

from datetime import datetime
from pathlib import Path

import orjson

from lovdata_processing.domain.models import FileMetadata, RawDatasetMetadata
from lovdata_processing.state.manager import PipelineStateManager


def _write_state(file_path: Path, payload: dict) -> None:
    file_path.write_bytes(orjson.dumps(payload))


def test_state_manager_prunes_fast_hash(tmp_path):
    """Ensure legacy fast_hash fields are removed when state loads."""

    state_file = tmp_path / "state.json"
    payload = {
        "raw_datasets": {
            "dataset1": {
                "filename": "dataset1.tar.bz2",
                "last_modified": datetime(2024, 1, 1).isoformat(),
                "files": {
                    "file1.xml": {
                        "path": "file1.xml",
                        "size": 42,
                        "sha256": "abc123",
                        "fast_hash": "legacy",
                        "modified": datetime(2024, 1, 1).isoformat(),
                    }
                },
            }
        }
    }
    _write_state(state_file, payload)

    with PipelineStateManager(state_file) as manager:
        files = manager.get_file_metadata("dataset1")
        assert set(files.keys()) == {"file1.xml"}
        file_metadata = files["file1.xml"]
        assert isinstance(file_metadata, FileMetadata)
        assert file_metadata.sha256 == "abc123"
        manager.update_file_metadata("dataset1", files)

    persisted = orjson.loads(state_file.read_bytes())
    file_entry = persisted["raw_datasets"]["dataset1"]["files"]["file1.xml"]
    assert "fast_hash" not in file_entry


def test_state_manager_handles_invalid_payload(tmp_path):
    """Gracefully handle malformed legacy state files."""

    state_file = tmp_path / "corrupt.json"
    _write_state(state_file, {"raw_datasets": ["not-a-dict"]})

    with PipelineStateManager(state_file) as manager:
        assert manager.get_file_metadata("dataset1") == {}
        assert manager.data.raw_datasets == {}
        manager.update_dataset_metadata(
            "dataset1",
            RawDatasetMetadata(
                filename=Path("dataset1.tar.bz2"),
                last_modified=datetime(2024, 1, 1),
                files={},
            ),
        )

    persisted = orjson.loads(state_file.read_bytes())
    assert "dataset1" in persisted["raw_datasets"]
