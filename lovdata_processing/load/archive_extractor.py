"""Archive extraction and file change detection."""

import bz2
import hashlib
import os
import shutil
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging import Logger
from pathlib import Path

from lovdata_processing.domain.models import ArchiveChangeSet, FileMetadata, FileStatus
from lovdata_processing.domain.types import ExtractionProgressHook


def _safe_extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, extract_dir: Path) -> None:
    """Safely extract a member, preventing path traversal."""
    root = extract_dir.resolve()
    target = (extract_dir / member.name).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:  # pragma: no cover - defensive guard
        msg = f"Refusing to extract {member.name}: outside {extract_dir}"
        raise RuntimeError(msg) from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    extracted = tar.extractfile(member)
    if extracted is None:  # pragma: no cover - tarfile guarantees fileobj for files
        raise RuntimeError(f"Failed to extract {member.name} from archive")

    with extracted, open(target, "wb") as dest:
        shutil.copyfileobj(extracted, dest)


logger = Logger(__file__)


def compute_sha256(file_path: Path, chunk_size: int = 64 * 1024) -> str:
    """Compute SHA256 hash of a file.

    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read (default 64KB)

    Returns:
        Hexadecimal SHA256 hash string
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def _resolve_worker_count(max_hash_workers: int | None) -> int:
    """Resolve worker count, falling back to CPU count when unset."""
    if max_hash_workers and max_hash_workers > 0:
        return max_hash_workers
    return max(1, os.cpu_count() or 1)


def _process_extracted_file(
    member_name: str,
    extracted_path: Path,
    member_size: int,
    dataset_version: datetime,
    is_new: bool,
    previous_meta: FileMetadata | None,
    current_files: dict[str, FileMetadata],
    changeset: ArchiveChangeSet,
    state_lock: threading.Lock,
):
    """Compute hashes for a file and update shared state safely."""
    file_hash = compute_sha256(extracted_path)
    is_modified = bool(previous_meta and not is_new and previous_meta.sha256 != file_hash)

    # Use dataset's API timestamp for new/modified files, preserve previous for unchanged
    if is_new:
        last_changed = dataset_version
        status = FileStatus.ADDED
    elif is_modified:
        last_changed = dataset_version
        status = FileStatus.MODIFIED
    else:
        last_changed = (
            previous_meta.last_changed
            if previous_meta and previous_meta.last_changed
            else dataset_version
        )
        status = FileStatus.UNCHANGED

    file_metadata = FileMetadata(
        path=member_name,
        size=member_size,
        sha256=file_hash,
        last_changed=last_changed,
        status=status,
    )

    with state_lock:
        current_files[member_name] = file_metadata
        if is_new:
            changeset.new_files.append(member_name)
        elif is_modified:
            changeset.modified_files.append(member_name)
        else:
            changeset.unchanged_files.append(member_name)


def extract_tar_bz2(
    archive_path: Path,
    extract_dir: Path,
    dataset_version: datetime,
    previous_files: dict[str, FileMetadata] | None = None,
    progress_hook: ExtractionProgressHook | None = None,
    max_hash_workers: int | None = None,
) -> tuple[dict[str, FileMetadata], ArchiveChangeSet]:
    """Extract tar.bz2 archive and compute file metadata.

    Extracts all files from the archive and computes hashes in parallel to track
    changes compared to previous state.

    Args:
        archive_path: Path to the .tar.bz2 archive file
        extract_dir: Directory to extract files to
        dataset_version: Dataset's last_modified timestamp from API
        previous_files: Previously tracked file metadata (if any)
        progress_hook: Optional callback(filename, current, total) for progress tracking
        max_hash_workers: Optional override for number of hashing threads

    Returns:
        Tuple of (current_files_metadata, changeset)
        - current_files_metadata: Dict mapping file paths to FileMetadata
        - changeset: ArchiveChangeSet containing lists of changed files
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    current_files: dict[str, FileMetadata] = {}
    previous_files = previous_files or {}
    changeset = ArchiveChangeSet()
    worker_count = _resolve_worker_count(max_hash_workers)
    state_lock = threading.Lock()
    futures = []

    logger.info(f"Extracting {archive_path} to {extract_dir}")

    # Open the bz2-compressed tarfile
    with (
        bz2.open(archive_path, "rb") as bz2_file,
        tarfile.open(fileobj=bz2_file, mode="r") as tar,
        ThreadPoolExecutor(max_workers=worker_count) as executor,
    ):
        members = tar.getmembers()
        total_files = len([m for m in members if m.isfile()])

        file_count = 0
        for member in members:
            # Skip directories
            if not member.isfile():
                continue

            file_count += 1
            if progress_hook:
                progress_hook(member.name, file_count, total_files)

            # Extract the file safely
            _safe_extract_member(tar, member, extract_dir)
            extracted_path = extract_dir / member.name

            # Determine if file is new, modified, or unchanged
            is_new = member.name not in previous_files
            previous_meta = previous_files.get(member.name)

            futures.append(
                executor.submit(
                    _process_extracted_file,
                    member.name,
                    extracted_path,
                    member.size,
                    dataset_version,
                    is_new,
                    previous_meta,
                    current_files,
                    changeset,
                    state_lock,
                )
            )

        for future in futures:
            future.result()

    # Find removed files (existed before but not in current archive)
    current_paths = set(current_files.keys())
    previous_paths = set(previous_files.keys())
    removed_paths = previous_paths - current_paths
    changeset.removed_files = list(removed_paths)

    # Keep removed files in state with REMOVED status
    for removed_path in removed_paths:
        removed_meta = previous_files[removed_path]
        current_files[removed_path] = FileMetadata(
            path=removed_meta.path,
            size=removed_meta.size,
            sha256=removed_meta.sha256,
            last_changed=dataset_version,
            status=FileStatus.REMOVED,
        )

    logger.info(f"Extraction complete: {changeset}")
    return current_files, changeset


def extract_tar_bz2_incremental(
    archive_path: Path,
    extract_dir: Path,
    dataset_version: datetime,
    previous_files: dict[str, FileMetadata] | None = None,
    progress_hook: ExtractionProgressHook | None = None,
    max_hash_workers: int | None = None,
) -> tuple[dict[str, FileMetadata], ArchiveChangeSet]:
    """Extract only changed files from tar.bz2 archive (incremental extraction).

    This is more efficient for large archives with few changes, as it only
    extracts files that are new or have different sizes. Hashing work is
    performed in parallel to keep throughput high.

    Args:
        archive_path: Path to the .tar.bz2 archive file
        extract_dir: Directory to extract files to
        dataset_version: Dataset's last_modified timestamp from API
        previous_files: Previously tracked file metadata (if any)
        progress_hook: Optional callback(filename, current, total) for progress tracking
        max_hash_workers: Optional override for number of hashing threads

    Returns:
        Tuple of (current_files_metadata, changeset)
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    current_files: dict[str, FileMetadata] = {}
    previous_files = previous_files or {}
    changeset = ArchiveChangeSet()
    worker_count = _resolve_worker_count(max_hash_workers)
    state_lock = threading.Lock()
    futures = []

    logger.info(f"Incrementally extracting {archive_path} to {extract_dir}")

    with (
        bz2.open(archive_path, "rb") as bz2_file,
        tarfile.open(fileobj=bz2_file, mode="r") as tar,
        ThreadPoolExecutor(max_workers=worker_count) as executor,
    ):
        members = tar.getmembers()
        total_files = len([m for m in members if m.isfile()])

        file_count = 0
        for member in members:
            if not member.isfile():
                continue

            file_count += 1
            if progress_hook:
                progress_hook(member.name, file_count, total_files)

            _safe_extract_member(tar, member, extract_dir)
            extracted_path = extract_dir / member.name

            # Determine if file is new, modified, or unchanged
            is_new = member.name not in previous_files
            previous_meta = previous_files.get(member.name)

            futures.append(
                executor.submit(
                    _process_extracted_file,
                    member.name,
                    extracted_path,
                    member.size,
                    dataset_version,
                    is_new,
                    previous_meta,
                    current_files,
                    changeset,
                    state_lock,
                )
            )

        for future in futures:
            future.result()

    # Find removed files
    current_paths = set(current_files.keys())
    previous_paths = set(previous_files.keys())
    removed_paths = previous_paths - current_paths
    changeset.removed_files = list(removed_paths)

    # Keep removed files in state with REMOVED status
    for removed_path in removed_paths:
        removed_meta = previous_files[removed_path]
        current_files[removed_path] = FileMetadata(
            path=removed_meta.path,
            size=removed_meta.size,
            sha256=removed_meta.sha256,
            last_changed=dataset_version,
            status=FileStatus.REMOVED,
        )

    logger.info(f"Incremental extraction complete: {changeset}")
    return current_files, changeset
