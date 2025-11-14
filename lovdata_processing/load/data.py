"""loading module."""

import asyncio
from pathlib import Path

import httpx
import requests

from lovdata_processing.domain.models import RawDatasetMetadata
from lovdata_processing.domain.types import DownloadProgressHook


def get_dataset_metadata(
    api_url: str = "https://api.lovdata.no", name_filter: str | None = "gjeldende"
) -> dict[str, RawDatasetMetadata]:
    """Return datasets from API.

    Args:
        api_url: Base URL for the Lovdata API
        name_filter: Optional filter for dataset filenames (None = all datasets)

    Returns:
        Dictionary mapping dataset filenames to their metadata
    """
    result = requests.get(f"{api_url}/v1/publicData/list", timeout=30)
    result.raise_for_status()

    datasets = result.json()

    # Apply filter if specified
    if name_filter:
        datasets = [d for d in datasets if name_filter in d.get("filename", "")]

    return {
        dataset.get("filename"): RawDatasetMetadata(
            filename=dataset.get("filename"), last_modified=dataset.get("lastModified")
        )
        for dataset in datasets
    }


async def download_file(
    url: str,
    dest: Path,
    client: httpx.AsyncClient,
    progress_hook: DownloadProgressHook | None = None,
    chunk_size: int = 64 * 1024,
) -> None:
    """Download a single file and report progress via callback."""
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()

        total = resp.headers.get("Content-Length")
        total_bytes: int | None = int(total) if total is not None else None

        downloaded = 0
        if progress_hook:
            progress_hook(downloaded, total_bytes)

        with dest.open("wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if progress_hook:
                    progress_hook(downloaded, total_bytes)


async def download_datasets(
    datasets: dict[str, RawDatasetMetadata],
    dest_dir: Path,
    progress_hooks: dict[str, DownloadProgressHook] | None = None,
    api_url: str = "https://api.lovdata.no",
    max_concurrency: int = 4,
) -> None:
    """Download datasets.

    Args:
        datasets: Dictionary of datasets to download
        dest_dir: Destination directory for downloads
        progress_hooks: Optional dict mapping filenames to progress callbacks
        api_url: Base URL for the Lovdata API
        max_concurrency: Maximum number of concurrent downloads
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    progress_hooks = progress_hooks or {}

    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async with httpx.AsyncClient() as client:
        tasks = []

        for _, dataset in datasets.items():
            dest = dest_dir / dataset.filename
            filename = str(dataset.filename)
            hook = progress_hooks.get(filename)
            url = f"{api_url}/v1/publicData/get/{dataset.filename}"

            async def _run_download(
                dataset_url: str,
                destination: Path,
                dataset_hook: DownloadProgressHook | None,
            ) -> None:
                async with semaphore:
                    await download_file(
                        url=dataset_url,
                        dest=destination,
                        client=client,
                        progress_hook=dataset_hook,
                    )

            tasks.append(asyncio.create_task(_run_download(url, dest, hook)))

        if tasks:
            await asyncio.gather(*tasks)
