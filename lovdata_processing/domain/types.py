"""Shared type definitions."""

from collections.abc import Callable

# Progress hook for download operations (downloaded bytes, total bytes)
DownloadProgressHook = Callable[[int, int | None], None]

# Progress hook for extraction operations (filename, current count, total count)
ExtractionProgressHook = Callable[[str, int, int], None]
