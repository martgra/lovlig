"""Reporter for pipeline output and progress tracking."""

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from lovdata_processing.domain.models import ArchiveChangeSet


class PipelineReporter:
    """Pipeline reporter with rich progress bars and formatted output."""

    CHANGE_PREVIEW_LIMIT = 10

    def __init__(self, silent: bool = False) -> None:
        """Initialize reporter.

        Args:
            silent: If True, suppress all output (for testing/automation).
        """
        self.silent = silent
        self.console = Console(quiet=silent)
        self._download_progress: Progress | None = None
        self._download_tasks: dict[str, int] = {}
        self._extraction_progress: Progress | None = None
        self._extraction_task_id: int | None = None

    def report_datasets_to_update(self, count: int) -> None:
        """Report how many datasets need updating."""
        if not self.silent:
            self.console.print(f"Downloading {count} updated datasets...")

    def create_download_progress_hook(self, filename: str):
        """Create a progress hook for downloading a specific file."""
        if self.silent:

            def hook(downloaded: int, total: int | None) -> None:
                pass

            return hook

        if self._download_progress is None:
            raise RuntimeError("Must be called within download_context")

        task_id = self._download_progress.add_task("", total=0, filename=filename)
        self._download_tasks[filename] = task_id
        first_update = True

        def hook(downloaded: int, total: int | None) -> None:
            nonlocal first_update
            if self._download_progress is None:
                return

            if total is not None and (
                first_update or self._download_progress.tasks[task_id].total != total
            ):
                self._download_progress.update(task_id, total=total)
                first_update = False

            self._download_progress.update(task_id, completed=downloaded)

        return hook

    def download_context(self):
        """Context manager for download progress display."""
        if self.silent:

            class NoOpContext:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return NoOpContext()

        class DownloadContext:
            def __init__(ctx_self, reporter):
                ctx_self.reporter = reporter

            def __enter__(ctx_self):
                ctx_self.reporter._download_progress = Progress(
                    TextColumn("[bold blue]{task.fields[filename]}"),
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeRemainingColumn(),
                    expand=True,
                )
                ctx_self.reporter._download_progress.__enter__()
                return ctx_self.reporter._download_progress

            def __exit__(ctx_self, *args):
                if ctx_self.reporter._download_progress:
                    ctx_self.reporter._download_progress.__exit__(*args)
                    ctx_self.reporter._download_progress = None
                    ctx_self.reporter._download_tasks.clear()

        return DownloadContext(self)

    def create_extraction_progress_hook(self):
        """Create a progress hook for extraction."""
        if self.silent:

            def hook(filename: str, current: int, total: int) -> None:
                pass

            return hook

        if self._extraction_progress is None:
            raise RuntimeError("Must be called within extraction_context")

        def hook(filename: str, current: int, total: int) -> None:
            if self._extraction_progress is None or self._extraction_task_id is None:
                return

            if self._extraction_progress.tasks[self._extraction_task_id].total is None:
                self._extraction_progress.update(self._extraction_task_id, total=total)
                self._extraction_progress.start_task(self._extraction_task_id)
            self._extraction_progress.update(self._extraction_task_id, completed=current)

        return hook

    def extraction_context(self):
        """Context manager for extraction progress display."""
        if self.silent:

            class NoOpContext:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return NoOpContext()

        class ExtractionContext:
            def __init__(ctx_self, reporter):
                ctx_self.reporter = reporter

            def __enter__(ctx_self):
                ctx_self.reporter._extraction_progress = Progress(
                    TextColumn("{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("•"),
                    TextColumn("{task.completed}/{task.total} files"),
                    TimeRemainingColumn(),
                )
                ctx_self.reporter._extraction_progress.__enter__()
                return ctx_self.reporter._extraction_progress

            def __exit__(ctx_self, *args):
                if ctx_self.reporter._extraction_progress:
                    ctx_self.reporter._extraction_progress.__exit__(*args)
                    ctx_self.reporter._extraction_progress = None
                    ctx_self.reporter._extraction_task_id = None

        return ExtractionContext(self)

    def start_extraction(self, dataset_key: str) -> None:
        """Signal the start of extraction for a dataset."""
        if self.silent:
            return

        if self._extraction_progress is None:
            raise RuntimeError("Must be called within extraction_context")

        self._extraction_task_id = self._extraction_progress.add_task(
            f"Extracting {dataset_key}", total=None, start=False
        )

    def complete_extraction(self) -> None:
        """Signal completion of current extraction."""
        if self.silent:
            return

        if self._extraction_progress is not None and self._extraction_task_id is not None:
            total = self._extraction_progress.tasks[self._extraction_task_id].total
            if total is not None:
                self._extraction_progress.update(self._extraction_task_id, completed=total)

    def report_changeset(self, dataset_key: str, changeset: "ArchiveChangeSet") -> None:
        """Report the changeset results."""
        if self.silent:
            return

        if changeset.has_changes:
            self.console.print(f"\n[bold]{dataset_key}[/bold] - Changes detected")
            self._render_file_list(
                "New files",
                changeset.new_files,
                "green",
                "✓",
            )
            self._render_file_list(
                "Modified files",
                changeset.modified_files,
                "yellow",
                "⚡",
            )
            self._render_file_list(
                "Removed files",
                changeset.removed_files,
                "red",
                "✗",
            )
            if changeset.unchanged_files:
                self.console.print(
                    f"  [dim]Unchanged files: {len(changeset.unchanged_files)}[/dim]"
                )
        else:
            unchanged_count = len(changeset.unchanged_files)
            self.console.print(
                f"\n[bold]{dataset_key}[/bold] - No changes ({unchanged_count} files)"
            )

    def report_warning(self, message: str) -> None:
        """Report a warning message."""
        if not self.silent:
            self.console.print(f"\n[yellow]Warning:[/yellow] {message}")

    def report_error(self, message: str) -> None:
        """Report an error message."""
        if not self.silent:
            self.console.print(f"\n[red]Error:[/red] {message}")

    def report_archive_not_found(self, archive_path) -> None:
        """Report that an archive file was not found."""
        if not self.silent:
            self.report_warning(f"Archive not found: {archive_path}")

    def _render_file_list(
        self,
        label: str,
        files: list[str],
        color: str,
        glyph: str,
    ) -> None:
        """Pretty-print a short list of files for the given change bucket."""
        if not files:
            return

        count = len(files)
        preview = files[: self.CHANGE_PREVIEW_LIMIT]
        self.console.print(f"  [{color}]{glyph} {label}: {count}[/{color}]")
        for file_path in preview:
            self.console.print(f"      {file_path}")

        remaining = count - len(preview)
        if remaining > 0:
            self.console.print(f"      ... (+{remaining} more)")
