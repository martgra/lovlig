"""Typer-based CLI for lovdata processing."""

import asyncio

import typer

from lovdata_processing.operations.download import download_datasets, fetch_datasets
from lovdata_processing.config import Settings
from lovdata_processing.domain.services import (
    DatasetUpdateService,
    FileManagementService,
    FileQueryService,
)
from lovdata_processing.orchestrators import DatasetSync
from lovdata_processing.state.manager import StateManager
from lovdata_processing.ui import Reporter
from lovdata_processing.ui.tables import (
    create_file_list_table,
    create_statistics_table,
    format_status_summary,
)

app = typer.Typer(help="Lovdata data pipeline")
files_app = typer.Typer(help="Query and manage files in state")
app.add_typer(files_app, name="files")


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Show help when no subcommand is provided."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


def _perform_download(force: bool, reporter: Reporter, config: Settings) -> None:
    """Download datasets according to the current state."""
    update_service = DatasetUpdateService()

    with StateManager(config.state_file) as state:
        datasets = fetch_datasets(config.api_url, config.dataset_filter)
        datasets_to_update = (
            datasets
            if force
            else update_service.get_datasets_to_update(datasets, state.data.raw_datasets)
        )

        if not datasets_to_update:
            reporter.report_datasets_to_update(0)
            typer.echo("All datasets already up to date")
            return

        reporter.report_datasets_to_update(len(datasets_to_update))

        # Create progress hooks for downloads
        progress_hooks = {}
        for _, dataset_metadata in datasets_to_update.items():
            filename = str(dataset_metadata.filename)
            progress_hooks[filename] = reporter.create_download_progress_hook(filename)

        # Download with progress tracking
        with reporter.download_context():
            asyncio.run(
                download_datasets(
                    datasets_to_update,
                    config.raw_data_dir,
                    progress_hooks,
                    config.api_url,
                    config.max_download_concurrency,
                )
            )

        # Persist the latest dataset metadata so future runs know timestamps
        for dataset_key, dataset_metadata in datasets.items():
            state.update_dataset_metadata(dataset_key, dataset_metadata)


@app.command()
def download(force: bool = typer.Option(False, "--force", help="Redownload all datasets")):
    """Download new or updated datasets without extraction."""
    reporter = Reporter()
    config = Settings()
    _perform_download(force, reporter, config)


@app.command()
def update(force: bool = typer.Option(False, "--force", help="Force redownload before update")):
    """Download and extract datasets, updating state."""
    reporter = Reporter()
    config = Settings()
    orchestrator = DatasetSync(config)
    orchestrator.sync_datasets(reporter=reporter, force_download=force)


# Files subcommands
@files_app.command("list")
def files_list(
    status: str = typer.Option(
        None,
        "--status",
        "-s",
        help="Filter by status: added, modified, unchanged, removed, or changed (added+modified)",
    ),
    dataset: str = typer.Option(None, "--dataset", "-d", help="Filter by dataset name"),
    limit: int = typer.Option(None, "--limit", "-n", help="Limit number of results"),
):
    """List files in state with optional filtering."""
    config = Settings()
    reporter = Reporter()

    valid_statuses = {"added", "modified", "unchanged", "removed", "changed"}
    if status and status not in valid_statuses:
        reporter.console.print(
            f"[red]Invalid status: {status}[/red]\n"
            f"Valid options: {', '.join(sorted(valid_statuses))}"
        )
        raise typer.Exit(1)

    with StateManager(config.state_file) as state:
        # Use service to query files
        query_service = FileQueryService()
        results = query_service.get_files_by_filter(
            state=state.data, status=status, dataset=dataset, limit=limit
        )

        if not results:
            reporter.console.print("[dim]No matching files found[/dim]")
            return

        # Use table utility to render results
        table = create_file_list_table(results)
        reporter.console.print(table)

        # Show summary
        summary = format_status_summary(results)
        reporter.console.print(f"\n[bold]Summary:[/bold] {summary}")


@files_app.command("stats")
def files_stats(
    dataset: str = typer.Option(None, "--dataset", "-d", help="Filter by dataset name"),
):
    """Show statistics about files in state."""
    config = Settings()
    reporter = Reporter()

    with StateManager(config.state_file) as state:
        # Use service to calculate statistics
        query_service = FileQueryService()
        dataset_stats = query_service.get_dataset_statistics(state=state.data, dataset=dataset)

        if not dataset_stats:
            reporter.console.print("[dim]No matching datasets found[/dim]")
            return

        # Use table utility to render statistics
        table = create_statistics_table(dataset_stats)
        reporter.console.print(table)


@files_app.command("prune")
def files_prune(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be pruned without deleting"
    ),
):
    """Remove files marked as REMOVED from state and disk."""
    config = Settings()
    reporter = Reporter()

    with StateManager(config.state_file) as state:
        # Use service to perform prune operation
        management_service = FileManagementService()
        result = management_service.prune_removed_files(
            state=state.data, extract_root_dir=config.extracted_data_dir, dry_run=dry_run
        )

        total_removed = result["total_removed_files"]
        total_deleted = result["total_deleted_files"]
        datasets_pruned = result["datasets_pruned"]

        # Report results per dataset
        for dataset_key in datasets_pruned:
            reporter.console.print(
                f"{'[DRY RUN] Would remove' if dry_run else 'Removed'} files from {dataset_key}"
            )

        # Report summary
        if total_removed > 0:
            reporter.console.print(
                f"\n[bold]{'Would prune' if dry_run else 'Pruned'} {total_removed} files "
                f"from {len(datasets_pruned)} dataset(s)[/bold]"
            )
            if not dry_run:
                reporter.console.print(f"Deleted {total_deleted} files from disk")
        else:
            reporter.console.print("[dim]No removed files to prune[/dim]")

        if dry_run and total_removed > 0:
            reporter.console.print("\n[yellow]Run without --dry-run to actually prune[/yellow]")


if __name__ == "__main__":
    app()
