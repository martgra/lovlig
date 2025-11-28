"""Typer-based CLI for lovdata processing."""

import json

import typer

from lovlig.config import Settings
from lovlig.domain.services import (
    FileManagementService,
    FileQueryService,
)
from lovlig.orchestrators import DatasetSync
from lovlig.state.manager import StateManager
from lovlig.ui import Reporter
from lovlig.ui.tables import (
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
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
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
            if json_output:
                typer.echo(json.dumps([], ensure_ascii=False))
            else:
                reporter.console.print("[dim]No matching files found[/dim]")
            return

        # Output as JSON or table
        if json_output:
            # Convert datetime to ISO format for JSON serialization
            json_results = [
                {
                    **result,
                    "last_changed": result["last_changed"].isoformat()
                    if result["last_changed"]
                    else None,
                }
                for result in results
            ]
            typer.echo(json.dumps(json_results, indent=2, ensure_ascii=False))
        else:
            # Use table utility to render results
            table = create_file_list_table(results)
            reporter.console.print(table)

            # Show summary
            summary = format_status_summary(results)
            reporter.console.print(f"\n[bold]Summary:[/bold] {summary}")


@files_app.command("stats")
def files_stats(
    dataset: str = typer.Option(None, "--dataset", "-d", help="Filter by dataset name"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show statistics about files in state."""
    config = Settings()
    reporter = Reporter()

    with StateManager(config.state_file) as state:
        # Use service to calculate statistics
        query_service = FileQueryService()
        dataset_stats = query_service.get_dataset_statistics(state=state.data, dataset=dataset)

        if not dataset_stats:
            if json_output:
                typer.echo(json.dumps({}, ensure_ascii=False))
            else:
                reporter.console.print("[dim]No matching datasets found[/dim]")
            return

        # Output as JSON or table
        if json_output:
            typer.echo(json.dumps(dataset_stats, indent=2, ensure_ascii=False))
        else:
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
