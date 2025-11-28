"""Table rendering utilities for CLI output."""

from rich.table import Table


def create_file_list_table(files: list[dict], title_suffix: str = "") -> Table:
    """Create a table for displaying file information.

    Args:
        files: List of file dictionaries with dataset, path, status, last_changed, size
        title_suffix: Optional suffix for table title

    Returns:
        Rich Table object ready for display
    """
    title = f"Files ({len(files)} total){title_suffix}"
    table = Table(title=title)
    table.add_column("Dataset", style="cyan")
    table.add_column("Path", style="white")
    table.add_column("Status", style="yellow")
    table.add_column("Last Changed", style="dim")
    table.add_column("Size", justify="right", style="dim")

    status_colors = {
        "added": "green",
        "modified": "yellow",
        "removed": "red",
        "unchanged": "dim",
    }

    for file in files:
        status_color = status_colors.get(file["status"], "white")
        table.add_row(
            file["dataset"],
            file["path"],
            f"[{status_color}]{file['status']}[/{status_color}]",
            file["last_changed"].strftime("%Y-%m-%d %H:%M") if file["last_changed"] else "-",
            f"{file['size']:,} B",
        )

    return table


def create_statistics_table(dataset_stats: dict[str, dict]) -> Table:
    """Create a table for displaying dataset statistics.

    Args:
        dataset_stats: Dictionary mapping dataset keys to statistics dict

    Returns:
        Rich Table object ready for display
    """
    table = Table(title="Dataset Statistics")
    table.add_column("Dataset", style="cyan")
    table.add_column("Total Files", justify="right")
    table.add_column("Added", justify="right", style="green")
    table.add_column("Modified", justify="right", style="yellow")
    table.add_column("Unchanged", justify="right", style="dim")
    table.add_column("Removed", justify="right", style="red")
    table.add_column("Total Size", justify="right", style="blue")

    total_all = {
        "total": 0,
        "added": 0,
        "modified": 0,
        "unchanged": 0,
        "removed": 0,
        "total_size": 0,
    }

    for dataset_key, stats in sorted(dataset_stats.items()):
        table.add_row(
            dataset_key,
            str(stats["total"]),
            str(stats["added"]) if stats["added"] else "-",
            str(stats["modified"]) if stats["modified"] else "-",
            str(stats["unchanged"]) if stats["unchanged"] else "-",
            str(stats["removed"]) if stats["removed"] else "-",
            f"{stats['total_size'] / 1024 / 1024:.1f} MB",
        )
        for key in total_all:
            total_all[key] += stats[key]

    # Add totals row if multiple datasets
    if len(dataset_stats) > 1:
        table.add_section()
        table.add_row(
            "[bold]TOTAL[/bold]",
            f"[bold]{total_all['total']}[/bold]",
            f"[bold]{total_all['added']}[/bold]" if total_all["added"] else "-",
            f"[bold]{total_all['modified']}[/bold]" if total_all["modified"] else "-",
            f"[bold]{total_all['unchanged']}[/bold]" if total_all["unchanged"] else "-",
            f"[bold]{total_all['removed']}[/bold]" if total_all["removed"] else "-",
            f"[bold]{total_all['total_size'] / 1024 / 1024:.1f} MB[/bold]",
        )

    return table


def format_status_summary(files: list[dict]) -> str:
    """Create a summary string of file counts by status.

    Args:
        files: List of file dictionaries with 'status' key

    Returns:
        Formatted summary string like "2 added, 3 modified"
    """
    from collections import Counter

    status_counts = Counter(f["status"] for f in files)
    return ", ".join(f"{count} {status}" for status, count in sorted(status_counts.items()))
