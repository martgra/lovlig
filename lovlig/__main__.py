"""Command-line entrypoint that defers to the Typer CLI."""

from lovlig.cli.app import app

if __name__ == "__main__":
    app()
