from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from dbt_multi_adapter_utils.config import load_config
from dbt_multi_adapter_utils.macro_generator import generate_macros
from dbt_multi_adapter_utils.model_rewriter import rewrite_models
from dbt_multi_adapter_utils.scanner import scan_project
from dbt_multi_adapter_utils.sqlglot_adapter import get_function_differences

app = typer.Typer(
    name="dbt-multi-adapter-utils",
    help="Generate cross-database compatible dbt macros using SQLGlot",
)
console = Console()


@app.command()
def scan(
    *,
    config_path: Path = typer.Option(
        Path(".dbt-multi-adapter.yml"),
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Scan dbt project and detect non-portable SQL functions."""
    console.print("[bold blue]Scanning dbt project...[/bold blue]")

    config = load_config(config_path)
    functions = scan_project(config)

    table = Table(title="Non-Portable Functions Detected")
    table.add_column("Function", style="cyan")
    table.add_column("Count", justify="right", style="green")

    for func, count in functions.items():
        table.add_row(func, str(count))

    console.print(table)
    console.print(f"\n[bold]Total unique functions:[/bold] {len(functions)}")


@app.command()
def generate(
    *,
    config_path: Path = typer.Option(
        Path(".dbt-multi-adapter.yml"),
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Generate portable_* macros for detected functions."""
    console.print("[bold blue]Generating macros...[/bold blue]")

    config = load_config(config_path)
    functions = scan_project(config)
    output_path = generate_macros(config, list(functions.keys()))

    console.print(f"[bold green]✓[/bold green] Generated macros at: {output_path}")


@app.command()
def generate_library(
    *,
    config_path: Path = typer.Option(
        Path(".dbt-multi-adapter.yml"),
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Generate macros for ALL known non-portable functions across adapters."""
    console.print("[bold blue]Generating complete function library...[/bold blue]")

    config = load_config(config_path)
    functions = get_function_differences(config.adapters)
    output_path = generate_macros(config, functions)

    console.print(f"[bold green]✓[/bold green] Generated {len(functions)} macros at: {output_path}")


@app.command()
def rewrite(
    *,
    config_path: Path = typer.Option(
        Path(".dbt-multi-adapter.yml"),
        "--config",
        "-c",
        help="Path to config file",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show changes without modifying files",
    ),
) -> None:
    """Rewrite SQL models to use portable_* macros."""
    action = "Preview" if dry_run else "Rewriting"
    console.print(f"[bold blue]{action} models...[/bold blue]")

    config = load_config(config_path)
    changes = rewrite_models(config, dry_run=dry_run)

    if dry_run:
        console.print(f"\n[bold]Would modify {len(changes)} file(s)[/bold]")
    else:
        console.print(f"[bold green]✓[/bold green] Modified {len(changes)} file(s)")


@app.command()
def migrate(
    *,
    config_path: Path = typer.Option(
        Path(".dbt-multi-adapter.yml"),
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Run complete migration: scan → generate → rewrite."""
    console.print("[bold blue]Starting migration...[/bold blue]\n")

    config = load_config(config_path)

    console.print("[1/3] Scanning project...")
    functions = scan_project(config)
    console.print(f"      Found {len(functions)} non-portable function(s)\n")

    console.print("[2/3] Generating macros...")
    output_path = generate_macros(config, list(functions.keys()))
    console.print(f"      Generated macros at: {output_path}\n")

    console.print("[3/3] Rewriting models...")
    changes = rewrite_models(config, dry_run=False)
    console.print(f"      Modified {len(changes)} file(s)\n")

    console.print("[bold green]✓ Migration complete![/bold green]")


if __name__ == "__main__":
    app()
