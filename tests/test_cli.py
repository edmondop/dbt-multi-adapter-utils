from pathlib import Path

from typer.testing import CliRunner

from dbt_multi_adapter_utils.cli import app

runner = CliRunner()


def test_cli_scan_command_runs(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "test.sql"
    sql_file.write_text("SELECT DATE_TRUNC('month', created_at) FROM users")

    result = runner.invoke(app, ["scan", "--config", str(config_file)])

    assert result.exit_code == 0


def test_cli_generate_command_runs(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: false
""")

    result = runner.invoke(app, ["generate", "--config", str(config_file)])

    assert result.exit_code == 0


def test_cli_generate_library_command_runs(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: false
""")

    result = runner.invoke(app, ["generate-library", "--config", str(config_file)])

    assert result.exit_code == 0
    macro_file = tmp_path / "macros" / "test.sql"
    assert macro_file.exists()


def test_cli_rewrite_dry_run_command_runs(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    result = runner.invoke(app, ["rewrite", "--config", str(config_file), "--dry-run"])

    assert result.exit_code == 0


def test_cli_migrate_command_runs(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "test.sql"
    sql_file.write_text("SELECT COUNT(*) FROM users")

    result = runner.invoke(app, ["migrate", "--config", str(config_file)])

    assert result.exit_code == 0


def test_cli_rewrite_without_dry_run_shows_modified_message(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/portable_functions.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "test.sql"
    model_file.write_text("SELECT DATE_TRUNC('month', created_at) FROM users")

    result = runner.invoke(app, ["rewrite", "--config", str(config_file)])

    assert result.exit_code == 0
    assert "Modified" in result.stdout
