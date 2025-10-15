from pathlib import Path

from dbt_multi_adapter_utils.config import load_config
from dbt_multi_adapter_utils.scanner import scan_project


def test_scanner_scans_project_successfully():
    config_path = Path("tests/fixtures/sample_dbt_project/.dbt-multi-adapter.yml")
    config = load_config(config_path)

    functions = scan_project(config)

    assert isinstance(functions, dict)
    assert len(functions) > 0


def test_scanner_handles_unreadable_file(tmp_path: Path):
    import os

    config_path = tmp_path / ".dbt-multi-adapter.yml"
    config_path.write_text("""
adapters:
  - postgres
  - snowflake
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    bad_file = models_dir / "test.sql"
    bad_file.write_text("SELECT 1")

    if os.name != "nt":
        bad_file.chmod(0o000)

        config = load_config(config_path)

        try:
            functions = scan_project(config)
            assert isinstance(functions, dict)
        finally:
            bad_file.chmod(0o644)


def test_scanner_handles_nonexistent_model_path(tmp_path: Path):
    config_path = tmp_path / ".dbt-multi-adapter.yml"
    config_path.write_text("""
adapters:
  - postgres
  - snowflake
model_paths:
  - nonexistent_models
""")

    config = load_config(config_path)
    functions = scan_project(config)

    assert functions == {}
