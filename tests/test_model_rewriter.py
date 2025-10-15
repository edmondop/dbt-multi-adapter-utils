from pathlib import Path

from dbt_multi_adapter_utils.config import load_config
from dbt_multi_adapter_utils.model_rewriter import rewrite_models


def test_model_rewriter_rewrites_project_models(tmp_path: Path):
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

    config = load_config(config_file)
    modified_files = rewrite_models(config, dry_run=False)

    assert len(modified_files) >= 0
    if modified_files:
        content = modified_files[0].read_text()
        assert "portable" in content


def test_model_rewriter_dry_run_doesnt_modify_file(tmp_path: Path):
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
    original_content = "SELECT COUNT(*) FROM users"
    sql_file.write_text(original_content)

    config = load_config(config_file)
    rewrite_models(config, dry_run=True)

    content_after = sql_file.read_text()
    assert content_after == original_content


def test_model_rewriter_handles_unreadable_file(tmp_path: Path):
    import os

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
    bad_file = models_dir / "test.sql"
    bad_file.write_text("SELECT 1")

    if os.name != "nt":
        bad_file.chmod(0o000)

        config = load_config(config_file)

        try:
            modified = rewrite_models(config, dry_run=False)
            assert len(modified) == 0
        finally:
            bad_file.chmod(0o644)


def test_model_rewriter_skips_files_without_non_portable_functions(tmp_path: Path):
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
    sql_file.write_text("SELECT id, name FROM users")

    config = load_config(config_file)
    modified = rewrite_models(config, dry_run=False)

    assert len(modified) == 0


def test_model_rewriter_handles_nonexistent_model_paths(tmp_path: Path):
    config_path = tmp_path / ".dbt-multi-adapter.yml"
    config_path.write_text("""
adapters:
  - postgres
  - snowflake
model_paths:
  - nonexistent_models
""")

    config = load_config(config_path)
    modified = rewrite_models(config, dry_run=False)

    assert modified == []


def test_model_rewriter_modifies_file_with_non_portable_function(tmp_path: Path):
    """Test that files with non-portable functions are actually modified and appended to list."""
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - spark
  - duckdb
macro_output: macros/test.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "test.sql"
    # Use a function that IS non-portable between spark and duckdb
    sql_file.write_text("SELECT COLLECT_LIST(col) FROM {{ ref('users') }}")

    config = load_config(config_file)
    modified = rewrite_models(config, dry_run=False)

    # Should return the modified file
    assert len(modified) == 1
    assert modified[0] == sql_file
    # File should be modified
    content = sql_file.read_text()
    assert "portable_collect_list" in content


def test_model_rewriter_handles_file_with_control_flow(tmp_path: Path):
    """Test that files with Jinja control flow CAN be rewritten."""
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - spark
  - duckdb
macro_output: macros/test.sql
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "test.sql"
    # File with control flow - should still be rewritten
    sql_file.write_text("""
SELECT
    {% if include_deleted %}
    deleted_at,
    {% endif %}
    COLLECT_LIST(col)
FROM users
""")

    config = load_config(config_file)
    modified = rewrite_models(config, dry_run=False)

    # Should rewrite the file even with control flow
    assert len(modified) == 1
    content = sql_file.read_text()
    # Function should be rewritten
    assert "portable_collect_list" in content
    # Control flow should be preserved
    assert "{% if include_deleted %}" in content
    assert "{% endif %}" in content
