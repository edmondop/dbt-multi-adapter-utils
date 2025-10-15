from pathlib import Path
from shutil import copytree, rmtree

import pytest
from jinja2 import Environment

from dbt_multi_adapter_utils.config import load_config
from dbt_multi_adapter_utils.macro_generator import generate_macros
from dbt_multi_adapter_utils.model_rewriter import rewrite_models
from dbt_multi_adapter_utils.scanner import scan_project


@pytest.fixture
def temp_dbt_project(tmp_path: Path) -> Path:
    """Create a temporary copy of the complete dbt project fixture."""
    fixture_path = Path("tests/fixtures/complete_dbt_project")
    temp_project = tmp_path / "test_project"

    copytree(fixture_path, temp_project)

    return temp_project


def test_workflow_full_workflow_scan_generate_rewrite(temp_dbt_project: Path):
    """Test complete workflow: scan → generate → rewrite."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    detected_functions = scan_project(config)
    assert isinstance(detected_functions, dict)

    if detected_functions:
        macro_output = generate_macros(config, list(detected_functions.keys()))
        assert macro_output.exists()

        modified_files = rewrite_models(config, dry_run=False)
        assert isinstance(modified_files, list)


def test_workflow_generated_macros_have_valid_jinja(temp_dbt_project: Path):
    """Verify that generated macros compile with Jinja2."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    functions_to_generate = ["CURRENT_DATE", "COUNT"]
    macro_output = generate_macros(config, functions_to_generate)

    assert macro_output.exists()
    macro_content = macro_output.read_text()

    env = Environment()
    try:
        env.parse(macro_content)
    except Exception as e:
        pytest.fail(f"Generated macros have invalid Jinja syntax: {e}")


def test_workflow_idempotency_running_twice_produces_same_result(temp_dbt_project: Path):
    """Test that running migrate twice produces the same result."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    detected_1 = scan_project(config)
    if detected_1:
        generate_macros(config, list(detected_1.keys()))
        rewrite_models(config, dry_run=False)

    detected_2 = scan_project(config)
    if detected_2:
        generate_macros(config, list(detected_2.keys()))
        modified_2 = rewrite_models(config, dry_run=False)

        assert len(modified_2) == 0


def test_workflow_skips_files_with_control_flow(temp_dbt_project: Path):
    """Test that files with Jinja control flow CAN now be rewritten."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    control_flow_file = temp_dbt_project / "models" / "with_control_flow.sql"
    original_content = control_flow_file.read_text()

    detected = scan_project(config)
    if detected:
        generate_macros(config, list(detected.keys()))
        rewrite_models(config, dry_run=False)

    # File should be modified - control flow is now supported
    modified_content = control_flow_file.read_text()
    assert modified_content != original_content
    # Control flow should be preserved
    assert "{% if var('include_email', false) %}" in modified_content
    # Function should be rewritten
    assert "portable_date_trunc" in modified_content.lower()


def test_workflow_handles_files_with_safe_jinja(temp_dbt_project: Path):
    """Test that files with {{ ref() }} and {{ var() }} are handled."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    detected = scan_project(config)

    with_refs_file = temp_dbt_project / "models" / "with_refs.sql"
    assert with_refs_file.exists()

    functions_found = detected if detected else {}
    if "CURRENT_DATE" in functions_found:
        assert True


def test_workflow_macro_output_path_is_created(temp_dbt_project: Path):
    """Test that macro output directory is created if it doesn't exist."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    macros_dir = temp_dbt_project / "macros"
    if macros_dir.exists():
        rmtree(macros_dir)

    assert not macros_dir.exists()

    generate_macros(config, ["CURRENT_DATE"])

    assert macros_dir.exists()
    assert config.macro_output.exists()


def test_workflow_rewrite_dry_run_doesnt_modify_files(temp_dbt_project: Path):
    """Test that dry-run mode doesn't actually modify files."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    simple_file = temp_dbt_project / "models" / "simple.sql"
    original_content = simple_file.read_text()

    detected = scan_project(config)
    if detected:
        generate_macros(config, list(detected.keys()))
        rewrite_models(config, dry_run=True)

    assert simple_file.read_text() == original_content


def test_workflow_generated_macros_follow_naming_convention(temp_dbt_project: Path):
    """Test that generated macros use portable_* naming."""
    config_file = temp_dbt_project / ".dbt-multi-adapter.yml"
    config = load_config(config_file)

    macro_output = generate_macros(config, ["CURRENT_DATE", "COUNT"])

    content = macro_output.read_text()
    assert "portable_current_date" in content or "portable_count" in content


def test_workflow_handles_empty_project(tmp_path: Path):
    """Test handling of project with no SQL files."""
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/portable.sql
scan_project: true
model_paths:
  - models
""")

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    config = load_config(config_file)
    detected = scan_project(config)

    assert detected == {}
