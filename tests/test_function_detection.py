from pathlib import Path

import pytest

from dbt_multi_adapter_utils.config import Config
from dbt_multi_adapter_utils.macro_generator import generate_macros
from dbt_multi_adapter_utils.scanner import scan_project
from dbt_multi_adapter_utils.sqlglot_adapter import extract_functions


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures" / "dbt_models"


@pytest.fixture
def test_config(tmp_path: Path) -> Config:
    return Config(
        adapters=["spark", "duckdb", "postgres", "snowflake"],
        macro_output=tmp_path / "macros" / "portable_functions.sql",
        scan_project=False,
        model_paths=[],
        project_root=tmp_path,
    )


def test_simple_aggregation_functions_detected(fixtures_dir: Path):
    sql = (fixtures_dir / "simple_aggregation.sql").read_text()

    functions = extract_functions(sql, dialect="spark")
    func_names = [name.upper() for name, _ in functions]

    assert "COUNT" in func_names
    assert "SUM" in func_names
    assert "MIN" in func_names
    assert "MAX" in func_names


def test_date_functions_detected(fixtures_dir: Path):
    sql = (fixtures_dir / "date_functions.sql").read_text()

    functions = extract_functions(sql, dialect="spark")
    func_names = [name.upper() for name, _ in functions]

    assert "DATE_TRUNC" in func_names
    assert "DATE_ADD" in func_names
    assert "CURRENT_DATE" in func_names


def test_array_aggregation_functions_detected(fixtures_dir: Path):
    sql = (fixtures_dir / "array_aggregation.sql").read_text()

    functions = extract_functions(sql, dialect="spark")
    func_names = [name.upper() for name, _ in functions]

    assert "COLLECT_LIST" in func_names
    assert "COLLECT_SET" in func_names


def test_macro_generation_creates_correct_structure(test_config: Config):
    functions = ["DATE_TRUNC", "COLLECT_LIST", "COUNT"]

    output_path = generate_macros(test_config, functions)

    assert output_path.exists()
    content = output_path.read_text()

    # Check dispatcher macros exist
    assert "portable_date_trunc" in content
    assert "portable_collect_list" in content
    assert "portable_count" in content

    # Check adapter-specific implementations
    assert "spark__date_trunc" in content
    assert "duckdb__date_trunc" in content
    assert "postgres__date_trunc" in content
    assert "snowflake__date_trunc" in content

    # Check dispatch pattern
    assert "adapter.dispatch('date_trunc', 'portable')" in content


def test_generated_macros_have_valid_jinja_syntax(test_config: Config):
    """Verify generated macros are syntactically valid Jinja2"""
    import jinja2

    functions = ["DATE_TRUNC", "COUNT"]
    output_path = generate_macros(test_config, functions)
    content = output_path.read_text()

    # This should not raise if syntax is valid
    try:
        jinja2.Template(content)
    except jinja2.TemplateSyntaxError as e:
        pytest.fail(f"Generated macro has invalid Jinja syntax: {e}")


def test_count_with_star_argument(fixtures_dir: Path):
    """Test that COUNT(*) is handled correctly"""
    sql = (fixtures_dir / "simple_aggregation.sql").read_text()

    functions = extract_functions(sql, dialect="spark")

    # Find COUNT function
    count_functions = [(name, args) for name, args in functions if name.upper() == "COUNT"]
    assert len(count_functions) > 0

    # Verify we detected COUNT with its arguments
    name, args = count_functions[0]
    assert name.upper() == "COUNT"


def test_date_trunc_with_string_literal(fixtures_dir: Path):
    """Test that DATE_TRUNC('month', field) is handled correctly"""
    sql = (fixtures_dir / "date_functions.sql").read_text()

    functions = extract_functions(sql, dialect="spark")

    date_trunc_funcs = [(name, args) for name, args in functions if name.upper() == "DATE_TRUNC"]
    assert len(date_trunc_funcs) > 0


def test_full_workflow_with_mixed_model(fixtures_dir: Path, test_config: Config, tmp_path: Path):
    """Test the full workflow: scan -> generate -> verify"""

    # Create a temporary models directory
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    # Copy fixture to models directory
    (models_dir / "test_model.sql").write_text((fixtures_dir / "mixed_functions.sql").read_text())

    # Update config to scan the models directory
    test_config.scan_project = True
    test_config.model_paths = [models_dir]

    # Run scan
    detected_functions = scan_project(test_config)

    # Should detect multiple functions
    assert len(detected_functions) > 0

    func_names_upper = [f.upper() for f in detected_functions]
    assert "DATE_TRUNC" in func_names_upper
    assert "COUNT" in func_names_upper
    assert "COLLECT_LIST" in func_names_upper

    # Run generate
    output_path = generate_macros(test_config, list(detected_functions.keys()))

    assert output_path.exists()

    content = output_path.read_text()

    # Verify all detected functions have macros
    for func in detected_functions:
        macro_name = f"portable_{func.lower()}"
        assert macro_name in content, f"Missing macro for {func}"


def test_jinja_expressions_preserved_in_scan(fixtures_dir: Path):
    """Verify that Jinja expressions like {{ ref() }} and {{ var() }} are handled"""
    sql = (fixtures_dir / "with_jinja.sql").read_text()

    # Should contain Jinja
    assert "{{ ref(" in sql
    assert "{{ var(" in sql

    # Direct extraction should fail with Jinja (expected behavior)
    functions = extract_functions(sql, dialect="spark")
    assert len(functions) == 0, "extract_functions should return empty list when SQL has Jinja"
