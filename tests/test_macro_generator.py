from pathlib import Path

from dbt_multi_adapter_utils.config import load_config
from dbt_multi_adapter_utils.macro_generator import generate_macros


def test_macro_generator_generates_macros_file(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: false
""")

    config = load_config(config_file)
    output_path = generate_macros(config, ["DATE_TRUNC", "REGEXP_EXTRACT"])

    assert output_path.exists()
    content = output_path.read_text()
    assert "portable_date_trunc" in content
    assert "portable_regexp_extract" in content
    assert "postgres__date_trunc" in content
    assert "snowflake__date_trunc" in content
    assert "adapter.dispatch('date_trunc', 'portable')" in content


def test_macro_generator_handles_empty_function_list(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("""
adapters:
  - postgres
  - snowflake
macro_output: macros/test.sql
scan_project: false
""")

    config = load_config(config_file)
    output_path = generate_macros(config, [])

    assert output_path == config.macro_output
