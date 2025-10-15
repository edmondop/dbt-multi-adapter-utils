from pathlib import Path

import pytest

from dbt_multi_adapter_utils.config import load_config


def test_config_loads_successfully_with_valid_file():
    config_path = Path("tests/fixtures/sample_dbt_project/.dbt-multi-adapter.yml")
    config = load_config(config_path)

    assert config.adapters == ["postgres", "snowflake", "bigquery"]
    assert config.scan_project is True
    assert len(config.model_paths) == 1


def test_config_raises_error_when_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config(Path("nonexistent.yml"))


def test_config_raises_error_when_too_few_adapters(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("adapters:\n  - postgres\n")

    with pytest.raises(ValueError, match="At least 2 adapters"):
        load_config(config_file)


def test_config_raises_error_when_file_is_empty(tmp_path: Path):
    config_file = tmp_path / ".dbt-multi-adapter.yml"
    config_file.write_text("")

    with pytest.raises(ValueError, match="Config file is empty"):
        load_config(config_file)
