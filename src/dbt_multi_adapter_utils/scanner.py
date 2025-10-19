from collections import Counter
from pathlib import Path

from dbt_multi_adapter_utils.config import Config
from dbt_multi_adapter_utils.jinja_parser import JinjaTemplate
from dbt_multi_adapter_utils.sqlglot_adapter import extract_functions, get_function_differences


def _scan_sql_file(file_path: Path, *, primary_dialect: str) -> list[str]:
    try:
        content = file_path.read_text()
    except Exception:
        return []

    template = JinjaTemplate(content)
    safe_regions = template.extract_safe_sql_regions()

    all_functions = []
    for region in safe_regions:
        functions = extract_functions(region.masked_sql, dialect=primary_dialect)
        all_functions.extend([func_name for func_name, _ in functions])

    return all_functions


def scan_project(config: Config) -> dict[str, int]:
    if not config.scan_project:
        return {}

    primary_dialect = config.adapters[0]
    all_functions = Counter()

    for model_path in config.model_paths:
        if not model_path.exists():
            continue

        sql_files = list(model_path.rglob("*.sql"))

        for sql_file in sql_files:
            functions = _scan_sql_file(sql_file, primary_dialect=primary_dialect)
            all_functions.update(functions)

    known_differences = set(get_function_differences(config.adapters))

    non_portable = {func: count for func, count in all_functions.items() if func in known_differences}

    return dict(non_portable)
