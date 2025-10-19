import re
from pathlib import Path

import sqlglot
from sqlglot import exp

from dbt_multi_adapter_utils.config import Config
from dbt_multi_adapter_utils.jinja_parser import JinjaTemplate
from dbt_multi_adapter_utils.sqlglot_adapter import normalize_dialect


def _should_rewrite_function(func_name: str, node: exp.Func) -> bool:
    func_sql = node.sql()
    func_lower = func_name.lower()

    if "*" in func_sql:
        return False

    return not (not node.args and func_lower in {"count", "sum", "min", "max", "avg"})


def _function_differs_across_dialects(node: exp.Func, *, dialects: list[str]) -> bool:
    transpiled_versions = set()

    for dialect in dialects:
        normalized = normalize_dialect(dialect)
        try:
            transpiled = node.sql(dialect=normalized)
            transpiled_versions.add(transpiled)
        except Exception:
            return True

    return len(transpiled_versions) > 1


def _create_macro_call(func_name: str, node: exp.Func, *, primary_dialect: str) -> str:
    func_lower = func_name.lower()
    macro_name = f"portable_{func_lower}"
    func_sql = node.sql(dialect=primary_dialect)

    match = re.match(r"[A-Z_][A-Z0-9_]*\s*\((.*)\)$", func_sql, re.IGNORECASE | re.DOTALL)

    if match:
        args_str = match.group(1).strip()
        if args_str and not args_str.startswith(("'", '"')):
            return f"{{{{ {macro_name}('{args_str}') }}}}"
        else:
            return f"{{{{ {macro_name}({args_str}) }}}}"
    else:
        return f"{{{{ {macro_name}() }}}}"


def _extract_function_name_from_sql(node: exp.Func, *, dialect: str) -> str | None:
    sql_output = node.sql(dialect=dialect)
    match = re.match(r"([A-Z_][A-Z0-9_]*)\s*\(", sql_output, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    match = re.match(r"^([A-Z_][A-Z0-9_]*)$", sql_output, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def _collect_functions_with_depth(
    node: exp.Expression, *, depth: int = 0, dialect: str = "spark"
) -> list[tuple[int, str, exp.Func]]:
    results = []

    if isinstance(node, exp.Func):
        func_name = _extract_function_name_from_sql(node, dialect=dialect)
        if func_name:
            results.append((depth, func_name, node))

    for child in node.iter_expressions():
        results.extend(_collect_functions_with_depth(child, depth=depth + 1, dialect=dialect))

    return results


def _read_and_validate_file(file_path: Path) -> str | None:
    try:
        return file_path.read_text()
    except Exception:
        return None


def _parse_region_to_ast(masked_sql: str, *, dialect: str) -> exp.Expression | None:
    try:
        return sqlglot.parse_one(masked_sql, dialect=dialect)
    except Exception:
        return None


def _filter_rewritable_functions(
    all_functions: list[tuple[int, str, exp.Func]],
    *,
    dialects: list[str],
    primary_dialect: str,
) -> list[tuple[int, str, str]]:
    functions_to_rewrite = []
    for depth, func_name, node in all_functions:
        if not _should_rewrite_function(func_name, node):
            continue

        if not _function_differs_across_dialects(node, dialects=dialects):
            continue

        func_sql = node.sql(dialect=primary_dialect)
        macro_call = _create_macro_call(func_name, node, primary_dialect=primary_dialect)
        functions_to_rewrite.append((depth, func_sql, macro_call))
    return functions_to_rewrite


def _find_pattern_in_region(func_sql: str, modified_region: str) -> str | None:
    if func_sql in modified_region:
        return func_sql

    func_sql_lower = func_sql.lower()
    modified_region_lower = modified_region.lower()

    if func_sql_lower in modified_region_lower:
        start_pos = modified_region_lower.find(func_sql_lower)
        return modified_region[start_pos : start_pos + len(func_sql)]

    return None


def _is_inside_jinja_macro(pattern_to_find: str, modified_region: str) -> bool:
    pattern_pos = modified_region.find(pattern_to_find)
    if pattern_pos == -1:
        return False

    before_pattern = modified_region[:pattern_pos]
    open_braces = before_pattern.count("{{") - before_pattern.count("}}")
    return open_braces > 0


def _apply_replacements_to_region(
    original_region_sql: str,
    functions_to_rewrite: list[tuple[int, str, str]],
) -> str:
    modified_region = original_region_sql

    for _depth, func_sql, macro_call in functions_to_rewrite:
        pattern_to_find = _find_pattern_in_region(func_sql, modified_region)
        if not pattern_to_find:
            continue

        if _is_inside_jinja_macro(pattern_to_find, modified_region):
            continue

        modified_region = modified_region.replace(pattern_to_find, macro_call, 1)

    return modified_region


def _process_single_region(
    region,
    modified_content: str,
    offset: int,
    *,
    dialects: list[str],
    primary_dialect: str,
) -> tuple[str, int]:
    ast = _parse_region_to_ast(region.masked_sql, dialect=primary_dialect)
    if not ast:
        return modified_content, offset

    all_functions = _collect_functions_with_depth(ast, dialect=primary_dialect)
    functions_to_rewrite = _filter_rewritable_functions(
        all_functions, dialects=dialects, primary_dialect=primary_dialect
    )

    if not functions_to_rewrite:
        return modified_content, offset

    functions_to_rewrite.sort(key=lambda x: -len(x[1]))

    adjusted_start = region.start + offset
    adjusted_end = region.end + offset
    original_region_sql = modified_content[adjusted_start:adjusted_end]

    modified_region = _apply_replacements_to_region(original_region_sql, functions_to_rewrite)

    if modified_region != original_region_sql:
        modified_content = modified_content[:adjusted_start] + modified_region + modified_content[adjusted_end:]
        offset += len(modified_region) - len(original_region_sql)

    return modified_content, offset


def _rewrite_sql_file(
    file_path: Path,
    *,
    dialects: list[str],
    primary_dialect: str,
    dry_run: bool = False,
) -> bool:
    original_content = _read_and_validate_file(file_path)
    if not original_content:
        return False

    template = JinjaTemplate(original_content)
    safety_check = template.can_safely_rewrite()
    if not safety_check.can_rewrite:
        return False

    safe_regions = template.extract_safe_sql_regions()
    modified_content = original_content
    offset = 0

    for region in safe_regions:
        modified_content, offset = _process_single_region(
            region,
            modified_content,
            offset,
            dialects=dialects,
            primary_dialect=primary_dialect,
        )

    if modified_content != original_content:
        if not dry_run:
            file_path.write_text(modified_content)
        return True

    return False


def rewrite_models(config: Config, *, dry_run: bool = False) -> list[Path]:
    primary_dialect = config.adapters[0]
    modified_files = []

    for model_path in config.model_paths:
        if not model_path.exists():
            continue

        sql_files = list(model_path.rglob("*.sql"))

        for sql_file in sql_files:
            was_modified = _rewrite_sql_file(
                sql_file,
                dialects=config.adapters,
                primary_dialect=primary_dialect,
                dry_run=dry_run,
            )

            if was_modified:
                modified_files.append(sql_file)

    return modified_files
