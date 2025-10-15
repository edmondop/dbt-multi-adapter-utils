from dbt_multi_adapter_utils.sqlglot_adapter import (
    extract_functions,
    get_function_differences,
    normalize_dialect,
    transpile_sql,
)


def test_sqlglot_adapter_normalizes_dialect_correctly():
    assert normalize_dialect("postgres") == "postgres"
    assert normalize_dialect("POSTGRES") == "postgres"
    assert normalize_dialect("postgresql") == "postgres"


def test_sqlglot_adapter_extracts_functions_from_sql():
    sql = "SELECT DATE_TRUNC('month', created_at), COUNT(*) FROM users"
    functions = extract_functions(sql, dialect="postgres")

    func_names = [name for name, _ in functions]
    assert any("TRUNC" in name.upper() for name in func_names)
    assert any("COUNT" in name.upper() for name in func_names)


def test_sqlglot_adapter_transpiles_sql_between_dialects():
    sql = "SELECT CURRENT_DATE()"
    transpiled = transpile_sql(sql, read_dialect="postgres", write_dialect="snowflake")

    assert transpiled is not None
    assert len(transpiled) > 0


def test_sqlglot_adapter_gets_function_differences():
    adapters = ["postgres", "snowflake"]
    functions = get_function_differences(adapters)

    assert isinstance(functions, list)
