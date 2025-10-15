import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

_DIALECT_MAP = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "spark": "spark",
    "databricks": "databricks",
    "redshift": "redshift",
    "duckdb": "duckdb",
    "trino": "trino",
    "presto": "presto",
}


def normalize_dialect(adapter: str) -> str:
    normalized = adapter.lower()
    return _DIALECT_MAP.get(normalized, normalized)


def _get_dialect(adapter: str) -> Dialect:
    normalized = normalize_dialect(adapter)
    return Dialect.get_or_raise(normalized)


def get_function_differences(adapters: list[str]) -> list[str]:
    dialect_classes = [_get_dialect(adapter) for adapter in adapters]

    all_functions = set()
    for dialect_cls in dialect_classes:
        if hasattr(dialect_cls, "Parser") and hasattr(dialect_cls.Parser, "FUNCTIONS"):
            all_functions.update(dialect_cls.Parser.FUNCTIONS.keys())

    differing_functions = []
    for func_name in all_functions:
        implementations = set()
        for dialect_cls in dialect_classes:
            if hasattr(dialect_cls, "Parser") and hasattr(dialect_cls.Parser, "FUNCTIONS"):
                impl = dialect_cls.Parser.FUNCTIONS.get(func_name)
                if impl:
                    implementations.add(str(impl))

        if len(implementations) > 1 or len(implementations) < len(dialect_classes):
            differing_functions.append(func_name)

    return sorted(differing_functions)


def _parse_sql(sql: str, *, dialect: str) -> exp.Expression:
    normalized = normalize_dialect(dialect)
    return sqlglot.parse_one(sql, dialect=normalized)


def transpile_sql(sql: str, *, read_dialect: str, write_dialect: str) -> str:
    read = normalize_dialect(read_dialect)
    write = normalize_dialect(write_dialect)

    result = sqlglot.transpile(sql, read=read, write=write)
    return result[0] if result else sql


def extract_functions(sql: str, *, dialect: str) -> list[tuple[str, exp.Func]]:
    try:
        ast = _parse_sql(sql, dialect=dialect)
    except Exception:
        return []

    normalized = normalize_dialect(dialect)
    functions = []
    for node in ast.walk():
        if isinstance(node, exp.Func):
            # Get the original SQL representation for the source dialect
            original_sql = node.sql(dialect=normalized)
            # Extract just the function name (before the opening parenthesis)
            func_name = original_sql.split("(")[0].strip().upper()
            functions.append((func_name, node))

    return functions
