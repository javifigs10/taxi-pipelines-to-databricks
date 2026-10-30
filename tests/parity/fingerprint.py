"""Engine-neutral table fingerprints for parity checks.

The same SQL is generated for DuckDB (the dbt oracle) and Databricks (dbt_ref
and the Lakeflow output): per bucket, the row count, an order-independent sum
of per-row MD5 hashes over a canonical string of every column, and the sum and
non-null count of every numeric column. Two tables fingerprint identically iff
(with overwhelming probability) they contain the same multiset of rows.

Floating-point columns (only the mart's avg_* columns) are left out: engines may
differ in the last bits of an average. They are compared row by row with a
relative tolerance instead (see test_parity.py).
"""
from __future__ import annotations

NUMERIC_PREFIXES = ("tinyint", "smallint", "int", "bigint", "hugeint", "decimal", "numeric", "double", "float", "real")
INTEGER_PREFIXES = ("tinyint", "smallint", "int", "bigint", "hugeint")


def kind(data_type: str) -> str:
    t = data_type.lower()
    if t.startswith(("double", "float", "real")):
        return "float"
    if t.startswith(INTEGER_PREFIXES):
        return "integer"
    if t.startswith(("decimal", "numeric")):
        return "decimal"
    if t.startswith("timestamp"):
        return "timestamp"
    if t.startswith("date"):
        return "date"
    return "string"


def canonical(col: str, data_type: str, dialect: str) -> str:
    c = f"`{col}`" if dialect == "databricks" else f'"{col}"'
    k = kind(data_type)
    if k == "float":
        expr = f"cast(cast(round({c}, 6) as decimal(38,6)) as string)"
    elif k == "decimal":
        expr = f"cast(cast({c} as decimal(38,3)) as string)"
    elif k == "timestamp":
        expr = f"date_format({c}, 'yyyy-MM-dd HH:mm:ss')" if dialect == "databricks" else f"strftime({c}, '%Y-%m-%d %H:%M:%S')"
    elif k == "date":
        expr = f"date_format({c}, 'yyyy-MM-dd')" if dialect == "databricks" else f"strftime({c}, '%Y-%m-%d')"
    else:
        expr = f"cast({c} as string)"
    if dialect == "duckdb":
        expr = expr.replace("as string)", "as varchar)")
    return f"coalesce({expr}, '<null>')"


def numeric_value(col: str, data_type: str, dialect: str) -> str:
    c = f"`{col}`" if dialect == "databricks" else f'"{col}"'
    if kind(data_type) == "float":
        return f"cast(round({c}, 6) as decimal(38,6))"
    return f"cast({c} as decimal(38,6))"


def bucket_exprs(columns: dict[str, str]) -> list[tuple[str, str]]:
    names = set(columns)
    if {"pickup_datetime", "service_type"} <= names:
        return [("service_type", "service_type"), ("bucket_day", "cast(pickup_datetime as date)")]
    if {"revenue_month", "service_type"} <= names:
        return [("service_type", "service_type"), ("bucket_day", "cast(revenue_month as date)")]
    return []


def fingerprint_sql(table: str, columns: dict[str, str], dialect: str) -> str:
    """`columns` maps column name -> engine data type, in a fixed order."""
    ordered = [c for c in sorted(columns) if kind(columns[c]) != "float"]
    row = "concat_ws('|', " + ", ".join(canonical(c, columns[c], dialect) for c in ordered) + ")"
    if dialect == "databricks":
        row_hash = f"cast(conv(substr(md5({row}), 1, 15), 16, 10) as decimal(20,0))"
        hash_sum = f"cast(sum({row_hash}) as decimal(38,0))"
    else:
        row_hash = f"('0x' || substr(md5({row}), 1, 15))::bigint"
        hash_sum = f"cast(sum({row_hash}) as decimal(38,0))"
    buckets = bucket_exprs(columns)
    select = [f"{expr} as {alias}" for alias, expr in buckets]
    select += ["count(*) as n_rows", f"{hash_sum} as row_hash_sum"]
    for c in ordered:
        if kind(columns[c]) in ("integer", "decimal"):
            v = numeric_value(c, columns[c], dialect)
            select += [f"cast(sum({v}) as decimal(38,6)) as `sum__{c}`", f"count({v}) as `count__{c}`"]
    sql = "select " + ",\n       ".join(select) + f"\nfrom {table}"
    if buckets:
        sql += "\ngroup by " + ", ".join(str(i + 1) for i in range(len(buckets)))
    if dialect == "duckdb":
        sql = sql.replace("`", '"')
    return sql
