"""Job task: put the devin / source_module Unity Catalog tags on every object the bundle owns.

Pipelines and jobs take tags in the bundle; schemas, the volume and the
pipeline's tables are tagged here because bundles can't set UC tags on them.
"""
import argparse

from databricks.sdk.runtime import spark

SOURCE_MODULE = {"nyc_taxi_bronze": "02-workflow-orchestration",
                 "nyc_taxi_silver": "04-analytics-engineering",
                 "nyc_taxi_gold": "04-analytics-engineering",
                 "nyc_taxi_ops": "ops"}
TABLE_MODULE = {("nyc_taxi_gold", "spark_monthly_zone_revenue"): "06-batch"}
KIND = {"MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE",
        "MANAGED": "TABLE", "EXTERNAL": "TABLE", "VIEW": "VIEW"}


def tags(module: str) -> str:
    return f"('devin' = 'true', 'source_module' = '{module}')"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    a = ap.parse_args()
    stmts = [f"ALTER VOLUME {a.catalog}.nyc_taxi_bronze.landing SET TAGS {tags(SOURCE_MODULE['nyc_taxi_bronze'])}"]
    for schema, module in SOURCE_MODULE.items():
        stmts.append(f"ALTER SCHEMA {a.catalog}.{schema} SET TAGS {tags(module)}")
        rows = spark.sql(
            f"SELECT table_name, table_type FROM {a.catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name NOT LIKE '\\_\\_%'").collect()  # skip pipeline internals
        for r in rows:
            stmts.append(f"ALTER {KIND.get(r.table_type, 'TABLE')} {a.catalog}.{schema}.`{r.table_name}` "
                         f"SET TAGS {tags(TABLE_MODULE.get((schema, r.table_name), module))}")
    failed = []
    for s in stmts:
        try:
            spark.sql(s)
            print("ok  ", s)
        except Exception as e:  # noqa: BLE001  report every failure, then fail the task
            failed.append(s)
            print("FAIL", s, "->", str(e).splitlines()[0])
    if failed:
        raise RuntimeError(f"{len(failed)} tag statements failed")


if __name__ == "__main__":
    main()
