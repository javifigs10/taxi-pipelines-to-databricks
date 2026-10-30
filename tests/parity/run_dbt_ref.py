"""Run the (B1-B6 patched) dbt project with dbt-databricks into devin_dev.dbt_ref.

This is issue #1's warehouse-side reference: dbt reads the *same* bronze
tables the Lakeflow pipeline reads. To replay the load sequence, the dbt
sources point at views in `dbt_ref_src` that expose only the files loaded so
far; after each month the views are widened and `dbt build` runs again
(full build first, incremental after), exactly like the oracle.

    python tests/parity/run_dbt_ref.py --months 2019-01,2019-02
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
PROJECT = REPO / "04-analytics-engineering" / "taxi_rides_ny"
TAGS = "('devin' = 'true', 'source_module' = '04-analytics-engineering')"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--taxis", default="green,yellow")
    ap.add_argument("--catalog", default="devin_dev")
    ap.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", "6b49dc9820b11687"))
    ap.add_argument("--work", type=pathlib.Path, default=pathlib.Path.home() / "parity-work" / "dbt_ref")
    a = ap.parse_args()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    shutil.rmtree(a.work, ignore_errors=True)
    project = a.work / "taxi_rides_ny"
    shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns("target", "dbt_packages", "logs"))
    subprocess.run(["git", "apply", "-p3", str(HERE / "oracle" / "patches" / "behaviours-B1-B6.patch")],
                   cwd=project, check=True)
    env = {**os.environ, "DATABRICKS_WAREHOUSE_ID": a.warehouse_id, "DBT_REF_CATALOG": a.catalog,
           "DBT_RAW_CATALOG": a.catalog, "DBT_RAW_SCHEMA": "dbt_ref_src"}
    dbt = [str(pathlib.Path(sys.executable).with_name("dbt"))]
    common = ["--project-dir", str(project), "--profiles-dir", str(HERE / "oracle" / "profiles"), "--target", "dbt_ref"]
    subprocess.run([*dbt, "deps", *common], check=True, env=env, cwd=project)

    spark.sql(f"DROP SCHEMA IF EXISTS {a.catalog}.dbt_ref CASCADE")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.dbt_ref_src COMMENT 'Load-sequence views over nyc_taxi_bronze for dbt_ref'")
    months = a.months.split(",")
    for i, month in enumerate(months):
        files = ", ".join(f"'{{taxi}}_tripdata_{m}.csv'" for m in months[: i + 1])
        for taxi in a.taxis.split(","):
            spark.sql(f"CREATE OR REPLACE VIEW {a.catalog}.dbt_ref_src.{taxi}_tripdata AS "
                      f"SELECT * FROM {a.catalog}.nyc_taxi_bronze.{taxi}_tripdata "
                      f"WHERE filename IN ({files.format(taxi=taxi)})")
        print(f"+ dbt build (batch {month})", flush=True)
        subprocess.run([*dbt, "build", *common, "--vars", json.dumps({"batch_file_month": month})],
                       check=True, env=env, cwd=project)
        print(month, spark.sql(f"SELECT count(*) AS n FROM {a.catalog}.dbt_ref.fct_trips").collect()[0].n, flush=True)

    for schema in ("dbt_ref", "dbt_ref_src"):
        spark.sql(f"ALTER SCHEMA {a.catalog}.{schema} SET TAGS {TAGS}")
        for r in spark.sql(f"SELECT table_name, table_type FROM {a.catalog}.information_schema.tables "
                           f"WHERE table_schema = '{schema}'").collect():
            kind = "VIEW" if r.table_type == "VIEW" else "TABLE"
            spark.sql(f"ALTER {kind} {a.catalog}.{schema}.`{r.table_name}` SET TAGS {TAGS}")


if __name__ == "__main__":
    main()
