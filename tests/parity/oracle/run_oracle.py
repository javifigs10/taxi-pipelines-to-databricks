"""Run the dbt project on DuckDB as the independent parity oracle.

    python tests/parity/oracle/run_oracle.py --scenario in_order --months 2019-01,2019-02

For each month in order: load the raw month Kestra-style, then `dbt build`
(the first build is a full build, later ones are incremental, exactly like a
scheduled dbt run after each Kestra load). The project is copied to a work dir
and the B1-B6 patch (docs/migration/04-analytics-engineering.md) is applied
unless --unpatched. Outputs per model: a fingerprint parquet (see
fingerprint.py) and, for small models, the full table.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

from fingerprint import fingerprint_sql
from kestra_raw import load_month

PROJECT = REPO / "04-analytics-engineering" / "taxi_rides_ny"
FULL_EXPORT_MAX_ROWS = 100_000


def dbt(project: pathlib.Path, env: dict, *args: str) -> None:
    cmd = [str(pathlib.Path(sys.executable).with_name("dbt")), *args, "--project-dir", str(project),
           "--profiles-dir", str(HERE / "profiles"), "--target", "oracle"]
    print("+", " ".join(args), flush=True)
    subprocess.run(cmd, check=True, env=env, cwd=project)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--months", required=True, help="comma-separated load order, e.g. 2019-02,2019-01")
    ap.add_argument("--taxis", default="green,yellow")
    ap.add_argument("--work", type=pathlib.Path, default=pathlib.Path.home() / "parity-work")
    ap.add_argument("--data", type=pathlib.Path, default=pathlib.Path.home() / "parity-work" / "data")
    ap.add_argument("--unpatched", action="store_true")
    ap.add_argument("--export-only", action="store_true", help="re-export an existing run")
    a = ap.parse_args()

    work = a.work / a.scenario
    db = work / "taxi_rides_ny.duckdb"  # file name = database name the dbt sources expect
    if a.export_only:
        export(work, db, json.loads((work / "export" / "summary.json").read_text()))
        return
    shutil.rmtree(work, ignore_errors=True)
    project = work / "taxi_rides_ny"
    shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns("target", "dbt_packages", "logs"))
    if not a.unpatched:
        subprocess.run(["git", "apply", "-p3", str(HERE / "patches" / "behaviours-B1-B6.patch")], cwd=project, check=True)

    env = {**os.environ, "ORACLE_DUCKDB_PATH": str(db)}
    dbt(project, env, "deps")

    summary: dict = {"scenario": a.scenario, "months": a.months.split(","), "patched": not a.unpatched, "runs": []}
    for month in summary["months"]:
        con = duckdb.connect(str(db))
        loaded = {t: load_month(con, a.data, t, month) for t in a.taxis.split(",")}
        con.close()
        dbt(project, env, "build", "--vars", json.dumps({"batch_file_month": month}))
        con = duckdb.connect(str(db), read_only=True)
        fct = con.execute("select count(*) from oracle.fct_trips").fetchone()[0]
        con.close()
        summary["runs"].append({"month": month, "raw_rows_loaded": loaded, "fct_trips_rows": fct})
        print(summary["runs"][-1], flush=True)

    export(work, db, summary)


def export(work: pathlib.Path, db: pathlib.Path, summary: dict) -> None:
    out = work / "export"
    shutil.rmtree(out / "fingerprints", ignore_errors=True)
    shutil.rmtree(out / "rows", ignore_errors=True)
    (out / "fingerprints").mkdir(parents=True)
    (out / "rows").mkdir()
    con = duckdb.connect(str(db), read_only=True)
    models = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_schema = 'oracle' order by 1").fetchall()]
    summary["models"] = {}
    for m in models:
        cols = dict(con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = 'oracle' and table_name = ? order by ordinal_position", [m]).fetchall())
        n = con.execute(f"select count(*) from oracle.{m}").fetchone()[0]
        con.execute(f"copy ({fingerprint_sql(f'oracle.{m}', cols, 'duckdb')}) to '{out}/fingerprints/{m}.parquet'")
        if n <= FULL_EXPORT_MAX_ROWS:
            con.execute(f"copy oracle.{m} to '{out}/rows/{m}.parquet'")
        summary["models"][m] = {"rows": n, "columns": cols}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps({m: v["rows"] for m, v in summary["models"].items()}, indent=2))


if __name__ == "__main__":
    main()
