"""Oracle for module 06: run the legacy batch job unchanged and export its report.

Legacy chain, as taught in 06-batch:
  download_data.sh          -> data/raw/<taxi>/<yyyy>/<mm>/<taxi>_tripdata_<yyyy>_<mm>.csv.gz
  code/05_taxi_schema.ipynb -> data/pq/<taxi>/<yyyy>/<mm>/   (explicit schema, csv -> parquet)
  code/06_spark_sql.py      -> report parquet               (spark-submit, as in 14-creating-a-local-spark-cluster.md)

The notebook cells and the script run as written. The only edit is the
hard-coded loop in conversion cells 5 and 10 (`year = 2020`, `range(1, 13)`),
which becomes the proof scope (2019, months 1-2 for --months 2019-01,2019-02);
the edit is printed. The notebook's 2021 cells are not run. Needs pyspark 3.3.2
(the course version) and Java 17:

  uv venv -p 3.10 ~/spark06 && VIRTUAL_ENV=~/spark06 uv pip install pyspark==3.3.2 duckdb pandas pyarrow
  ~/spark06/bin/python tests/parity/legacy06/run_legacy_06.py --months 2019-01,2019-02
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
import pyspark

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parent / "oracle"))
from fingerprint import fingerprint_sql
from kestra_raw import fetch

CODE = REPO / "06-batch" / "code"
NOTEBOOK = CODE / "05_taxi_schema.ipynb"
SCRIPT = CODE / "06_spark_sql.py"
SETUP_CELLS = (0, 1, 3, 4)          # imports, SparkSession, types, green/yellow schemas
CONVERT_CELLS = {"green": 5, "yellow": 10}  # the notebook's year-2020 loop, re-pointed at the proof scope
MODEL = "spark_monthly_zone_revenue"


def cell(nb: dict, i: int) -> str:
    return "".join(nb["cells"][i]["source"])


def stage_raw(data_dir: pathlib.Path, work: pathlib.Path, months: list[str]) -> None:
    for taxi in ("green", "yellow"):
        for m in months:
            yyyy, mm = m.split("-")
            dest = work / "data" / "raw" / taxi / yyyy / mm / f"{taxi}_tripdata_{yyyy}_{mm}.csv.gz"
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                shutil.copyfile(fetch(data_dir, taxi, m), dest)


def convert_to_parquet(work: pathlib.Path, months: list[str]) -> None:
    years = sorted({m.split("-")[0] for m in months})
    assert len(years) == 1, "one year per run (the notebook loops per year)"
    month_nums = sorted(int(m.split("-")[1]) for m in months)
    nb = json.loads(NOTEBOOK.read_text())
    ns: dict = {}
    os.chdir(work)
    for i in SETUP_CELLS:
        exec(cell(nb, i), ns)  # noqa: S102  the notebook, as written
    for taxi, i in CONVERT_CELLS.items():
        src = cell(nb, i)
        edited = src.replace("year = 2020", f"year = {years[0]}") \
                    .replace("range(1, 13)", f"range({month_nums[0]}, {month_nums[-1] + 1})")
        assert edited != src
        print(f"notebook cell {i} ({taxi}): year = 2020 -> {years[0]}, range(1, 13) -> "
              f"range({month_nums[0]}, {month_nums[-1] + 1})")
        shutil.rmtree(work / "data" / "pq" / taxi, ignore_errors=True)
        exec(edited, ns)  # noqa: S102
    ns["spark"].stop()


def run_report(work: pathlib.Path, year: str) -> pathlib.Path:
    out = work / "data" / f"report-{year}"
    bindir = pathlib.Path(sys.executable).parent
    env = {**os.environ, "SPARK_HOME": os.path.dirname(pyspark.__file__), "PYSPARK_PYTHON": sys.executable,
           "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}"}
    subprocess.run([str(bindir / "spark-submit"), "--master", "local[*]", str(SCRIPT),
                    f"--input_green=data/pq/green/{year}/*/",
                    f"--input_yellow=data/pq/yellow/{year}/*/",
                    f"--output={out.relative_to(work)}"], cwd=work, check=True, env=env)
    return out


def export(work: pathlib.Path, report: pathlib.Path, summary: dict) -> None:
    out = work / "export"
    shutil.rmtree(out, ignore_errors=True)
    (out / "fingerprints").mkdir(parents=True)
    (out / "rows").mkdir()
    con = duckdb.connect()
    con.execute(f"create view r as select * from read_parquet('{report}/*.parquet')")
    cols = dict(con.execute("select column_name, column_type from (describe r)").fetchall())
    con.execute(f"copy ({fingerprint_sql('r', cols, 'duckdb')}) to '{out}/fingerprints/{MODEL}.parquet'")
    con.execute(f"copy r to '{out}/rows/{MODEL}.parquet'")
    summary["models"] = {MODEL: {"rows": con.execute("select count(*) from r").fetchone()[0], "columns": cols}}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary["models"], indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default="2019-01,2019-02")
    ap.add_argument("--work", default=str(pathlib.Path.home() / "parity-work" / "legacy06"))
    ap.add_argument("--data-dir", default=str(pathlib.Path.home() / "parity-work" / "data"))
    a = ap.parse_args()
    months = a.months.split(",")
    work = pathlib.Path(a.work)
    work.mkdir(parents=True, exist_ok=True)
    stage_raw(pathlib.Path(a.data_dir), work, months)
    convert_to_parquet(work, months)
    report = run_report(work, months[0].split("-")[0])
    export(work, report, {"months": months, "script": str(SCRIPT.relative_to(REPO)),
                          "notebook": str(NOTEBOOK.relative_to(REPO))})


if __name__ == "__main__":
    main()
