"""Parity for module 06: Lakeflow spark_monthly_zone_revenue vs the legacy Spark job run unchanged.

The oracle is tests/parity/legacy06/run_legacy_06.py (06_spark_sql.py via spark-submit on
pyspark 3.3.2). Every column of the legacy report is DOUBLE, so the grouping keys are compared
exactly and every measure at FLOAT_RTOL (double sums depend on summation order).
    pytest tests/parity/test_legacy06.py -v
"""
from __future__ import annotations

import json
import math
import os
import pathlib

import pandas as pd
import pytest

TABLE = "nyc_taxi_gold.spark_monthly_zone_revenue"
MODEL = "spark_monthly_zone_revenue"
KEYS = ["revenue_zone", "revenue_month", "service_type"]
FLOAT_RTOL = 1e-9
FLOAT_ATOL = 1e-6  # sums that cancel to ~0 (refunds) have no meaningful relative error


@pytest.fixture(scope="module")
def legacy_export() -> pathlib.Path:
    root = pathlib.Path(os.environ.get("PARITY_WORK", pathlib.Path.home() / "parity-work"))
    path = root / "legacy06" / "export"
    if not (path / "summary.json").exists():
        pytest.fail(f"no legacy export at {path}; run tests/parity/legacy06/run_legacy_06.py first")
    return path


@pytest.fixture(scope="module")
def frames(spark, catalog, legacy_export):
    want = pd.read_parquet(legacy_export / "rows" / f"{MODEL}.parquet")
    got = spark.table(f"{catalog}.{TABLE}").toPandas()
    return want, got


def test_schema_matches_legacy(spark, catalog, legacy_export):
    want = json.loads((legacy_export / "summary.json").read_text())["models"][MODEL]["columns"]
    got = [f.name for f in spark.table(f"{catalog}.{TABLE}").schema.fields]
    assert got == list(want)


def test_rows_match_legacy(frames):
    want, got = frames
    want = want.assign(revenue_month=pd.to_datetime(want.revenue_month)).set_index(KEYS).sort_index()
    got = got.assign(revenue_month=pd.to_datetime(got.revenue_month)).set_index(KEYS).sort_index()
    assert want.index.is_unique and got.index.is_unique
    only_legacy, only_lakeflow = want.index.difference(got.index), got.index.difference(want.index)
    assert not len(only_legacy) and not len(only_lakeflow), \
        f"group mismatch: legacy-only {list(only_legacy[:10])}, lakeflow-only {list(only_lakeflow[:10])}"
    got = got.loc[want.index]
    for c in want.columns:
        bad = [k for k, x, y in zip(want.index, want[c], got[c])
               if not ((pd.isna(x) and pd.isna(y)) or
                       (not pd.isna(x) and not pd.isna(y) and
                        math.isclose(x, y, rel_tol=FLOAT_RTOL, abs_tol=FLOAT_ATOL)))]
        assert not bad, f"{c}: {len(bad)} groups differ, e.g. {[(k, want.at[k, c], got.at[k, c]) for k in bad[:5]]}"
