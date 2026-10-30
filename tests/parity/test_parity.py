"""Parity: Lakeflow output vs the DuckDB dbt oracle and vs dbt-databricks (dbt_ref).

Run after the job has replayed the same load sequence as the oracle scenario:
    PARITY_SCENARIO=in_order pytest tests/parity -v
"""
from __future__ import annotations

import json
import math
from decimal import Decimal

import pandas as pd
import pytest
from fingerprint import bucket_exprs, fingerprint_sql, kind

# oracle model -> Lakeflow table (schema.table). Staging models are temporary views in Lakeflow.
LAKEFLOW = {
    "taxi_zone_lookup": "nyc_taxi_silver.taxi_zone_lookup",
    "payment_type_lookup": "nyc_taxi_silver.payment_type_lookup",
    "int_trips_unioned": "nyc_taxi_silver.int_trips_unioned",
    "int_trips": "nyc_taxi_silver.int_trips",
    "dim_zones": "nyc_taxi_gold.dim_zones",
    "dim_vendors": "nyc_taxi_gold.dim_vendors",
    "fct_trips": "nyc_taxi_gold.fct_trips",
    "fct_monthly_zone_revenue": "nyc_taxi_gold.fct_monthly_zone_revenue",
}
MARTS = ["dim_zones", "dim_vendors", "fct_trips", "fct_monthly_zone_revenue"]
FLOAT_RTOL = 1e-9


def summary(oracle_export):
    return json.loads((oracle_export / "summary.json").read_text())


def normalise(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    df = df.copy()
    for k in keys:
        df[k] = df[k].astype(str)
    for c in df.columns:
        if c not in keys:
            df[c] = df[c].map(lambda v: None if v is None or (isinstance(v, float) and math.isnan(v)) else Decimal(str(v)))
    return df.set_index(keys).sort_index() if keys else df


def spark_columns(spark, table: str, wanted: list[str]) -> dict[str, str]:
    types = {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}
    missing = [c for c in wanted if c not in types]
    assert not missing, f"{table} is missing oracle columns {missing}"
    return {c: types[c] for c in wanted}


@pytest.mark.parametrize("model", list(LAKEFLOW))
def test_lakeflow_matches_duckdb_oracle(spark, catalog, oracle_export, model):
    """Every row of every model, via bucketed fingerprints (row hash + numeric sums/counts)."""
    oracle_cols = summary(oracle_export)["models"][model]["columns"]
    table = f"{catalog}.{LAKEFLOW[model]}"
    cols = spark_columns(spark, table, list(oracle_cols))
    keys = [alias for alias, _ in bucket_exprs(cols)]
    got = normalise(spark.sql(fingerprint_sql(table, cols, "databricks")).toPandas(), keys)
    want = normalise(pd.read_parquet(oracle_export / "fingerprints" / f"{model}.parquet"), keys)
    assert list(got.columns) == list(want.columns)
    joined = want.join(got, how="outer", lsuffix="_oracle", rsuffix="_lakeflow")
    diffs = [c for c in want.columns
             if not joined[f"{c}_oracle"].fillna(Decimal(-1)).equals(joined[f"{c}_lakeflow"].fillna(Decimal(-1)))]
    if diffs:
        bad = joined[[x for c in diffs for x in (f"{c}_oracle", f"{c}_lakeflow")]]
        bad = bad[(bad.iloc[:, 0::2].values != bad.iloc[:, 1::2].values).any(axis=1)]
        pytest.fail(f"{model}: {len(bad)} bucket(s) differ in {diffs}\n{bad.head(20).to_string()}")


@pytest.mark.parametrize("model", ["dim_zones", "dim_vendors", "fct_monthly_zone_revenue", "payment_type_lookup"])
def test_small_models_row_level(spark, catalog, oracle_export, model):
    """Small models row by row; float columns (avg_*) at 1e-9 relative tolerance."""
    want = pd.read_parquet(oracle_export / "rows" / f"{model}.parquet")
    got = spark.table(f"{catalog}.{LAKEFLOW[model]}").select(*want.columns).toPandas()
    types = summary(oracle_export)["models"][model]["columns"]
    exact = [c for c in want.columns if kind(types[c]) != "float"]
    floats = [c for c in want.columns if kind(types[c]) == "float"]

    def key(df):
        return df.assign(**{c: df[c].map(lambda v: None if pd.isna(v) else str(v)) for c in exact}) \
                 .sort_values(exact, na_position="last").reset_index(drop=True)

    want_n, got_n = key(want), key(got)
    assert len(want_n) == len(got_n), f"{model}: {len(want_n)} oracle rows vs {len(got_n)} Lakeflow rows"
    for c in exact:
        mism = want_n[c].fillna("<null>") != got_n[c].fillna("<null>")
        assert not mism.any(), f"{model}.{c}: {mism.sum()} rows differ\n{pd.concat([want_n[mism], got_n[mism]]).head(10)}"
    for c in floats:
        w, g = want_n[c].astype(float), got_n[c].astype(float)
        close = [(math.isnan(x) and math.isnan(y)) or math.isclose(x, y, rel_tol=FLOAT_RTOL) for x, y in zip(w, g)]
        assert all(close), f"{model}.{c}: {close.count(False)} rows beyond rtol {FLOAT_RTOL}"


@pytest.mark.parametrize("model", MARTS)
def test_lakeflow_matches_dbt_ref(spark, catalog, model):
    """Issue #1: per mart, row count plus sum and count of every numeric column; zero diff."""
    lake = spark.table(f"{catalog}.{LAKEFLOW[model]}")
    ref = spark.table(f"{catalog}.dbt_ref.{model}")
    numeric = [f.name for f in ref.schema.fields if kind(f.dataType.simpleString()) in ("integer", "decimal", "float")]
    assert numeric or model == "dim_zones" or model == "dim_vendors"

    def aggs(df):
        types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        exprs = ["count(*) AS n_rows"]
        for c in numeric:
            if kind(types[c]) == "float":
                exprs += [f"sum(`{c}`) AS `sum__{c}`", f"count(`{c}`) AS `count__{c}`"]
            else:
                exprs += [f"cast(sum(`{c}`) AS decimal(38,6)) AS `sum__{c}`", f"count(`{c}`) AS `count__{c}`"]
        return df.selectExpr(*exprs).collect()[0].asDict()

    got, want = aggs(lake), aggs(ref)
    diffs = {}
    for k, w in want.items():
        g = got[k]
        if isinstance(w, float):
            if not math.isclose(w, g, rel_tol=FLOAT_RTOL):
                diffs[k] = (w, g)
        elif w != g:
            diffs[k] = (w, g)
    assert not diffs, f"{model}: dbt_ref vs Lakeflow differ: {diffs}"


def test_dbt_tests_as_expectations_pass(spark, catalog):
    """The non-row-level dbt tests, as materialised by the pipeline's dq_test_results view."""
    rows = spark.table(f"{catalog}.nyc_taxi_gold.dq_test_results").collect()
    assert len(rows) == 8
    failing = {r.test_name: r.failures for r in rows if r.failures}
    assert not failing, failing
