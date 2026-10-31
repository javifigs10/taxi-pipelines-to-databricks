# Parity harness

Proves the Lakeflow pipeline in `databricks/` reproduces the dbt project
(`04-analytics-engineering/taxi_rides_ny`) row for row. Design and decisions:
[docs/migration/04-analytics-engineering.md](../../docs/migration/04-analytics-engineering.md).

Three sides, same B1–B6 patch (`oracle/patches/behaviours-B1-B6.patch`) on both dbt runs:

| Side | What | Where |
|---|---|---|
| Oracle (independent "before") | dbt on DuckDB over raw files loaded Kestra-style (`oracle/kestra_raw.py`) | local DuckDB, exported to `~/parity-work/<scenario>/export` |
| Reference (issue #1) | dbt-databricks over the **same bronze tables** Lakeflow reads | `devin_dev.dbt_ref` (sources: load-sequence views in `devin_dev.dbt_ref_src`) |
| Target | Lakeflow pipeline + job | `devin_dev.nyc_taxi_{bronze,silver,gold}` |

Both dbt runs replay the load sequence (load a month, `dbt build`, next month,
`dbt build`), because `fct_trips` is incremental and its result depends on it.

## Run

```bash
uv venv -p 3.12 ~/dbc && VIRTUAL_ENV=~/dbc uv pip install -r tests/parity/requirements.txt
export DATABRICKS_HOST=... DATABRICKS_CLIENT_ID=... DATABRICKS_CLIENT_SECRET=...   # service principal

# 1. oracle, one scenario per load order
~/dbc/bin/python tests/parity/oracle/run_oracle.py --scenario in_order --months 2019-01,2019-02
# 2. Databricks: replay the same order through the job (fresh landing volume + full refresh first)
(cd databricks && databricks bundle run nyc_taxi_daily --params month=2019-01 \
               && databricks bundle run nyc_taxi_daily --params month=2019-02)
# 3. dbt-databricks reference over the same bronze
~/dbc/bin/python tests/parity/run_dbt_ref.py --months 2019-01,2019-02
# 4. compare
PARITY_SCENARIO=in_order ~/dbc/bin/pytest tests/parity -v
```

## DBU-free tests (CI)

`.github/workflows/ci.yml` runs these on every PR against main: `ruff`, `tests/unit` (the `fct_trips` last-merge rewrite checked against the B1 definition on DuckDB) and `test_bundle_seeds.py`, plus `databricks bundle validate` over GitHub OIDC. Everything else in this folder needs serverless compute and stays manual.

## What the tests check

- `test_lakeflow_matches_duckdb_oracle`: every published model, every row. Per
  (service_type, pickup day) bucket: row count, the sum of a per-row MD5 over a
  canonical string of all columns, and the exact sum and count of every numeric
  column (`fingerprint.py`, identical SQL on DuckDB and Databricks).
- `test_small_models_row_level`: dims and the revenue mart row by row; `avg_*`
  doubles at 1e-9 relative tolerance.
- `test_lakeflow_matches_dbt_ref`: issue #1's check, per mart: row count plus
  sum and count of every numeric column, zero diff.
- `test_dbt_tests_as_expectations_pass`: the non-row-level dbt tests (unique,
  relationships, unique combination) as materialised by `dq_test_results`.
  Row-level dbt tests are `FAIL UPDATE` expectations, so a successful pipeline
  update means they passed.
- `test_bundle_seed_matches_dbt_seed`: the bundle's seed copies equal the dbt seeds.
