# Migration: 06-batch Spark revenue report → Lakeflow

Second migration in this repo, same pattern as [04-analytics-engineering](04-analytics-engineering.md):
run the legacy job unchanged as the "before", port it, prove zero diff against that before.

## Scope

- **Legacy job:** `06-batch/code/06_spark_sql.py`, a PySpark script that unions green and yellow
  trips and writes a monthly revenue report grouped by pickup zone id, month and service type.
  Its input is parquet produced by the course notebook `06-batch/code/05_taxi_schema.ipynb`
  (CSV → parquet with explicit schemas).
- **Data:** the same two months as module 04, green and yellow 2019-01 and 2019-02.
- **Out of scope:** `06_spark_sql_big_query.py` (the Dataproc/BigQuery variant) and the other
  `06-batch` notebooks, which are teaching examples.

## Oracle: the legacy job, run unchanged

[`tests/parity/legacy06/run_legacy_06.py`](../../tests/parity/legacy06/run_legacy_06.py) runs the
legacy chain the way the course teaches it, on pyspark 3.3.2 (the course version), Java 17, UTC:

1. Stage the raw `csv.gz` files in the layout `download_data.sh` produces.
2. Execute the notebook's setup cells and its green/yellow conversion cells (5 and 10) as written.
   **The only edit:** the notebook hard-codes `year = 2020` and `range(1, 13)`; those become
   `2019` and `range(1, 3)`. The runner prints the substitution.
3. `spark-submit` the unmodified `06_spark_sql.py` with the arguments from
   `14-creating-a-local-spark-cluster.md`.
4. Export the report rows and summary.

Result: **1,402 rows**, 263 zone ids, 13 columns, every measure `DOUBLE`, `revenue_month` a `TIMESTAMP`.

**Caveat, same as module 04: we never ran the legacy BigQuery pipeline.** No GCP account. The
module 04 "before" is dbt on DuckDB; the module 06 "before" is local Spark, not Dataproc.

## Finding: the two legacy revenue reports disagree

Module 04's `fct_monthly_zone_revenue` (dbt) and module 06's report (Spark) both claim to be
"monthly revenue by pickup zone and service". On the same two months of raw data they do not agree.
We did **not** reconcile them: module 06 lands as its own table, `nyc_taxi_gold.spark_monthly_zone_revenue`,
and `fct_monthly_zone_revenue` is untouched. Whoever consumes either report needs to know which one they
are reading.

Measured with [`tests/parity/legacy06/compare_legacy_jobs.py`](../../tests/parity/legacy06/compare_legacy_jobs.py)
(Spark report vs the module 04 DuckDB oracle, in-order scenario):

| revenue_month | service | Spark `total_amount` | dbt `total_amount` | gap | gap % |
|---|---|---:|---:|---:|---:|
| 2019-01 | green | 10,139,394.19 | 10,083,962.83 | 55,431.36 | 0.55 |
| 2019-01 | yellow | 120,243,616.52 | 116,547,208.35 | 3,696,408.17 | 3.17 |
| 2019-02 | green | 9,661,850.22 | 9,610,933.36 | 50,916.86 | 0.53 |
| 2019-02 | yellow | 130,247,484.46 | 126,226,167.80 | 4,021,316.66 | 3.19 |

The gap reconciles to the cent, step by step, over pickups in 2019-01..02:

| step | green rows | green `total_amount` | yellow rows | yellow `total_amount` |
|---|---:|---:|---:|---:|
| raw rows (= what the Spark job sums) | 1,206,455 | 19,801,244.41 | 14,686,384 | 250,491,100.98 |
| − `vendorid is not null` (dbt staging) | 0 | 0.00 | 0 | 0.00 |
| − rows removed by the `int_trips` dedup | −9,083 | −106,098.47 | −383,433 | −7,712,741.15 |
| − late rows dropped by `fct_trips` (B1) | −24 | −249.75 | −290 | −4,983.68 |
| − unmatched pickup zones | 0 | 0.00 | 0 | 0.00 |
| = dbt mart | | 19,694,896.19 | | 242,773,376.15 |

What that means:

- **Almost the whole gap is the dbt dedup.** dbt keeps one row per (vendor, pickup time, pickup
  zone, service type); Spark keeps all of them. 392,516 rows (≈ \$7.8M) are in one report and not
  the other. The data cannot tell us whether those are true duplicates or distinct trips that share
  the key, so we can't say which report is right. That is the finding.
- **The B1 component exists only because of our bounded high-water mark.** The dbt project copied
  exactly would have loaded nothing in February (the 2088 row, see module 04), so legacy-vs-legacy
  the February gap would have been ~100%.
- **Different grain.** Spark groups by location id (263 ids → 1,402 rows); dbt groups by zone name,
  and the lookup gives ids 56/57 ("Corona") and 103/104/105 (the island zones) the same name, so
  dbt rolls those up (1,362 rows).
- **Different columns.** Spark reports `congestion_surcharge`; dbt reports `ehail_fee` and
  `total_monthly_trips`. Spark's `revenue_month` is a timestamp, dbt's a date (B4).
- **Different months.** Both reports carry rows for out-of-range pickup months (2001, 2008, 2088,
  …) that come from bad timestamps in the raw files; dbt loses the late ones to B1, Spark keeps all.

## Target design

| Legacy | Lakeflow |
|---|---|
| notebook 05 CSV → parquet with explicit schema | existing bronze streaming tables `nyc_taxi_bronze.{green,yellow}_tripdata` (S8) |
| `06_spark_sql.py` union + `spark.sql(...)` | SQL materialized view `nyc_taxi_gold.spark_monthly_zone_revenue`, same pipeline `nyc_taxi`, legacy SELECT verbatim |
| `spark-submit` by hand | existing job `nyc_taxi_daily` (no new task: the MV refreshes with the pipeline) |
| — | tags `devin=true`, `source_module=06-batch` (table properties + `apply_tags` override) |

## Behaviours we copied on purpose

- **S1 No dedup, no vendor filter.** The Spark job sums every raw row. Copied: that is the job.
- **S2 Zone id grain.** `revenue_zone` is `PULocationID`; no zone lookup. Copied.
- **S3 `revenue_month` stays a timestamp.** `date_trunc('month', …)` works on Databricks as written;
  B4 does not apply here.
- **S4 Double arithmetic.** The notebook schema reads every amount as `DoubleType`, so the port casts
  bronze's `DECIMAL(38,9)` to `DOUBLE` before summing. Parity compares measures at 1e-9 relative
  tolerance because double sums depend on summation order.
- **S5 Column names, typos included.** `avg_montly_passenger_count`, `avg_montly_trip_distance`
  are kept so downstream readers of the legacy report don't break.
- **S6 Out-of-range months.** Rows for 2001, 2088 and the rest stay in the report, as in legacy.
- **S7 No tests.** The legacy job has none, so the MV has no expectations. Its correctness check is
  the parity test.
- **S8 Input is bronze, not the raw files.** Legacy reads every raw row; bronze applies Kestra's
  `unique_row_id` MERGE. On these two months that removes nothing (bronze 1,206,603 green /
  14,687,167 yellow = the legacy parquet row counts), so the inputs are identical. If a future month
  has MD5 identity collisions, the port will sum fewer rows than legacy would have.

## Parity test

[`tests/parity/test_legacy06.py`](../../tests/parity/test_legacy06.py), Databricks Connect, after the
pipeline has loaded the same two months:

- column names and order equal the legacy report's;
- the set of (zone, month, service) groups is identical;
- every measure in every group matches at `rel_tol=1e-9` (`abs_tol=1e-6` for sums that cancel to ~0).

Before deploying, the port's SELECT was run on local Spark over the raw CSVs typed exactly as bronze
types them: same 1,402 groups, 0 differences in every column.

## Results

Not deployed yet; held until the billing question is settled.
