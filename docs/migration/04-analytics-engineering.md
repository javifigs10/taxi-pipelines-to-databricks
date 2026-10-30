# Migration mapping: 04-analytics-engineering → Databricks

Status: **design agreed, build pending.** All numbers below come from running the unmodified dbt project on DuckDB (see [Oracle](#oracle)). Statements marked *verify on deploy* are expectations about Databricks behaviour that have not been executed yet.

## Why parity before porting: the 2088 row

`yellow_tripdata_2019-01.csv.gz` contains two trips (VendorID 2) with pickups on **2088-01-24**:

| trip_id | pickup_datetime | dropoff_datetime | pickup → dropoff zone | distance | total |
|---|---|---|---|---|---|
| `bf3aeedfe155db73e2c6a2d0fb7450cd` | 2088-01-24 00:25:39 | 2088-01-24 07:28:25 | Bloomingdale → Midtown East | 4.05 | 15.30 |
| `5982d5879fb87875e96e000fa682ee5d` | 2088-01-24 00:15:42 | 2088-01-24 00:19:46 | Central Harlem → Morningside Heights | 0.63 | 5.30 |

They pass every dbt test and land in `fct_trips`. `fct_trips` is incremental with `where pickup_datetime > (select max(pickup_datetime) from {{ this }})`, so after the January load the high-water mark is 2088. On the February run dbt reports success (PASS=43) and **adds 0 rows**: `int_trips` grows to 15.5M, `fct_trips` stays at 8.1M, and the February revenue mart holds only January's spillover rows. Every later month would be dropped the same way, with nothing failing.

The legacy pipeline was already broken and nobody knew. A straight port would have copied that faithfully. See B1 for what we do instead.

## Scope

The legacy pipeline being migrated:

| Layer | Legacy component | Notes |
|---|---|---|
| Ingestion (bronze) | Kestra `02-workflow-orchestration/flows/08_gcp_taxi.yaml` (manual) and `09_gcp_taxi_scheduled.yaml` (monthly schedule + backfill) | wget monthly `csv.gz` → GCS → BigQuery external table → per-month table with `unique_row_id` (MD5) + `filename` → `MERGE ... WHEN NOT MATCHED THEN INSERT` into `green_tripdata` / `yellow_tripdata` |
| Transformation (silver/gold) | dbt project `04-analytics-engineering/taxi_rides_ny` | `stg_green_tripdata`, `stg_yellow_tripdata` (views) → `int_trips_unioned`, `int_trips` (tables) → `fct_trips` (incremental merge), `dim_zones`, `dim_vendors`, `fct_monthly_zone_revenue` (tables); seeds `taxi_zone_lookup`, `payment_type_lookup`; 33 data tests |

Data for the proof: **green + yellow, 2019-01 and 2019-02** (`DataTalksClub/nyc-tlc-data` releases, the same files Kestra downloads). 15,893,770 raw rows. This proves the pattern; it isn't a backfill.

## Oracle

The "before" numbers come from dbt on DuckDB (`dbt-core 1.12.6`, `dbt-duckdb 1.11.1`), not from our own Databricks code.

Run sequence (this mimics the monthly cadence of Kestra flow 09 followed by a dbt build):

1. Load 2019-01 green + yellow raw (`read_csv_auto`, as in `setup/local_setup.md`) → `dbt build`
2. Append 2019-02 green + yellow raw → `dbt build` (exercises the `fct_trips` incremental path)

The dbt target is named `oracle`, not `dev`, so the `target.name == 'dev'` sampling filter in staging doesn't apply.

Unmodified-project baseline (all 43 nodes/tests PASS on both runs):

| Object | Rows after run 1 | Rows after run 2 |
|---|---|---|
| raw green / yellow | 630,918 / 7,667,792 | 1,206,603 / 14,687,167 |
| `int_trips` | 8,095,819 | 15,501,254 |
| `fct_trips` | 8,095,819 (Yellow 7,469,815, Green 626,004) | **8,095,819, unchanged** (see B1) |
| `fct_monthly_zone_revenue`, 2019-01 | Green 625,891 trips / 10,083,962.830; Yellow 7,469,279 trips / 116,542,555.410 | same |

The comparison oracle for Databricks parity is the **patched oracle**: the same dbt project with exactly the deltas listed in [Behaviours](#behaviours-we-copied-on-purpose-and-the-ones-we-didnt), run with the same sequence. Each delta's effect on the unmodified baseline is recorded in that section. The oracle harness (load script, profile, patch set, comparison queries) gets committed in the build phase.

## Target design

| Concern | Decision |
|---|---|
| Engine | One Lakeflow Declarative Pipeline (serverless, Unity Catalog), Python API `from pyspark import pipelines as dp` for bronze + silver, SQL files for gold. One pipeline can hold both languages, so gold and silver keep a single dependency graph. |
| Catalog / schemas | `devin_dev.nyc_taxi_bronze`, `devin_dev.nyc_taxi_silver`, `devin_dev.nyc_taxi_gold`, all created by the bundle. Landing files go in the volume `devin_dev.nyc_taxi_bronze.landing`. |
| Orchestration | Lakeflow Job `nyc_taxi_monthly`: task 1 `download_month` (serverless Python, params `taxi`, `month`) writes `csv.gz` to the landing volume → task 2 `refresh_pipeline`. A monthly cron replaces Kestra's `trigger.date`. Backfills are job runs with explicit `month` params, which replaces Kestra's backfill UI. |
| Packaging | Databricks Asset Bundle under `databricks/` (pipeline, job, schemas, volume), single target `dev` → catalog `devin_dev`. |
| Tests | dbt tests become expectations (mapping below). Uniqueness and FK tests become validation views with `expect_or_fail`, because expectations are row-level. |

### Where the dbt project argues against the design

1. **`fct_trips` isn't declarative in dbt** (decided: load provenance, inside the pipeline). Its incremental filter `pickup_datetime > (select max(pickup_datetime) from {{ this }})` makes the output depend on run history, and a materialized view can only be a function of its inputs. Resolution: make load history an input. Bronze records which file each row came from and when that file was first ingested. `fct_trips` is then a SQL MV that, per load batch, keeps rows whose `pickup_datetime` is above the running high-water mark of all *earlier* batches. That reproduces the dbt result for the oracle run sequence, and it gives the same answer under full refresh (dbt `--full-refresh` does not). The cost is that the MV is effectively recomputed on every refresh: fine at this size, revisit before a real backfill. Rejected alternative: a SQL `MERGE` task in the job. It's a literal copy of dbt, but `fct_trips` and everything downstream would leave the pipeline graph.
2. **`int_trips` dedup is global** (`row_number()` over all history), so silver after staging must be MVs, not streaming tables. Only bronze streams (Auto Loader). Agreed.
3. Everything else maps directly. No objection to SQL MVs for marts, expectations, the job, the bundle or serverless.

## Object mapping

| Legacy | Target | Type | Notes |
|---|---|---|---|
| Kestra `extract` (wget) | job task `download_month` | serverless Python | writes `/Volumes/devin_dev/nyc_taxi_bronze/landing/{taxi}/{taxi}_tripdata_{yyyy-MM}.csv.gz` |
| Kestra GCS upload + external table + per-month `_tripdata_yyyy_MM` table | none | n/a | transient staging, replaced by Auto Loader |
| Kestra `MERGE` into `{taxi}_tripdata` | `nyc_taxi_bronze.green_tripdata`, `yellow_tripdata` | streaming table (Python, Auto Loader) | explicit schema = Kestra DDL types; adds `unique_row_id`, `filename`, `_ingested_at`, `_rescued_data` (B7) |
| seeds `taxi_zone_lookup`, `payment_type_lookup` | `nyc_taxi_silver.taxi_zone_lookup`, `payment_type_lookup` | MV (Python) | CSVs ship in the bundle and are read from the synced workspace path (*verify on deploy*) |
| `stg_green_tripdata`, `stg_yellow_tripdata` (views) | same names | temporary view (Python) | casts, `vendorid is not null` filter; carries `filename`/`_ingested_at` through |
| `int_trips_unioned` (table) | `nyc_taxi_silver.int_trips_unioned` | MV (Python) | yellow gets `trip_type = 1`, `ehail_fee = 0` |
| `int_trips` (table) | `nyc_taxi_silver.int_trips` | MV (Python) | surrogate key + payment lookup + dedup (B2, B9) |
| `dim_zones` | `nyc_taxi_gold.dim_zones` | MV (SQL) | rename `locationid` → `location_id` |
| `fct_trips` (incremental merge, contract enforced) | `nyc_taxi_gold.fct_trips` | MV (SQL) with explicit column list/types | zone joins, duration (B5), load-batch HWM (B1) |
| `dim_vendors` | `nyc_taxi_gold.dim_vendors` | MV (SQL) | `get_vendor_data` CASE inlined (B8) |
| `fct_monthly_zone_revenue` | `nyc_taxi_gold.fct_monthly_zone_revenue` | MV (SQL) | B4, B6 |

Macros: `safe_cast` → `try_cast` (B3). `get_trip_duration_minutes` → explicit boundary-count expression (B5). `get_vendor_data` → inline CASE (B8). `dbt_utils.generate_surrogate_key` → hand-written equivalent (B9).

## dbt tests → expectations

| dbt test | Model(s) | Expectation |
|---|---|---|
| `not_null` (vendor_id, pickup_datetime, trip_id, service_type, total_amount, location_id, …) | stg, int, fct, dims, mart | `@dp.expect_or_fail` / SQL `CONSTRAINT … EXPECT (col IS NOT NULL) ON VIOLATION FAIL UPDATE` on the same columns |
| `accepted_values` service_type ∈ {Green, Yellow} | int_trips, fct_trips, mart | `expect_or_fail service_type IN ('Green','Yellow')` |
| `unique` (trip_id, location_id, vendor_id) | int_trips, fct_trips, dim_zones, dim_vendors | validation view `_check_unique_<model>` = keys with `count(*) > 1`, plus `expect_or_fail` on a zero-row-count column |
| `relationships` pickup/dropoff_location_id → dim_zones | fct_trips | validation view of orphan keys + `expect_or_fail` |
| `dbt_utils.unique_combination_of_columns` (pickup_zone, revenue_month, service_type) | mart | validation view, same pattern |
| model contract `enforced: true` | fct_trips | explicit column list with types in the MV definition |

dbt's default severity is `error` and a failure stops downstream nodes, so `FAIL UPDATE` is the closest equivalent. The parity test confirms all of these pass on the proof data, just as all 33 pass in the oracle.

## Behaviours we copied on purpose (and the ones we didn't)

Every item below is applied identically to the patched oracle and to Databricks, so parity measures the migration and not the decisions. The "Diff vs unmodified" column is what changes against the legacy baseline.

| # | Behaviour | Decision | Diff vs unmodified (2019-01/02) | Status |
|---|---|---|---|---|
| B1 | `fct_trips` incremental high-water mark drops late rows | Keep the mechanism (late backfills are dropped). **Bound the watermark to each load's file month**, i.e. the HWM is the max `pickup_datetime` of earlier batches that falls inside that batch's own month. | Unmodified: yellow 2019-01 contains a pickup dated **2088-01-24**, so after run 1 the HWM is 2088 and **run 2 adds 0 rows to `fct_trips`** (int_trips has 15.5M, fct_trips stays at 8.1M). Bounded: the HWM after run 1 is 2019-01-31 23:59:58; run 2 adds every Feb-file row except 44 green + 387 yellow (pre-dedup) with Jan pickups, which are still dropped as late. | Agreed: bounded cutoff. Deliberately differs from legacy, because an exact copy stops loading after the first month (see [the 2088 row](#why-parity-before-porting-the-2088-row)). |
| B2 | `int_trips` dedup orders by `dropoff_datetime` only | Add a deterministic tiebreak: `order by dropoff_datetime, dropoff_location_id, total_amount, fare_amount, tip_amount, trip_distance, passenger_count, payment_type, rate_code_id, store_and_fwd_flag, trip_type, extra, mta_tax, tolls_amount, improvement_surcharge, ehail_fee` (all remaining columns; rows equal on all of them are interchangeable) | 392,516 rows removed by dedup either way (Yellow 373,022 groups, Green 8,995). In **21,306 groups (Yellow 16,673, Green 4,633; 42,615 rows)** the winning `dropoff_datetime` is tied and the tied rows differ, so the unmodified winner is arbitrary and can change between runs or engines. With the tiebreak the winner is fixed. Row counts are unchanged; column values can differ for up to 21,306 trips. | Agreed (your default) |
| B3 | `safe_cast` is BigQuery-only; plain `cast` elsewhere | `try_cast` for the three `safe_cast` columns (green `ratecodeid`, `trip_type`, `payment_type`). That's the faithful translation of BigQuery `SAFE_CAST`, and with ANSI mode on (serverless default, *verify on deploy*) a plain `cast` would error. Other columns keep plain `cast` as in dbt. | 0 rows: every value in these columns casts cleanly in both months | Agreed (your default) |
| B4 | `fct_monthly_zone_revenue.revenue_month` has only bigquery/duckdb branches, so it's an empty expression on any other adapter | Fix: `cast(date_trunc('MONTH', pickup_datetime) as date)`, which matches the BigQuery branch's type. Proposed same fix in the dbt model: `cast({{ dbt.date_trunc('month', 'pickup_datetime') }} as date)` for all adapters. | Values identical; DuckDB type changes timestamp → date | Agreed (your default) |
| B5 | `get_trip_duration_minutes` = `dbt.datediff(..., 'minute')`. DuckDB counts minute **boundaries crossed** (10:00:59 → 10:01:01 = 1) | Copy the oracle's semantics explicitly: `timestampdiff(MINUTE, date_trunc('MINUTE', pickup), date_trunc('MINUTE', dropoff))`. A naive `timestampdiff` returns whole elapsed minutes (*verify on deploy*). | 0 vs oracle. A naive port would differ on **3,956,545 of 8,095,819 `fct_trips` rows (48.9%)**. BigQuery's behaviour for the legacy prod run isn't verified. | Agreed |
| B6 | dbt casts amounts with `numeric`. DuckDB `numeric` = `DECIMAL(18,3)`; Spark `DECIMAL`/`NUMERIC` without precision = `DECIMAL(10,0)` (whole numbers) | Cast to `DECIMAL(18,3)` explicitly. `avg()` outputs cast to `DOUBLE`, matching DuckDB's `avg(decimal)` return type. | 0 vs oracle. A naive port would round every fare to whole dollars. | Agreed |
| B7 | Kestra MERGE dedups on `unique_row_id` (GCP flow hashes VendorID, pickup, dropoff, PU, DO; omits fare/distance, unlike the Postgres flow) | Bronze computes the same hash (GCP column list) and keeps within-file duplicates. That matches `MERGE … WHEN NOT MATCHED INSERT`, which inserts every unmatched source row (BigQuery semantics, not executed). Re-delivering the same file is a no-op through Auto Loader file tracking, which is the idempotency MERGE gave. Rows whose hash already exists from a *different* file are kept, not dropped, plus a `warn` expectation that counts them. Hash values won't equal BigQuery's (timestamp-to-string formats differ). Nothing downstream reads them. | Within-file hash duplicates kept: green 4,682, yellow 15,981 (same as the oracle, which loads raw files directly). Cross-file collisions: **0**. | Proposed |
| B8 | `get_vendor_data` has no `ELSE`, so unknown vendor ids → NULL `vendor_name` | Copy exactly | 0: only vendor ids 1, 2, 4 appear | Proposed |
| B9 | `trip_id` = `dbt_utils.generate_surrogate_key` (md5 of `-`-joined varchar casts, nulls → `_dbt_utils_surrogate_key_null_`) | Re-implement byte-for-byte so `trip_id` matches the oracle and parity can join row-for-row. Depends on Spark rendering whole-second timestamps as `yyyy-MM-dd HH:mm:ss` like DuckDB (*verify on deploy*). | 0 if the format matches; otherwise parity falls back to the natural key | Proposed |
| B10 | Staging `dev` sampling filter (`target.name == 'dev'` → Jan 2019 only) | Not ported. One `dev` target with two months of data needs no sampling. | n/a | Proposed |
| B11 | Source freshness (`warn_after 24h`, `error_after 48h` on pickup columns) | Not ported. It would warn constantly on historical data. Revisit as a job health rule if this goes to production. | n/a | Proposed |

## Parity test (build phase)

- Patched oracle vs Databricks, same run sequence (load 2019-01 → refresh → load 2019-02 → refresh).
- Exact row counts for every table. Row-level diff of `fct_trips` and `int_trips` on `trip_id` (all columns, decimals exact). Exact sums on the mart; `avg` columns compared at 1e-9 relative tolerance.
- Late-backfill scenario: load 2019-02 first, then 2019-01. Both sides must drop the same Jan rows (B1).
- Idempotency: re-run the job for an already-loaded month. Bronze and gold counts must not change (B7).
- All expectations pass, mirroring the 33 dbt tests.

## Prerequisites before build

- A Databricks workspace with Unity Catalog and serverless, plus permission to create schemas/volumes in `devin_dev`. Auth for the bundle (`DATABRICKS_HOST` + token or service principal) isn't on the build machine yet.
