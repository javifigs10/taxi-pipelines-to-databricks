# nyc_taxi bundle

Lakeflow port of Kestra `08/09_gcp_taxi` (bronze) and dbt `taxi_rides_ny`
(silver/gold). Mapping and decisions: [docs/migration/04-analytics-engineering.md](../docs/migration/04-analytics-engineering.md).

| Resource | Name |
|---|---|
| Schemas | `devin_dev.nyc_taxi_bronze`, `nyc_taxi_silver`, `nyc_taxi_gold` |
| Volume | `devin_dev.nyc_taxi_bronze.landing` (`<taxi>/<taxi>_tripdata_yyyy-MM.csv.gz`, `_seeds/`) |
| Pipeline | `nyc_taxi`: serverless; `src/pipeline/bronze.py`, `silver.py` (Python), `gold/*.sql` (SQL MVs) |
| Job | `nyc_taxi_daily`: `download_month` → `has_new_data` → `refresh_pipeline` → `apply_tags`, daily 06:00 UTC |

## Setup and auth

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
# DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET = service principal (OAuth M2M)
databricks current-user me && databricks auth describe   # must show the service principal
```

## Deploy and run

```bash
cd databricks
databricks bundle validate
databricks bundle deploy                                  # --var schedule_pause_status=PAUSED while testing
databricks bundle run nyc_taxi_daily                      # next month not yet landed (no-op after max_month)
databricks bundle run nyc_taxi_daily --params month=2019-01   # backfill / replay a month
```

The daily run lands the month after the latest one in the volume, up to
`max_month` (proof scope: 2019-02); when there is nothing new it skips the
refresh. A replayed month that was already landed is not re-downloaded and
Auto Loader does not re-ingest it, so counts stay the same.

## Operations

- **Alerts** go to `var.alert_email` (default `javifigs10@gmail.com`): job `on_failure`, a job duration warning at 60 minutes (`health` rule), and pipeline update/flow failures.
- **`check_refresh`** runs after `refresh_pipeline`. It writes per-flow durations to `nyc_taxi_ops.refresh_metrics` and quarantine counts to `nyc_taxi_ops.quarantine_counts`, and fails the task (which sends the on_failure email) when `fct_trips` > 900 s, `int_trips_candidates` + `int_trips` > 1200 s, the update > 3600 s, or the quarantine grew.
- **Quarantine:** rows that break a row-level check are kept in `nyc_taxi_silver.quarantine_trips` with `failed_rules` (ADR-0020).
- **Runbook:** [docs/operations/runbook.md](../docs/operations/runbook.md).
- **Gotcha:** `full_refresh_selection` needs fully qualified names (`devin_dev.nyc_taxi_bronze.green_tripdata`) and does **not** cascade to downstream MVs. Follow it with a normal update. A failed update is retried automatically (`RETRY_ON_FAILURE`), several times, before the pipeline goes idle.
