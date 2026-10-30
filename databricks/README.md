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
