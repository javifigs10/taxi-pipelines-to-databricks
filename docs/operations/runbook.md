# Runbook: `nyc_taxi_daily` alert

One page, for whoever gets paged. Workspace `dbc-b50571e1-447f`, catalog `devin_dev`, job `nyc_taxi_daily` (52470242913383), pipeline `nyc_taxi` (58fb3a98-398f-4eae-b809-2e1e1714eb0a). Consumers read gold after 08:00 UTC; the job starts at 06:00 UTC.

## 1. Which alert is it? (2 minutes)

Open the job run from the email and look at which task is red.

| Red task | What it means | Data already published? |
|---|---|---|
| `download_month` | The monthly file couldn't be fetched (GitHub release down, network, volume permission). | No change; gold still shows yesterday. |
| `refresh_pipeline` | The pipeline update failed. Open the update link in the task output. | No. A failed update publishes nothing; gold still shows the last good refresh. |
| `check_refresh` | Refresh succeeded but crossed a threshold, or rows were quarantined. The error message lists which. | **Yes.** This is a warning, not an outage. |
| `apply_tags` | Tagging failed. Cosmetic. | Yes. |
| none, "duration warning" email | The run has passed 60 minutes. | Not yet. Watch it. |

## 2. `check_refresh` alerts

- **"N new rows in quarantine_trips":** some input rows broke a row-level check and were kept aside instead of loaded (ADR-0020). Look at them:
  ```sql
  SELECT failed_rules, file_month, filename, count(*) FROM devin_dev.nyc_taxi_silver.quarantine_trips
  GROUP BY ALL ORDER BY 4 DESC;
  ```
  A handful of rows: note it in the ticket, nothing to do at night. Thousands, or a whole file (`file_month_valid`, or every row failing the same rule): the source format changed. The rest of the month is published without those rows; escalate in business hours.
- **"fct_trips took Ns > 900s" or "dedup took Ns > 1200s":** a scaling threshold from [scaling.md](../architecture/scaling.md) was crossed. Not urgent unless the update is approaching the 08:00 deadline. History is in `devin_dev.nyc_taxi_ops.refresh_metrics`:
  ```sql
  SELECT checked_at, flow, seconds FROM devin_dev.nyc_taxi_ops.refresh_metrics
  WHERE flow IN ('_update_span', 'fct_trips', 'int_trips_candidates', 'int_trips') ORDER BY checked_at DESC LIMIT 40;
  ```
  Two breaches in a row on `fct_trips` → ticket for the AUTO CDC change (scaling.md §3.1).

## 3. `refresh_pipeline` failed

Open the update, then the first red flow and its error.

- **An expectation failed** (`int_trips`, `fct_trips`, `dq_test_results`, the seeds): row-level data problems go to quarantine, so a failing expectation here means a bug or a broken invariant (duplicate `trip_id`, a relationship broken by a seed change). Don't rerun. Escalate.
- **Schema / parse error in bronze** (`yellow_tripdata`, `green_tripdata`): the source file changed format. Move the new file from `/Volumes/devin_dev/nyc_taxi_bronze/landing/<taxi>/` to `/Volumes/devin_dev/nyc_taxi_bronze/landing/_rejected/` (outside the folder Auto Loader reads), rerun, escalate. If bad rows already reached bronze, a selective full refresh of that bronze table (fully qualified name) must be followed by a normal update: the selection does not cascade to silver and gold.
- **Infrastructure** (serverless capacity, timeout, `INTERNAL_ERROR`): **Repair run** on the job once. Rerunning is safe: files already landed are never downloaded twice, and a refresh with nothing new changes nothing (verified, see the migration docs).

## 4. Rerun, and check it recovered

1. Job → the failed run → **Repair run** (reruns only the failed task and its children).
2. When green, check the newest month arrived and the gate views are clean:
   ```sql
   SELECT max(pickup_datetime), count(*) FROM devin_dev.nyc_taxi_gold.fct_trips;
   SELECT * FROM devin_dev.nyc_taxi_gold.dq_test_results WHERE failures > 0;  -- expect no rows
   ```
3. If a consumer saw wrong numbers after 08:00, follow the rollback in [cutover-and-rollback.md](../architecture/cutover-and-rollback.md#5-rollback-in-under-an-hour).

## 5. Escalate when

- The same task fails twice after a repair.
- An expectation (not quarantine) failed.
- Gold won't be refreshed by 08:00 UTC and consumers need today's month.
- Quarantine took a whole file or more than 1% of a month.

Never: delete rows from quarantine, switch an expectation to "drop" to get green, or full-refresh the pipeline at night (it re-ingests and recomputes all history; at scale that runs past 08:00).
