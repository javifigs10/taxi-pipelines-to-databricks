# ADR-0020: Quarantine rows that break row-level checks

- **Status:** **Accepted** 2026-10-31. Requested by the user ("a quarantine table for rows that break expectations so we keep them and look at them instead of dropping them or killing the run").
- **Behaviour:** new; changes how B-series tests act on bad rows ([04-analytics-engineering.md](../migration/04-analytics-engineering.md#dbt-tests--expectations))
- **Decided:** architecture build, [PR #6](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/6)
- **Differs from legacy output:** no, on any data legacy could have loaded. dbt would stop the run on a bad row; we publish without it.

## Context

Every row-level dbt test was ported as `FAIL UPDATE`, so one bad row in a monthly file stopped the whole refresh. The staging `vendorid is not null` filter silently dropped rows.

## Decision

A temporary view `trips_checked` evaluates six rules on the unioned staging rows and records the names of the rules each row breaks in `failed_rules`:
`vendor_id_not_null`, `pickup_datetime_not_null`, `total_amount_not_null`, `file_month_valid` (filename gives a `yyyy-MM` month), `pickup_location_known`, `dropoff_location_known` (id is null or in `taxi_zone_lookup`).

- Rows with no failed rule go to `int_trips_unioned` and on to the marts.
- Rows with any failed rule go to the MV `nyc_taxi_silver.quarantine_trips`, with all columns, `filename`, `file_month` and `_file_modified_at`.
- The bronze `filename_is_monthly_file` expectation becomes warn-only; the file's rows are quarantined by `file_month_valid` instead.
- Staging expectations become warn-only (metrics still in the event log).
- Expectations from `int_trips` on, the seeds' and `dq_test_results` stay `FAIL UPDATE`. With bad rows quarantined, they can only fail on a code or seed bug, and then stopping is right.
- The `check_refresh` job task alerts when the quarantine count grows (see the [runbook](../operations/runbook.md)).

## Consequences

- **Good:** a bad row no longer costs the whole day's refresh, and nothing is dropped silently. The dbt staging filter's dropped rows are now visible.
- **Good:** valid-row output is unchanged: parity stayed 19/19 against the DuckDB oracle.
- **Bad:** a month can be published missing rows. The alert and runbook exist for that; anything above 1% of a month is an escalation.
- **Bad:** `quarantine_trips` is a full-recompute MV like the rest of silver, so it can't hold rows from files that have since been removed from the volume.

## Evidence

Synthetic proof run (update `21b9c4db`): a `green_tripdata_2019-03.csv.gz` with 3 good rows and 4 bad ones (one per rule above), plus a badly named file with 1 row. The update completed. `quarantine_trips` held exactly 5 rows, each tagged with the expected rule; `int_trips_unioned`, `int_trips` and `fct_trips` each grew by exactly 3 rows; `load_batches` gained batch 3 with 3 rows. `check_refresh` raised "5 new rows in quarantine_trips". The files were then removed, bronze green full-refreshed (`ed724875`) and a normal update run (`1cdcf013`), since a selective full refresh doesn't cascade to silver and gold. After that: quarantine empty, `fct_trips` back to 15,500,838, parity 19/19 + module 06 2/2.

## Alternatives considered

- **`expect_or_drop`:** rejected. Drops rows; the user asked to keep them.
- **`expect` (warn) and keep the rows:** rejected. Bad rows would flow into the marts.
- **Quarantine in bronze:** rejected. Bronze must stay a faithful copy of what Kestra loaded (B7); the checks need the typed staging columns.
