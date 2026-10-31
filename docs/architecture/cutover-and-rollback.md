# Cutover and rollback: legacy → Lakeflow

Status: **plan only. Nothing here is built or deployed.**

Assumption from the brief: a BI dashboard and a downstream job both read the gold marts today (`fct_trips`, `fct_monthly_zone_revenue`, and the Spark report that `spark_monthly_zone_revenue` replaces). The legacy side is Kestra 08/09 → BigQuery → dbt, plus the module 06 Spark job.

**Caveat that shapes the whole plan:** we have never run the legacy BigQuery pipeline. Every parity result so far compares Lakeflow with dbt on DuckDB and with Spark on a local machine. The first time the gate below runs, legacy production becomes the oracle for the first time. Expect findings the size of the 2088 row.

## 1. Principles

1. **Move the readers first, then the data.** Consumers stop reading legacy tables directly and read a serving layer we control, while that layer still points at legacy. Cutover and rollback then become the same small operation in opposite directions.
2. **Rollback repoints; it never recomputes.** At 100× a full refresh takes hours (see [scaling.md](scaling.md)). A rollback that depends on rebuilding data can't promise an hour.
3. **Known differences are allowed only with a receipt.** Lakeflow deliberately differs from legacy in places (ADR-0001, ADR-0002, ADR-0004). Each allowed difference needs an ADR and a query that reproduces the delta to the cent. Anything else counts as a failure.
4. **One consumer at a time.** The downstream job goes first because its output can be diffed by machine. The dashboard goes second.

## 2. Running side by side

```
             monthly csv.gz (same release files, checksummed)
                 │                                │
   Kestra 08/09 → BigQuery raw          download_month → landing volume
   dbt (BigQuery) → legacy marts        nyc_taxi pipeline → Lakeflow gold
                 │                                │
                 └──────►  parity job (daily)  ◄──┘
                                  │
                     taxi_serving.<mart> views  ◄── BI dashboard, downstream job
                     (point at legacy until the flip)
```

- **Same inputs.** Both sides ingest the same monthly files. `download_month` records each file's SHA-256 and row count. The parity job compares them with the files Kestra loaded (BigQuery raw tables keep `filename`). If the inputs differ, mart parity means nothing.
- **Lakeflow in its own production catalog.** Add a `prod` bundle target (for example catalog `taxi_prod`), run as a dedicated production service principal, schedule unpaused. `devin_dev` stays the sandbox. Today the bundle only has `dev`.
- **Legacy stays the system of record** for the whole shadow period. Nothing about it changes.
- **Legacy data reachable from Databricks.** The parity job and the serving views need to read the legacy marts. Preferred: Lakehouse Federation to BigQuery (a foreign catalog, read-only). Fallback: a daily export of the legacy marts to parquet in a volume. **I haven't checked federation availability on this workspace** (it's Free Edition).
- **Serving layer.** One view per consumed mart in `taxi_serving`, with exactly the column names and types consumers read today. Where Lakeflow's types differ from legacy, the Lakeflow-side view casts back:
  - `revenue_month` is a date on Lakeflow (ADR-0004).
  - The Spark report's typo'd column names are already kept (ADR-0016).
- **Moving readers onto the serving views** is itself a change for each consumer (a new connection, and Databricks SQL instead of BigQuery SQL for the dashboard). It's done and validated during shadow, while the views still return legacy data. If something breaks there, it's a connection problem, not a data problem.
- **Parity job.** It runs daily after both pipelines finish and writes one row per mart per check to `taxi_ops.parity_results`. It reuses the comparisons in `tests/parity`, pointed at legacy instead of DuckDB.

## 3. The parity gate

All of these must pass, from automated results, before **each** consumer flips:

| # | Gate | Pass criterion |
|---|---|---|
| G1 | Inputs | Every month in scope: same file checksums and the same raw row counts on both sides |
| G2 | Marts | For every consumed mart and every month in scope: same key set, same row count, exact sum and count of every decimal column, doubles within 1e-9 relative. `fct_trips` is also compared per pickup month by hash sum of `trip_id` plus all columns. **Unexplained diff = 0.** Each explained diff cites its ADR and its reproduction query, and the query has to match the delta exactly. |
| G3 | Duration | At least 2 consecutive monthly loads in shadow (the refresh only runs when a month lands), plus at least 14 consecutive daily scheduled runs with no failure. In-order, late-file and rerun scenarios exercised in shadow, the same three the parity suite already covers. |
| G4 | Freshness | On every G3 run, the Lakeflow marts are updated at least 1 hour before the consumers' read time. This is what B11's dropped dbt freshness check turns into (ADR-0011). |
| G5 | Contract | Lakeflow-side serving view schemas are identical to the legacy-side ones: names, types, nullability. |
| G6 | Consumer dry run | The downstream job runs against the Lakeflow-side serving views into a scratch target, and its output diffs clean against its production output. The dashboard renders against both sources, and its owner compares the key tiles. The downstream job must be rerunnable per period (needed for rollback, step 4 below). |
| G7 | Rollback drill | The rollback runbook below is run end to end in shadow, timed, and grants verified afterwards. Target: under 15 minutes to repoint. |
| G8 | Sign-off | The dashboard owner and the downstream-job owner accept the explained diffs in writing. The main ones: **ADR-0001** (Lakeflow keeps loading after a future-dated pickup; legacy stops, so Lakeflow will show months legacy is missing), **ADR-0002** (up to ~21k trips can carry different column values), and the Spark-vs-dbt revenue disagreement in `docs/migration/06-batch.md`, which this migration keeps as two separate tables on purpose. |

G2 is where the never-run BigQuery caveat gets paid down. If legacy production has already loaded a month with a future-dated pickup, its `fct_trips` will be frozen at that month. G2 will then show Lakeflow with more rows than legacy, and ADR-0001's reproduction query has to account for every one of them.

## 4. The flip

For each consumer (downstream job first, dashboard after at least one more monthly load):

1. Confirm the latest G1–G7 results are green and G8 is signed.
2. Snapshot the current legacy-side view definitions. They're the rollback target and live in the repo as `rollback/<mart>.sql`.
3. `ALTER VIEW taxi_serving.<mart> AS SELECT … FROM taxi_prod.nyc_taxi_gold.<mart>` for each mart that consumer reads. `ALTER VIEW … AS` keeps the view object and its grants. The rollback drill (G7) checks that.
4. Have the consumer run once and check it.
5. **Legacy keeps running** for at least two monthly loads after the last consumer flips. The parity job keeps running and is now the alarm. Legacy is decommissioned only after that, by a separate decision.

## 5. Rollback in under an hour

**Triggers:** a parity-job failure on a flipped mart, a failed expectation, or a consumer reporting a wrong number.

**Rule:** repoint first, diagnose after. Nobody debugs root cause while consumers are reading a wrong number.

| Time | Step | Who |
|---|---|---|
| 0–10 min | Run the G2 comparison for the affected mart (one query, already scheduled; run it on demand). If it shows an unexplained diff, or the consumer's number is wrong and legacy's is right, roll back. No debate beyond that. | on-call |
| 10–20 min | Run `rollback/<mart>.sql`: `ALTER VIEW taxi_serving.<mart> AS SELECT … FROM <legacy source>`. One statement per mart, already tested in G7. Only the affected marts move; others stay on Lakeflow. | on-call |
| 20–30 min | Preserve the evidence before the next refresh overwrites it: `CREATE TABLE taxi_ops.incident_<mart>_<ts> AS SELECT * FROM taxi_prod.nyc_taxi_gold.<mart>` (for `fct_trips`, only the affected pickup months). Pause the `nyc_taxi_daily` schedule only if something else reads Lakeflow gold directly. | on-call |
| 30–45 min | Dashboard: force-refresh its cache or extracts. Downstream job: list its runs between the flip and the rollback (job run history) and rerun them against the restored source. This is why G6 requires it to be rerunnable per period. | consumer owners |
| 45–60 min | Verify: run G2 between the serving view and legacy (it should now be zero diff by definition), confirm with both consumers, post the incident note. | on-call |

**After legacy is decommissioned**, there's nothing left to repoint to. The rollback target then becomes the last validated Lakeflow output:
- After each run that passes the parity checks and expectations, a job task writes `taxi_serving_snapshots.<mart>_<run_id>`. That's cheap for the marts (1,362 and 1,402 rows today).
- Rollback means `ALTER VIEW` onto the last good snapshot, with the same timeline as above.
- `fct_trips` is too big to copy every run at 100×. If it's a streaming table by then ([scaling.md §3.1](scaling.md#31-fct_trips-full-recompute), step 2), Delta time travel (`VERSION AS OF`) gives the same rollback without a copy. While it's still an MV, keep the snapshot approach and only snapshot the latest months.

**What would break the one-hour promise, and how the plan avoids it:**
- **Rebuilding data.** Never part of rollback, see principle 2.
- **A consumer bound to a table instead of the view.** G6 and G7 run through the views.
- **Grants lost on the view.** `ALTER VIEW` keeps them, and G7 checks.
- **A downstream job that can't be rerun for a period.** That's a G6 prerequisite, so the flip doesn't happen until it can.
