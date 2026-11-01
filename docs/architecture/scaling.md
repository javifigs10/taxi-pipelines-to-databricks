# Scaling the nyc_taxi pipeline to 10× and 100×

Status: **analysis only. Nothing in this document is built or deployed.**

This covers the three places the user asked about: the `fct_trips` full recompute, the `int_trips` dedup across all history, and the silver materialized views. For each one it says what breaks, what to do about it, and the point at which the switch is worth it. Section 5 lists what I'd do now rather than later.

## 1. What we measured today

Source: the `nyc_taxi` pipeline event log (`devin_dev.nyc_taxi_silver.event_log_58fb3a98_…`), all 8 updates run so far. Serverless, Photon, channel CURRENT. "Span" is first flow start to last flow completion and excludes update initialization.

Data: 2 months, green + yellow, 15,893,770 raw rows. Bronze Delta size: green 42.2 MB, yellow 464.1 MB (`DESCRIBE DETAIL`). The MVs can't be measured that way (`DESCRIBE DETAIL` refuses views).

| Update | What it was | Rows in history | Span (s) | `int_trips_unioned` | `int_trips_candidates` | `int_trips` | `fct_trips` |
|---|---|---:|---:|---:|---:|---:|---:|
| `bd13bf83` | in-order: Jan, full refresh | 8.3M | 158 | 11.3 s | 22.1 s | 17.6 s | 21.4 s |
| `8f726698` | in-order: Feb lands (normal path) | 15.9M | 216 | 16.1 s | 22.2 s | 17.7 s | 28.3 s |
| `080e20f3` | late: Feb first, full refresh | 7.6M | 150 | 11.0 s | 19.5 s | 14.6 s | 20.6 s |
| `adcb3a91` | late: Jan lands | 15.9M | 219 | 15.7 s | 20.9 s | 15.7 s | 25.6 s |
| `60e7a721` | rerun, no new data | 15.9M | 136 | 2.9 s | 34.9 s | 15.7 s | 26.8 s |
| `9d75103b` | rerun, no new data (module 06 idempotency test) | 15.9M | 159 | 6.0 s | 34.7 s | 22.7 s | 30.9 s |

Bronze Auto Loader: about 71–77 s for a 7.0–7.7M-row yellow `csv.gz` file and 18–20 s for green. Trivial flows (the 3-row `dim_vendors`) take about 5 s, which is the per-flow floor.

What the planner chose, from the `planning_information` events:

| Flow | Chosen on updates with new data | Reason logged |
|---|---|---|
| `int_trips_unioned` | complete recompute | `DATA_HAS_CHANGED`, `EXPECTATIONS_NOT_SUPPORTED` (row-based and partition-overwrite were considered) |
| `load_batches` | complete recompute | `PLAN_NOT_INCREMENTALIZABLE` (one row per load, so it doesn't matter) |
| `int_trips_candidates` | complete recompute | `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL`. On no-data reruns it picked the incremental window-function plan, which took 35 s against 22 s for a complete recompute. |
| `int_trips` | complete recompute | `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL`, every time |
| `fct_trips` | complete recompute | `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL`, every time |
| `fct_monthly_zone_revenue`, `dq_test_results` | complete recompute | `DATA_HAS_CHANGED` |
| `spark_monthly_zone_revenue` | no-op on rerun | group-aggregate incremental plan was considered |

What this tells us and what it doesn't:

- **At two months, the pipeline is overhead-bound.** Doubling history (8.3M → 15.9M) moved `int_trips_candidates` and `int_trips` by under a second and `fct_trips` by about 7 s. Serverless scaled out and absorbed it. So these numbers **can't be extrapolated linearly**. Everything below about 10× and 100× is reasoning from the query shapes, not a measurement.
- **Every refresh that sees new data recomputes the whole silver/gold chain from all of history.** The planner never chose an incremental plan for `int_trips` or `fct_trips`.
- **Days with no new month cost almost nothing.** The job's `has_new_data` condition skips the pipeline refresh. Today the expensive refresh happens once a month.
- Planning evidence is limited to what the event log records. I didn't run `EXPLAIN` on any flow (that would be a build step).

## 2. What 10× and 100× mean here

| | Rows in history | Bronze Delta | Load batches if growth is more months | Load batches if growth is daily files |
|---|---:|---:|---:|---:|
| today | 15.9M | 0.5 GB | 2 | n/a |
| 10× | ~160M | ~5 GB | ~20 | ~600 |
| 100× | ~1.6B | ~50 GB | ~200 | ~6,000 |

Growth can come from more history (more monthly batches) or from bigger or more frequent batches (adding FHV data, or moving to daily files). It matters which, because one of the three problems grows with the number of batches, not just with rows.

Write `D` for rows in history and `B` for the number of load batches. Per refresh that sees new data:

| Stage | Work per refresh today | Why |
|---|---|---|
| bronze | new files only | streaming, Auto Loader |
| `int_trips_unioned` | ∝ D | complete recompute (measured) |
| `load_batches` | ∝ D | full scan of `int_trips_unioned` to get one row per file month |
| `int_trips_candidates` | ∝ D log D | window over all history (B2 dedup rank), complete recompute |
| `int_trips` | ∝ D | filter of candidates, complete recompute |
| `fct_trips` | ∝ D × B | `trip_keys` joined to every batch on a non-equi condition, then a window |
| marts, `dq_test_results` | ∝ D | aggregates and `GROUP BY trip_id` uniqueness checks over all of `int_trips` and `fct_trips` |

So the monthly refresh cost grows with total history, and the cumulative cost of running for N months grows roughly with N². `fct_trips` also grows with B on top of that.

## 3. The three hot spots

### 3.1 `fct_trips` full recompute

**How it works today.** `fct_trips` is a declarative MV that re-derives the dbt incremental result from load provenance (ADR-0001). The `last_merge` CTE joins every distinct trip to every load batch on `b.batch_seq = 1 OR k.pickup_datetime > coalesce(b.hwm, …)`. There is no equality key in that condition. `EXPLAIN FORMATTED` confirms Spark runs it as a `PhotonBroadcastNestedLoopJoin Inner BuildRight` with join condition `(batch_seq = 1) OR (pickup_datetime > coalesce(hwm, 1900-01-01))`: every distinct trip is checked against every broadcast batch. **Step 1 below is now built** (ADR-0021).

**What breaks.**
- **At 10×** it's slower but works: about 160M × 20 ≈ 3.2B predicate evaluations, plus a full group-by and window over 160M rows, once a month. I'd expect minutes, not hours. That's an estimate, not a measurement.
- **At 100×** it's the first thing to break. With ~200 monthly batches it's 1.6B × 200 ≈ 320B evaluations every refresh. With daily files it's ~6,000 batches, and the join dominates everything else in the pipeline. Every MV definition change (the planner logs `QUERY_FINGERPRINT_CHANGED`) also forces a full recompute, which turns a schema tweak into a long planned event.

**What I'd do, in two steps.**
1. **Remove the D × B term and keep the MV.** For a trip with pickup `p`, the last merge is the highest `batch_seq` among batches with `hwm < p`, plus batch 1. That's a step function of `p` with at most B steps. Precompute it from `load_batches` (B rows: `hwm` ascending, running max of `batch_seq`) and look up each trip with a range join. The work drops from D × B to about D log B. The semantics don't change, so the existing parity harness proves it: zero diff against the DuckDB oracle in both load orders is the acceptance test.
   **As built (ADR-0021)** it's bucketed by month instead of a range join, because a batch's `hwm` is always the last in-month pickup of an earlier file: batches whose `hwm` is before the trip's pickup month always qualify (`month_base`, months × B rows), and only batches whose `hwm` falls inside that month need the timestamp test (`month_steps`). Both joins to the 15.5M trips are equi-joins on the month, broadcast. The nested loop is left only on the months × batches side (a few hundred rows at 100×).
   **Measured.** Plan before/after and timings are in ADR-0021. At B = 2 the refresh time didn't move (`fct_trips` flow 28–31 s before, 27 s after), as expected: the D × B term is small when B is 2. On a synthetic 200-batch `load_batches` over the real 15.5M trips, the `last_merge` step took 10.7 s old vs 3.3 s new, with identical output.
2. **Make `fct_trips` incremental by batch.** Under B1, a load only inserts or overwrites trips whose pickup is above the previous high-water mark. History rows only change when a later batch carries the same `trip_id` above its HWM, which in practice means future-dated rows like the 2088 trips. That's exactly the shape of a keyed upsert: a streaming table fed by an AUTO CDC flow (`dp.create_auto_cdc_flow`, keys `trip_id`, `sequence_by` `(batch_seq, -dedup_rank)` so the latest batch wins and, within it, the B2 winner), with the HWM filter applied in the source flow. It stays declarative and inside the pipeline. The side `MERGE` task you rejected stays rejected. This needs an append-only per-batch feed out of silver, so it goes together with 3.3.

**When it's worth it.**
- **Step 1:** before B passes ~24 batches, before any move to daily files, and before any historical backfill. Do it as soon as either of those is on the table. It's the only term in the pipeline that grows with the number of loads.
- **Step 2:** when the `fct_trips` flow exceeds 15 minutes on two consecutive refreshes, or history passes ~500M rows, whichever comes first. Below that, the full recompute is cheaper to own than an AUTO CDC flow and its sequencing rules.

### 3.2 Dedup across all history (`int_trips_candidates` → `int_trips`)

**How it works today.** `row_number()` over `(vendor_id, pickup_datetime, pickup_location_id, service_type)`, ordered by `dropoff_datetime` plus the 15 B2 tie-break columns, over every row ever loaded, on every refresh that sees new data.

**What breaks.**
- **At 10×** it's a full shuffle and sort of ~160M wide rows once a month. That's heavy but fine on serverless.
- **At 100×** it's a full shuffle of ~1.6B rows (tens of GB) every refresh, even though nearly every winner is unchanged. This is the main cost of the monthly refresh, and it becomes unacceptable if refreshes go daily.

**The property that makes this fixable.** `pickup_datetime` is part of the dedup key. So a duplicate group lives entirely at one pickup timestamp, and a new batch can only change winners in groups at the pickup timestamps it brings. Those are almost all in the batch's own month, plus a few strays: 431 rows with January pickups in the February files, 24 rows outside the month in January's files (the 2088 rows among them). Partition the dedup by pickup month and only the months a new batch touches need recomputing. B2's deterministic tie-break is what makes that safe: a partial recompute picks the same winner a global one would (ADR-0002).

**What I'd do.** Cluster silver by pickup month and recompute only the touched months. There are two declarative ways to do it inside the pipeline, and I'd prototype both against the parity harness:
- An AUTO CDC flow into `int_trips`, keyed on the dedup key, with a `sequence_by` struct that encodes the B2 ordering. AUTO CDC keeps the row with the highest sequence and B2 keeps the lowest, so the struct has to invert the B2 order column by column.
- Keep the MV, cluster it by pickup month, and check whether the planner picks partition overwrite. The planner already considers partition overwrite for `int_trips_unioned`, but it has rejected every incremental plan for `int_trips` so far. I wouldn't count on it without seeing it chosen.

**When it's worth it:** when `int_trips_candidates` + `int_trips` together exceed 20 minutes on a refresh, or history passes ~1B rows, or refreshes move to daily. At 10× with monthly refreshes, leave it alone.

### 3.3 Silver materialized views

**What breaks.** Two things, both small today:
- **`int_trips_unioned` is a full recompute of a pure projection.** It's append-only, but the planner logged `EXPECTATIONS_NOT_SUPPORTED` next to the incremental plans it considered and recomputed it in full. The expectations it means sit on the two staging temporary views that feed it. It's 11–16 s now, and it's ∝ D.
- **`load_batches` scans all of `int_trips_unioned`** to produce one row per file month. One row per file could come from bronze metadata (`filename`, `_ingested_at`) instead.
- **Three full copies of history are materialized:** `int_trips_candidates`, `int_trips` and `fct_trips`. At 100× that's ~3× bronze in storage, plus the predictive optimization maintenance on all three. It's cheap in money, but it isn't free.

**What I'd do.**
- Make `int_trips_unioned` a streaming table: two append flows from the bronze streaming tables, with the staging expectations moved onto it. Streaming tables support expectations.
- Derive `load_batches` from it with one row per file.
- That turns the front of silver into an append-only feed, which is the prerequisite for both 3.1 step 2 and 3.2.
- `int_trips_candidates` then either goes away (if dedup becomes AUTO CDC) or becomes month-partitioned.

**When it's worth it:** together with the first of 3.1 step 2 or 3.2. On its own it saves seconds at 10× and a few minutes at 100×, which doesn't justify a pipeline full refresh by itself.

### Other things that grow, briefly

- **`dq_test_results`** runs `GROUP BY trip_id … HAVING count(*) > 1` over all of `int_trips` and `fct_trips` every refresh. Once `fct_trips` is keyed by AUTO CDC, uniqueness holds by construction and that check can cover only the touched months.
- **Bronze** is incremental and doesn't grow with history. But a `csv.gz` file can't be split, so one file is read by one task: 71–77 s for a yellow month. More files parallelise fine. Much bigger single files (for example yearly dumps) would not.
- **Full refresh of bronze** re-reads every file in the landing volume. At 100× that's the slowest possible recovery path. The cutover plan never depends on it ([cutover-and-rollback.md](cutover-and-rollback.md)).

## 4. Switch points

These are tied to the time window we actually have, not just to row counts. The job starts at 06:00 UTC. Assume consumers read at 08:00 UTC, a 2-hour window. The rule: no single flow should use more than about a quarter of it, and the whole update no more than half.

| Change | Switch when (first one hit) | Signal to watch |
|---|---|---|
| 3.1 step 1: step-function lookup in `fct_trips` | B > ~24 batches, a move to daily files, or any backfill planned | `select count(*) from nyc_taxi_silver.load_batches` |
| 3.1 step 2: AUTO CDC `fct_trips` | `fct_trips` flow > 15 min on two refreshes in a row, or D > ~500M | per-flow duration query below |
| 3.2: month-partitioned dedup | candidates + `int_trips` > 20 min, D > ~1B, or daily refreshes | per-flow duration query below |
| 3.3: streaming `int_trips_unioned` | together with 3.1 step 2 or 3.2 | n/a |
| Any of the above, regardless of time | the monthly refresh cost passes what the business will pay | `system.billing.usage` by pipeline ID (needs the billing grant the Devin principal doesn't have yet) |

The duration signal is the same query used for section 1:

```sql
with p as (
  select origin.update_id u, origin.flow_name f, details:flow_progress.status st, timestamp t
  from devin_dev.nyc_taxi_silver.event_log_58fb3a98_398f_4eae_b809_2e1e1714eb0a
  where event_type = 'flow_progress')
select u, f,
       (unix_millis(max(t) filter (where st = 'COMPLETED'))
        - unix_millis(min(t) filter (where st = 'RUNNING'))) / 1000 as secs
from p group by u, f order by secs desc
```

The row-count thresholds (500M, 1B) are my estimates of where those times are hit. The time thresholds are what decide.

## 5. What I'd do now rather than later

Nothing needs to change for performance at today's size. A refresh takes about 2.5–3.5 minutes, once a month. Two things are worth doing before more data goes in, because they get more expensive later. Both have since been built.

1. **Monitoring, now.** *Built:* the `check_refresh` job task (see [runbook](../operations/runbook.md)). Originally: schedule the duration query above with an alert on the section 4 thresholds, plus a weekly `count(*)` on `load_batches`. Without it, the switch points above are guesses that nobody checks. Cost: one small query a day on the warehouse.
2. *Built (ADR-0021).* **3.1 step 1 (the `fct_trips` step-function lookup), before `max_month` is raised past a handful of months or any backfill starts.** It's the only cost that grows with the number of loads, so a backfill is when it bites. It's also cheapest to prove now: the DuckDB oracle, both load-order scenarios and the parity suite exist and run in minutes on two months. Proving the same rewrite later means rerunning the oracle at the larger size.

One more thing I'd do before trusting any of the 10× numbers: **a single 10× load test** (for example green + yellow for all of 2019 and 2020) through the existing job and parity harness. That replaces this section's estimates with measurements. It costs DBUs and a pipeline run, so it's a build step, not part of this pass.
