# ADR-0001: Bound the `fct_trips` high-water mark to each load's file month

- **Status:** **Accepted.**
- **Behaviour:** B1 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** user decision in [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2) review ("bounded cutoff, take it"); implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** Yes, on purpose. Legacy stops loading after a future-dated pickup; Lakeflow doesn't.

## Context

dbt `fct_trips` is incremental with `where pickup_datetime > (select max(pickup_datetime) from {{ this }})`. `yellow_tripdata_2019-01.csv.gz` has two trips with pickups on **2088-01-24**. After the January load, the high-water mark (HWM) is 2088, so the February run reports success and adds **0 rows**, and so does every month after it. The legacy pipeline was already broken, and nothing failed to say so.

The same filter also drops late rows (a pickup older than the HWM in a later file). That is legacy behaviour the user chose to keep.

## Decision

Keep the mechanism, so late rows are still dropped. But compute each load's HWM only from earlier loads' pickups **that fall inside their own file month**. `load_batches` records one row per file month with its `batch_seq` and that bounded HWM. `fct_trips` is a declarative MV that re-derives, from this load provenance, what the dbt merge would have produced after each load. There is no side `MERGE` task.

## Consequences

- **Good:** February and later months load. The result depends only on the inputs and the landing order, so a full refresh gives the same answer (dbt `--full-refresh` doesn't).
- **Bad:** Late rows are still dropped: 431 rows with January pickups in the February files (44 green, 387 yellow, before dedup). In the late scenario (February landed first), the January load keeps only 24 rows. That is legacy semantics, kept knowingly. Fixing it is a separate business decision, not a migration one.
- **Bad:** Lakeflow `fct_trips` will **not** match legacy production after any month that follows a future-dated pickup. Lakeflow has those months and legacy doesn't. Consumers must be told before cutover ([cutover gate G2/G8](../architecture/cutover-and-rollback.md#3-the-parity-gate)).
- **At scale:** The MV is fully recomputed on every refresh, and its batch join grows with rows × loads. It's the first thing to break at 100× ([scaling.md §3.1](../architecture/scaling.md#31-fct_trips-full-recompute)). The bounded HWM itself is cheap.

## Evidence

Unmodified dbt on DuckDB: `fct_trips` 8,095,819 after January and after February. Bounded: HWM after January is 2019-01-31 23:59:58. `fct_trips` is 15,500,838 in order and 7,405,469 late, identical across the DuckDB oracle, `dbt_ref` and Lakeflow (19/19 parity tests in both orders).

## Alternatives considered

- **Copy exactly:** rejected. The migrated pipeline would stop loading after the first month.
- **Side SQL `MERGE` task in the job:** rejected by the user. `fct_trips` and everything downstream would leave the pipeline graph.
- **Drop the late-row filter:** not taken. It changes business semantics, so it isn't the migration's call.
