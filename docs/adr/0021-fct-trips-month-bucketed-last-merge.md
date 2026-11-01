# ADR-0021: Month-bucketed last-merge lookup in `fct_trips`

- **Status:** **Accepted** 2026-10-31. Requested by the user ("the fct_trips rewrite you recommended. Do it.").
- **Behaviour:** implements B1 ([ADR-0001](0001-b1-bounded-fct-trips-high-water-mark.md)) differently; [scaling.md §3.1](../architecture/scaling.md#31-fct_trips-full-recompute) step 1
- **Decided:** architecture build, [PR #6](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/6)
- **Differs from legacy output:** no.

## Context

`last_merge` joined every distinct trip to every load batch on `b.batch_seq = 1 OR k.pickup_datetime > coalesce(b.hwm, …)`. `EXPLAIN FORMATTED` on the deployed query shows node `(17) PhotonBroadcastNestedLoopJoin Inner BuildRight` with exactly that condition: cost D × B.

## Decision

Each batch's `hwm` is the max in-month pickup of an earlier file, so it sits inside one calendar month. For a trip in month `m`:
- every batch with `hwm < m` (and batch 1) qualifies: `month_base` = the max such `batch_seq` per month, computed on months × batches;
- only batches with `hwm` inside `m` need the timestamp test: `month_steps`, left-joined on `hwm_month = pickup_month AND pickup_datetime > hwm`.

`last_merge_batch = greatest(base_batch, max(step batch_seq))`. Both joins to the trips are equi-joins on the month, with a `BROADCAST(mb, s)` hint so the month key (few distinct values) isn't used as a shuffle key. `fct_trips` stays a declarative MV in the pipeline.

## Consequences

- **Good:** the trips side no longer pays per batch. The nested loop remains only on months × batches.
- **Neutral at today's size:** with B = 2 the refresh time is unchanged (below).
- **Bad:** one more aggregation pass (distinct pickup months). Measured as noise.

## Evidence

**Query plans** (`EXPLAIN FORMATTED`, serverless, kept outside the repo):
- before: `PhotonBroadcastNestedLoopJoin` trips × `load_batches` on the OR/`>` condition;
- after: `PhotonShuffledHashJoin` on `pickup_month` (trips ⋈ `month_base`), `PhotonBroadcastHashJoin LeftOuter` on `pickup_month = hwm_month` (trips ⟕ `month_steps`); the only `PhotonBroadcastNestedLoopJoin` is distinct months × batches (`batch_seq = 1 OR hwm < pickup_month`). The broadcast hint was added after this plan to stop the months join shuffling on a low-cardinality key.

**Same output:** old vs new full query, `EXCEPT ALL` both ways = 0 rows, 15,500,838 rows. Parity 19/19 (module 04) + 2/2 (module 06) after deploy. **Late order** (Feb first, then Jan), emulated ad hoc by swapping `batch_seq` and giving batch 2 the hwm February sets: old vs new `EXCEPT ALL` both ways = 0, both 7,405,469 rows, the late-scenario oracle count. Then run for real through the pipeline: both January files were re-landed so they're newer than February (load order comes from file modification time), followed by one full refresh (update `afdcfc4b`) and a dbt_ref rebuild in late order. `PARITY_SCENARIO=late` parity passes 19/19 (module 04) + 2/2 (module 06) on the rewritten `fct_trips`; `fct_trips` = 7,405,469 on all three sides. `tests/unit/test_fct_trips_last_merge.py` also checks the rewrite against the B1 definition on DuckDB for in-order, late, 30-month backfill and shuffled 24-load cases (CI, no DBUs).

**Refresh time, in the pipeline** (same in-order Jan+Feb state):

| | update | span | `fct_trips` flow |
|---|---|---:|---:|
| before | `8f726698` (Feb load) | 216 s | 28.3 s |
| before | `9d75103b` (no-data rerun) | 159 s | 30.9 s |
| after | `e67092ab` (definition change, full recompute) | 162 s | 27 s |
| after | `21b9c4db` (3-row batch, quarantine proof) | 191 s | 27 s |
| after | `1cdcf013` (back to Jan+Feb after the proof) | 165 s | 27 s |

**Ad hoc, Databricks Connect, noop write of the whole query, two runs each:** old 11.3 / 10.7 s, new 10.9 / 11.4 s.

**Scaling with B** (`last_merge` only, real 15.5M trip keys, synthetic `load_batches`, identical output at every B):

| B | old | new |
|---:|---:|---:|
| 2 | 3.6 s | 4.7 s |
| 24 | 3.9 s | 3.4 s |
| 200 | 10.7 s | 3.3 s |

## Alternatives considered

- **Range join on a precomputed step table** (the original §3.1 wording): equivalent; the month bucket gives Spark an equality key without relying on range-join optimisation.
- **AUTO CDC streaming `fct_trips`:** step 2 in scaling.md, not yet worth it.
- **A side `MERGE` task:** rejected earlier by the user.
