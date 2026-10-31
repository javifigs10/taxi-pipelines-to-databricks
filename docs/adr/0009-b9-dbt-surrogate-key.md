# ADR-0009: Reproduce `dbt_utils.generate_surrogate_key` byte for byte for `trip_id`

- **Status:** **Accepted** 2026-10-31. The user approved it earlier and approved it again on this date. Implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4).
- **Behaviour:** B9 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No.

## Context

`trip_id` is `md5` of the `-`-joined varchar casts of the dedup key, with NULLs replaced by `_dbt_utils_surrogate_key_null_`. Parity joins Lakeflow and the oracle on `trip_id`.

## Decision

Re-implement it exactly in `int_trips_candidates`. This relies on Spark rendering whole-second timestamps as `yyyy-MM-dd HH:mm:ss`, as DuckDB does (verified).

## Consequences

- **Good:** `trip_id` is identical across DuckDB, dbt-databricks and Lakeflow, so parity can compare row by row.
- **Bad:** It's fragile. Any change in how a key column renders as a string (fractional seconds, a type change) changes every `trip_id`.
- **At scale:** If `fct_trips` moves to an AUTO CDC flow keyed on `trip_id` ([scaling.md §3.1](../architecture/scaling.md#31-fct_trips-full-recompute)), a key change would duplicate history instead of failing. Pin it with a test before that move.

## Evidence

`trip_id` is inside the row hash parity compares, and it matches.

## Alternatives considered

- **A native `xxhash64`/`sha2` key:** rejected for the migration. It breaks row-level parity and every consumer that stores `trip_id`.
