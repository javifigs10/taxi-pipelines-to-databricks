# ADR-0006: Explicit `DECIMAL(18,3)` amounts and sum/count averages

- **Status:** **Accepted.**
- **Behaviour:** B6 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** agreed in [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2) review; the avg rewrite was found during parity and shipped in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No against the oracle.

## Context

dbt casts amounts with bare `numeric`. That's `DECIMAL(18,3)` on DuckDB but `DECIMAL(10,0)` on Spark, which would round every fare to whole dollars. Separately, Spark's `avg(DECIMAL(18,3))` returns `DECIMAL(22,7)`, so averages are rounded to 7 places before any cast.

## Decision

Cast amounts to `DECIMAL(18,3)` explicitly. Averages are `DOUBLE`, matching DuckDB's `avg(decimal)`. `avg_monthly_trip_distance` is `cast(sum(trip_distance) as double) / count(trip_distance)`.

## Consequences

- **Good:** Exact parity on every amount and sum.
- **Bad:** The averages are a formula, not `avg()`. Someone tidying the SQL could reintroduce the bug. The parity test is the guard.
- **Bad:** `DECIMAL(18,3)` overflows above 10^15. That isn't plausible for a fare. With ANSI on, overflow fails the update instead of wrapping.
- **At scale:** None.

## Evidence

With plain `avg`, 446 of 823 mart rows were off by more than 1e-9 relative. After the fix: zero diff.

## Alternatives considered

- **Bare `DECIMAL`:** rejected (whole dollars).
- **`DOUBLE` amounts:** rejected. Inexact sums would break exact parity.
