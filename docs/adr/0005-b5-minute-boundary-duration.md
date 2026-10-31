# ADR-0005: Keep DuckDB's minute-boundary trip duration

- **Status:** **Accepted.**
- **Behaviour:** B5 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** agreed in [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2) review; implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No against the DuckDB oracle. BigQuery's behaviour isn't verified.

## Context

`get_trip_duration_minutes` is `dbt.datediff(pickup, dropoff, 'minute')`. On DuckDB that counts minute **boundaries crossed**: 10:00:59 → 10:01:01 is 1. Databricks `timestampdiff(MINUTE, …)` counts whole elapsed minutes and returns 0 there (verified on serverless).

## Decision

`timestampdiff(MINUTE, date_trunc('MINUTE', pickup_datetime), date_trunc('MINUTE', dropoff_datetime))`.

## Consequences

- **Good:** Matches the oracle exactly.
- **Bad:** It's a semantic many readers won't expect ("duration" that isn't elapsed time). The expression carries a comment.
- **Unknown:** BigQuery production may behave differently. Gate G2 will show it if so.
- **At scale:** None.

## Evidence

A naive `timestampdiff` would differ on 3,956,545 of 8,095,819 `fct_trips` rows (48.9%).

## Alternatives considered

- **Plain `timestampdiff`:** rejected. A silent change to half the rows.
