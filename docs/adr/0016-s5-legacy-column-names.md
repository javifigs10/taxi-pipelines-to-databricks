# ADR-0016: Spark report keeps legacy column names, typos included

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S5 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No.

## Context

The legacy output has `avg_montly_passenger_count` and `avg_montly_trip_distance`.

## Decision

Keep the names.

## Consequences

- **Good:** Readers of the legacy report don't break. The serving view needs no rename (gate G5).
- **Bad:** The typos live on. Renaming later is a consumer-facing contract change.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Fix the spelling:** deferred until after cutover, as its own change.
