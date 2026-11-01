# ADR-0013: Spark report keeps zone-id grain

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S2 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No.

## Context

The Spark job groups by `PULocationID`, with no zone lookup. dbt groups by zone name, and the lookup gives ids 56/57 and 103/104/105 the same name, so dbt has 1,362 rows where Spark has 1,402.

## Decision

Keep `revenue_zone` = `PULocationID`.

## Consequences

- **Good:** Same rows as legacy.
- **Bad:** The two revenue reports can't be joined row for row without the zone lookup.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Join to `dim_zones`:** rejected. It changes the grain.
