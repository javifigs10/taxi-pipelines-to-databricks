# ADR-0017: Spark report keeps out-of-range months

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S6 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No.

## Context

The raw files contain pickups far outside the file month (2001, 2088 and others). The Spark job reports them.

## Decision

Keep them.

## Consequences

- **Good:** Same rows as legacy. The odd months stay visible rather than silently filtered.
- **Bad:** Consumers see month rows for 2001 and 2088.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Filter to the file month:** rejected. It changes the report.
