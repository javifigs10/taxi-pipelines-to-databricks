# ADR-0018: Spark report has no expectations

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S7 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** n/a.

## Context

The legacy job has no tests.

## Decision

No expectations on the MV. Its correctness check is the parity test against the legacy output.

## Consequences

- **Good:** No behaviour the legacy job didn't have, such as failing an update on data legacy would have reported.
- **Bad:** After legacy is decommissioned, nothing checks this table automatically. Add expectations then, as a separate decision.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Add not-null expectations now:** deferred. They would be new behaviour.
