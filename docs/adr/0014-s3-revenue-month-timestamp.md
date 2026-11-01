# ADR-0014: Spark report keeps `revenue_month` as a timestamp

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S3 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No.

## Context

`date_trunc('month', …)` in the Spark SQL works on Databricks as written and returns a timestamp. The B4 problem (a missing adapter branch) doesn't exist here.

## Decision

Leave it as a timestamp.

## Consequences

- **Good:** Same type as legacy.
- **Bad:** The two revenue reports use different types for the month (date vs timestamp).

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Align with B4 (date):** rejected. It would change the legacy report's contract.
