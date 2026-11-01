# ADR-0012: Spark revenue report: no dedup, no vendor filter, kept as its own table

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S1 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No.

## Context

`06_spark_sql.py` sums every raw row. The dbt mart dedups first. Run unchanged on the same two months, the two legacy reports disagree: Spark is about 3.2% higher for yellow and 0.5% higher for green. The gap reconciles to the cent: dbt's dedup removes 392,516 rows, and B1 drops a few hundred late rows. The data can't tell us whether those rows are real duplicates.

## Decision

Port the Spark job as written (no dedup, no vendor filter), as its own MV `nyc_taxi_gold.spark_monthly_zone_revenue`. Don't fold it into `fct_monthly_zone_revenue`. Record the disagreement as a finding (user: "that's a finding, not a bug to paper over").

## Consequences

- **Good:** Each report matches its own legacy job exactly, and nobody passes off a business choice as a migration result.
- **Bad:** Two revenue numbers stay in gold. Consumers have to know which one they read. That's part of cutover sign-off (G8).
- **At scale:** The planner considered a group-aggregate incremental plan for it. Cheap.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Fold into the dbt mart / pick a winner:** rejected by the user.
