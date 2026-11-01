# ADR-0019: Spark report reads bronze, not the raw files

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S8 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No on 2019-01/02. It could differ if a future month has hash collisions across files.

## Context

Legacy reads every raw row from parquet. The port reads the bronze streaming tables, which apply the Kestra `unique_row_id` identity (ADR-0007).

## Decision

Read bronze.

## Consequences

- **Good:** One ingestion path for both modules. No second copy of the raw files.
- **Bad:** If a future month has MD5 identity collisions across files, the port sums fewer rows than legacy would have. The rerun parity check won't see it, because both sides read bronze. Gate G1 (input parity) is what catches it at cutover.

## Evidence

Bronze has 1,206,603 green / 14,687,167 yellow rows, equal to the legacy parquet row counts, so the inputs are identical. `spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **A separate raw-file read for module 06:** rejected. It duplicates ingestion for no difference on the proof data.
