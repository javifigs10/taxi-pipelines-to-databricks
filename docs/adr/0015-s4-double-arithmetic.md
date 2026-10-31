# ADR-0015: Spark report keeps double arithmetic

- **Status:** **Accepted.** Approved in [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5), which is still open by choice.
- **Behaviour:** S4 in [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose)
- **Decided:** module 06 port, [PR #5](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/5)
- **Differs from legacy output:** No beyond 1e-9 relative.

## Context

The notebook schema reads every amount as `DoubleType`. Bronze stores `DECIMAL(38,9)`.

## Decision

Cast to `DOUBLE` before summing. Parity compares at 1e-9 relative, because double sums depend on summation order.

## Consequences

- **Good:** Same numbers as legacy, to the precision legacy had.
- **Bad:** Sums aren't bit-reproducible across runs, so exact equality checks on this table will flake. Use the tolerance.

## Evidence

`spark_monthly_zone_revenue` matches the unchanged legacy `06_spark_sql.py` output: 1,402 rows, every measure within 1e-9 relative (`tests/parity/test_legacy06.py`, 3/3 including the opt-in rerun test).

## Alternatives considered

- **Decimal sums:** rejected. Not what legacy computed.
