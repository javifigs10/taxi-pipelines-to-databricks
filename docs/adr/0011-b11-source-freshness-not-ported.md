# ADR-0011: Don't port dbt source freshness; replace it at cutover

- **Status:** **Accepted** 2026-10-31. The user approved it earlier and approved it again on this date. Implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4).
- **Behaviour:** B11 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** n/a (a monitoring check, not data).

## Context

dbt sources declare `warn_after 24h` / `error_after 48h` on the pickup columns. On historical data that warns all the time.

## Decision

Not ported into the pipeline. Before production, freshness becomes a job health rule on **load time**, not pickup time.

## Consequences

- **Good:** No permanent false warnings.
- **Bad:** Today nothing alerts if a month fails to arrive.
- **Cutover:** Covered by gate G4: the Lakeflow marts must update at least 1 hour before the consumers read ([cutover-and-rollback.md](../architecture/cutover-and-rollback.md#3-the-parity-gate)).

## Evidence

n/a.

## Alternatives considered

- **Port the thresholds as-is:** rejected. It would warn on every run.
