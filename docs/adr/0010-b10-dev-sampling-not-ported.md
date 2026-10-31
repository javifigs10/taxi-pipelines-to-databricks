# ADR-0010: Don't port the dev-only January sampling filter

- **Status:** **Accepted** 2026-10-31. The user approved it earlier and approved it again on this date. Implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4).
- **Behaviour:** B10 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No (the filter never applies to production).

## Context

Staging models filter to January 2019 when `target.name == 'dev'`.

## Decision

Not ported. The bundle has one `dev` target over two months of data, which doesn't need sampling. The oracle's target is named `oracle` so the filter doesn't apply there either.

## Consequences

- **Good:** No target-dependent logic in the pipeline.
- **Bad:** A future small dev environment has no built-in sampling. Use a smaller landing volume instead.
- **At scale / cutover:** None.

## Evidence

n/a.

## Alternatives considered

- **Port it as a pipeline configuration flag:** not needed today.
