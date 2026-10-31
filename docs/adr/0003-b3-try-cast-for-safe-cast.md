# ADR-0003: Translate BigQuery `SAFE_CAST` to `try_cast`

- **Status:** **Accepted.**
- **Behaviour:** B3 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** user default before [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2); implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No (0 rows affected on the proof data).

## Context

The dbt `safe_cast` macro emits `SAFE_CAST` on BigQuery and plain `cast` elsewhere. It is used on green `ratecodeid`, `trip_type` and `payment_type`. Databricks serverless runs with ANSI mode on (verified: `cast('x' as int)` raises `CAST_INVALID_INPUT`), so a plain `cast` would fail the update on a bad value, where BigQuery returned NULL.

## Decision

Use `try_cast` for those three columns, the faithful equivalent of `SAFE_CAST`. Every other column keeps plain `cast`, as in dbt.

## Consequences

- **Good:** Same NULL-on-bad-value behaviour as legacy production.
- **Bad:** Bad source values become NULL silently. `payment_type` NULL is then coalesced to 0 ("Unknown"). No expectation counts them. A non-blocking expectation on the casted columns would make it visible. Not built.
- **At scale / cutover:** None.

## Evidence

0 rows change on 2019-01/02: every value casts cleanly.

## Alternatives considered

- **Plain `cast`:** rejected. It errors under ANSI mode, which legacy didn't do.
- **Turn ANSI off:** rejected. It's pipeline-wide and hides other errors.
