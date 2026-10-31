# Architecture decision records

One ADR per behaviour we copied (or deliberately didn't copy) from the legacy pipelines. B1–B11 come from [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt) (Kestra 08/09 + dbt). S1–S8 come from [06-batch.md](../migration/06-batch.md#behaviours-we-copied-on-purpose) (the Spark revenue report). The tables in those docs stay as the summaries; these files are the records.

Status meanings:
- **Accepted:** the user decided it, or approved the PR that carries it.
- **Proposed:** implemented and merged, but never explicitly signed off. These are the five to accept or reject in review.

Format: Context, Decision, Consequences (good, bad, at scale, for cutover), Evidence, Alternatives considered. A superseded ADR stays in place with status `Superseded by ADR-NNNN`.

| ADR | Behaviour | Decision | Status |
|---|---|---|---|
| [ADR-0001](0001-b1-bounded-fct-trips-high-water-mark.md) | B1 | Bound the `fct_trips` high-water mark to each load's file month | Accepted |
| [ADR-0002](0002-b2-deterministic-dedup-tiebreak.md) | B2 | Deterministic tie-break in the `int_trips` dedup | Accepted |
| [ADR-0003](0003-b3-try-cast-for-safe-cast.md) | B3 | Translate BigQuery `SAFE_CAST` to `try_cast` | Accepted |
| [ADR-0004](0004-b4-revenue-month-date.md) | B4 | Fix `revenue_month` for Databricks | Accepted |
| [ADR-0005](0005-b5-minute-boundary-duration.md) | B5 | Keep DuckDB's minute-boundary trip duration | Accepted |
| [ADR-0006](0006-b6-explicit-decimal-precision.md) | B6 | Explicit `DECIMAL(18,3)` amounts and sum/count averages | Accepted |
| [ADR-0007](0007-b7-kestra-hash-identity-bronze.md) | B7 | Reproduce Kestra's `unique_row_id` identity in bronze; Auto Loader for file idempotency | Proposed |
| [ADR-0008](0008-b8-vendor-mapping-no-else.md) | B8 | Keep the vendor mapping without an `ELSE` | Proposed |
| [ADR-0009](0009-b9-dbt-surrogate-key.md) | B9 | Reproduce `dbt_utils.generate_surrogate_key` byte for byte for `trip_id` | Proposed |
| [ADR-0010](0010-b10-dev-sampling-not-ported.md) | B10 | Don't port the dev-only January sampling filter | Proposed |
| [ADR-0011](0011-b11-source-freshness-not-ported.md) | B11 | Don't port dbt source freshness; replace it at cutover | Proposed |
| [ADR-0012](0012-s1-spark-report-no-dedup-own-table.md) | S1 | Spark revenue report: no dedup, no vendor filter, kept as its own table | Accepted |
| [ADR-0013](0013-s2-zone-id-grain.md) | S2 | Spark report keeps zone-id grain | Accepted |
| [ADR-0014](0014-s3-revenue-month-timestamp.md) | S3 | Spark report keeps `revenue_month` as a timestamp | Accepted |
| [ADR-0015](0015-s4-double-arithmetic.md) | S4 | Spark report keeps double arithmetic | Accepted |
| [ADR-0016](0016-s5-legacy-column-names.md) | S5 | Spark report keeps legacy column names, typos included | Accepted |
| [ADR-0017](0017-s6-out-of-range-months.md) | S6 | Spark report keeps out-of-range months | Accepted |
| [ADR-0018](0018-s7-no-expectations.md) | S7 | Spark report has no expectations | Accepted |
| [ADR-0019](0019-s8-input-is-bronze.md) | S8 | Spark report reads bronze, not the raw files | Accepted |

Related: [scaling.md](../architecture/scaling.md), [cutover-and-rollback.md](../architecture/cutover-and-rollback.md).
