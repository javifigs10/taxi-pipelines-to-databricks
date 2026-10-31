# ADR-0007: Reproduce Kestra's `unique_row_id` identity in bronze; Auto Loader for file idempotency

- **Status:** **Proposed.** Implemented and merged in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4), but never explicitly signed off; the migration doc still lists it as Proposed. Accept or reject in review.
- **Behaviour:** B7 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No on the proof data (0 cross-file collisions).

## Context

Kestra's GCP flows add `unique_row_id` (MD5 of VendorID, pickup, dropoff, PU, DO; fare and distance are *not* included, unlike the Postgres flow) and `MERGE … WHEN NOT MATCHED THEN INSERT` into the per-taxi table. Rerunning a month is a no-op. Duplicate rows within one file are all inserted, because MERGE inserts every unmatched source row (BigQuery semantics, not executed by us).

## Decision

Bronze computes the same hash with the same column list and keeps duplicates within a file. Rerunning a month is a no-op through Auto Loader's file tracking. A row whose hash already exists from a **different** file is kept, not dropped. The proposed `warn` expectation to count those wasn't built: a row-level expectation on a streaming table can't see other files.

## Consequences

- **Good:** Rerunning a month (the [idempotency test](../migration/04-analytics-engineering.md#implementation-and-parity-results)) changes nothing.
- **Bad:** Cross-file hash collisions are kept, where Kestra's MERGE would drop the later row. That's 0 today and invisible if it happens.
- **Bad:** Hash values won't equal BigQuery's (timestamp-to-string formats differ). Nothing downstream reads them.
- **Cutover/ops:** Auto Loader tracks files by path. A corrected file re-published under the same name is **not** re-ingested by default. Correcting a month needs an explicit procedure (new file name, or a targeted bronze repair). Write that procedure before go-live.
- **Spark report:** It reads bronze, so it inherits this (ADR-0019).

## Evidence

Duplicates kept within a file: green 4,682, yellow 15,981 (same as the oracle). Cross-file collisions: 0.

## Alternatives considered

- **Drop rows whose hash exists in any earlier file:** not built. It needs a stateful streaming dedup or an MV, and the measured count is 0.
