# ADR-0002: Deterministic tie-break in the `int_trips` dedup

- **Status:** **Accepted.**
- **Behaviour:** B2 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** user default before [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2); implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** Possibly, for up to 21,306 trips. The legacy winner is arbitrary, so legacy can disagree with itself.

## Context

dbt `int_trips` keeps `row_number() = 1` over `(vendor_id, pickup_datetime, pickup_location_id, service_type)` ordered by `dropoff_datetime` only. When several rows tie on `dropoff_datetime` and differ elsewhere, the engine picks a winner arbitrarily, and the pick can change between runs and between engines.

## Decision

Order by `dropoff_datetime, dropoff_location_id, total_amount, fare_amount, tip_amount, trip_distance, passenger_count, payment_type, rate_code_id, store_and_fwd_flag, trip_type, extra, mta_tax, tolls_amount, improvement_surcharge, ehail_fee`, which is every remaining column. Rows equal on all of them are interchangeable.

## Consequences

- **Good:** The same input always gives the same `int_trips`, so parity can compare row by row.
- **Good:** It makes the dedup safe to partition. Recomputing only the touched pickup months picks the same winner as a global recompute. That's the precondition for the scale fix in [scaling.md §3.2](../architecture/scaling.md#32-dedup-across-all-history-int_trips_candidates--int_trips).
- **Bad:** Column values can differ from a given legacy run for up to 21,306 trips. Row counts don't change.
- **At scale:** A 16-column sort key inside each dedup group. Groups are tiny, so the cost is in the full shuffle (§3.2), not the sort.

## Evidence

392,516 rows removed by dedup either way (Yellow 373,022 groups, Green 8,995). 21,306 groups (Yellow 16,673, Green 4,633; 42,615 rows) have a tied winning `dropoff_datetime` with differing rows.

## Alternatives considered

- **Copy exactly (dropoff only):** rejected. Parity would be flaky by construction, and so would any incremental rewrite.
