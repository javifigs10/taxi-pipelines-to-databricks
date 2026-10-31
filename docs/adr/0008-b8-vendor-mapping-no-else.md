# ADR-0008: Keep the vendor mapping without an `ELSE`

- **Status:** **Proposed.** Implemented and merged in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4), but never explicitly signed off; the migration doc still lists it as Proposed. Accept or reject in review.
- **Behaviour:** B8 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** No.

## Context

`get_vendor_data` is a `CASE` with no `ELSE`, so unknown vendor ids map to a NULL `vendor_name`.

## Decision

Copy exactly: inline `CASE` in `dim_vendors`, no `ELSE`.

## Consequences

- **Good:** Identical to legacy.
- **Bad:** A new vendor id shows up as a NULL name rather than "Unknown". Nothing fails.
- **At scale / cutover:** None.

## Evidence

Only vendor ids 1, 2 and 4 appear in 2019-01/02.

## Alternatives considered

- **Add `ELSE 'Unknown'`:** not taken. It's a change in behaviour, and the user didn't ask for it.
