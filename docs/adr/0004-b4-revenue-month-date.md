# ADR-0004: Fix `revenue_month` for Databricks

- **Status:** **Accepted.**
- **Behaviour:** B4 in [04-analytics-engineering.md](../migration/04-analytics-engineering.md#behaviours-we-copied-on-purpose-and-the-ones-we-didnt)
- **Decided:** user default before [PR #2](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/2) ("just broken on databricks"); implemented in [PR #4](https://github.com/javifigs10/taxi-pipelines-to-databricks/pull/4)
- **Differs from legacy output:** Values no. Type: date, which matches the BigQuery branch; DuckDB had a timestamp.

## Context

`fct_monthly_zone_revenue.revenue_month` only has `bigquery` and `duckdb` branches. On any other adapter it renders to an empty expression, so the model doesn't compile.

## Decision

`cast(date_trunc('MONTH', pickup_datetime) as date)`. The same fix is proposed upstream for the dbt model: `cast({{ dbt.date_trunc('month', 'pickup_datetime') }} as date)` on every adapter.

## Consequences

- **Good:** It compiles, and the values are identical.
- **Bad:** The type is `date`. Anything that compared it with the DuckDB oracle's timestamp needs a cast. The parity harness patches the oracle to match.
- **Cutover:** If a consumer reads a timestamp today, the Lakeflow-side serving view casts it back (gate G5). This doesn't apply to the Spark report, which keeps its timestamp (ADR-0014).

## Evidence

Parity on `fct_monthly_zone_revenue`: 1,362 rows, zero diff.

## Alternatives considered

- **Timestamp to match DuckDB:** rejected. BigQuery, the real legacy platform, produced a date.
