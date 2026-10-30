-- dbt marts/reporting/fct_monthly_zone_revenue
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.fct_monthly_zone_revenue (
  pickup_zone STRING,
  revenue_month DATE,
  service_type STRING,
  revenue_monthly_fare DECIMAL(28,3),
  revenue_monthly_extra DECIMAL(28,3),
  revenue_monthly_mta_tax DECIMAL(28,3),
  revenue_monthly_tip_amount DECIMAL(28,3),
  revenue_monthly_tolls_amount DECIMAL(28,3),
  revenue_monthly_ehail_fee DECIMAL(28,3),
  revenue_monthly_improvement_surcharge DECIMAL(28,3),
  revenue_monthly_total_amount DECIMAL(28,3),
  total_monthly_trips BIGINT,
  avg_monthly_passenger_count DOUBLE,
  avg_monthly_trip_distance DOUBLE,
  CONSTRAINT pickup_zone_not_null EXPECT (pickup_zone IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT revenue_month_not_null EXPECT (revenue_month IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT service_type_not_null EXPECT (service_type IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT service_type_accepted_values EXPECT (service_type IN ('Green', 'Yellow')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT revenue_monthly_total_amount_not_null EXPECT (revenue_monthly_total_amount IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT total_monthly_trips_not_null EXPECT (total_monthly_trips IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'dbt fct_monthly_zone_revenue'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '04-analytics-engineering')
AS SELECT
  coalesce(pickup_zone, 'Unknown Zone') AS pickup_zone,
  cast(date_trunc('MONTH', pickup_datetime) AS DATE) AS revenue_month,  -- B4
  service_type,
  sum(fare_amount) AS revenue_monthly_fare,
  sum(extra) AS revenue_monthly_extra,
  sum(mta_tax) AS revenue_monthly_mta_tax,
  sum(tip_amount) AS revenue_monthly_tip_amount,
  sum(tolls_amount) AS revenue_monthly_tolls_amount,
  sum(ehail_fee) AS revenue_monthly_ehail_fee,
  sum(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
  sum(total_amount) AS revenue_monthly_total_amount,
  count(trip_id) AS total_monthly_trips,
  cast(avg(passenger_count) AS DOUBLE) AS avg_monthly_passenger_count,  -- B6
  -- Spark's avg(decimal(18,3)) rounds to 7 decimals before the cast; exact sum / count is what DuckDB returns
  cast(sum(trip_distance) AS DOUBLE) / count(trip_distance) AS avg_monthly_trip_distance
FROM ${taxi.catalog}.nyc_taxi_gold.fct_trips
-- dbt groups by the raw pickup_zone (NULL and 'Unknown Zone' stay separate groups)
GROUP BY pickup_zone, cast(date_trunc('MONTH', pickup_datetime) AS DATE), service_type;
