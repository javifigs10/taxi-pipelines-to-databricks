-- dbt tests that are not row-level (unique, relationships, unique combination)
-- as one validation MV: one row per test, refresh fails if any test fails.
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.dq_test_results (
  test_name STRING,
  failures BIGINT,
  CONSTRAINT test_passes EXPECT (failures = 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'dbt unique / relationships / unique_combination_of_columns tests'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '04-analytics-engineering')
AS
SELECT 'unique_payment_type_lookup_payment_type' AS test_name, count(*) AS failures
FROM (SELECT payment_type FROM ${taxi.catalog}.nyc_taxi_silver.payment_type_lookup GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'unique_int_trips_trip_id', count(*)
FROM (SELECT trip_id FROM ${taxi.catalog}.nyc_taxi_silver.int_trips GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'unique_dim_zones_location_id', count(*)
FROM (SELECT location_id FROM ${taxi.catalog}.nyc_taxi_gold.dim_zones GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'unique_dim_vendors_vendor_id', count(*)
FROM (SELECT vendor_id FROM ${taxi.catalog}.nyc_taxi_gold.dim_vendors GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'unique_fct_trips_trip_id', count(*)
FROM (SELECT trip_id FROM ${taxi.catalog}.nyc_taxi_gold.fct_trips GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'relationships_fct_trips_pickup_location_id__location_id__dim_zones', count(*)
FROM ${taxi.catalog}.nyc_taxi_gold.fct_trips AS f
LEFT ANTI JOIN ${taxi.catalog}.nyc_taxi_gold.dim_zones AS z ON f.pickup_location_id = z.location_id
WHERE f.pickup_location_id IS NOT NULL
UNION ALL
SELECT 'relationships_fct_trips_dropoff_location_id__location_id__dim_zones', count(*)
FROM ${taxi.catalog}.nyc_taxi_gold.fct_trips AS f
LEFT ANTI JOIN ${taxi.catalog}.nyc_taxi_gold.dim_zones AS z ON f.dropoff_location_id = z.location_id
WHERE f.dropoff_location_id IS NOT NULL
UNION ALL
SELECT 'unique_combination_fct_monthly_zone_revenue', count(*)
FROM (SELECT pickup_zone, revenue_month, service_type FROM ${taxi.catalog}.nyc_taxi_gold.fct_monthly_zone_revenue
      GROUP BY ALL HAVING count(*) > 1);
