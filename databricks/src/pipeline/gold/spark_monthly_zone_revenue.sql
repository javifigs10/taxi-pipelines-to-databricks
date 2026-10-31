-- 06-batch/code/06_spark_sql.py, ported as written: the SELECT below is the legacy query verbatim.
-- Kept as its own table on purpose, not folded into fct_monthly_zone_revenue: the two legacy
-- reports disagree (docs/migration/06-batch.md). trips_data reproduces the script's input
-- (notebook 05_taxi_schema types: IntegerType ids/counts, DoubleType amounts) from bronze (S8).
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.spark_monthly_zone_revenue (
  revenue_zone INT,
  revenue_month TIMESTAMP,
  service_type STRING,
  revenue_monthly_fare DOUBLE,
  revenue_monthly_extra DOUBLE,
  revenue_monthly_mta_tax DOUBLE,
  revenue_monthly_tip_amount DOUBLE,
  revenue_monthly_tolls_amount DOUBLE,
  revenue_monthly_improvement_surcharge DOUBLE,
  revenue_monthly_total_amount DOUBLE,
  revenue_monthly_congestion_surcharge DOUBLE,
  avg_montly_passenger_count DOUBLE,
  avg_montly_trip_distance DOUBLE
)
COMMENT '06-batch 06_spark_sql.py monthly revenue report (zone id grain, no dedup)'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '06-batch')
AS WITH trips_data AS (
  SELECT
    cast(PULocationID AS INT) AS PULocationID,
    lpep_pickup_datetime AS pickup_datetime,
    cast(passenger_count AS INT) AS passenger_count,
    cast(trip_distance AS DOUBLE) AS trip_distance,
    cast(fare_amount AS DOUBLE) AS fare_amount,
    cast(extra AS DOUBLE) AS extra,
    cast(mta_tax AS DOUBLE) AS mta_tax,
    cast(tip_amount AS DOUBLE) AS tip_amount,
    cast(tolls_amount AS DOUBLE) AS tolls_amount,
    cast(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    cast(total_amount AS DOUBLE) AS total_amount,
    cast(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    'green' AS service_type
  FROM ${taxi.catalog}.nyc_taxi_bronze.green_tripdata
  UNION ALL
  SELECT
    cast(PULocationID AS INT),
    tpep_pickup_datetime,
    cast(passenger_count AS INT),
    cast(trip_distance AS DOUBLE),
    cast(fare_amount AS DOUBLE),
    cast(extra AS DOUBLE),
    cast(mta_tax AS DOUBLE),
    cast(tip_amount AS DOUBLE),
    cast(tolls_amount AS DOUBLE),
    cast(improvement_surcharge AS DOUBLE),
    cast(total_amount AS DOUBLE),
    cast(congestion_surcharge AS DOUBLE),
    'yellow'
  FROM ${taxi.catalog}.nyc_taxi_bronze.yellow_tripdata
)
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
