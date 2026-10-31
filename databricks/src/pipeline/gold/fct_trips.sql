-- dbt marts/fct_trips (incremental merge on trip_id, contract enforced).
--
-- B1, declaratively: dbt merged, after each load b, every int_trips row whose
-- pickup_datetime was above that load's high-water mark H(b) (the first load
-- is a full build). load_batches carries H(b) = max pickup of earlier loads that
-- fell inside their own file month. A trip is therefore last merged at the
-- latest load b it already existed in with pickup > H(b), and its value is the
-- dedup winner among the rows known at that load (lowest B2 dedup_rank).
-- Same answer on every refresh, full or not.
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.fct_trips (
  trip_id STRING,
  vendor_id INT,
  service_type STRING,
  rate_code_id INT,
  pickup_location_id INT,
  pickup_borough STRING,
  pickup_zone STRING,
  dropoff_location_id INT,
  dropoff_borough STRING,
  dropoff_zone STRING,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  passenger_count INT,
  trip_distance DECIMAL(18,3),
  trip_type INT,
  trip_duration_minutes BIGINT,
  fare_amount DECIMAL(18,3),
  extra DECIMAL(18,3),
  mta_tax DECIMAL(18,3),
  tip_amount DECIMAL(18,3),
  tolls_amount DECIMAL(18,3),
  ehail_fee DECIMAL(18,3),
  improvement_surcharge DECIMAL(18,3),
  total_amount DECIMAL(18,3),
  payment_type INT,
  payment_type_description STRING,
  CONSTRAINT trip_id_not_null EXPECT (trip_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT vendor_id_not_null EXPECT (vendor_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT service_type_not_null EXPECT (service_type IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT service_type_accepted_values EXPECT (service_type IN ('Green', 'Yellow')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT pickup_datetime_not_null EXPECT (pickup_datetime IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT total_amount_not_null EXPECT (total_amount IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'dbt fct_trips; B1 bounded high-water mark from load provenance'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '04-analytics-engineering')
AS WITH
candidates AS (
  SELECT * FROM ${taxi.catalog}.nyc_taxi_silver.int_trips_candidates
),
batches AS (
  SELECT batch_seq, coalesce(hwm, TIMESTAMP '1900-01-01') AS hwm FROM ${taxi.catalog}.nyc_taxi_silver.load_batches
),
trip_keys AS (
  SELECT trip_id, min(batch_seq) AS first_batch, min(pickup_datetime) AS pickup_datetime,
         date_trunc('MONTH', min(pickup_datetime)) AS pickup_month
  FROM candidates
  GROUP BY trip_id
),
-- Load b merges trip k iff b = 1 or k.pickup_datetime > H(b). Loads whose H(b) is
-- before k's pickup month always qualify (month_base); only loads whose H(b) falls
-- inside that month need the timestamp comparison (month_steps). Both joins to
-- trip_keys are equi-joins on the month, so the cost no longer grows with
-- trips x loads (the old single non-equi join ran as a broadcast nested loop).
month_base AS (
  SELECT m.pickup_month, max(b.batch_seq) AS base_batch
  FROM (SELECT DISTINCT pickup_month FROM trip_keys) AS m
  JOIN batches AS b ON b.batch_seq = 1 OR b.hwm < m.pickup_month
  GROUP BY m.pickup_month
),
month_steps AS (
  SELECT batch_seq, hwm, date_trunc('MONTH', hwm) AS hwm_month FROM batches
),
last_merge AS (
  SELECT /*+ BROADCAST(mb, s) */ k.trip_id, greatest(mb.base_batch, coalesce(max(s.batch_seq), 0)) AS last_merge_batch
  FROM trip_keys AS k
  JOIN month_base AS mb ON mb.pickup_month = k.pickup_month
  LEFT JOIN month_steps AS s ON s.hwm_month = k.pickup_month AND k.pickup_datetime > s.hwm
  GROUP BY k.trip_id, k.first_batch, mb.base_batch
  HAVING greatest(mb.base_batch, coalesce(max(s.batch_seq), 0)) >= k.first_batch
),
trips AS (
  SELECT c.*
  FROM candidates AS c
  JOIN last_merge AS m
    ON c.trip_id = m.trip_id AND c.batch_seq <= m.last_merge_batch
  QUALIFY row_number() OVER (PARTITION BY c.trip_id ORDER BY c.dedup_rank) = 1
),
zones AS (
  SELECT * FROM ${taxi.catalog}.nyc_taxi_gold.dim_zones
)
SELECT
  trips.trip_id,
  trips.vendor_id,
  trips.service_type,
  trips.rate_code_id,
  trips.pickup_location_id,
  pz.borough AS pickup_borough,
  pz.zone AS pickup_zone,
  trips.dropoff_location_id,
  dz.borough AS dropoff_borough,
  dz.zone AS dropoff_zone,
  trips.pickup_datetime,
  trips.dropoff_datetime,
  trips.store_and_fwd_flag,
  trips.passenger_count,
  trips.trip_distance,
  trips.trip_type,
  -- B5: minute boundaries crossed, like DuckDB date_diff and dbt-databricks datediff
  timestampdiff(MINUTE, date_trunc('MINUTE', trips.pickup_datetime), date_trunc('MINUTE', trips.dropoff_datetime)) AS trip_duration_minutes,
  trips.fare_amount,
  trips.extra,
  trips.mta_tax,
  trips.tip_amount,
  trips.tolls_amount,
  trips.ehail_fee,
  trips.improvement_surcharge,
  trips.total_amount,
  trips.payment_type,
  trips.payment_type_description
FROM trips
LEFT JOIN zones AS pz ON trips.pickup_location_id = pz.location_id
LEFT JOIN zones AS dz ON trips.dropoff_location_id = dz.location_id;
