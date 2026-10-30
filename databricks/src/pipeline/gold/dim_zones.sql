-- dbt marts/dim_zones
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.dim_zones (
  location_id INT,
  borough STRING,
  zone STRING,
  service_zone STRING,
  CONSTRAINT location_id_not_null EXPECT (location_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'dbt dim_zones'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '04-analytics-engineering')
AS SELECT
  locationid AS location_id,
  borough,
  zone,
  service_zone
FROM ${taxi.catalog}.nyc_taxi_silver.taxi_zone_lookup;
