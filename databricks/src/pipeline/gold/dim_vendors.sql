-- dbt marts/dim_vendors; get_vendor_data inlined, no ELSE on purpose (B8)
CREATE OR REFRESH MATERIALIZED VIEW ${taxi.catalog}.nyc_taxi_gold.dim_vendors (
  vendor_id INT,
  vendor_name STRING,
  CONSTRAINT vendor_id_not_null EXPECT (vendor_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'dbt dim_vendors'
TBLPROPERTIES ('devin' = 'true', 'source_module' = '04-analytics-engineering')
AS SELECT DISTINCT
  vendor_id,
  CASE vendor_id
    WHEN 1 THEN 'Creative Mobile Technologies'
    WHEN 2 THEN 'VeriFone Inc.'
    WHEN 4 THEN 'Unknown/Other'
  END AS vendor_name
FROM ${taxi.catalog}.nyc_taxi_gold.fct_trips;
