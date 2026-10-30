"""Bronze: Kestra 08/09 GCS -> BigQuery load, as Auto Loader streaming tables.

One streaming table per taxi colour, fed from the landing volume. Column types
follow the Kestra CREATE TABLE statements; `unique_row_id` and `filename` are
computed the same way (B7). Auto Loader's file tracking makes re-delivering a
month a no-op, which is the idempotency Kestra's MERGE ... WHEN NOT MATCHED gave.
`_file_modified_at` is the load provenance that fct_trips' bounded high-water
mark is built on (B1).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.conf.get("taxi.catalog")  # noqa: F821
LANDING = spark.conf.get("taxi.landing")  # noqa: F821
TAGS = {"devin": "true", "source_module": "02-workflow-orchestration"}

NUM = "decimal(38,9)"  # BigQuery NUMERIC
KESTRA_COLUMNS = {
    "green": [
        ("VendorID", "string"), ("lpep_pickup_datetime", "timestamp"), ("lpep_dropoff_datetime", "timestamp"),
        ("store_and_fwd_flag", "string"), ("RatecodeID", "string"), ("PULocationID", "string"),
        ("DOLocationID", "string"), ("passenger_count", "bigint"), ("trip_distance", NUM), ("fare_amount", NUM),
        ("extra", NUM), ("mta_tax", NUM), ("tip_amount", NUM), ("tolls_amount", NUM), ("ehail_fee", NUM),
        ("improvement_surcharge", NUM), ("total_amount", NUM), ("payment_type", "bigint"), ("trip_type", "string"),
        ("congestion_surcharge", NUM),
    ],
    "yellow": [
        ("VendorID", "string"), ("tpep_pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "timestamp"),
        ("passenger_count", "bigint"), ("trip_distance", NUM), ("RatecodeID", "string"),
        ("store_and_fwd_flag", "string"), ("PULocationID", "string"), ("DOLocationID", "string"),
        ("payment_type", "bigint"), ("fare_amount", NUM), ("extra", NUM), ("mta_tax", NUM), ("tip_amount", NUM),
        ("tolls_amount", NUM), ("improvement_surcharge", NUM), ("total_amount", NUM), ("congestion_surcharge", NUM),
    ],
}
PREFIX = {"green": "lpep", "yellow": "tpep"}


def unique_row_id(taxi: str):
    p = PREFIX[taxi]
    parts = [F.col("VendorID"), F.col(f"{p}_pickup_datetime").cast("string"),
             F.col(f"{p}_dropoff_datetime").cast("string"), F.col("PULocationID"), F.col("DOLocationID")]
    return F.md5(F.concat(*[F.coalesce(c, F.lit("")) for c in parts]))


def define_bronze(taxi: str) -> None:
    cols = KESTRA_COLUMNS[taxi]

    @dp.table(
        name=f"{CATALOG}.nyc_taxi_bronze.{taxi}_tripdata",
        comment=f"Kestra 08_gcp_taxi {taxi}_tripdata: monthly csv.gz via Auto Loader",
        table_properties=TAGS,
    )
    @dp.expect_or_fail("filename_is_monthly_file", rf"filename rlike '^{taxi}_tripdata_[0-9]{{4}}-[0-9]{{2}}\.csv$'")
    @dp.expect("no_rescued_data", "_rescued_data is null")
    def bronze():
        raw = (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(StructType([StructField(c, StringType()) for c, _ in cols]))
            .load(f"{LANDING}/{taxi}/")
        )
        typed = raw.select(
            *[F.col(c).cast(t).alias(c) for c, t in cols],
            F.regexp_replace(F.col("_metadata.file_name"), r"\.gz$", "").alias("filename"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_rescued_data"),
        )
        return typed.select(unique_row_id(taxi).alias("unique_row_id"), "filename",
                            *[c for c, _ in cols], "_file_modified_at", "_ingested_at", "_rescued_data")


for _taxi in KESTRA_COLUMNS:
    define_bronze(_taxi)
