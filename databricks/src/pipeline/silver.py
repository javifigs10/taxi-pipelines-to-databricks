"""Silver: dbt seeds, staging and intermediate models of taxi_rides_ny.

Behaviour ids (B1..B9) refer to docs/migration/04-analytics-engineering.md.
Staging views stay views; everything after staging is a materialized view
because int_trips deduplicates across all history.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("taxi.catalog")  # noqa: F821
LANDING = spark.conf.get("taxi.landing")  # noqa: F821
BRONZE = f"{CATALOG}.nyc_taxi_bronze"
TAGS = {"devin": "true", "source_module": "04-analytics-engineering"}
AMOUNT = "decimal(18,3)"  # B6: dbt `numeric` on DuckDB; bare DECIMAL on Spark is (10,0)

AMOUNT_COLUMNS = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
                  "improvement_surcharge", "total_amount"]
# B2: deterministic tie-break after dropoff_datetime (every remaining column, nulls last)
DEDUP_ORDER = ["dropoff_datetime", "dropoff_location_id", "total_amount", "fare_amount", "tip_amount",
               "trip_distance", "passenger_count", "payment_type", "rate_code_id", "store_and_fwd_flag",
               "trip_type", "extra", "mta_tax", "tolls_amount", "improvement_surcharge", "ehail_fee"]
DEDUP_KEY = ["vendor_id", "pickup_datetime", "pickup_location_id", "service_type"]
INT_TRIPS_COLUMNS = ["trip_id", "vendor_id", "service_type", "rate_code_id", "pickup_location_id",
                     "dropoff_location_id", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag",
                     "passenger_count", "trip_distance", "trip_type", *AMOUNT_COLUMNS, "payment_type",
                     "payment_type_description"]


def file_month(col: str = "filename"):
    """B1: 'yellow_tripdata_2019-01.csv' -> '2019-01' (same expression as the dbt file_month macro)."""
    return F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col(col), "green_tripdata_", ""),
                                             "yellow_tripdata_", ""), r"\.csv", "")


# --- seeds -------------------------------------------------------------------
@dp.materialized_view(name="taxi_zone_lookup", comment="dbt seed taxi_zone_lookup", table_properties=TAGS)
@dp.expect_or_fail("locationid_not_null", "locationid is not null")
def taxi_zone_lookup():
    return spark.read.csv(f"{LANDING}/_seeds/taxi_zone_lookup.csv", header=True,  # noqa: F821
                          schema="locationid int, borough string, zone string, service_zone string")


@dp.materialized_view(name="payment_type_lookup", comment="dbt seed payment_type_lookup", table_properties=TAGS)
@dp.expect_or_fail("payment_type_not_null", "payment_type is not null")
def payment_type_lookup():
    return spark.read.csv(f"{LANDING}/_seeds/payment_type_lookup.csv", header=True,  # noqa: F821
                          schema="payment_type int, description string")


# --- staging (views) -----------------------------------------------------------
def staging(taxi: str):
    p = "lpep" if taxi == "green" else "tpep"
    src = spark.read.table(f"{BRONZE}.{taxi}_tripdata")  # noqa: F821
    safe = (lambda c: F.expr(f"try_cast({c} as int)")) if taxi == "green" else (lambda c: F.col(c).cast("int"))  # B3
    cols = [
        F.col("vendorid").cast("int").alias("vendor_id"),
        safe("ratecodeid").alias("rate_code_id"),
        F.col("pulocationid").cast("int").alias("pickup_location_id"),
        F.col("dolocationid").cast("int").alias("dropoff_location_id"),
        F.col(f"{p}_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col(f"{p}_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("filename").cast("string").alias("filename"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast(AMOUNT).alias("trip_distance"),
    ]
    if taxi == "green":
        cols.append(safe("trip_type").alias("trip_type"))
    cols += [F.col(c).cast(AMOUNT).alias(c) for c in AMOUNT_COLUMNS if taxi == "green" or c != "ehail_fee"]
    cols += [safe("payment_type").alias("payment_type"), F.col("_file_modified_at")]
    return src.where(F.col("vendorid").isNotNull()).select(*cols)


@dp.temporary_view(name="stg_green_tripdata", comment="dbt stg_green_tripdata")
@dp.expect_all_or_fail({"vendor_id_not_null": "vendor_id is not null",
                        "pickup_datetime_not_null": "pickup_datetime is not null"})
def stg_green_tripdata():
    return staging("green")


@dp.temporary_view(name="stg_yellow_tripdata", comment="dbt stg_yellow_tripdata")
@dp.expect_all_or_fail({"vendor_id_not_null": "vendor_id is not null",
                        "pickup_datetime_not_null": "pickup_datetime is not null"})
def stg_yellow_tripdata():
    return staging("yellow")


# --- intermediate ---------------------------------------------------------------
UNIONED_COLUMNS = ["vendor_id", "rate_code_id", "pickup_location_id", "dropoff_location_id", "pickup_datetime",
                   "dropoff_datetime", "store_and_fwd_flag", "passenger_count", "trip_distance", "trip_type",
                   *AMOUNT_COLUMNS[:5], "ehail_fee", *AMOUNT_COLUMNS[6:], "payment_type", "filename", "service_type"]


@dp.materialized_view(name="int_trips_unioned", comment="dbt int_trips_unioned + load provenance",
                      table_properties=TAGS)
def int_trips_unioned():
    green = spark.read.table("stg_green_tripdata").withColumn("service_type", F.lit("Green"))  # noqa: F821
    yellow = (spark.read.table("stg_yellow_tripdata")  # noqa: F821
              .withColumn("trip_type", F.lit(1).cast("int"))
              .withColumn("ehail_fee", F.lit(0).cast(AMOUNT))
              .withColumn("service_type", F.lit("Yellow")))
    cols = [*UNIONED_COLUMNS, "_file_modified_at"]
    return green.select(*cols).unionByName(yellow.select(*cols)).withColumn("file_month", file_month())


@dp.materialized_view(
    name="load_batches", table_properties=TAGS,
    comment="B1: one row per load (file month) in landing order, with the bounded high-water mark "
            "of all earlier loads (max pickup that fell inside its own file month).")
def load_batches():
    u = spark.read.table("int_trips_unioned")  # noqa: F821
    in_month = F.substring(F.col("pickup_datetime").cast("string"), 1, 7) == F.col("file_month")
    per = u.groupBy("file_month").agg(
        F.min("_file_modified_at").alias("first_landed_at"),
        F.max(F.when(in_month, F.col("pickup_datetime"))).alias("in_month_max_pickup"),
        F.count(F.lit(1)).alias("rows"),
    )
    order = Window.orderBy("first_landed_at", "file_month")
    return (per.withColumn("batch_seq", F.row_number().over(order))
               .withColumn("hwm", F.max("in_month_max_pickup").over(order.rowsBetween(Window.unboundedPreceding, -1))))


@dp.materialized_view(
    name="int_trips_candidates", table_properties=TAGS,
    comment="dbt int_trips before the dedup filter: surrogate key (B9), payment lookup, load batch, "
            "and the B2 dedup rank over all history.")
def int_trips_candidates():
    u = spark.read.table("int_trips_unioned")  # noqa: F821
    pt = spark.read.table("payment_type_lookup")  # noqa: F821
    batches = spark.read.table("load_batches").select("file_month", "batch_seq")  # noqa: F821
    null = F.lit("_dbt_utils_surrogate_key_null_")
    trip_id = F.md5(F.concat_ws("-", *[F.coalesce(F.col(c).cast("string"), null) for c in DEDUP_KEY]))  # B9
    payment = F.coalesce(F.col("payment_type"), F.lit(0))
    rows = (u.withColumn("trip_id", trip_id)
             .withColumn("payment_type", payment)
             .join(pt.withColumnRenamed("description", "payment_type_description"), "payment_type", "left")
             .withColumn("payment_type_description", F.coalesce("payment_type_description", F.lit("Unknown")))
             .join(batches, "file_month", "left"))
    rank = Window.partitionBy(*DEDUP_KEY).orderBy(*[F.col(c).asc_nulls_last() for c in DEDUP_ORDER])
    return rows.select(*INT_TRIPS_COLUMNS, "file_month", "batch_seq",
                       F.row_number().over(rank).alias("dedup_rank"))


@dp.materialized_view(name="int_trips", comment="dbt int_trips (deduplicated, B2 tie-break)", table_properties=TAGS)
@dp.expect_all_or_fail({
    "trip_id_not_null": "trip_id is not null",
    "vendor_id_not_null": "vendor_id is not null",
    "service_type_not_null": "service_type is not null",
    "service_type_accepted_values": "service_type in ('Green', 'Yellow')",
    "pickup_datetime_not_null": "pickup_datetime is not null",
    "total_amount_not_null": "total_amount is not null",
})
def int_trips():
    return spark.read.table("int_trips_candidates").where("dedup_rank = 1").select(*INT_TRIPS_COLUMNS)  # noqa: F821
