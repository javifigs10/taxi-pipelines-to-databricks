"""Kestra 08_gcp_taxi semantics for the DuckDB oracle's raw layer.

Each monthly file is loaded with the same column types as the Kestra BigQuery
table, a `filename` column (`{taxi}_tripdata_{yyyy-MM}.csv`, Kestra's
`vars.file`) and the `unique_row_id` hash; rows are MERGEd on `unique_row_id`
(insert when not matched), so re-loading a month is a no-op.
"""
import pathlib
import urllib.request

import duckdb

RELEASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_{month}.csv.gz"

# Column -> type, in the order of the Kestra CREATE TABLE statements.
COLUMNS = {
    "green": {
        "VendorID": "varchar", "lpep_pickup_datetime": "timestamp", "lpep_dropoff_datetime": "timestamp",
        "store_and_fwd_flag": "varchar", "RatecodeID": "varchar", "PULocationID": "varchar",
        "DOLocationID": "varchar", "passenger_count": "bigint", "trip_distance": "decimal(38,9)",
        "fare_amount": "decimal(38,9)", "extra": "decimal(38,9)", "mta_tax": "decimal(38,9)",
        "tip_amount": "decimal(38,9)", "tolls_amount": "decimal(38,9)", "ehail_fee": "decimal(38,9)",
        "improvement_surcharge": "decimal(38,9)", "total_amount": "decimal(38,9)", "payment_type": "bigint",
        "trip_type": "varchar", "congestion_surcharge": "decimal(38,9)",
    },
    "yellow": {
        "VendorID": "varchar", "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "bigint", "trip_distance": "decimal(38,9)", "RatecodeID": "varchar",
        "store_and_fwd_flag": "varchar", "PULocationID": "varchar", "DOLocationID": "varchar",
        "payment_type": "bigint", "fare_amount": "decimal(38,9)", "extra": "decimal(38,9)",
        "mta_tax": "decimal(38,9)", "tip_amount": "decimal(38,9)", "tolls_amount": "decimal(38,9)",
        "improvement_surcharge": "decimal(38,9)", "total_amount": "decimal(38,9)",
        "congestion_surcharge": "decimal(38,9)",
    },
}
PREFIX = {"green": "lpep", "yellow": "tpep"}


def fetch(data_dir: pathlib.Path, taxi: str, month: str) -> pathlib.Path:
    path = data_dir / f"{taxi}_tripdata_{month}.csv.gz"
    if not path.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(RELEASE_URL.format(taxi=taxi, month=month), path)
    return path


def unique_row_id_sql(taxi: str) -> str:
    p = PREFIX[taxi]
    parts = ["VendorID", f"cast({p}_pickup_datetime as varchar)", f"cast({p}_dropoff_datetime as varchar)",
             "PULocationID", "DOLocationID"]
    return "md5(concat(" + ", ".join(f"coalesce({c}, '')" for c in parts) + "))"


def load_month(con: duckdb.DuckDBPyConnection, data_dir: pathlib.Path, taxi: str, month: str) -> int:
    src = fetch(data_dir, taxi, month)
    cols = COLUMNS[taxi]
    table = f"prod.{taxi}_tripdata"
    con.execute("create schema if not exists prod")
    ddl = ", ".join(f"{c} {t}" for c, t in cols.items())
    con.execute(f"create table if not exists {table} (unique_row_id varchar, filename varchar, {ddl})")
    typed = ", ".join(f"cast(nullif({c}, '') as {t}) as {c}" for c, t in cols.items())
    before = con.execute(f"select count(*) from {table}").fetchone()[0]
    con.execute(f"""
        insert into {table}
        with s as (select {typed} from read_csv('{src}', header = true, all_varchar = true)),
             h as (select {unique_row_id_sql(taxi)} as unique_row_id,
                          '{taxi}_tripdata_{month}.csv' as filename, * from s)
        select h.* from h anti join {table} t using (unique_row_id)
    """)
    return con.execute(f"select count(*) from {table}").fetchone()[0] - before
