"""Finding: the two legacy revenue reports disagree. Quantify by how much and why.

Compares the module 06 Spark report (run unchanged, run_legacy_06.py) with the
module 04 dbt mart fct_monthly_zone_revenue (DuckDB oracle, in-order scenario)
at the grain both share, (revenue_month, service_type), and decomposes the
total_amount gap with the oracle's own raw and intermediate tables.

  ~/dbc/bin/python tests/parity/legacy06/compare_legacy_jobs.py
"""
from __future__ import annotations

import argparse
import pathlib
import shutil
import tempfile

import duckdb

W = pathlib.Path.home() / "parity-work"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark-report", default=str(W / "legacy06" / "export" / "rows" / "spark_monthly_zone_revenue.parquet"))
    ap.add_argument("--dbt-db", default=str(W / "in_order" / "taxi_rides_ny.duckdb"))
    a = ap.parse_args()
    db = pathlib.Path(tempfile.mkdtemp()) / "oracle.duckdb"
    shutil.copyfile(a.dbt_db, db)  # dbt may hold a lock on the original
    c = duckdb.connect()
    c.execute(f"attach '{db}' as ora (read_only)")
    c.execute(f"create temp view spark as select * from read_parquet('{a.spark_report}')")

    def show(title: str, sql: str) -> None:
        print(f"\n## {title}\n")
        df = c.execute(sql).df()
        print(df.to_markdown(index=False, floatfmt=",.2f"))

    show("Grain and coverage", """
        select 'spark (06)' report, count(*) n_rows, count(distinct revenue_zone) n_zones,
               count(distinct date_trunc('month', revenue_month)) n_months,
               string_agg(distinct service_type, ',' order by service_type) service_types
        from spark
        union all
        select 'dbt (04)', count(*), count(distinct pickup_zone), count(distinct revenue_month),
               string_agg(distinct service_type, ',' order by service_type)
        from ora.oracle.fct_monthly_zone_revenue""")

    show("total_amount by month and service (in-scope months)", """
        with s as (select cast(date_trunc('month', revenue_month) as date) m, lower(service_type) svc,
                          sum(revenue_monthly_total_amount) spark_total,
                          sum(revenue_monthly_congestion_surcharge) spark_congestion
                   from spark group by all),
             d as (select revenue_month m, lower(service_type) svc,
                          sum(revenue_monthly_total_amount)::double dbt_total,
                          sum(revenue_monthly_ehail_fee)::double dbt_ehail,
                          sum(total_monthly_trips) dbt_trips
                   from ora.oracle.fct_monthly_zone_revenue group by all)
        select coalesce(s.m, d.m) revenue_month, coalesce(s.svc, d.svc) service_type,
               spark_total, dbt_total, spark_total - dbt_total gap,
               100 * (spark_total - dbt_total) / dbt_total gap_pct
        from s full join d on s.m = d.m and s.svc = d.svc
        where coalesce(s.m, d.m) between date '2019-01-01' and date '2019-02-01'
        order by 1, 2""")

    show("Where the gap comes from (total_amount, both months, in-scope pickups)", """
        with raw as (
            select 'green' svc, VendorID, lpep_pickup_datetime p, total_amount from ora.prod.green_tripdata
            union all
            select 'yellow', VendorID, tpep_pickup_datetime, total_amount from ora.prod.yellow_tripdata),
        scope as (select * from raw where p >= timestamp '2019-01-01' and p < timestamp '2019-03-01')
        select step, svc service_type, rows, amount from (
            select 1 o, 'raw rows (= what the Spark job sums)' step, svc, count(*) rows, sum(total_amount)::double amount
            from scope group by svc
            union all
            select 2, 'minus VendorID is null (dbt staging filter)', svc,
                   -count(*), -sum(total_amount)::double from scope where VendorID is null group by svc
            union all
            select 3, 'minus duplicates removed by int_trips dedup', lower(t.service_type),
                   -(u.n - t.n), -(u.amt - t.amt)
            from (select service_type, count(*) n, sum(total_amount)::double amt from ora.oracle.int_trips
                  where pickup_datetime >= timestamp '2019-01-01' and pickup_datetime < timestamp '2019-03-01' group by 1) t
            join (select service_type, count(*) n, sum(total_amount)::double amt from ora.oracle.int_trips_unioned
                  where pickup_datetime >= timestamp '2019-01-01' and pickup_datetime < timestamp '2019-03-01' group by 1) u
              using (service_type)
            union all
            select 4, 'minus late rows dropped by fct_trips (B1)', lower(i.service_type), -(i.n - f.n), -(i.amt - f.amt)
            from (select service_type, count(*) n, sum(total_amount)::double amt from ora.oracle.int_trips
                  where pickup_datetime >= timestamp '2019-01-01' and pickup_datetime < timestamp '2019-03-01' group by 1) i
            join (select service_type, count(*) n, sum(total_amount)::double amt from ora.oracle.fct_trips
                  where pickup_datetime >= timestamp '2019-01-01' and pickup_datetime < timestamp '2019-03-01' group by 1) f
              using (service_type)
            union all
            select 5, 'minus pickup zone not in dim_zones / null zone (dbt inner joins)', lower(f.service_type),
                   -(f.n - m.n), -(f.amt - m.amt)
            from (select service_type, count(*) n, sum(total_amount)::double amt from ora.oracle.fct_trips
                  where pickup_datetime >= timestamp '2019-01-01' and pickup_datetime < timestamp '2019-03-01' group by 1) f
            join (select service_type, sum(total_monthly_trips) n, sum(revenue_monthly_total_amount)::double amt
                  from ora.oracle.fct_monthly_zone_revenue
                  where revenue_month between date '2019-01-01' and date '2019-02-01' group by 1) m
              using (service_type)
        ) order by svc, o""")


if __name__ == "__main__":
    main()
