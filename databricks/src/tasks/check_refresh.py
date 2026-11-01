"""Job task: refresh-time and quarantine checks after each pipeline refresh.

Reads the pipeline event log for the latest completed update, appends per-flow
durations to nyc_taxi_ops.refresh_metrics and quarantine counts to
nyc_taxi_ops.quarantine_counts, then fails (so the job's on_failure email goes
out) if a threshold from docs/architecture/scaling.md is crossed or rows were
added to quarantine. The data is already published when this runs: a failure
here is an alert, not a bad refresh. Runbook: docs/operations/runbook.md.
"""
import argparse
from datetime import UTC, datetime

FLOWS_SQL = """
with p as (
  select origin.flow_name f, details:flow_progress.status st, timestamp t,
         details:flow_progress.metrics.num_output_rows::bigint n
  from {el} where event_type = 'flow_progress' and origin.update_id = '{u}')
select f, (unix_millis(max(t) filter (where st = 'COMPLETED'))
           - unix_millis(min(t) filter (where st = 'RUNNING'))) / 1000.0 secs,
       min(t) filter (where st = 'RUNNING') started, max(t) filter (where st = 'COMPLETED') ended, max(n) n
from p group by f having started is not null
"""


def run(spark, a) -> list[str]:
    el = f"{a.catalog}.nyc_taxi_silver.event_log_{a.pipeline_id.replace('-', '_')}"
    ops = f"{a.catalog}.nyc_taxi_ops"
    latest = spark.sql(f"select origin.update_id u from {el} where event_type = 'update_progress' "
                       f"and details:update_progress.state = 'COMPLETED' order by timestamp desc limit 1").first()
    if latest is None:
        return [f"no completed update in {el}"]
    u, now = latest.u, datetime.now(UTC)
    flows = {r.f.split(".")[-1]: r for r in spark.sql(FLOWS_SQL.format(el=el, u=u)).collect()}
    span = (max(r.ended for r in flows.values() if r.ended) - min(r.started for r in flows.values())).total_seconds()

    spark.sql(f"create table if not exists {ops}.refresh_metrics "
              "(checked_at timestamp, update_id string, flow string, seconds double, output_rows bigint)")
    rows = [(now, u, f, float(r.secs or 0), r.n) for f, r in flows.items()] + [(now, u, "_update_span", span, None)]
    spark.createDataFrame(rows, "checked_at timestamp, update_id string, flow string, seconds double, output_rows bigint") \
        .write.mode("append").saveAsTable(f"{ops}.refresh_metrics")

    secs = {f: float(r.secs or 0) for f, r in flows.items()}
    breaches = []
    if span > a.max_update_seconds:
        breaches.append(f"update {u} took {span:.0f}s > {a.max_update_seconds}s")
    if secs.get("fct_trips", 0) > a.max_fct_trips_seconds:
        breaches.append(f"fct_trips took {secs['fct_trips']:.0f}s > {a.max_fct_trips_seconds}s (scaling.md 3.1 step 2)")
    dedup = secs.get("int_trips_candidates", 0) + secs.get("int_trips", 0)
    if dedup > a.max_dedup_seconds:
        breaches.append(f"dedup (int_trips_candidates + int_trips) took {dedup:.0f}s > {a.max_dedup_seconds}s (scaling.md 3.2)")

    spark.sql(f"create table if not exists {ops}.quarantine_counts "
              "(checked_at timestamp, update_id string, rule string, quarantined_rows bigint)")
    prev = spark.sql(f"select coalesce(sum(quarantined_rows), 0) n from {ops}.quarantine_counts where rule = '_total' "
                     f"and checked_at = (select max(checked_at) from {ops}.quarantine_counts)").first().n
    by_rule = {r.rule: r.n for r in spark.sql(
        f"select rule, count(*) n from {a.catalog}.nyc_taxi_silver.quarantine_trips "
        f"lateral view explode(failed_rules) as rule group by rule").collect()}
    total = spark.sql(f"select count(*) n from {a.catalog}.nyc_taxi_silver.quarantine_trips").first().n
    spark.createDataFrame([(now, u, k, v) for k, v in {**by_rule, "_total": total}.items()],
                          "checked_at timestamp, update_id string, rule string, quarantined_rows bigint") \
        .write.mode("append").saveAsTable(f"{ops}.quarantine_counts")
    if total > prev:
        breaches.append(f"{total - prev} new rows in nyc_taxi_silver.quarantine_trips (total {total}; by rule {by_rule})")

    print(f"update {u}: span {span:.0f}s, flows " + ", ".join(f"{f}={s:.0f}s" for f, s in sorted(secs.items())))
    print(f"quarantine: {total} rows (previous check {prev}); by rule {by_rule}")
    return breaches


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--pipeline-id", required=True)
    ap.add_argument("--max-update-seconds", type=float, default=3600)
    ap.add_argument("--max-fct-trips-seconds", type=float, default=900)
    ap.add_argument("--max-dedup-seconds", type=float, default=1200)
    a = ap.parse_args()
    from databricks.sdk.runtime import spark

    breaches = run(spark, a)
    if breaches:
        raise RuntimeError("ALERT: " + " | ".join(breaches))


if __name__ == "__main__":
    main()
