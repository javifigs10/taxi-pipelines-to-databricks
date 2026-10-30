"""Job task: land monthly taxi files in the volume (replaces Kestra's extract + GCS upload).

--month=yyyy-MM lands that month (backfill / replay; always triggers a refresh).
Without --month, lands the month after the latest one already in the volume
(starting at --start-month), as long as it is <= --max-month; otherwise it does
nothing and tells the job to skip the pipeline refresh.
Files already in the volume are never re-downloaded.
"""
import argparse
import os
import shutil
import tempfile
import urllib.request

RELEASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_{month}.csv.gz"


def next_month(month: str) -> str:
    y, m = map(int, month.split("-"))
    return f"{y + m // 12}-{m % 12 + 1:02d}"


def landed_months(landing: str, taxi: str) -> set[str]:
    d = os.path.join(landing, taxi)
    prefix = f"{taxi}_tripdata_"
    return {f[len(prefix):len(prefix) + 7] for f in os.listdir(d) if f.startswith(prefix)} if os.path.isdir(d) else set()


def land(landing: str, taxi: str, month: str) -> bool:
    dest_dir = os.path.join(landing, taxi)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, f"{taxi}_tripdata_{month}.csv.gz")
    if os.path.exists(dest):
        print(f"{dest} already landed")
        return False
    url = RELEASE_URL.format(taxi=taxi, month=month)
    with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as tmp, urllib.request.urlopen(url, timeout=600) as r:
        shutil.copyfileobj(r, tmp)
    shutil.copyfile(tmp.name, dest)  # single write into the volume, so Auto Loader never sees a partial file
    os.unlink(tmp.name)
    print(f"landed {dest} ({os.path.getsize(dest)} bytes)")
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--landing", required=True)
    ap.add_argument("--seeds-dir", required=True)
    ap.add_argument("--month", default="")
    ap.add_argument("--taxis", default="green,yellow")
    ap.add_argument("--start-month", default="2019-01")
    ap.add_argument("--max-month", default="2019-02")
    a = ap.parse_args()
    taxis = [t.strip() for t in a.taxis.split(",") if t.strip()]

    seeds = os.path.join(a.landing, "_seeds")
    os.makedirs(seeds, exist_ok=True)
    for f in sorted(os.listdir(a.seeds_dir)):
        if f.endswith(".csv"):
            shutil.copyfile(os.path.join(a.seeds_dir, f), os.path.join(seeds, f))

    if a.month:
        month, refresh = a.month, True
    else:
        done = set.intersection(*(landed_months(a.landing, t) for t in taxis))
        month = next_month(max(done)) if done else a.start_month
        refresh = month <= a.max_month
        if not refresh:
            print(f"nothing to do: next month {month} is after max month {a.max_month}")
    if refresh:
        for t in taxis:
            land(a.landing, t, month)

    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key="landed", value="true" if refresh else "false")
    dbutils.jobs.taskValues.set(key="month", value=month)


if __name__ == "__main__":
    main()
