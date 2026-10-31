"""The month-bucketed last_merge in fct_trips.sql (ADR-0021) must equal the B1 definition
(ADR-0001) it replaced, for any load order and any high-water marks. Runs on DuckDB: no DBUs."""
import datetime as dt
import pathlib
import random

import duckdb
import pytest

SQL = pathlib.Path(__file__).resolve().parents[2] / "databricks" / "src" / "pipeline" / "gold" / "fct_trips.sql"

# B1 as originally written: load b merges trip k iff b = 1 or k's pickup is above H(b).
REFERENCE = """
WITH trip_keys AS (
  SELECT trip_id, min(batch_seq) AS first_batch, min(pickup_datetime) AS pickup_datetime
  FROM candidates GROUP BY trip_id
)
SELECT k.trip_id, max(b.batch_seq) AS last_merge_batch
FROM trip_keys AS k
JOIN load_batches AS b ON b.batch_seq = 1 OR k.pickup_datetime > coalesce(b.hwm, TIMESTAMP '1900-01-01')
GROUP BY k.trip_id, k.first_batch
HAVING max(b.batch_seq) >= k.first_batch
"""


def rewritten() -> str:
    sql = SQL.read_text().replace("${taxi.catalog}.nyc_taxi_silver.", "")
    ctes = sql[sql.index("batches AS ("):sql.index("trips AS (")].rstrip().rstrip(",")
    return f"WITH {ctes} SELECT trip_id, last_merge_batch FROM last_merge"


def scenario(seed: int, months: list[str]):
    rnd = random.Random(seed)
    candidates, batches = [], []
    for seq, month in enumerate(months, start=1):
        start = dt.datetime.fromisoformat(f"{month}-01")
        for _ in range(300):
            p = start + dt.timedelta(seconds=rnd.randrange(31 * 86400))  # spills into the next month
            if rnd.random() < 0.02:
                p = dt.datetime(2088, 1, 24, rnd.randrange(24))           # the future-dated rows
            candidates.append((f"t{rnd.randrange(2000)}", seq, p))        # shared ids across loads
        hwm = None if seq == 1 else start + dt.timedelta(seconds=rnd.randrange(-40 * 86400, 40 * 86400))
        batches.append((seq, hwm))
    return candidates, batches


CASES = {
    "in_order": ["2019-01", "2019-02"],
    "late": ["2019-02", "2019-01"],
    "backfill_30": [f"{2017 + i // 12}-{i % 12 + 1:02d}" for i in range(30)][::-1],
    "shuffled_24": random.Random(7).sample([f"2019-{m:02d}" for m in range(1, 13)] * 2, 24),
}


@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize("case", CASES)
def test_rewrite_matches_b1_definition(case, seed):
    candidates, batches = scenario(seed, CASES[case])
    con = duckdb.connect()
    con.execute("CREATE TABLE candidates (trip_id VARCHAR, batch_seq INT, pickup_datetime TIMESTAMP)")
    con.execute("CREATE TABLE load_batches (batch_seq INT, hwm TIMESTAMP)")
    con.executemany("INSERT INTO candidates VALUES (?, ?, ?)", candidates)
    con.executemany("INSERT INTO load_batches VALUES (?, ?)", batches)
    want = sorted(con.execute(REFERENCE).fetchall())
    got = sorted(con.execute(rewritten()).fetchall())
    assert want, "scenario produced no merged trips"
    assert got == want
