"""The bundle ships copies of the dbt seeds; they must not drift from the dbt project."""
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
SEEDS = ["taxi_zone_lookup.csv", "payment_type_lookup.csv"]


@pytest.mark.parametrize("seed", SEEDS)
def test_bundle_seed_matches_dbt_seed(seed):
    dbt = REPO / "04-analytics-engineering" / "taxi_rides_ny" / "seeds" / seed
    bundle = REPO / "databricks" / "src" / "seeds" / seed
    assert bundle.read_bytes() == dbt.read_bytes()
