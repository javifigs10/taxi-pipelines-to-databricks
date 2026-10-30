import os
import pathlib

import pytest


@pytest.fixture(scope="session")
def spark():
    from databricks.connect import DatabricksSession

    session = DatabricksSession.builder.serverless(True).getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session


@pytest.fixture(scope="session")
def catalog() -> str:
    return os.environ.get("PARITY_CATALOG", "devin_dev")


@pytest.fixture(scope="session")
def oracle_export() -> pathlib.Path:
    scenario = os.environ.get("PARITY_SCENARIO", "in_order")
    root = pathlib.Path(os.environ.get("PARITY_WORK", pathlib.Path.home() / "parity-work"))
    path = root / scenario / "export"
    if not (path / "summary.json").exists():
        pytest.fail(f"no oracle export at {path}; run tests/parity/oracle/run_oracle.py --scenario {scenario} first")
    return path
