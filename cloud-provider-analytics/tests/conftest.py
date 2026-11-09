import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", 2)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path):
    base = str(tmp_path)
    paths = {z: f"{base}/{z}" for z in ["landing", "bronze", "silver", "gold", "quarantine", "checkpoints", "meta"]}
    paths["base"] = base
    return {"paths": paths,
            "streaming": {"max_files_per_trigger": 1, "watermark": "60 days", "trigger": "available_now",
                          "processing_time": "5 minutes"},
            "quality": {"cost_tolerance_usd": -0.01, "anomaly_threshold": 3.5, "nps_min": -100, "nps_max": 100}}


class FakeLog:
    """Reemplaza a RunLog en los tests: solo guarda lo que se registró."""
    def __init__(self):
        self.rows = []

    def add(self, *args, **kwargs):
        self.rows.append((args, kwargs))
        return "OK"


@pytest.fixture
def fake_log():
    return FakeLog()
