from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from wattlake.lake import spark_session

sys.path.insert(0, str(Path(__file__).parent))


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = spark_session("local[2]", driver_memory="1g")
    # The test tables are tiny: a few partitions run much faster than the default 200.
    session.conf.set("spark.sql.shuffle.partitions", "4")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
