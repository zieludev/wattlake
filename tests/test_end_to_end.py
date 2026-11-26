"""The whole pipeline, from zips to the results page, on one real athlete and one made up."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ElementTree
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
from factory import REAL_ATHLETE, Ride, athlete_zip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from wattlake import bronze, gold, report, silver
from wattlake.lake import Lake

REAL_ID = REAL_ATHLETE.stem


@pytest.fixture(scope="module")
def lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Lake:
    lake = Lake(tmp_path_factory.mktemp("lake"))
    lake.raw.mkdir(parents=True)
    (lake.raw / REAL_ATHLETE.name).write_bytes(REAL_ATHLETE.read_bytes())
    athlete_zip(lake.raw, "made-up", a_season())
    for stage in (bronze, silver, gold):
        stage.build(spark, lake)
    return lake


def a_season() -> list[Ride]:
    """Rides every third day, with a hard effort of a different length in each."""
    generator = np.random.default_rng(11)
    rides: list[Ride] = []
    for i in range(30):
        effort = (180, 300, 480, 600, 1200)[i % 5]
        watts = generator.normal(180, 25, 5400).clip(0)
        watts[1800 : 1800 + effort] = 250 + 20_000 / effort
        start = datetime(2018, 1, 1, 8) + timedelta(days=3 * i)
        rides.append(Ride(start, [float(w) for w in watts.round()]))
    return rides


def test_every_file_of_the_real_athlete_finds_its_ride(spark: SparkSession, lake: Lake) -> None:
    rides = lake.read(spark, "silver", "rides").where(F.col("athlete_id") == REAL_ID)

    assert rides.count() == 11
    assert {r["offset_s"] for r in rides.collect()} == {7200}


def test_mmp_of_real_rides_agrees_with_goldencheetahs_own(spark: SparkSession, lake: Lake) -> None:
    with zipfile.ZipFile(REAL_ATHLETE) as archive:
        doc = json.loads(archive.read(f"{{{REAL_ID}}}.json"))
    theirs = {
        ride["date"]: dict(
            zip(ride["METRICS"]["power_mmp_secs"], ride["METRICS"]["power_mmp"], strict=True)
        )
        for ride in doc["RIDES"]
        if "power_mmp" in ride["METRICS"]
    }
    ours = (
        lake.read(spark, "gold", "ride_mmp")
        .join(lake.read(spark, "silver", "rides"), ["athlete_id", "activity"])
        .where(F.col("athlete_id") == REAL_ID)
        .select(
            F.date_format("start_utc", "yyyy/MM/dd HH:mm:ss 'UTC'").alias("date"),
            "duration_s",
            "watts",
        )
        .collect()
    )

    compared = [
        (r["watts"], theirs[r["date"]][r["duration_s"]])
        for r in ours
        if r["duration_s"] in theirs[r["date"]]
    ]
    assert len(compared) > 50
    assert all(abs(watts - gc) <= max(0.01 * gc, 1) for watts, gc in compared)


def test_the_made_up_season_fits_its_own_critical_power(spark: SparkSession, lake: Lake) -> None:
    season = lake.read(spark, "gold", "seasons").where(F.col("athlete_id") == "made-up").first()

    assert season is not None
    assert season["cp_watts"] == pytest.approx(250, abs=1)
    assert season["w_prime_j"] == pytest.approx(20_000, rel=0.02)


def test_the_results_page_has_every_section_and_charts_that_parse(
    spark: SparkSession, lake: Lake, tmp_path: Path
) -> None:
    report.write(spark, lake, tmp_path)

    page = (tmp_path / "results.md").read_text()
    for heading in (
        "The data",
        "Agreement with GoldenCheetah",
        "Power profile",
        "Critical power",
        "Training load",
    ):
        assert f"## {heading}\n" in page
    charts = sorted(p.name for p in (tmp_path / "img").iterdir())
    assert charts == [
        f"{c}-{s}.svg" for c in ("power-profile", "training-load") for s in ("dark", "light")
    ]
    for chart in (tmp_path / "img").iterdir():
        svg = chart.read_text()
        ElementTree.fromstring(svg)
        assert "nan" not in svg.lower()
