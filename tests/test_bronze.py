from __future__ import annotations

import zipfile
from datetime import datetime
from pathlib import Path

from factory import REAL_ATHLETE, Ride, athlete_zip, constant
from pyspark.sql import SparkSession

from wattlake import bronze
from wattlake.lake import Lake


def test_samples_of_a_zip_carry_the_athlete_and_the_file(tmp_path: Path) -> None:
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), [100.0, None, 300.0])
    path = athlete_zip(tmp_path, "a1", [ride])

    table = [row for batch in bronze.samples_of(path) for row in batch.to_pylist()]

    assert [(r["athlete_id"], r["activity"], r["secs"], r["power"]) for r in table] == [
        ("a1", "2018_05_01_09_00_00", 0.0, 100.0),
        ("a1", "2018_05_01_09_00_00", 1.0, None),
        ("a1", "2018_05_01_09_00_00", 2.0, 300.0),
    ]


def test_a_file_missing_columns_has_them_empty_and_a_broken_file_is_skipped(tmp_path: Path) -> None:
    path = tmp_path / "a1.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("{a1}.json", '{"ATHLETE": {}, "RIDES": []}')
        archive.writestr("2018_05_01_09_00_00.csv", "secs,power\n0,150\n1,160\n")
        archive.writestr("2018_05_02_09_00_00.csv", 'secs,power\n0,"150\n')

    rows = [row for batch in bronze.samples_of(path) for row in batch.to_pylist()]

    assert [(r["activity"], r["power"], r["hr"]) for r in rows] == [
        ("2018_05_01_09_00_00", 150.0, None),
        ("2018_05_01_09_00_00", 160.0, None),
    ]


def test_build_writes_every_sample_and_every_ride(spark: SparkSession, tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    athlete_zip(
        lake.raw, "a1", [Ride(datetime(2018, 5, d, 9, 0, 0), constant(200, 90)) for d in (1, 2)]
    )
    (lake.raw / REAL_ATHLETE.name).write_bytes(REAL_ATHLETE.read_bytes())

    bronze.build(spark, lake)

    rides = lake.read(spark, "bronze", "rides")
    assert rides.groupBy("athlete_id").count().orderBy("athlete_id").collect() == [
        ("69215d1d-cd66-4c5a-8ffe-e95f7b29faa6", 14),
        ("a1", 2),
    ]
    samples = lake.read(spark, "bronze", "samples")
    assert samples.where("athlete_id = 'a1'").count() == 180
    assert samples.select("activity").distinct().count() == 2 + 11
