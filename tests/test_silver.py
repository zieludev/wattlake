from __future__ import annotations

import zipfile
from datetime import datetime
from pathlib import Path

import pytest
from factory import Ride, athlete_zip, blocks, constant
from pyspark.sql import SparkSession

from wattlake import bronze, silver
from wattlake.lake import Lake


def build(spark: SparkSession, tmp_path: Path, rides: list[Ride], athlete: str = "a1") -> Lake:
    lake = Lake(tmp_path)
    athlete_zip(lake.raw, athlete, rides)
    bronze.build(spark, lake)
    silver.build(spark, lake)
    return lake


def matches(spark: SparkSession, lake: Lake) -> dict[str, int]:
    return {r["activity"]: r["offset_s"] for r in lake.read(spark, "silver", "rides").collect()}


def test_files_named_in_local_time_find_their_rides_in_utc(
    spark: SparkSession, tmp_path: Path
) -> None:
    rides = [
        Ride(datetime(2018, 3, 1, 8, 0, 5), constant(200, 120), utc_offset_h=-7),
        # The night after a change to summer time.
        Ride(datetime(2018, 3, 12, 0, 30, 0), constant(200, 120), utc_offset_h=-6),
        Ride(datetime(2018, 6, 1, 17, 15, 9), constant(200, 120), utc_offset_h=5.5),
    ]

    lake = build(spark, tmp_path, rides)

    assert matches(spark, lake) == {
        "2018_03_01_08_00_05": -7 * 3600,
        "2018_03_12_00_30_00": -6 * 3600,
        "2018_06_01_17_15_09": int(5.5 * 3600),
    }


def test_when_a_file_could_be_two_rides_the_athletes_usual_offset_decides(
    spark: SparkSession, tmp_path: Path
) -> None:
    usual = [
        Ride(datetime(2018, 4, d, 7, 0, 0), constant(200, 120), utc_offset_h=2) for d in (1, 2, 3)
    ]
    # A ride at 10:00 UTC and one at 13:00 UTC on the same day, with files named 12:00 and
    # 15:00 local. The file of 15:00 is also +5 h from the first ride.
    pair = [
        Ride(datetime(2018, 4, 5, 12, 0, 0), constant(200, 120), utc_offset_h=2),
        Ride(datetime(2018, 4, 5, 15, 0, 0), constant(200, 120), utc_offset_h=2),
    ]

    lake = build(spark, tmp_path, usual + pair)

    assert set(matches(spark, lake).values()) == {2 * 3600}
    assert len(matches(spark, lake)) == 5


def test_a_ride_without_a_file_and_a_file_without_a_ride_are_left_out(
    spark: SparkSession, tmp_path: Path
) -> None:
    rides = [
        Ride(datetime(2018, 5, 1, 9, 0, 0), constant(200, 120)),
        Ride(datetime(2018, 5, 2, 9, 0, 0), constant(200, 120), has_file=False),
        # Seven minutes off any quarter hour: no time zone explains it.
        Ride(datetime(2018, 5, 3, 9, 7, 0), constant(200, 120), utc_offset_h=2 + 7 / 60),
    ]

    lake = build(spark, tmp_path, rides)

    assert matches(spark, lake) == {"2018_05_01_09_00_00": 7200}


@pytest.mark.parametrize(
    ("sport", "speed", "cycling"),
    [
        ("Bike", None, True),
        ("VirtualRide", None, True),
        (" mountain bike ", None, True),
        ("", 25.0, True),
        ("", 0.0, True),
        ("", 9.0, False),
        ("Run", 11.0, False),
    ],
)
def test_which_rides_are_cycling(
    spark: SparkSession, tmp_path: Path, sport: str, speed: float | None, cycling: bool
) -> None:
    metrics: dict[str, object] = {} if speed is None else {"average_speed": f"{speed:.5f}"}
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), constant(200, 120), sport=sport, metrics=metrics)

    lake = build(spark, tmp_path, [ride])

    row = lake.read(spark, "silver", "rides").first()
    assert row is not None
    assert row["is_cycling"] is cycling


def grid(spark: SparkSession, lake: Lake) -> list[float]:
    rows = lake.read(spark, "silver", "power_1hz").orderBy("t").collect()
    assert [r["t"] for r in rows] == list(range(rows[0]["t"], rows[0]["t"] + len(rows)))
    return [r["power"] for r in rows]


def test_a_recording_every_1_26_s_fills_every_second(spark: SparkSession, tmp_path: Path) -> None:
    secs = [round(i * 1.26, 2) for i in range(100)]
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), [float(i) for i in range(100)], secs=secs)

    power = grid(spark, build(spark, tmp_path, [ride]))

    assert len(power) == int(secs[-1]) + 1
    # Second 4 has no reading of its own (3.78 s and 5.04 s around it) and keeps 3.78's.
    assert power[3:6] == [3.0, 3.0, 4.0]


def test_a_short_gap_holds_the_last_reading_and_a_long_one_is_a_stop(
    spark: SparkSession, tmp_path: Path
) -> None:
    secs = [*range(60), *range(63, 70), *range(90, 150)]
    watts = blocks((200, 60), (300, 7), (100, 60))
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), watts, secs=secs)

    power = grid(spark, build(spark, tmp_path, [ride]))

    assert power[59:64] == [200.0, 200.0, 200.0, 200.0, 300.0]
    assert power[69:75] == [300.0] * 6
    assert power[75:90] == [0.0] * 15
    assert power[90] == 100.0


def test_spikes_and_dropouts_keep_the_reading_before(spark: SparkSession, tmp_path: Path) -> None:
    watts: list[float | None] = [200.0] * 100
    watts[50], watts[51] = 9000.0, None
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), watts)

    assert grid(spark, build(spark, tmp_path, [ride])) == [200.0] * 100


def test_rides_under_a_minute_are_dropped(spark: SparkSession, tmp_path: Path) -> None:
    ride = Ride(datetime(2018, 5, 1, 9, 0, 0), constant(200, 59))

    lake = build(spark, tmp_path, [ride])

    assert lake.read(spark, "silver", "power_1hz").count() == 0


def test_a_file_whose_name_is_not_a_start_time_is_ignored(
    spark: SparkSession, tmp_path: Path
) -> None:
    lake = Lake(tmp_path)
    path = athlete_zip(lake.raw, "a1", [Ride(datetime(2018, 5, 1, 9, 0, 0), constant(200, 120))])
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr("2015_01_20_18_50_00.csv.bak.csv", "secs,power\n0,100\n")
    bronze.build(spark, lake)

    silver.build(spark, lake)

    assert matches(spark, lake) == {"2018_05_01_09_00_00": 7200}
