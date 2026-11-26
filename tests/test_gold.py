from __future__ import annotations

from datetime import date, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession

from wattlake import gold, power

ID = "athlete_id string, activity string"


def measured(spark: SparkSession, rides: dict[str, dict[int, float]]) -> DataFrame:
    """ride_power rows with only the columns gold reads; every ride lasts an hour at 200 W NP."""
    columns = ", ".join(f"mmp_{d} double" for d in power.DURATIONS)
    rows = [
        ("a", activity, 3600, 180.0, 648.0, 200.0, *(bests.get(d) for d in power.DURATIONS))
        for activity, bests in rides.items()
    ]
    schema = f"{ID}, duration_s long, avg_watts double, work_kj double, np_watts double, {columns}"
    return spark.createDataFrame(rows, schema)


def silver_rides(spark: SparkSession, days: dict[str, date], weight: float = 75.0) -> DataFrame:
    rows = [("a", activity, "M", day, weight, None) for activity, day in days.items()]
    schema = f"{ID}, gender string, local_date date, weight_kg double, gc_np double"
    return spark.createDataFrame(rows, schema).selectExpr(
        "*", "cast(local_date as timestamp) as start_utc"
    )


def test_ftp_comes_only_from_the_90_days_before_a_ride(spark: SparkSession) -> None:
    days = {"r1": date(2018, 1, 1), "r2": date(2018, 1, 31), "r3": date(2018, 4, 10)}
    bests = {"r1": {1200: 300.0}, "r2": {1200: 200.0}, "r3": {1200: 250.0}}

    rides = {
        r["activity"]: r
        for r in gold.rides(measured(spark, bests), silver_rides(spark, days)).collect()
    }

    assert rides["r1"]["ftp_watts"] is None
    assert rides["r1"]["tss"] is None
    assert rides["r2"]["ftp_watts"] == pytest.approx(0.95 * 300)
    # One hour at an NP of 200 W against an FTP of 285 W: IF 0.70, TSS 49.
    assert rides["r2"]["tss"] == pytest.approx(100 * (200 / 285) ** 2)
    # r1 is 99 days back and out of reach; r2 is 69 days back.
    assert rides["r3"]["ftp_watts"] == pytest.approx(0.95 * 200)


def test_a_ride_beyond_human_power_per_kilogram_is_left_out(spark: SparkSession) -> None:
    days = {"real": date(2018, 1, 1), "spike": date(2018, 1, 2)}
    bests = {"real": {300: 400.0}, "spike": {300: 900.0}}

    kept = gold.without_implausible(measured(spark, bests), silver_rides(spark, days))

    assert [r["activity"] for r in kept.collect()] == ["real"]


def test_critical_power_and_w_prime_come_back_from_bests_on_the_model(spark: SparkSession) -> None:
    cp, w_prime = 260.0, 18_000.0
    durations = (*gold.CP_DURATIONS, 60, 3600)
    mmp = spark.createDataFrame(
        [("a", 2018, d, cp + w_prime / d, (cp + w_prime / d) / 70) for d in durations],
        "athlete_id string, year int, duration_s int, watts double, wkg double",
    )
    rides = spark.createDataFrame(
        [("a", 2018, "M")] * gold.MIN_RIDES_PER_SEASON, "athlete_id string, year int, gender string"
    )

    season = gold.seasons(mmp, rides).where("duration_s = 300").first()

    assert season is not None
    assert season["cp_watts"] == pytest.approx(cp)
    assert season["w_prime_j"] == pytest.approx(w_prime)
    assert season["cp_r2"] == pytest.approx(1.0)


def test_a_season_with_too_few_rides_is_not_a_season(spark: SparkSession) -> None:
    mmp = spark.createDataFrame(
        [("a", 2018, 300, 300.0, 4.0)],
        "athlete_id string, year int, duration_s int, watts double, wkg double",
    )
    rides = spark.createDataFrame(
        [("a", 2018, "M")] * (gold.MIN_RIDES_PER_SEASON - 1),
        "athlete_id string, year int, gender string",
    )

    assert gold.seasons(mmp, rides).count() == 0


def test_fitness_and_fatigue_build_up_on_steady_training_and_decay_on_rest(
    spark: SparkSession,
) -> None:
    start = date(2018, 1, 1)
    rows = [("a", f"r{i}", start + timedelta(days=i), 100.0) for i in range(42)]
    rows.append(("a", "after", start + timedelta(days=60), 0.0))
    rides = spark.createDataFrame(rows, f"{ID}, local_date date, tss double")

    days = {r["date"]: r for r in gold.training_load(rides).collect()}

    assert len(days) == 61
    day_42 = days[start + timedelta(days=41)]
    assert day_42["ctl"] == pytest.approx(100 * (1 - (41 / 42) ** 42))
    assert day_42["atl"] == pytest.approx(100 * (1 - (6 / 7) ** 42))
    assert day_42["tsb"] == pytest.approx(day_42["ctl"] - day_42["atl"])
    assert days[start + timedelta(days=60)]["ctl"] == pytest.approx(day_42["ctl"] * (41 / 42) ** 19)


def test_population_counts_each_rider_once_with_their_best_season(spark: SparkSession) -> None:
    seasons = spark.createDataFrame(
        [
            ("a", "M", 2017, 300, 3.0),
            ("a", "M", 2018, 300, 4.0),
            ("b", "F", 2018, 300, 5.0),
        ],
        "athlete_id string, gender string, year int, duration_s int, wkg double",
    )

    rows = {r["gender"]: r for r in gold.population(seasons).collect()}

    assert {g: r["riders"] for g, r in rows.items()} == {"M": 1, "F": 1, "all": 2}
    assert rows["M"]["p50"] == 4.0
    assert rows["all"]["p90"] == 5.0
