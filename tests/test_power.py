from __future__ import annotations

import numpy as np
import pytest
from pyspark.sql import DataFrame, SparkSession

from wattlake import power


def power_1hz(spark: SparkSession, rides: dict[str, list[float]]) -> DataFrame:
    rows = [
        ("a", activity, t, float(w))
        for activity, watts in rides.items()
        for t, w in enumerate(watts)
    ]
    return spark.createDataFrame(rows, "athlete_id string, activity string, t int, power float")


def mmp(df: DataFrame) -> dict[tuple[str, int], float]:
    return {(r["activity"], r["duration_s"]): r["watts"] for r in df.collect()}


@pytest.mark.parametrize("method", list(power.METHODS))
def test_steady_ride_is_its_own_best_at_every_duration_it_lasts(
    spark: SparkSession, method: str
) -> None:
    result = mmp(power.METHODS[method](power_1hz(spark, {"r": [200.0] * 600}), power.DURATIONS))

    assert result == {("r", d): pytest.approx(200.0) for d in power.DURATIONS if d <= 600}


@pytest.mark.parametrize("method", list(power.METHODS))
def test_an_interval_sets_the_short_bests_and_the_long_ones_average_around_it(
    spark: SparkSession, method: str
) -> None:
    ride = [100.0] * 300 + [400.0] * 60 + [100.0] * 300

    result = mmp(power.METHODS[method](power_1hz(spark, {"r": ride}), (1, 60, 120, 600)))

    assert result == {
        ("r", 1): pytest.approx(400),
        ("r", 60): pytest.approx(400),
        ("r", 120): pytest.approx(250),
        ("r", 600): pytest.approx((540 * 100 + 60 * 400) / 600),
    }


def test_the_three_methods_agree_on_random_rides(spark: SparkSession) -> None:
    generator = np.random.default_rng(7)
    rides = {
        f"r{i}": list(generator.gamma(2, 100, size=n).round())
        for i, n in enumerate((45, 700, 2500))
    }
    df = power_1hz(spark, rides)

    results = [mmp(method(df, power.DURATIONS)) for method in power.METHODS.values()]

    assert results[0].keys() == results[1].keys() == results[2].keys()
    for key, watts in results[0].items():
        assert results[1][key] == pytest.approx(watts)
        assert results[2][key] == pytest.approx(watts)


def test_ride_power_matches_a_plain_numpy_reading_of_the_definitions(spark: SparkSession) -> None:
    watts = np.random.default_rng(3).gamma(2, 100, size=1800).round()

    ride = power.ride_power(power_1hz(spark, {"r": list(watts)})).first()

    assert ride is not None
    rolling = np.convolve(watts, np.ones(30) / 30, mode="valid")
    assert ride["np_watts"] == pytest.approx(np.mean(rolling**4) ** 0.25)
    assert ride["avg_watts"] == pytest.approx(watts.mean())
    assert ride["work_kj"] == pytest.approx(watts.sum() / 1000)
    assert ride["duration_s"] == 1800
    assert ride["mmp_300"] == pytest.approx(
        np.convolve(watts, np.ones(300) / 300, mode="valid").max()
    )
    assert ride["mmp_3600"] is None
