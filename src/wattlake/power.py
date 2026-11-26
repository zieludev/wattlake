"""Mean-maximal power and Normalized Power of each ride, from its power at 1 Hz.

The mean-maximal power for a duration d is the best average over any d consecutive seconds of
the ride. Three ways to compute it give the same numbers and run at very different speeds
(see docs/benchmark.md):

- `pandas`: each ride is handed to pandas with `applyInPandas`, and numpy takes the difference
  of two running sums, d seconds apart. The fastest of the three, so the pipeline uses it.
- `lag`: the same running sums in Spark's own window functions, one `lag` per duration. About
  1.4 times slower: Spark evaluates a window row by row, numpy a whole ride at once.
- `frame`: an `avg` over a sliding frame of d rows. Spark recomputes a sliding frame for every
  row, so the work per row grows with d: some 150 times slower.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

ID = ["athlete_id", "activity"]
DURATIONS = (1, 5, 10, 15, 20, 30, 60, 120, 180, 300, 480, 600, 1200, 1800, 3600)
NP_WINDOW_S = 30
MMP_SCHEMA = "athlete_id string, activity string, duration_s int, watts double"


def ride_power(power_1hz: DataFrame, durations: Sequence[int] = DURATIONS) -> DataFrame:
    """One row per ride: its length, average power, work, Normalized Power and one column
    `mmp_<d>` for each duration. A ride shorter than d has no value for d."""
    schema = ", ".join(
        [
            "athlete_id string",
            "activity string",
            "duration_s long",
            "avg_watts double",
            "work_kj double",
            "np_watts double",
            *[f"mmp_{d} double" for d in durations],
        ]
    )
    return power_1hz.groupBy(*ID).applyInPandas(_pandas_ride_power(durations), schema)


def _pandas_ride_power(durations: Sequence[int]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def summary(ride: pd.DataFrame) -> pd.DataFrame:
        power = ride.sort_values("t")["power"].to_numpy(dtype=np.float64)
        running = _running_sum(power)
        rolling = _averages(running, NP_WINDOW_S)
        row: dict[str, object] = {
            "athlete_id": ride["athlete_id"].iloc[0],
            "activity": ride["activity"].iloc[0],
            "duration_s": len(power),
            "avg_watts": float(power.mean()),
            "work_kj": float(power.sum() / 1000),
            # Normalized Power: the fourth root of the mean fourth power of the 30 s average.
            "np_watts": float(np.mean(rolling**4) ** 0.25) if len(rolling) else None,
        }
        for d in durations:
            averages = _averages(running, d)
            row[f"mmp_{d}"] = float(averages.max()) if len(averages) else None
        return pd.DataFrame([row])

    return summary


def _running_sum(power: np.ndarray) -> np.ndarray:
    return np.concatenate([[0.0], np.cumsum(power)])


def _averages(running: np.ndarray, d: int) -> np.ndarray:
    """The average of every d consecutive seconds."""
    return (running[d:] - running[:-d]) / d


def mmp_long(rides: DataFrame, durations: Sequence[int] = DURATIONS) -> DataFrame:
    """The `mmp_<d>` columns of `ride_power` as rows of (duration_s, watts)."""
    pairs = [
        F.struct(F.lit(d).alias("duration_s"), F.col(f"mmp_{d}").alias("watts")) for d in durations
    ]
    return (
        rides.select(*ID, F.explode(F.array(*pairs)).alias("mmp"))
        .select(*ID, "mmp.duration_s", "mmp.watts")
        .where(F.col("watts").isNotNull())
    )


def mmp_lag(power_1hz: DataFrame, durations: Sequence[int] = DURATIONS) -> DataFrame:
    ride = Window.partitionBy(*ID).orderBy("t")
    running = power_1hz.withColumn(
        "running", F.sum("power").over(ride.rowsBetween(Window.unboundedPreceding, 0))
    )
    wide = (
        running.select(*ID, *[_last_seconds(d, ride).alias(f"avg_{d}") for d in durations])
        .groupBy(*ID)
        .agg(*[F.max(f"avg_{d}").alias(f"mmp_{d}") for d in durations])
    )
    return mmp_long(wide, durations)


def _last_seconds(d: int, ride: WindowSpec) -> Column:
    if d == 1:
        return F.col("power").cast("double")
    before = F.coalesce(F.lag("running", d).over(ride), F.lit(0.0))
    full = F.lag("running", d - 1).over(ride).isNotNull()
    return F.when(full, (F.col("running") - before) / d)


def mmp_frame(power_1hz: DataFrame, durations: Sequence[int] = DURATIONS) -> DataFrame:
    ride = Window.partitionBy(*ID).orderBy("t")
    averages = [
        F.when(
            F.count(F.lit(1)).over(ride.rowsBetween(-(d - 1), 0)) == d,
            F.avg("power").over(ride.rowsBetween(-(d - 1), 0)),
        ).alias(f"avg_{d}")
        for d in durations
    ]
    wide = (
        power_1hz.select(*ID, *averages)
        .groupBy(*ID)
        .agg(*[F.max(f"avg_{d}").alias(f"mmp_{d}") for d in durations])
    )
    return mmp_long(wide, durations)


def mmp_pandas(power_1hz: DataFrame, durations: Sequence[int] = DURATIONS) -> DataFrame:
    return power_1hz.groupBy(*ID).applyInPandas(_pandas_mmp(durations), MMP_SCHEMA)


def _pandas_mmp(durations: Sequence[int]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def mmp(ride: pd.DataFrame) -> pd.DataFrame:
        running = _running_sum(ride.sort_values("t")["power"].to_numpy(dtype=np.float64))
        fitting = [d for d in durations if d < len(running)]
        return pd.DataFrame(
            {
                "athlete_id": ride["athlete_id"].iloc[0],
                "activity": ride["activity"].iloc[0],
                "duration_s": np.array(fitting, dtype=np.int32),
                "watts": [float(_averages(running, d).max()) for d in fitting],
            }
        )

    return mmp


METHODS: dict[str, Callable[[DataFrame, Sequence[int]], DataFrame]] = {
    "lag": mmp_lag,
    "frame": mmp_frame,
    "pandas": mmp_pandas,
}
