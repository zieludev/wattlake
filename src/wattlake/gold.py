"""Gold: what the rides say about their riders.

- `rides`: each cycling ride with its power numbers, an FTP estimate from the rider's own
  earlier rides, and the Training Stress Score that follows from it.
- `ride_mmp`: the mean-maximal power of each ride, in watts and in watts per kilogram.
- `seasons`: each rider's best power per calendar year, and the critical power and W' fitted
  to it.
- `training_load`: each rider's fitness (CTL), fatigue (ATL) and form (TSB), day by day.
- `population`: percentiles of the riders' best power per kilogram, by duration and gender.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wattlake import power
from wattlake.lake import Lake

ID = ["athlete_id", "activity"]

FTP_DURATION_S = 1200
FTP_SHARE = 0.95
FTP_LOOKBACK_DAYS = 90
# IF above this means the FTP estimate is too low to trust, not a superhuman ride.
MAX_INTENSITY = 1.3

# The best power-per-kilogram ever recorded is around these values; a ride above them has a bad
# power meter or a wrong weight, and is left out of every best.
CEILING_WKG = {1: 30.0, 5: 26.0, 60: 13.0, 300: 8.5, 1200: 7.0, 3600: 6.6}

CP_DURATIONS = (180, 300, 480, 600, 1200)
CP_RANGE_W = (80, 550)
W_PRIME_RANGE_J = (2_000, 50_000)
MIN_CP_R2 = 0.95

CTL_DAYS, ATL_DAYS = 42, 7
PERCENTILES = (0.1, 0.25, 0.5, 0.75, 0.9)
MIN_RIDES_PER_SEASON = 10


def build(spark: SparkSession, lake: Lake) -> None:
    measured = power.ride_power(lake.read(spark, "silver", "power_1hz"))
    lake.write(measured, "gold", "ride_power")
    measured = lake.read(spark, "gold", "ride_power")
    silver = lake.read(spark, "silver", "rides")

    plausible = without_implausible(measured, silver)
    lake.write(rides(plausible, silver), "gold", "rides")
    gold_rides = lake.read(spark, "gold", "rides")

    lake.write(ride_mmp(plausible, gold_rides), "gold", "ride_mmp")
    mmp = lake.read(spark, "gold", "ride_mmp")
    lake.write(seasons(mmp, gold_rides), "gold", "seasons")
    lake.write(training_load(gold_rides), "gold", "training_load")
    lake.write(population(lake.read(spark, "gold", "seasons")), "gold", "population")


def without_implausible(measured: DataFrame, silver: DataFrame) -> DataFrame:
    weight = silver.select(*ID, "weight_kg")
    over = [F.col(f"mmp_{d}") / F.col("weight_kg") > ceiling for d, ceiling in CEILING_WKG.items()]
    too_strong = F.coalesce(F.greatest(*over), F.lit(False))
    return measured.join(weight, ID).where(~too_strong).drop("weight_kg")


def rides(measured: DataFrame, silver: DataFrame) -> DataFrame:
    meta = silver.select(*ID, "gender", "local_date", "start_utc", "weight_kg", "gc_np")
    history = (
        Window.partitionBy("athlete_id")
        .orderBy(F.unix_date("local_date"))
        .rangeBetween(-FTP_LOOKBACK_DAYS, -1)
    )
    ftp = FTP_SHARE * F.max(f"mmp_{FTP_DURATION_S}").over(history)
    intensity = F.col("np_watts") / F.col("ftp_watts")
    tss = F.col("duration_s") * F.col("np_watts") * intensity / (F.col("ftp_watts") * 36)
    return (
        measured.join(meta, ID)
        .withColumn("year", F.year("local_date"))
        .withColumn("ftp_watts", ftp)
        .withColumn("intensity", F.when(intensity <= MAX_INTENSITY, intensity))
        .withColumn("tss", F.when(F.col("intensity").isNotNull(), tss))
    )


def ride_mmp(measured: DataFrame, gold_rides: DataFrame) -> DataFrame:
    return (
        power.mmp_long(measured)
        .join(gold_rides.select(*ID, "local_date", "year", "weight_kg"), ID)
        .withColumn("wkg", F.col("watts") / F.col("weight_kg"))
    )


def seasons(mmp: DataFrame, gold_rides: DataFrame) -> DataFrame:
    """Best watts and W/kg per rider, year and duration, with the year's critical power and W'
    fitted to the bests between 3 and 20 minutes. The fit is the linear work-time model: the
    work of a best effort is W' + CP * t, so CP is the slope and W' the intercept."""
    active = (
        gold_rides.groupBy("athlete_id", "year", "gender")
        .agg(F.count(F.lit(1)).alias("rides"))
        .where(F.col("rides") >= MIN_RIDES_PER_SEASON)
    )
    best = mmp.groupBy("athlete_id", "year", "duration_s").agg(
        F.max("watts").alias("watts"), F.max("wkg").alias("wkg")
    )
    fit_points = best.where(F.col("duration_s").isin(*CP_DURATIONS))
    work, t = F.col("watts") * F.col("duration_s"), F.col("duration_s")
    fits = (
        fit_points.groupBy("athlete_id", "year")
        .agg(
            F.regr_slope(work, t).alias("cp_watts"),
            F.regr_intercept(work, t).alias("w_prime_j"),
            F.regr_r2(work, t).alias("cp_r2"),
            F.count(F.lit(1)).alias("cp_points"),
        )
        .where(_plausible_fit())
    )
    return active.join(best, ["athlete_id", "year"]).join(fits, ["athlete_id", "year"], "left")


def _plausible_fit() -> Column:
    return (
        (F.col("cp_points") == len(CP_DURATIONS))
        & F.col("cp_watts").between(*CP_RANGE_W)
        & F.col("w_prime_j").between(*W_PRIME_RANGE_J)
        & (F.col("cp_r2") >= MIN_CP_R2)
    )


TRAINING_LOAD_SCHEMA = (
    "athlete_id string, date date, tss double, ctl double, atl double, tsb double"
)


def training_load(gold_rides: DataFrame) -> DataFrame:
    """CTL and ATL are exponentially weighted averages of the daily TSS, with time constants of
    42 and 7 days, and TSB is CTL - ATL. Each day depends on the day before, which no window
    function expresses, so each rider's days go to pandas as a whole."""
    daily = (
        gold_rides.where(F.col("tss").isNotNull())
        .groupBy("athlete_id", F.col("local_date").alias("date"))
        .agg(F.sum("tss").alias("tss"))
    )
    return daily.groupBy("athlete_id").applyInPandas(_load_of_rider, TRAINING_LOAD_SCHEMA)


def _load_of_rider(days: pd.DataFrame) -> pd.DataFrame:
    tss = days.set_index(pd.to_datetime(days["date"]))["tss"].sort_index()
    every_day = pd.date_range(tss.index[0], tss.index[-1])
    tss = tss.reindex(every_day, fill_value=0.0)
    ctl, atl = _ewma(tss.to_numpy(), CTL_DAYS), _ewma(tss.to_numpy(), ATL_DAYS)
    return pd.DataFrame(
        {
            "athlete_id": days["athlete_id"].iloc[0],
            "date": [day.date() for day in every_day],
            "tss": tss.to_numpy(),
            "ctl": ctl,
            "atl": atl,
            "tsb": ctl - atl,
        }
    )


def _ewma(values: np.ndarray, days: int) -> np.ndarray:
    """Starts from 0, as a rider with no history has no fitness."""
    out = np.empty_like(values, dtype=np.float64)
    level = 0.0
    for i, value in enumerate(values):
        level += (value - level) / days
        out[i] = level
    return out


def population(season_bests: DataFrame) -> DataFrame:
    """Each rider counts once per duration, with their best season."""
    best = season_bests.groupBy("athlete_id", "gender", "duration_s").agg(F.max("wkg").alias("wkg"))
    quantiles = F.percentile_approx("wkg", list(PERCENTILES), 10_000)
    by_group = [F.coalesce("gender", F.lit("?")).alias("gender"), "duration_s"]
    per_gender = best.groupBy(*by_group).agg(
        F.count(F.lit(1)).alias("riders"), quantiles.alias("q")
    )
    everyone = best.groupBy(F.lit("all").alias("gender"), "duration_s").agg(
        F.count(F.lit(1)).alias("riders"), quantiles.alias("q")
    )
    return per_gender.unionByName(everyone).select(
        "gender",
        "duration_s",
        "riders",
        *[F.col("q")[i].alias(f"p{round(p * 100)}") for i, p in enumerate(PERCENTILES)],
    )
