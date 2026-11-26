"""Silver: rides joined to their sample files, and the power of every cycling ride at 1 Hz.

An activity file is named after the local time the ride started, while the athlete's JSON gives
that start in UTC. A file and a ride belong together when the two differ by a whole number of
quarter hours, up to 14 hours: a time zone. When a file could belong to more than one ride, the
athlete's usual offset decides.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wattlake.lake import Lake

ID = ["athlete_id", "activity"]
MAX_OFFSET_S = 14 * 3600
OFFSET_STEP_S = 900
# A reading above this is a spike of the power meter, not a rider.
MAX_WATTS = 2500
# A gap in the recording up to this long keeps the last reading; a longer one is a stop, at 0 W.
HOLD_S = 5
MIN_RIDE_S = 60
MAX_RIDE_S = 24 * 3600
MIN_WEIGHT_KG, MAX_WEIGHT_KG = 35, 150

CYCLING_SPORTS = [
    "bike",
    "cycling",
    "ride",
    "road",
    "mtb",
    "mountain bike",
    "rolle",
    "trainer",
    "virtualride",
]
# The sport is often empty. An empty activity with power is nearly always a ride, but runners
# with a power meter exist, and 98.8 % of the runs with power in the data are slower than this.
MIN_UNLABELLED_RIDE_KMH = 15


def build(spark: SparkSession, lake: Lake) -> None:
    samples = lake.read(spark, "bronze", "samples")
    rides = matched_rides(activities(samples), parse_rides(lake.read(spark, "bronze", "rides")))
    lake.write(rides, "silver", "rides")
    cycling = lake.read(spark, "silver", "rides").where("is_cycling AND has_power")
    lake.write(power_1hz(samples, cycling), "silver", "power_1hz")


def activities(samples: DataFrame) -> DataFrame:
    return samples.groupBy(*ID).agg(
        F.count(F.lit(1)).alias("samples"),
        (F.count("power") > 0).alias("has_power"),
        # Spark 4 runs in ANSI mode, where a value that does not parse stops the job. One file
        # in the first 150 athletes is called 2015_01_20_18_50_00.csv.bak.csv.
        F.try_to_timestamp("activity", F.lit("yyyy_MM_dd_HH_mm_ss")).alias("local_start"),
    )


def parse_rides(rides: DataFrame) -> DataFrame:
    sport = F.lower(F.trim(F.coalesce("sport", F.lit(""))))
    speed = _metric("average_speed")
    unlabelled_ride = (
        (sport == "")
        & F.col("data").substr(4, 1).eqNullSafe("P")
        & (speed.isNull() | (speed == 0) | (speed >= MIN_UNLABELLED_RIDE_KMH))
    )
    weight = _metric("athlete_weight")
    return rides.select(
        "athlete_id",
        F.when(F.col("gender").isin("M", "F"), F.col("gender")).alias("gender"),
        F.try_to_timestamp("date", F.lit("yyyy/MM/dd HH:mm:ss 'UTC'")).alias("start_utc"),
        F.col("sport"),
        (sport.isin(CYCLING_SPORTS) | unlabelled_ride).alias("is_cycling"),
        F.when(weight.between(MIN_WEIGHT_KG, MAX_WEIGHT_KG), weight).alias("weight_kg"),
        _metric("coggan_np[0]").alias("gc_np"),
        F.from_json(F.get_json_object("metrics", "$.power_mmp"), "array<double>").alias("gc_mmp"),
        F.from_json(F.get_json_object("metrics", "$.power_mmp_secs"), "array<int>").alias(
            "gc_mmp_secs"
        ),
    )


def _metric(path: str) -> Column:
    return F.get_json_object("metrics", f"$.{path}").try_cast("double")


def matched_rides(activities: DataFrame, rides: DataFrame) -> DataFrame:
    """Each activity file with the ride it records. Files and rides without a partner are left
    out."""
    candidates = _candidates(activities, rides)
    usual = _usual_offsets(candidates)
    by_activity = Window.partitionBy(*ID).orderBy(*_closeness())
    by_ride = Window.partitionBy("athlete_id", "start_utc").orderBy(*_closeness())
    return (
        candidates.join(usual, "athlete_id", "left")
        .withColumn("rank", F.row_number().over(by_activity))
        .where("rank = 1")
        .withColumn("rank", F.row_number().over(by_ride))
        .where("rank = 1")
        .drop("rank", "usual_offset_s")
        .withColumn("local_date", F.to_date("local_start"))
    )


def _candidates(activities: DataFrame, rides: DataFrame) -> DataFrame:
    # A shared calendar day as the join key, so each file meets the rides of three days and
    # not every ride of its athlete.
    days = F.explode(F.array(*[F.date_add(F.to_date("start_utc"), d) for d in (-1, 0, 1)]))
    offset = F.unix_timestamp("local_start") - F.unix_timestamp("start_utc")
    return (
        activities.withColumn("day", F.to_date("local_start"))
        .join(rides.withColumn("day", days), ["athlete_id", "day"])
        .withColumn("offset_s", offset)
        .where((F.abs("offset_s") <= MAX_OFFSET_S) & (F.col("offset_s") % OFFSET_STEP_S == 0))
        .drop("day")
    )


def _usual_offsets(candidates: DataFrame) -> DataFrame:
    unambiguous = candidates.withColumn("n", F.count(F.lit(1)).over(Window.partitionBy(*ID))).where(
        "n = 1"
    )
    most_common = Window.partitionBy("athlete_id").orderBy(F.desc("count"), "offset_s")
    return (
        unambiguous.groupBy("athlete_id", "offset_s")
        .count()
        .withColumn("rank", F.row_number().over(most_common))
        .where("rank = 1")
        .select("athlete_id", F.col("offset_s").alias("usual_offset_s"))
    )


def _closeness() -> list[Column]:
    return [
        F.abs(F.col("offset_s") - F.coalesce("usual_offset_s", F.lit(0))),
        F.abs("offset_s"),
        F.col("activity"),
    ]


def power_1hz(samples: DataFrame, rides: DataFrame) -> DataFrame:
    """Power on a 1 s grid. Some head units record every 1.26 s, others pause when the rider
    stops, so the samples cannot be used as they are: each second takes the last reading in
    it, and a second without one holds the reading before for up to HOLD_S seconds."""
    activity = Window.partitionBy(*ID)
    readings = (
        samples.join(F.broadcast(rides.select(*ID)), ID, "left_semi")
        .where(F.col("secs").between(0, MAX_RIDE_S))
        .where(F.col("power").between(0, MAX_WATTS))
        .withColumn("t", F.floor("secs").cast("int"))
        # Everything after this is per activity, so this is the only shuffle: the aggregation
        # and the windows below keep its partitioning.
        .repartition(*ID)
        .groupBy(*ID, "t")
        .agg(F.max_by("power", "secs").alias("power"))
        .withColumn("span", F.max("t").over(activity) - F.min("t").over(activity) + 1)
        .where(F.col("span").between(MIN_RIDE_S, MAX_RIDE_S))
    )
    # Each reading covers the seconds up to the next one: held for HOLD_S, then 0 W.
    until = F.coalesce(F.lead("t").over(activity.orderBy("t")) - 1, F.col("t"))
    second = F.explode(F.sequence("t", "until")).alias("second")
    return (
        readings.withColumn("until", until)
        .select(*ID, "t", "power", second)
        .select(
            *ID,
            F.col("second").alias("t"),
            F.when(F.col("second") - F.col("t") <= HOLD_S, F.col("power"))
            .otherwise(0.0)
            .cast("float")
            .alias("power"),
        )
    )
