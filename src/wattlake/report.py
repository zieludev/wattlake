"""The results page, docs/results.md, and its charts, from the gold tables."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Sequence
from datetime import date
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wattlake import charts, power
from wattlake.lake import Lake

PROFILE_DURATIONS = (5, 60, 300, 1200, 3600)
MIN_GROUP = 10
CHECKED_DURATIONS = (1, 5, 60, 300, 1200, 3600)


def write(spark: SparkSession, lake: Lake, out: Path) -> None:
    (out / "img").mkdir(parents=True, exist_ok=True)
    sections = [
        _header(spark, lake),
        _coverage(spark, lake),
        _agreement(spark, lake),
        _profile(spark, lake, out),
        _critical_power(spark, lake),
        _training_load(spark, lake, out),
    ]
    (out / "results.md").write_text("\n\n".join(sections) + "\n")


def _header(spark: SparkSession, lake: Lake) -> str:
    athletes = lake.read(spark, "bronze", "rides").select("athlete_id").distinct().count()
    return (
        "# Results\n\n"
        f"What `wattlake build` and `wattlake report` found in {athletes:,} athletes of the "
        "GoldenCheetah OpenData set, counting those with at least one ride. `wattlake fetch` "
        "takes the first athletes by id, a random sample because the ids are random UUIDs; the "
        "[README](../README.md#try-it) has the command that fetched these. "
        f"Spark {pyspark.__version__}, written {date.today():%Y-%m-%d}."
    )


def _coverage(spark: SparkSession, lake: Lake) -> str:
    rides = lake.read(spark, "bronze", "rides")
    files = lake.read(spark, "bronze", "samples").select("athlete_id", "activity").distinct()
    matched = lake.read(spark, "silver", "rides")
    cycling = matched.where("is_cycling AND has_power")
    seconds = lake.read(spark, "silver", "power_1hz").count()
    gold_rides = lake.read(spark, "gold", "rides").count()
    n_files, n_matched = files.count(), matched.count()
    rows = [
        ("rides the athletes' JSON describes", f"{rides.count():,}"),
        ("activity files with samples", f"{n_files:,}"),
        ("files matched to a ride", f"{n_matched:,} ({n_matched / max(n_files, 1):.1%})"),
        ("cycling rides with power", f"{cycling.count():,}"),
        ("of them in the gold tables", f"{gold_rides:,}"),
        ("hours of riding at 1 Hz", f"{seconds / 3600:,.0f}"),
    ]
    return (
        "## The data\n\n"
        + _table(("", "count"), rows)
        + (
            "\n\nA ride leaves the gold tables when it lasts under a minute or over a day, or when "
            "its power per kilogram beats what any rider has done: a power meter gone wrong or a "
            "wrong weight."
        )
    )


def _agreement(spark: SparkSession, lake: Lake) -> str:
    """GoldenCheetah stored its own mean-maximal power with every ride. Comparing the two is a
    check of the whole path from zip to gold table against independent code."""
    theirs = (
        lake.read(spark, "silver", "rides")
        .where(F.col("gc_mmp").isNotNull())
        .select(
            "athlete_id",
            "activity",
            F.explode(F.arrays_zip("gc_mmp_secs", "gc_mmp")).alias("p"),
        )
        .select(
            "athlete_id",
            "activity",
            F.col("p.gc_mmp_secs").alias("duration_s"),
            F.col("p.gc_mmp").alias("gc_watts"),
        )
        .where(F.col("duration_s").isin(*CHECKED_DURATIONS) & (F.col("gc_watts") > 0))
    )
    ours = lake.read(spark, "gold", "ride_mmp").select(
        "athlete_id", "activity", "duration_s", "watts"
    )
    error = F.abs(F.col("watts") - F.col("gc_watts")) / F.col("gc_watts")
    stats = (
        ours.join(theirs, ["athlete_id", "activity", "duration_s"])
        .groupBy("duration_s")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.percentile_approx(error, 0.5).alias("median"),
            F.avg((error <= 0.01).cast("double")).alias("within_1"),
            F.avg((error <= 0.05).cast("double")).alias("within_5"),
        )
        .orderBy("duration_s")
        .collect()
    )
    rows = [
        (
            _duration(r["duration_s"]),
            f"{r['n']:,}",
            f"{r['median']:.2%}",
            f"{r['within_1']:.1%}",
            f"{r['within_5']:.1%}",
        )
        for r in stats
    ]
    return (
        "## Agreement with GoldenCheetah\n\n"
        "GoldenCheetah stored its own mean-maximal power with every ride, computed by its own "
        "code from the same file. The two are compared ride by ride, so this checks the whole "
        "path from zip to gold table: the matching of files to rides, the 1 Hz grid and the "
        "running sums.\n\n"
        + _table(("duration", "rides", "median difference", "within 1 %", "within 5 %"), rows)
    )


def _profile(spark: SparkSession, lake: Lake, out: Path) -> str:
    population = lake.read(spark, "gold", "population").collect()
    everyone = sorted(
        (r for r in population if r["gender"] == "all"), key=lambda r: r["duration_s"]
    )
    if not everyone:
        return "## Power profile\n\nNo rider has a season of ten rides with power."
    bands = [
        charts.Band(r["duration_s"], r["p10"], r["p25"], r["p50"], r["p75"], r["p90"])
        for r in everyone
    ]
    riders = max(r["riders"] for r in everyone)
    _save_chart(out, "power-profile", lambda theme: charts.power_profile(bands, riders, theme))
    rows = [
        (
            _gender(r["gender"]),
            _duration(r["duration_s"]),
            f"{r['riders']:,}",
            *(_number(r[p], ".2f") for p in ("p10", "p25", "p50", "p75", "p90")),
        )
        for r in sorted(
            population, key=lambda r: (r["gender"] != "all", r["gender"], r["duration_s"])
        )
        if r["duration_s"] in PROFILE_DURATIONS and r["riders"] >= MIN_GROUP
    ]
    small = sorted({_gender(r["gender"]) for r in population if r["riders"] < MIN_GROUP})
    note = f" Left out, with fewer than {MIN_GROUP} riders: {', '.join(small)}." if small else ""
    return (
        "## Power profile\n\n"
        + _picture("power-profile", "Percentiles of best power per kilogram by effort length")
        + "\n\nBest power per kilogram of body weight, each rider counted once with their best "
        f"season. A season counts when it has at least ten rides with power.{note}\n\n"
        + _table(("riders", "duration", "n", "10th", "25th", "median", "75th", "90th"), rows)
    )


def _critical_power(spark: SparkSession, lake: Lake) -> str:
    seasons = lake.read(spark, "gold", "seasons")
    fits = (
        seasons.where(F.col("cp_watts").isNotNull())
        .select("athlete_id", "year", "cp_watts", "w_prime_j", "cp_r2")
        .distinct()
    )
    weights = (
        lake.read(spark, "gold", "rides")
        .groupBy("athlete_id", "year")
        .agg(F.percentile_approx("weight_kg", 0.5).alias("weight_kg"))
    )
    fits = fits.join(weights, ["athlete_id", "year"], "left").withColumn(
        "cp_wkg", F.col("cp_watts") / F.col("weight_kg")
    )
    all_seasons = seasons.select("athlete_id", "year").distinct().count()
    if all_seasons == 0:
        return "## Critical power\n\nNo rider has a season of ten rides with power."
    if fits.isEmpty():
        return (
            f"## Critical power\n\nNone of {all_seasons:,} rider seasons has a fit that holds up."
        )
    quartiles = [0.25, 0.5, 0.75]
    q = fits.agg(
        *[
            F.percentile_approx(c, quartiles).alias(c)
            for c in ("cp_watts", "cp_wkg", "w_prime_j", "cp_r2")
        ]
    ).first()
    assert q is not None
    rows = [
        (name, *(_number(v, spec, scale) for v in q[column] or [None] * len(quartiles)))
        for name, column, spec, scale in (
            ("CP, W", "cp_watts", ".0f", 1),
            ("CP, W/kg", "cp_wkg", ".2f", 1),
            ("W', kJ", "w_prime_j", ".1f", 1000),
            ("R² of the fit", "cp_r2", ".3f", 1),
        )
    ]
    return (
        "## Critical power\n\n"
        f"{fits.count():,} of {all_seasons:,} rider seasons have a fit that holds up: the best "
        "efforts of 3, 5, 8, 10 and 20 minutes all exist, the line through their work fits "
        "with R² of at least 0.95, and CP and W' are in a human range.\n\n"
        + _table(("", "25th", "median", "75th"), rows)
    )


def _training_load(spark: SparkSession, lake: Lake, out: Path) -> str:
    load = lake.read(spark, "gold", "training_load").withColumn("year", F.year("date"))
    busiest = (
        load.where("tss > 0")
        .groupBy("athlete_id", "year")
        .count()
        .orderBy(F.desc("count"), "athlete_id")
        .first()
    )
    if busiest is None:
        return "## Training load\n\nNo ride has a TSS: no 20 minute best came before it."
    season = (
        load.where(
            (F.col("athlete_id") == busiest["athlete_id"]) & (F.col("year") == busiest["year"])
        )
        .orderBy("date")
        .collect()
    )
    days = [charts.Day(r["date"], r["ctl"], r["atl"], r["tsb"]) for r in season]
    _save_chart(out, "training-load", lambda theme: charts.training_load(days, theme))
    monthly = [
        (d.date.strftime("%b"), f"{d.ctl:.0f}", f"{d.atl:.0f}", f"{d.tsb:.0f}")
        for d in days
        if d.date.day == 1
    ]
    return (
        "## Training load\n\n"
        + _picture("training-load", "CTL, ATL and TSB of one rider through a season")
        + f"\n\nThe rider with the most days of riding in one year: {busiest['count']} days in "
        f"{busiest['year']}. TSS needs an FTP; each ride takes 95 % of the rider's best 20 "
        "minutes in the 90 days before it.\n\n" + _table(("first of", "CTL", "ATL", "TSB"), monthly)
    )


def benchmark(
    power_1hz: DataFrame, methods: Sequence[str], repeat: int
) -> Iterator[tuple[str, float]]:
    """The best of `repeat` runs of each method over every ride, written to Spark's noop sink:
    everything is computed and nothing is stored."""
    power_1hz = power_1hz.cache()
    power_1hz.count()
    for method in methods:
        times: list[float] = []
        for _ in range(repeat):
            started = time.monotonic()
            power.METHODS[method](power_1hz, power.DURATIONS).write.format("noop").mode(
                "overwrite"
            ).save()
            times.append(time.monotonic() - started)
        yield method, min(times)


def _save_chart(out: Path, name: str, draw: Callable[[charts.Theme], str]) -> None:
    for scheme, theme in charts.THEMES.items():
        (out / "img" / f"{name}-{scheme}.svg").write_text(draw(theme))


def _picture(name: str, alt: str) -> str:
    return (
        "<picture>\n"
        f'  <source media="(prefers-color-scheme: dark)" srcset="img/{name}-dark.svg">\n'
        f'  <img alt="{alt}" src="img/{name}-light.svg">\n'
        "</picture>"
    )


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(" --- " for _ in header) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def _number(value: float | None, spec: str, scale: float = 1) -> str:
    return "-" if value is None else format(value / scale, spec)


def _duration(seconds: int) -> str:
    return f"{seconds // 60} min" if seconds >= 60 else f"{seconds} s"


def _gender(code: str) -> str:
    return {"M": "men", "F": "women", "all": "all"}.get(code, "not given")
