"""Power analytics for thousands of cyclists, on the GoldenCheetah OpenData set."""

from __future__ import annotations

import argparse
import functools
import time
from pathlib import Path

from wattlake import bronze, gold, osf, power, report, silver
from wattlake.lake import Lake, spark_session

STAGES = {"bronze": bronze.build, "silver": silver.build, "gold": gold.build}
log = functools.partial(print, flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="wattlake", description=__doc__)
    parser.add_argument("--data", type=Path, default=Path("data"), help="default: ./data")
    spark = argparse.ArgumentParser(add_help=False)
    spark.add_argument("--master", default="local[*]", help="Spark master, default: local[*]")
    spark.add_argument("--driver-memory", default="4g")
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser("fetch", help="download athletes from OSF into DATA/raw")
    fetch.add_argument("--athletes", type=int, default=50, help="how many, default: 50")
    fetch.add_argument("--max-mb", type=float, help="leave out athletes with bigger zips")
    fetch.add_argument("--workers", type=int, default=4, help="parallel downloads")

    build = commands.add_parser("build", parents=[spark], help="raw -> bronze -> silver -> gold")
    build.add_argument(
        "stages", nargs="*", metavar="STAGE", help="bronze, silver, gold; default: all"
    )

    results = commands.add_parser("report", parents=[spark], help="write the results page")
    results.add_argument("--out", type=Path, default=Path("docs"), help="default: ./docs")

    bench = commands.add_parser("benchmark", parents=[spark], help="time the MMP methods")
    bench.add_argument(
        "--methods", nargs="+", choices=list(power.METHODS), default=list(power.METHODS)
    )
    bench.add_argument("--repeat", type=int, default=3)

    args = parser.parse_args(argv)
    if args.command == "build":
        unknown = set(args.stages) - set(STAGES)
        if unknown:
            parser.error(f"unknown stage: {', '.join(sorted(unknown))}")
        args.stages = args.stages or list(STAGES)
    lake = Lake(args.data)
    if args.command == "fetch":
        osf.fetch(lake.raw, args.athletes, args.max_mb, args.workers, log=log)
        return

    session = spark_session(args.master, args.driver_memory)
    session.sparkContext.setLogLevel("WARN")
    if args.command == "build":
        for stage in args.stages:
            started = time.monotonic()
            STAGES[stage](session, lake)
            log(f"{stage}: {time.monotonic() - started:.0f} s")
    elif args.command == "report":
        report.write(session, lake, args.out)
        log(f"wrote {args.out}")
    elif args.command == "benchmark":
        power_1hz = lake.read(session, "silver", "power_1hz")
        for method, seconds in report.benchmark(power_1hz, args.methods, args.repeat):
            log(f"{method}: {seconds:.1f} s")
