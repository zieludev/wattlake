"""Bronze: the athlete zips as two tables, unchanged apart from the format.

`samples` has one row per recorded sample of every activity file, `rides` one row per ride the
athlete's JSON describes, with GoldenCheetah's metrics kept as a JSON string. Each zip is
unpacked by one Python task on an executor, which reads it straight from disk: the raw
directory has to be on a filesystem every executor mounts.
"""

from __future__ import annotations

import json
import zipfile
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.csv as pacsv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wattlake.lake import Lake

SAMPLE_COLUMNS = ("secs", "km", "power", "hr", "cad", "alt")
# A reading has at most 6 significant digits, which float32 holds. The time stays float64:
# 1.26 s steps, hours into a ride, need more.
SAMPLES = pa.schema(
    [("athlete_id", pa.string()), ("activity", pa.string()), ("secs", pa.float64())]
    + [(name, pa.float32()) for name in SAMPLE_COLUMNS[1:]]
)
RIDES = pa.schema(
    [
        ("athlete_id", pa.string()),
        ("gender", pa.string()),
        ("yob", pa.string()),
        ("date", pa.string()),
        ("sport", pa.string()),
        ("data", pa.string()),
        ("metrics", pa.string()),
    ]
)
CSV_OPTIONS = pacsv.ConvertOptions(
    column_types=dict.fromkeys(SAMPLE_COLUMNS, pa.float64()),
    include_columns=list(SAMPLE_COLUMNS),
    include_missing_columns=True,
)


def build(spark: SparkSession, lake: Lake) -> None:
    zips = zip_files(spark, lake.raw)
    lake.write(zips.mapInArrow(read_samples, _spark_schema(SAMPLES)), "bronze", "samples")
    lake.write(zips.mapInArrow(read_rides, _spark_schema(RIDES)), "bronze", "rides")


def zip_files(spark: SparkSession, raw_dir: Path) -> DataFrame:
    """As many partitions as zips, to spread them over the tasks. Only the paths are read here:
    without the content column, the binaryFile source opens no file."""
    files = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.zip")
        .load(str(raw_dir.resolve()))
        .select("path")
    )
    return files.repartition(max(files.count(), 1), F.col("path"))


def read_samples(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        for url in batch.column("path").to_pylist():
            yield from samples_of(_local(url))


def samples_of(path: Path) -> Iterator[pa.RecordBatch]:
    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            if not member.endswith(".csv"):
                continue
            try:
                with archive.open(member) as f:
                    table = pacsv.read_csv(f, convert_options=CSV_OPTIONS)
            except (pa.ArrowInvalid, zipfile.BadZipFile, EOFError):
                continue
            ids = {"athlete_id": path.stem, "activity": Path(member).stem}
            for name, value in ids.items():
                table = table.append_column(name, pa.array([value] * table.num_rows))
            yield from table.select(SAMPLES.names).cast(SAMPLES).to_batches()


def read_rides(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        for url in batch.column("path").to_pylist():
            rows = list(rides_of(_local(url)))
            yield pa.RecordBatch.from_pylist(rows, schema=RIDES)


def rides_of(path: Path) -> Iterator[dict[str, str | None]]:
    with zipfile.ZipFile(path) as archive:
        member = next((n for n in archive.namelist() if n.endswith(".json")), None)
        if member is None:
            return
        try:
            doc = json.loads(archive.read(member))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
    athlete = doc.get("ATHLETE", {})
    for ride in doc.get("RIDES", []):
        yield {
            "athlete_id": path.stem,
            "gender": athlete.get("gender"),
            "yob": athlete.get("yob"),
            "date": ride.get("date"),
            "sport": ride.get("sport"),
            "data": ride.get("data"),
            "metrics": json.dumps(ride.get("METRICS", {})),
        }


def _local(url: str) -> Path:
    return Path(unquote(urlparse(url).path))


def _spark_schema(schema: pa.Schema) -> str:
    types = {pa.string(): "string", pa.float64(): "double", pa.float32(): "float"}
    return ", ".join(f"{field.name} {types[field.type]}" for field in schema)
