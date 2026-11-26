"""Where each table lives, and the Spark session that reads and writes them."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Lake:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    def table(self, layer: str, name: str) -> str:
        return str((self.root / layer / name).resolve())

    def read(self, spark: SparkSession, layer: str, name: str) -> DataFrame:
        return spark.read.parquet(self.table(layer, name))

    def write(self, df: DataFrame, layer: str, name: str) -> None:
        df.write.mode("overwrite").parquet(self.table(layer, name))


def spark_session(master: str = "local[*]", driver_memory: str = "4g") -> SparkSession:
    return (
        SparkSession.builder.appName("wattlake")
        .master(master)
        # Only takes effect when this call starts the JVM, which is the case for the CLI.
        .config("spark.driver.memory", driver_memory)
        # Ride times are UTC in the data; a session in local time would shift them on read.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
