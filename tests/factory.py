"""Athlete zips in the layout of the GoldenCheetah OpenData set, made up for the tests."""

from __future__ import annotations

import json
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

REAL_ATHLETE = Path(__file__).parent / "data" / "69215d1d-cd66-4c5a-8ffe-e95f7b29faa6.zip"


@dataclass
class Ride:
    local_start: datetime
    power: Sequence[float | None]
    utc_offset_h: float = 2
    sport: str = "Bike"
    secs: Sequence[float] | None = None
    weight_kg: float = 75
    metrics: dict[str, object] = field(default_factory=dict[str, object])
    has_file: bool = True

    @property
    def activity(self) -> str:
        return self.local_start.strftime("%Y_%m_%d_%H_%M_%S")


def athlete_zip(raw_dir: Path, athlete_id: str, rides: Sequence[Ride], gender: str = "M") -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{athlete_id}.zip"
    doc = {
        "VERSION": "1.8",
        "ATHLETE": {"gender": gender, "yob": "1980", "id": f"{{{athlete_id}}}"},
        "RIDES": [_ride_json(ride) for ride in rides],
    }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{{{athlete_id}}}.json", json.dumps(doc))
        for ride in rides:
            if ride.has_file:
                archive.writestr(f"{ride.activity}.csv", _csv(ride))
    return path


def _ride_json(ride: Ride) -> dict[str, object]:
    utc = ride.local_start - timedelta(hours=ride.utc_offset_h)
    has_power = any(p is not None for p in ride.power)
    return {
        "date": utc.strftime("%Y/%m/%d %H:%M:%S UTC"),
        "data": "TDS" + ("P" if has_power else "-") + "-C---------",
        "sport": ride.sport,
        "METRICS": {"athlete_weight": f"{ride.weight_kg:.5f}", **ride.metrics},
    }


def _csv(ride: Ride) -> str:
    secs = ride.secs if ride.secs is not None else range(len(ride.power))
    lines = ["secs,km,power,hr,cad,alt"]
    for s, p in zip(secs, ride.power, strict=True):
        lines.append(f"{s:g},{s * 0.008:g},{'' if p is None else f'{p:g}'},140,90,")
    return "\n".join(lines) + "\n"


def constant(watts: float, seconds: int) -> list[float | None]:
    return [watts] * seconds


def blocks(*parts: tuple[float, int]) -> list[float | None]:
    """Power as consecutive blocks of (watts, seconds)."""
    return [watts for watts, seconds in parts for _ in range(seconds)]
