"""The two charts of the results page, as SVG, one file per colour scheme.

GitHub shows a README image without running anything in it, so each chart is drawn twice and
the page picks one with `<picture>`. The values of both charts are also in tables on the page.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from html import escape

WIDTH, HEIGHT = 760, 420
LEFT, RIGHT, TOP, BOTTOM = 56, 132, 72, 44
FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif"


@dataclass(frozen=True)
class Theme:
    surface: str
    ink: str
    ink_muted: str
    grid: str
    axis: str
    series: tuple[str, str, str]


THEMES = {
    "light": Theme(
        "#ffffff", "#191919", "#666666", "#e7e7e7", "#c2c2c2", ("#257de0", "#f47230", "#00b894")
    ),
    "dark": Theme(
        "#191919", "#efefef", "#9c9c9c", "#303030", "#474747", ("#338ced", "#e76624", "#00a888")
    ),
}


@dataclass(frozen=True)
class Band:
    duration_s: int
    p10: float
    p25: float
    p50: float
    p75: float
    p90: float


@dataclass(frozen=True)
class Day:
    date: date
    ctl: float
    atl: float
    tsb: float


class Plot:
    def __init__(self, theme: Theme, title: str, subtitle: str) -> None:
        self.theme = theme
        self.parts: list[str] = [
            f'<rect width="{WIDTH}" height="{HEIGHT}" rx="8" fill="{theme.surface}"/>',
            self.text(LEFT, 30, title, size=16, weight=600),
            self.text(LEFT, 50, subtitle, size=12, color=theme.ink_muted),
        ]

    def text(
        self,
        x: float,
        y: float,
        content: str,
        size: int = 11,
        color: str | None = None,
        weight: int = 400,
        anchor: str = "start",
    ) -> str:
        return (
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
            f'fill="{color or self.theme.ink}" text-anchor="{anchor}">{escape(content)}</text>'
        )

    def add(self, *parts: str) -> None:
        self.parts.extend(parts)

    def svg(self, label: str) -> str:
        body = "\n  ".join(self.parts)
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
            f'viewBox="0 0 {WIDTH} {HEIGHT}" font-family="{FONT}" role="img" '
            f'aria-label="{escape(label)}">\n  {body}\n</svg>\n'
        )


def _line(x1: float, y1: float, x2: float, y2: float, color: str) -> str:
    return (
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
        f'stroke="{color}" stroke-width="1"/>'
    )


def _path(points: Sequence[tuple[float, float]]) -> str:
    return "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in points)


def _nice_step(span: float, ticks: int = 5) -> float:
    raw = span / ticks
    magnitude = 10 ** math.floor(math.log10(raw))
    return next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw)


def _y_axis(plot: Plot, low: float, high: float, step: float, y_of: _Scale) -> None:
    value = math.ceil(low / step) * step
    while value <= high + 1e-9:
        y = y_of(value)
        color = plot.theme.axis if abs(value) < 1e-9 else plot.theme.grid
        plot.add(
            _line(LEFT, y, WIDTH - RIGHT, y, color),
            plot.text(LEFT - 8, y + 4, f"{value:g}", color=plot.theme.ink_muted, anchor="end"),
        )
        value += step


class _Scale:
    def __init__(self, domain: tuple[float, float], pixels: tuple[float, float]) -> None:
        self.domain, self.pixels = domain, pixels

    def __call__(self, value: float) -> float:
        (d0, d1), (p0, p1) = self.domain, self.pixels
        return p0 + (value - d0) / (d1 - d0) * (p1 - p0)


DURATION_TICKS = {
    1: "1 s",
    5: "5 s",
    15: "15 s",
    60: "1 min",
    300: "5 min",
    1200: "20 min",
    3600: "60 min",
}


def power_profile(bands: Sequence[Band], riders: int, theme: Theme) -> str:
    plot = Plot(
        theme,
        "Best power per kilogram, by how long the effort lasts",
        f"{riders:,} riders, best season each · line: median, bands: middle half and middle 80 %",
    )
    bands = sorted(bands, key=lambda b: b.duration_s)
    highest = max(b.p90 for b in bands)
    step = _nice_step(highest)
    top = math.ceil(highest / step) * step
    x_of = _Scale(
        (math.log10(bands[0].duration_s), math.log10(bands[-1].duration_s)), (LEFT, WIDTH - RIGHT)
    )
    y_of = _Scale((0, top), (HEIGHT - BOTTOM, TOP))

    _y_axis(plot, 0, top, step, y_of)
    plot.add(plot.text(LEFT - 8, TOP - 14, "W/kg", color=theme.ink_muted, anchor="end"))
    for seconds, label in DURATION_TICKS.items():
        if bands[0].duration_s <= seconds <= bands[-1].duration_s:
            x = x_of(math.log10(seconds))
            plot.add(
                plot.text(x, HEIGHT - BOTTOM + 18, label, color=theme.ink_muted, anchor="middle")
            )

    def points(values: Sequence[float]) -> list[tuple[float, float]]:
        return [
            (x_of(math.log10(b.duration_s)), y_of(v)) for b, v in zip(bands, values, strict=True)
        ]

    blue = theme.series[0]
    for low, high, opacity in (("p10", "p90", 0.10), ("p25", "p75", 0.18)):
        upper = points([getattr(b, high) for b in bands])
        lower = points([getattr(b, low) for b in bands])[::-1]
        plot.add(f'<path d="{_path(upper + lower)} Z" fill="{blue}" fill-opacity="{opacity}"/>')
    median = points([b.p50 for b in bands])
    plot.add(
        f'<path d="{_path(median)}" fill="none" stroke="{blue}" stroke-width="2" '
        'stroke-linejoin="round" stroke-linecap="round"/>'
    )
    end_x, end_y = median[-1]
    plot.add(
        f'<circle cx="{end_x:.1f}" cy="{end_y:.1f}" r="4" fill="{blue}" '
        f'stroke="{theme.surface}" stroke-width="2"/>'
    )

    last = bands[-1]
    labels = [
        (y_of(v), f"{name} {v:.1f}")
        for v, name in ((last.p90, "top 10 %"), (last.p50, "median"), (last.p10, "bottom 10 %"))
    ]
    _end_labels(plot, [(y, text, None) for y, text in labels])
    return plot.svg(
        "Percentiles of the riders' best power per kilogram, for efforts from 1 s to "
        + DURATION_TICKS.get(last.duration_s, f"{last.duration_s} s")
    )


def _end_labels(plot: Plot, labels: Sequence[tuple[float, str, str | None]]) -> None:
    """Labels at the right end of the lines, level with their points where they fit, pushed
    apart where they would overlap, with a leader back to the point. A colour adds a key."""
    placed: list[float] = []
    for y, text, color in sorted(labels):
        label_y = max(y, placed[-1] + 16) if placed else y
        placed.append(label_y)
        x = WIDTH - RIGHT + 12
        plot.add(_line(WIDTH - RIGHT + 6, y, x - 2, label_y, plot.theme.axis))
        if color is not None:
            plot.add(
                f'<rect x="{x:.1f}" y="{label_y - 5:.1f}" width="10" height="10" rx="2" '
                f'fill="{color}"/>'
            )
            x += 16
        plot.add(plot.text(x, label_y + 4, text, color=plot.theme.ink_muted))


def training_load(days: Sequence[Day], theme: Theme) -> str:
    plot = Plot(
        theme,
        "Fitness, fatigue and form of one rider through a season",
        "CTL and ATL: TSS per day, averaged over 42 and 7 days · TSB = CTL - ATL",
    )
    days = sorted(days, key=lambda d: d.date)
    low = min(0.0, *(d.tsb for d in days))
    high = max(max(d.ctl, d.atl) for d in days)
    step = _nice_step(high - low)
    low, high = math.floor(low / step) * step, math.ceil(high / step) * step
    first = days[0].date.toordinal()
    x_of = _Scale((first, days[-1].date.toordinal()), (LEFT, WIDTH - RIGHT))
    y_of = _Scale((low, high), (HEIGHT - BOTTOM, TOP))

    _y_axis(plot, low, high, step, y_of)
    for d in days:
        if d.date.day == 1:
            plot.add(
                plot.text(
                    x_of(d.date.toordinal()),
                    HEIGHT - BOTTOM + 18,
                    d.date.strftime("%b"),
                    color=theme.ink_muted,
                    anchor="middle",
                )
            )

    names = ("CTL fitness", "ATL fatigue", "TSB form")
    ends: list[tuple[float, str, str | None]] = []
    for name, color, field in zip(names, theme.series, ("ctl", "atl", "tsb"), strict=True):
        line = [(x_of(d.date.toordinal()), y_of(getattr(d, field))) for d in days]
        plot.add(
            f'<path d="{_path(line)}" fill="none" stroke="{color}" stroke-width="2" '
            'stroke-linejoin="round" stroke-linecap="round"/>',
            f'<circle cx="{line[-1][0]:.1f}" cy="{line[-1][1]:.1f}" r="4" fill="{color}" '
            f'stroke="{theme.surface}" stroke-width="2"/>',
        )
        ends.append((line[-1][1], name, color))

    _end_labels(plot, ends)
    return plot.svg("Daily CTL, ATL and TSB of one rider over a season")
