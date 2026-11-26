# Mean-maximal power, three ways

The mean-maximal power of a ride for a duration d is its best average over any d consecutive
seconds. Gold computes it for 15 durations, from 1 s to 60 min, for every ride, so it is the
heaviest computation in the pipeline. `src/wattlake/power.py` has three implementations that
give the same numbers (`tests/test_power.py` checks that they do):

| method | how |
| --- | --- |
| `frame` | `avg(power)` over a window frame of the last d rows, one frame per duration |
| `lag` | a running sum over the ride; the average of the last d seconds is the running sum minus its value d rows back (`lag`), divided by d |
| `pandas` | each ride's seconds go to numpy through `applyInPandas`, where the same running-sum difference runs over the whole ride at once |

## Results

`wattlake benchmark` reads `silver/power_1hz`, caches it, and computes the MMP of every ride
with each method into Spark's `noop` sink, which runs everything and stores nothing.

| data | seconds of riding | `frame` | `lag` | `pandas` |
| --- | --- | --- | --- | --- |
| 16 athletes | 11.5 million | 402.1 s | 2.7 s | 2.1 s |
| 146 athletes | 204 million | not run | 98.5 s | 71.5 s |

The time of `lag` and `pandas` is the best of three runs, the time of `frame` a single run.

The whole of gold's `ride_power` (MMP, NP, average power and work in one pass) the way the
pipeline runs it, from Parquet without a cache, alternating the two methods twice:

| | first round | second round |
| --- | --- | --- |
| `lag` | 115.4 s | 114.9 s |
| `pandas` | 86.6 s | 83.0 s |

The machine: a laptop with an Intel Core Ultra 7 155H (22 threads) and 30 GB of memory, Spark
in local mode with all threads, in Docker, on battery and with other work running. The absolute
times move with all of that; the order of the three methods and the ratios between them held
in every run.

## Why

**`frame` is about 150 times slower.** For a frame that slides with the current row, Spark
recomputes the aggregate over the whole frame for every row: it has no way to take the row
that leaves the frame out of an average. A frame of d rows costs d additions per row, and the
15 durations add up to 8,421. `lag` costs one subtraction per duration and row, whatever d is.

**`pandas` is about 1.35 times faster than `lag`.** Both do the same arithmetic. Spark
evaluates window functions row by row, while numpy runs the running sum and the differences of a
whole ride as loops in compiled code. Moving each ride to Python and back through Arrow costs
less than that difference. The pipeline therefore uses `pandas`.

`lag` has a case of its own: it is plain Spark SQL, so it needs no Python on the executors and
would run unchanged from Scala or SQL.

## Run it yourself

```bash
uv run wattlake benchmark                         # lag, frame and pandas, best of 3
uv run wattlake benchmark --methods lag pandas    # leave out frame on large samples
```
