# The data

[GoldenCheetah](https://www.goldencheetah.org/) is a desktop application for cyclists and
triathletes. Since 2018 its users can share their whole history, anonymised, with the
[OpenData project](https://github.com/GoldenCheetah/OpenData), which publishes it on the Open
Science Framework as [project 6hfpz](https://osf.io/6hfpz/) under CC0 1.0.

The numbers below are from the copy on OSF as `wattlake fetch` found it, and from the first 150
athletes with zips of up to 150 MB.

## What is in it

| | |
| --- | --- |
| athletes | 6,613, one zip each |
| size | 115.6 GB of zips; the median zip 8.5 MB, the largest 264 MB, 23 over 150 MB |
| one zip | `{athlete id}.json` and one CSV per recorded activity |
| one CSV | `secs,km,power,hr,cad,alt`, one row per sample |
| the JSON | gender, year of birth, and per ride: start time, sport, which channels were recorded, and some 150 metrics that GoldenCheetah computed |

Listing the files through the OSF API (`api.osf.io`) pages a hundred at a time, and while this
was written it answered every request with HTTP 500 for a while. OSF's file server (`files.osf.io`) returns all 6,614 entries
(the zips and an `INDEX` folder) in one response, with a SHA-256 for each file, so `fetch`
lists from there and checks every download against its hash.

The S3 bucket that the OpenData README and its Python library point to answers 403.

## What it took to read it

**File names are local time, the JSON is UTC.** The CSV of a ride that the JSON says started at
`2017/05/05 12:56:28 UTC` is called `2017_05_05_14_56_28.csv`: the rider was in UTC+2. Nothing
says which zone a file is in, and riders travel and change to summer time. A file and a ride
belong together when they differ by a whole number of quarter hours, up to 14 hours. When that
leaves a file with more than one ride, the offset the athlete has most often wins. In the 150
athletes there are 18 different offsets, and 57,729 of 57,807 files (99.9 %) find their ride.

**Some rides are recorded twice.** The athlete in `tests/data` has pairs of activities two
seconds apart, one with heart rate and no power, the other with power: most likely two devices
on the same ride. They are separate rides in the JSON, with separate files, and quarter-hour
matching keeps them apart.

**Not every head unit records once a second.** Of 305 million samples, 97 % are 1 s apart.
Others are 1.26 s, 0.5 s or 2 s apart, and a pause leaves a gap. Silver puts every ride on a
1 s grid: each second takes the last reading in it, a second without one keeps the reading
before it for up to 5 seconds, and a longer gap counts as stopped, at 0 W.

**The sport is often empty.** The labels, lower-cased:

| sport | rides |
| --- | --- |
| bike | 42,421 |
| run | 7,389 |
| (empty) | 4,469 |
| swim | 2,479 |
| virtualride | 1,449 |
| workout, walk, weighttraining, hike, 0, nordicski, other and more | under 400 each |

An empty sport with a power channel is nearly always a ride, but runners with a power meter on
their shoe exist. An empty-sport activity counts as cycling when it has power and its average
speed is 0 (a trainer without a speed sensor) or at least 15 km/h. Of the 684 runs with power
in the 150 athletes, 98.8 % are slower than that.

**Power meters spike.** Readings above 2,500 W are dropped, and a ride whose best power per
kilogram beats what anyone has done (30 W/kg for a second, 7 W/kg for 20 minutes, and so on)
is left out of every best, because a meter went wrong or the weight is wrong.

**The year of birth is not usable.** 17 of the 146 athletes were born after 2010 by it, some in
the 2060s and 2080s. The results do not split by age.

**Few women.** 143 of the 146 athletes are men, so the results show women separately only once
a group has ten riders.

**And the odd one.** One file in the 150 athletes is called `2015_01_20_18_50_00.csv.bak.csv`.
Spark 4 runs SQL in ANSI mode, where a value that does not parse stops the whole job, so
silver parses names, times and metrics with `try_to_timestamp` and `try_cast` and leaves out
what does not parse.

Four of the 150 zips hold only a JSON with an empty list of rides.

## GoldenCheetah's own numbers

Each ride in the JSON comes with GoldenCheetah's metrics, computed by its own code from the same
file. Three of them are used here:

- `athlete_weight`, the only source of the rider's weight, for watts per kilogram;
- `power_mmp` and `power_mmp_secs`, the ride's mean-maximal power curve;
- `coggan_np`, its Normalized Power.

The last two are never used to compute anything. The results page compares them with what
wattlake computes from the raw samples, which checks the whole path from zip to gold table
against independent code.
