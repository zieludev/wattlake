"""The GoldenCheetah OpenData files on the Open Science Framework, project 6hfpz.

Every athlete is one zip at the top of the project's storage, named after the athlete's id.
OSF publishes a SHA-256 of each file, so a download is checked before it is kept. The listing
comes from OSF's file server, which returns all of the files in one response; the paged
listing of api.osf.io is slower and fails more often.
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import IO, Any, TypeVar

LISTING_URL = "https://files.osf.io/v1/resources/6hfpz/providers/osfstorage/"
INDEX_FILE = "index.json"
USER_AGENT = "wattlake (+https://github.com/zieludev/wattlake)"

Open = Callable[[str], IO[bytes]]
Log = Callable[[str], None]
T = TypeVar("T")


@dataclass(frozen=True)
class AthleteFile:
    name: str
    size: int
    sha256: str
    url: str


def http_open(url: str, timeout: float = 120) -> IO[bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(request, timeout=timeout)


def with_retries(action: Callable[[], T], attempts: int = 6, first_wait: float = 2) -> T:
    for attempt in range(attempts):
        try:
            return action()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt == attempts - 1 or not _transient(error):
                raise
            time.sleep(first_wait * 2**attempt)
    raise AssertionError("unreachable")


def _transient(error: Exception) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return error.code == 429 or error.code >= 500
    return True


def list_files(open_url: Open = http_open, log: Log = print) -> list[AthleteFile]:
    listing = with_retries(lambda: _read_json(open_url, LISTING_URL))
    files = list(_athlete_files(listing))
    log(f"OSF lists {len(files)} athletes")
    return files


def _read_json(open_url: Open, url: str) -> dict[str, Any]:
    with open_url(url) as response:
        return json.load(response)


def _athlete_files(listing: dict[str, Any]) -> Iterator[AthleteFile]:
    for item in listing["data"]:
        attributes = item["attributes"]
        if attributes["kind"] != "file" or not attributes["name"].endswith(".zip"):
            continue
        yield AthleteFile(
            name=attributes["name"],
            size=attributes["size"],
            sha256=attributes["extra"]["hashes"]["sha256"],
            url=item["links"]["download"],
        )


def choose(files: list[AthleteFile], athletes: int, max_mb: float | None) -> list[AthleteFile]:
    """The first athletes by id. Ids are random UUIDs, so this is a random sample that every
    run picks the same way."""
    fitting = [f for f in files if max_mb is None or f.size <= max_mb * 1_000_000]
    return sorted(fitting, key=lambda f: f.name)[:athletes]


def load_index(raw_dir: Path, open_url: Open = http_open, log: Log = print) -> list[AthleteFile]:
    index = raw_dir / INDEX_FILE
    if index.exists():
        return [AthleteFile(**entry) for entry in json.loads(index.read_text())]
    files = list_files(open_url, log)
    raw_dir.mkdir(parents=True, exist_ok=True)
    index.write_text(json.dumps([asdict(f) for f in files], indent=1))
    return files


def download(file: AthleteFile, raw_dir: Path, open_url: Open = http_open) -> bool:
    """Returns False when a verified copy was already there."""
    target = raw_dir / file.name
    if target.exists() and _sha256(target) == file.sha256:
        return False
    partial = target.with_suffix(".part")

    def attempt() -> None:
        with open_url(file.url) as response, partial.open("wb") as out:
            while chunk := response.read(1 << 20):
                out.write(chunk)

    with_retries(attempt)
    if (actual := _sha256(partial)) != file.sha256:
        partial.unlink()
        raise ValueError(f"{file.name}: SHA-256 is {actual}, OSF lists {file.sha256}")
    partial.rename(target)
    return True


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(
    raw_dir: Path,
    athletes: int,
    max_mb: float | None = None,
    workers: int = 4,
    open_url: Open = http_open,
    log: Log = print,
) -> list[Path]:
    chosen = choose(load_index(raw_dir, open_url, log), athletes, max_mb)
    total_mb = sum(f.size for f in chosen) / 1e6
    log(f"{len(chosen)} athletes, {total_mb:,.0f} MB")
    with ThreadPoolExecutor(workers) as pool:
        fresh = [
            job.result() for job in [pool.submit(download, f, raw_dir, open_url) for f in chosen]
        ]
    log(f"downloaded {sum(fresh)}, already had {len(fresh) - sum(fresh)}")
    return [raw_dir / f.name for f in chosen]
