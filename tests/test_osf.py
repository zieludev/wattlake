from __future__ import annotations

import hashlib
import io
import json
import urllib.error
from collections.abc import Callable
from email.message import Message
from pathlib import Path
from typing import IO

import pytest

from wattlake import osf


def listing(*files: tuple[str, bytes]) -> bytes:
    data: list[dict[str, object]] = [
        {"attributes": {"kind": "folder", "name": "INDEX", "size": None, "extra": {}}, "links": {}}
    ]
    for name, content in files:
        data.append(
            {
                "attributes": {
                    "kind": "file",
                    "name": name,
                    "size": len(content),
                    "extra": {"hashes": {"sha256": hashlib.sha256(content).hexdigest()}},
                },
                "links": {"download": f"https://files.example/{name}"},
            }
        )
    return json.dumps({"data": data}).encode()


class Server:
    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.requests: list[str] = []

    def __call__(self, url: str) -> IO[bytes]:
        self.requests.append(url)
        return io.BytesIO(self.responses[url])


def test_the_listing_has_the_athlete_zips_and_nothing_else() -> None:
    server = Server({osf.LISTING_URL: listing(("b.zip", b"bb"), ("a.zip", b"a"))})

    files = osf.list_files(server, log=lambda _: None)

    assert [(f.name, f.size, f.url) for f in files] == [
        ("b.zip", 2, "https://files.example/b.zip"),
        ("a.zip", 1, "https://files.example/a.zip"),
    ]


def test_the_sample_is_the_first_athletes_by_id_that_fit() -> None:
    files = [
        osf.AthleteFile(name, size, "", "")
        for name, size in (("c", 1), ("a", 5_000_000), ("b", 1), ("d", 1))
    ]

    assert [f.name for f in osf.choose(files, 2, max_mb=None)] == ["a", "b"]
    assert [f.name for f in osf.choose(files, 2, max_mb=1)] == ["b", "c"]


def test_fetch_downloads_checks_and_keeps(tmp_path: Path) -> None:
    server = Server({osf.LISTING_URL: listing(("a.zip", b"zip a"))})
    server.responses["https://files.example/a.zip"] = b"zip a"

    osf.fetch(tmp_path, athletes=1, open_url=server, log=lambda _: None)
    osf.fetch(tmp_path, athletes=1, open_url=server, log=lambda _: None)

    assert (tmp_path / "a.zip").read_bytes() == b"zip a"
    # The listing is kept in index.json and a verified zip is not fetched again.
    assert server.requests == [osf.LISTING_URL, "https://files.example/a.zip"]


def test_a_download_that_does_not_match_its_hash_is_thrown_away(tmp_path: Path) -> None:
    file = osf.AthleteFile(
        "a.zip", 5, hashlib.sha256(b"zip a").hexdigest(), "https://files.example/a.zip"
    )
    server = Server({file.url: b"zip b"})

    with pytest.raises(ValueError, match="SHA-256"):
        osf.download(file, tmp_path, server)

    assert list(tmp_path.iterdir()) == []


def failing(errors: list[Exception]) -> Callable[[], str]:
    def action() -> str:
        if errors:
            raise errors.pop(0)
        return "ok"

    return action


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "", Message(), None)


def test_server_errors_are_tried_again_and_client_errors_are_not() -> None:
    assert osf.with_retries(failing([http_error(500), TimeoutError()]), first_wait=0) == "ok"

    with pytest.raises(urllib.error.HTTPError):
        osf.with_retries(failing([http_error(404)]), first_wait=0)
