"""Landing-zone discovery and file hashing.

Data contract:
    <landing_root>/<source>/<delivered_date>/<source>_<batch_date>.<csv|jsonl>
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

KNOWN_SOURCES = frozenset({"pos", "web", "partner"})
FILE_PATTERN = re.compile(
    r"^(?P<source>[a-z][a-z0-9_]*)_(?P<batch_date>\d{4}-\d{2}-\d{2})\.(?P<fmt>csv|jsonl)$"
)


@dataclass(frozen=True)
class LandingFile:
    path: Path
    source: str
    delivered_date: date
    batch_date: date
    fmt: str  # "csv" or "jsonl"


def parse_landing_path(path: Path) -> LandingFile:
    """Parse and validate a landing path. Raises ValueError if it breaks the contract."""
    match = FILE_PATTERN.match(path.name)
    if not match:
        raise ValueError(f"filename does not match contract: {path.name}")

    source = match.group("source")
    if source not in KNOWN_SOURCES:
        raise ValueError(f"unknown source '{source}' in {path}")

    source_dir = path.parent.parent.name
    if source_dir != source:
        raise ValueError(f"file source '{source}' does not match directory '{source_dir}'")

    try:
        delivered = date.fromisoformat(path.parent.name)
        batch = date.fromisoformat(match.group("batch_date"))
    except ValueError as exc:
        raise ValueError(f"invalid date in {path}: {exc}") from exc

    return LandingFile(
        path=path,
        source=source,
        delivered_date=delivered,
        batch_date=batch,
        fmt=match.group("fmt"),
    )


def discover(root: Path, source: str | None = None) -> list[LandingFile]:
    """Find all valid landing files, optionally for one source. Invalid files are logged and skipped."""
    if not root.exists():
        return []

    found: list[LandingFile] = []
    for path in root.glob("*/*/*"):
        if not path.is_file():
            continue
        try:
            lf = parse_landing_path(path)
        except ValueError as exc:
            logger.warning("skipping invalid landing file: %s", exc)
            continue
        if source is None or lf.source == source:
            found.append(lf)

    return sorted(found, key=lambda f: (f.batch_date, f.delivered_date, f.source))


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()