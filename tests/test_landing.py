from datetime import date
from pathlib import Path

import pytest

from ingestion.landing import discover, parse_landing_path, sha256_file


def _touch(root: Path, source: str, delivered: str, name: str, content: str = "x") -> Path:
    d = root / source / delivered
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text(content)
    return p


def test_parse_valid_path(tmp_path):
    p = _touch(tmp_path, "pos", "2024-03-02", "pos_2024-03-01.csv")
    lf = parse_landing_path(p)
    assert lf.source == "pos"
    assert lf.fmt == "csv"
    assert lf.delivered_date == date(2024, 3, 2)
    assert lf.batch_date == date(2024, 3, 1)


def test_rejects_source_directory_mismatch(tmp_path):
    p = _touch(tmp_path, "web", "2024-03-02", "pos_2024-03-01.csv")
    with pytest.raises(ValueError, match="does not match directory"):
        parse_landing_path(p)


def test_rejects_bad_filename(tmp_path):
    p = _touch(tmp_path, "pos", "2024-03-02", "pos-2024-03-01.csv")
    with pytest.raises(ValueError, match="does not match contract"):
        parse_landing_path(p)


def test_rejects_unknown_source(tmp_path):
    p = _touch(tmp_path, "mystery", "2024-03-02", "mystery_2024-03-01.csv")
    with pytest.raises(ValueError, match="unknown source"):
        parse_landing_path(p)


def test_rejects_invalid_date(tmp_path):
    p = _touch(tmp_path, "pos", "2024-02-30", "pos_2024-03-01.csv")
    with pytest.raises(ValueError, match="invalid date"):
        parse_landing_path(p)


def test_discover_skips_invalid_and_sorts(tmp_path):
    _touch(tmp_path, "pos", "2024-03-03", "pos_2024-03-02.csv")
    _touch(tmp_path, "pos", "2024-03-02", "pos_2024-03-01.csv")
    _touch(tmp_path, "web", "2024-03-02", "garbage.jsonl")
    found = discover(tmp_path)
    assert [f.batch_date for f in found] == [date(2024, 3, 1), date(2024, 3, 2)]


def test_discover_filters_by_source(tmp_path):
    _touch(tmp_path, "pos", "2024-03-02", "pos_2024-03-01.csv")
    _touch(tmp_path, "web", "2024-03-02", "web_2024-03-01.jsonl")
    assert {f.source for f in discover(tmp_path, source="web")} == {"web"}


def test_discover_missing_root_returns_empty(tmp_path):
    assert discover(tmp_path / "nope") == []


def test_sha256_matches_hashlib(tmp_path):
    import hashlib

    p = tmp_path / "f.bin"
    p.write_bytes(b"hello landing zone")
    assert sha256_file(p) == hashlib.sha256(b"hello landing zone").hexdigest()