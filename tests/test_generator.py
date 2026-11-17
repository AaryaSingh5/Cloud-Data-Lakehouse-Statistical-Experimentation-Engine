import csv
import filecmp
import json
from datetime import date

import pytest

from data_generator.generate import generate
from data_generator.profiles import ALL_FAULTS


def _rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_no_faults_landing_equals_clean(tmp_path):
    generate(tmp_path, date(2025, 1, 1), 3, 50, 1, "none")
    assert _rows(tmp_path / "ground_truth" / "ground_truth.csv") == []
    for b in _rows(tmp_path / "ground_truth" / "batches.csv"):
        clean = tmp_path / "clean" / b["source"] / f"{b['batch_date']}.{'jsonl' if b['source'] == 'web' else 'csv'}"
        assert filecmp.cmp(clean, tmp_path / b["file_path"], shallow=False)
        assert b["delivered_date"] == b["batch_date"]


def test_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, date(2025, 1, 1), 5, 80, 7, "high")
    generate(b, date(2025, 1, 1), 5, 80, 7, "high")
    for name in ("ground_truth.csv", "batches.csv"):
        assert (a / "ground_truth" / name).read_text() == (b / "ground_truth" / name).read_text()
    assert (a / "ground_truth" / "ground_truth.csv").read_text() != ""


def test_high_profile_covers_all_fault_types(tmp_path):
    generate(tmp_path, date(2025, 1, 1), 60, 100, 3, "high")
    seen = {r["fault_type"] for r in _rows(tmp_path / "ground_truth" / "ground_truth.csv")}
    assert seen == set(ALL_FAULTS)


def test_duplicate_faults_produce_duplicate_keys(tmp_path):
    generate(tmp_path, date(2025, 1, 1), 60, 100, 3, "high")
    gt = _rows(tmp_path / "ground_truth" / "ground_truth.csv")
    ev = next(r for r in gt if r["fault_type"] == "duplicate_exact" and r["source"] == "pos")
    keys = json.loads(ev["row_keys"])
    path = tmp_path / "landing" / "pos" / ev["delivered_date"] / f"pos_{ev['batch_date']}.csv"
    lines = path.read_text().splitlines()[1:]
    for k in keys:
        assert sum(1 for ln in lines if ln.startswith(k + ",")) == 2


def test_late_files_land_in_later_folder(tmp_path):
    generate(tmp_path, date(2025, 1, 1), 60, 50, 3, "high")
    late = [b for b in _rows(tmp_path / "ground_truth" / "batches.csv") if b["delivered_date"] != b["batch_date"]]
    assert late and all((tmp_path / b["file_path"]).exists() for b in late)


def test_empty_file_has_no_data_rows(tmp_path):
    generate(tmp_path, date(2025, 1, 1), 60, 50, 3, "high")
    ev = next(r for r in _rows(tmp_path / "ground_truth" / "ground_truth.csv") if r["fault_type"] == "empty_file")
    b = next(x for x in _rows(tmp_path / "ground_truth" / "batches.csv") if x["batch_id"] == ev["batch_id"])
    assert b["rows_delivered"] == "0"
    assert len((tmp_path / b["file_path"]).read_text().splitlines()) <= 1


def test_unknown_profile_rejected(tmp_path):
    with pytest.raises(KeyError):
        generate(tmp_path, date(2025, 1, 1), 1, 10, 1, "nope")
