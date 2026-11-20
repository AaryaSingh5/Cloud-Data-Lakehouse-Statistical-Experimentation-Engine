"""Ground truth for the benchmark: which batches were faulty, and what Gold should be.

Gold truth comes from the fault-free copies in data/clean, so it is exact.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from quality.config import Registry
from transform.conform import CONFORM

# Timing faults are not data faults. The platform is not expected to detect them.
TIMING_FAULTS = frozenset({"late_arriving"})


def faulty_batches(ground_truth_csv: Path) -> set[tuple[str, date]]:
    out: set[tuple[str, date]] = set()
    with ground_truth_csv.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["fault_type"] not in TIMING_FAULTS:
                out.add((row["source"], date.fromisoformat(row["batch_date"])))
    return out


def clean_batches(clean_root: Path) -> set[tuple[str, date]]:
    return {
        (path.parent.name, date.fromisoformat(path.stem))
        for path in clean_root.glob("*/*")
        if path.is_file()
    }


def _records(path: Path):
    if path.suffix == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as fh:
            yield from csv.DictReader(fh)
    else:
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip():
                yield json.loads(line)


def clean_gold(clean_root: Path, registry: Registry) -> dict[date, tuple[int, float]]:
    """Orders and revenue per sale date, from fault-free data."""
    totals: dict[date, list] = defaultdict(lambda: [0, 0.0])
    for path in sorted(clean_root.glob("*/*")):
        source = path.parent.name
        if not path.is_file() or source not in CONFORM:
            continue
        f = CONFORM[source]
        for rec in _records(path):
            when = datetime.strptime(str(rec[f["order_ts"]]), registry.ts_format).date()
            revenue = round(int(str(rec[f["quantity"]])) * float(str(rec[f["unit_price"]])), 2)
            totals[when][0] += 1
            totals[when][1] += revenue
    return {d: (n, round(rev, 2)) for d, (n, rev) in totals.items()}