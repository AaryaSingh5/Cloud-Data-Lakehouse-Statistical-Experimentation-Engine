"""Read a landing file into (raw_payload, line_no) pairs. Pure Python, no Spark.

CSV: raw_payload is a JSON object for the row. Empty values and missing trailing
fields are omitted, the same way nulls were omitted before. The header is line 1,
so the first data row is line 2.

JSONL: raw_payload is the line text, valid or not. Blank lines are skipped
but keep their line numbers.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path


def landing_rows(path: Path, fmt: str) -> list[tuple[int, str]]:
    lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
    if fmt == "jsonl":
        return [(n, line) for n, line in enumerate(lines, start=1) if line.strip()]
    if fmt != "csv":
        raise ValueError(f"unsupported format: {fmt}")
    if not lines:
        return []
    header = next(csv.reader([lines[0]]), [])
    out: list[tuple[int, str]] = []
    for n, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        fields = next(csv.reader([line]), [])
        rec = {
            col: fields[i]
            for i, col in enumerate(header)
            if i < len(fields) and fields[i] != ""
        }
        out.append((n, json.dumps(rec)))
    return out
    