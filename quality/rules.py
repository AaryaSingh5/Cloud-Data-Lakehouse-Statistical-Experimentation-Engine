"""Row-level rules: unparseable records, nulls, invalid values, duplicates (4.4, 4.5, 4.6)."""
from __future__ import annotations

import json
import re
from collections import defaultdict

from quality.config import SchemaVersion
from quality.parsing import parse_value
from quality.types import Finding

ROW_RULES = (
    "unparseable_record",
    "null_value",
    "invalid_value",
    "duplicate_exact",
    "duplicate_key_conflict",
)


def check_unparseable(payloads: list[dict | None]) -> list[Finding]:
    return [
        Finding(i, "unparseable_record", None, "record is not a JSON object")
        for i, p in enumerate(payloads)
        if p is None
    ]


def check_nulls(version: SchemaVersion, payloads: list[dict | None]) -> list[Finding]:
    out: list[Finding] = []
    for i, p in enumerate(payloads):
        if p is None:
            continue
        for col in version.columns:
            if p.get(col) is None:
                out.append(Finding(i, "null_value", col, "missing or null"))
    return out


def check_values(
    version: SchemaVersion,
    payloads: list[dict | None],
    validity: dict[str, dict],
    ts_format: str,
) -> list[Finding]:
    out: list[Finding] = []
    for i, p in enumerate(payloads):
        if p is None:
            continue
        for col, vtype in version.columns.items():
            val = p.get(col)
            if val is None:
                continue
            ok, parsed = parse_value(vtype, val, ts_format)
            if not ok:
                out.append(Finding(i, "invalid_value", col, f"not a valid {vtype}: {val!r}"))
                continue
            rule = validity.get(col, {})
            if "min" in rule and parsed < rule["min"]:
                out.append(Finding(i, "invalid_value", col, f"{parsed} below min {rule['min']}"))
            elif "max" in rule and parsed > rule["max"]:
                out.append(Finding(i, "invalid_value", col, f"{parsed} above max {rule['max']}"))
            elif "regex" in rule and not re.fullmatch(rule["regex"], str(val)):
                out.append(Finding(i, "invalid_value", col, f"{val!r} does not match {rule['regex']}"))
            elif "allowed" in rule and str(val) not in rule["allowed"]:
                out.append(Finding(i, "invalid_value", col, f"{val!r} not in allowed set"))
    return out


def check_duplicates(key: str, payloads: list[dict | None]) -> list[Finding]:
    groups: dict[object, list[int]] = defaultdict(list)
    for i, p in enumerate(payloads):
        if p is None or p.get(key) is None:
            continue
        groups[p[key]].append(i)

    out: list[Finding] = []
    for k, idxs in groups.items():
        if len(idxs) < 2:
            continue
        signatures = {json.dumps(payloads[i], sort_keys=True, default=str) for i in idxs}
        if len(signatures) == 1:
            # exact copies: keep the first occurrence, flag the rest
            for i in idxs[1:]:
                out.append(Finding(i, "duplicate_exact", key, f"exact copy of row {idxs[0]}"))
        else:
            # conflicting values for one key: flag every occurrence, nothing is safe to keep
            for i in idxs:
                out.append(
                    Finding(i, "duplicate_key_conflict", key,
                            f"key {k!r} appears {len(idxs)} times with different values")
                )
    return out