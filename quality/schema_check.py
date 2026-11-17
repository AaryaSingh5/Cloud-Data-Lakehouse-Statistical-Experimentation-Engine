"""Schema validation and drift classification (4.2) and evolution policy (4.3).

Breaking drift blocks the batch: missing expected columns, unmapped renames,
value types or timestamp formats that do not parse in most rows.
Non-breaking drift is auto-merged with a warning: extra columns (under the
auto_merge policy) and renames listed in the registry aliases.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from quality.config import SchemaVersion, SourceSchema
from quality.parsing import parse_value

DRIFT_FRACTION = 0.5  # more than this share of present values failing to parse = drift


@dataclass
class SchemaReport:
    version: SchemaVersion
    schema_hash: str
    breaking: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)
    alias_mapped: dict[str, str] = field(default_factory=dict)
    mapping: dict[str, str] = field(default_factory=dict)  # observed name -> canonical name


def schema_hash(columns: list[str]) -> str:
    return hashlib.sha256(",".join(sorted(columns)).encode("utf-8")).hexdigest()[:12]


def _map_columns(version: SchemaVersion, observed: list[str]):
    mapping: dict[str, str] = {}
    alias_mapped: dict[str, str] = {}
    unknown: list[str] = []
    targets: set[str] = set()
    for col in observed:
        if col in version.columns:
            canon: str | None = col
        elif version.aliases.get(col) in version.columns:
            canon = version.aliases[col]
        else:
            canon = None
        if canon is None or canon in targets:
            unknown.append(col)
            continue
        targets.add(canon)
        mapping[col] = canon
        if canon != col:
            alias_mapped[col] = canon
    return mapping, alias_mapped, unknown


def value_drift(
    version: SchemaVersion,
    mapping: dict[str, str],
    payloads: list[dict | None],
    ts_format: str,
) -> list[str]:
    reasons: list[str] = []
    inverse = {canon: obs for obs, canon in mapping.items()}
    for canon, vtype in version.columns.items():
        if vtype not in ("int", "float", "timestamp"):
            continue
        obs = inverse.get(canon)
        if obs is None:
            continue  # missing column is already reported
        total = fails = 0
        for p in payloads:
            if p is None or p.get(obs) is None:
                continue
            total += 1
            ok, _ = parse_value(vtype, p[obs], ts_format)
            if not ok:
                fails += 1
        if total and fails / total > DRIFT_FRACTION:
            kind = "format_drift" if vtype == "timestamp" else "type_drift"
            reasons.append(f"{kind}:{canon}")
    return reasons


def check_schema(
    schema: SourceSchema,
    observed: list[str],
    payloads: list[dict | None],
    ts_format: str,
    unknown_policy: str,
) -> SchemaReport:
    """Pick the best-matching registered version and classify drift against it."""
    best = None
    for v in schema.versions:
        mapping, alias_mapped, unknown = _map_columns(v, observed)
        present = set(mapping.values())
        breaking = [f"missing:{c}" for c in v.columns if c not in present]
        if unknown_policy == "quarantine":
            breaking += [f"unknown:{c}" for c in unknown]
        candidate = (len(breaking), -v.version, v, mapping, alias_mapped, unknown, breaking)
        if best is None or candidate[:2] < best[:2]:
            best = candidate

    _, _, v, mapping, alias_mapped, unknown, breaking = best
    breaking = breaking + value_drift(v, mapping, payloads, ts_format)
    return SchemaReport(
        version=v,
        schema_hash=schema_hash(observed),
        breaking=breaking,
        unknown=unknown,
        alias_mapped=alias_mapped,
        mapping=mapping,
    )