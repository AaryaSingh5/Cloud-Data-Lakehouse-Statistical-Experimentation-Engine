"""Plain data types passed between quality modules."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class RawRecord:
    index: int
    payload: dict | None      # None = not a JSON object (unparseable)


@dataclass(frozen=True)
class BatchInput:
    source: str
    batch_id: str | None
    batch_date: date
    observed_columns: list[str]
    records: list[RawRecord]


@dataclass(frozen=True)
class BatchMetrics:
    row_count: int
    mean_unit_price: float | None = None
    mean_quantity: float | None = None


@dataclass(frozen=True)
class RuleOutcome:
    rule_id: str
    triggered: bool
    observed: float | None
    threshold: str
    detail: str


@dataclass(frozen=True)
class Finding:
    row_index: int
    rule_id: str
    field: str | None
    detail: str


@dataclass(frozen=True)
class BatchResult:
    verdict: str                 # PASS | WARN | BLOCK
    outcomes: list[RuleOutcome]
    findings: list[Finding]
    metrics: BatchMetrics
    schema_version: int | None
    schema_hash: str | None
    bad_rows: int