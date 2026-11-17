"""Quality engine: runs schema, row and anomaly checks, then assigns a batch verdict (4.8).

Pure Python, no Spark or database access, so it is unit-testable.
"""
from __future__ import annotations

from quality.anomaly import check_anomalies
from quality.config import RulesConfig, SourceSchema
from quality.parsing import parse_value
from quality.rules import (
    ROW_RULES,
    check_duplicates,
    check_nulls,
    check_unparseable,
    check_values,
)
from quality.schema_check import check_schema
from quality.types import BatchInput, BatchMetrics, BatchResult, RuleOutcome


def _mean(canon: list[dict | None], field_name: str, ts_format: str) -> float | None:
    vals: list[float] = []
    for p in canon:
        if p is None or p.get(field_name) is None:
            continue
        ok, parsed = parse_value("float", p[field_name], ts_format)
        if ok:
            vals.append(parsed)
    return round(sum(vals) / len(vals), 4) if vals else None


def _result(outcomes, findings, metrics, version, schema_hash, bad_rows, rules) -> BatchResult:
    fired = [o for o in outcomes if o.triggered]
    if any(rules.severity(o.rule_id) == "block" for o in fired):
        verdict = "BLOCK"
    elif fired:
        verdict = "WARN"
    else:
        verdict = "PASS"
    return BatchResult(
        verdict=verdict,
        outcomes=outcomes,
        findings=findings,
        metrics=metrics,
        schema_version=version,
        schema_hash=schema_hash,
        bad_rows=bad_rows,
    )


def evaluate(
    batch: BatchInput,
    schema: SourceSchema,
    rules: RulesConfig,
    ts_format: str,
    history: list[BatchMetrics],
) -> BatchResult:
    n = len(batch.records)
    if n == 0:
        outcome = RuleOutcome("empty_batch", True, 0.0, "> 0 rows", "no rows were delivered")
        return _result([outcome], [], BatchMetrics(row_count=0), None, None, 0, rules)

    payloads = [r.payload for r in batch.records]
    report = check_schema(schema, batch.observed_columns, payloads, ts_format, rules.unknown_column)
    version = report.version
    canon = [
        None if p is None else {report.mapping.get(k, k): v for k, v in p.items()}
        for p in payloads
    ]

    findings = []
    findings += check_unparseable(canon)
    findings += check_nulls(version, canon)
    findings += check_values(version, canon, rules.validity.get(schema.name, {}), ts_format)
    findings += check_duplicates(schema.key, canon)

    outcomes = [
        RuleOutcome(
            "schema_breaking",
            bool(report.breaking),
            float(len(report.breaking)),
            "0",
            "; ".join(report.breaking) or f"matches version {version.version}",
        ),
        RuleOutcome(
            "schema_unknown_column",
            bool(report.unknown),
            float(len(report.unknown)),
            "0",
            ", ".join(report.unknown) or "none",
        ),
        RuleOutcome(
            "schema_alias_mapped",
            bool(report.alias_mapped),
            float(len(report.alias_mapped)),
            "0",
            ", ".join(f"{a}->{b}" for a, b in report.alias_mapped.items()) or "none",
        ),
    ]

    for rule_id in ROW_RULES:
        rows_hit = len({f.row_index for f in findings if f.rule_id == rule_id})
        outcomes.append(
            RuleOutcome(rule_id, rows_hit > 0, float(rows_hit), "0 rows", f"{rows_hit} of {n} rows")
        )

    bad_rows = len({f.row_index for f in findings})
    max_rate = float(rules.defaults["max_bad_row_rate"])
    rate = bad_rows / n
    outcomes.append(
        RuleOutcome(
            "bad_row_rate",
            rate > max_rate,
            round(rate, 4),
            f"<= {max_rate}",
            f"{bad_rows} of {n} rows failed row-level rules",
        )
    )

    metrics = BatchMetrics(
        row_count=n,
        mean_unit_price=_mean(canon, schema.price_field, ts_format),
        mean_quantity=_mean(canon, schema.quantity_field, ts_format),
    )
    outcomes += check_anomalies(metrics, list(history), rules.defaults)

    return _result(outcomes, findings, metrics, version.version, report.schema_hash, bad_rows, rules)