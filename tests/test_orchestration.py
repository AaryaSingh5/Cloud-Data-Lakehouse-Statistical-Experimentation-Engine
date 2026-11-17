"""Orchestration helpers and DAG structure. Pure Python, runs on the host."""
import ast
import json
from datetime import date
from pathlib import Path

from orchestration.alerts import in_range, payload

DAG = Path(__file__).resolve().parents[1] / "orchestration" / "dags" / "drp_daily.py"


def test_in_range_is_inclusive():
    assert in_range(date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 7))
    assert in_range(date(2025, 1, 7), date(2025, 1, 1), date(2025, 1, 7))
    assert not in_range(date(2025, 1, 8), date(2025, 1, 1), date(2025, 1, 7))


def test_payload_is_json_with_all_fields():
    body = json.loads(payload("day_held", "warning", "2025-01-03", "waiting on pos"))
    assert body == {
        "kind": "day_held",
        "severity": "warning",
        "subject": "2025-01-03",
        "message": "waiting on pos",
    }


def test_dag_runs_the_daily_chain_in_order():
    tree = ast.parse(DAG.read_text(encoding="utf-8"))
    task_ids = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "BashOperator":
            for kw in node.keywords:
                if kw.arg == "task_id":
                    task_ids.append(kw.value.value)
    assert task_ids == ["ingest", "quality", "transform", "lineage", "metrics", "alerts"]


def test_dag_attaches_failure_callback():
    text = DAG.read_text(encoding="utf-8")
    assert "on_failure_callback" in text
    assert "task_failed" in text