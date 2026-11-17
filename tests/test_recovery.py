"""Recovery tests: transient classification, retry backoff, and quarantine item rules. Pure Python."""
import pytest

from common.retry import is_transient, with_retry
from quality.types import BatchMetrics, BatchResult, Finding, RuleOutcome
from recovery.quarantine import build_items

RAWS = ["r0", "r1", "r2"]
LINES = [2, 3, 4]


def test_transient_classification():
    assert is_transient(ConnectionError("reset"))
    assert is_transient(TimeoutError())
    assert is_transient(Exception("java.net.SocketTimeoutException: Read timed out"))
    assert not is_transient(ValueError("column not found"))


def test_retry_recovers_after_transient_failures():
    calls = {"n": 0}
    sleeps: list[float] = []

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("connection reset")
        return "ok"

    result, attempts = with_retry(flaky, "test", attempts=3, base_delay=1.0, sleep=sleeps.append)
    assert result == "ok"
    assert attempts == 3
    assert sleeps == [1.0, 2.0]


def test_retry_does_not_retry_data_errors():
    calls = {"n": 0}

    def bad():
        calls["n"] += 1
        raise ValueError("bad data")

    with pytest.raises(ValueError):
        with_retry(bad, "test", sleep=lambda s: None)
    assert calls["n"] == 1


def _result(verdict, findings=(), outcomes=()):
    return BatchResult(
        verdict=verdict,
        outcomes=list(outcomes),
        findings=list(findings),
        metrics=BatchMetrics(row_count=len(RAWS)),
        schema_version=1,
        schema_hash="x",
        bad_rows=len({f.row_index for f in findings}),
    )


def test_pass_quarantines_nothing():
    assert build_items(_result("PASS"), RAWS, LINES) == []


def test_warn_quarantines_only_flagged_rows_with_line_numbers():
    res = _result("WARN", findings=[Finding(1, "null_value", "product_id", "missing")])
    items = build_items(res, RAWS, LINES)
    assert [(i["line_no"], i["reason_codes"]) for i in items] == [(3, "null_value")]


def test_block_quarantines_every_row_with_batch_reasons():
    res = _result(
        "BLOCK",
        findings=[Finding(0, "null_value", "product_id", "missing")],
        outcomes=[RuleOutcome("schema_breaking", True, 1.0, "0", "missing:region")],
    )
    items = build_items(res, RAWS, LINES)
    assert [i["line_no"] for i in items] == [2, 3, 4]
    assert items[0]["reason_codes"] == "null_value,schema_breaking"


def test_block_without_specific_reasons_still_quarantines_all_rows():
    items = build_items(_result("BLOCK"), RAWS, LINES)
    assert len(items) == 3
    assert all(i["reason_codes"] == "batch_blocked" for i in items)