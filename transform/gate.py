"""Gold publication gate (5.3). A day publishes only when every expected source is PASS or WARN."""
from __future__ import annotations


def decide(expected: list[str], verdicts: dict[str, str]) -> tuple[str, list[str], list[str]]:
    ok = [s for s in expected if verdicts.get(s) in ("PASS", "WARN")]
    not_ok = [s for s in expected if s not in ok]
    status = "published" if not not_ok else "held"
    return status, ok, not_ok
    