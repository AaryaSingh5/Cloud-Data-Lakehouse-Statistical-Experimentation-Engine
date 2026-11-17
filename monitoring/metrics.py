"""Pure metric helpers. No database, so they're testable on the host."""
from __future__ import annotations

from datetime import datetime


def freshness_hours(now: datetime, last: datetime | None) -> float | None:
    """Hours since the last load. None when the source has never loaded."""
    if last is None:
        return None
    return (now - last).total_seconds() / 3600.0


def pass_rate(passed: int, total: int) -> float | None:
    """Share of PASS verdicts. None when there is nothing to measure."""
    if not total:
        return None
    return passed / total