"""Retries with exponential backoff for transient failures (6.2). Data errors are never retried."""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("retry")

TRANSIENT_MARKERS = (
    "connection reset", "connection refused", "connection timed out", "timed out",
    "timeout", "slowdown", "service unavailable", "temporarily unavailable",
    "broken pipe", "socket", "503",
)


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    text = str(exc).lower()
    return any(marker in text for marker in TRANSIENT_MARKERS)


def with_retry(
    fn: Callable[[], Any],
    label: str,
    attempts: int = 3,
    base_delay: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[Any, int]:
    """Run fn. Retry transient failures with backoff. Returns (result, attempts_used)."""
    for attempt in range(1, attempts + 1):
        try:
            return fn(), attempt
        except Exception as exc:  # noqa: BLE001
            if not is_transient(exc) or attempt == attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("%s attempt %d failed (transient), retrying in %.1fs: %s",
                           label, attempt, delay, exc)
            sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover