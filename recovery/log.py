"""Recovery outcome log (6.5). Every retry, reconcile, replay, and rollback is recorded here."""
from __future__ import annotations

from datetime import datetime, timezone


def record(
    conn,
    action: str,
    target: str,
    started: datetime,
    outcome: str,
    detail: str = "",
    run_id: str | None = None,
) -> None:
    finished = datetime.now(timezone.utc)
    duration_ms = int((finished - started).total_seconds() * 1000)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO recovery_log
                (action, target, run_id, started_at, finished_at, duration_ms, outcome, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (action, target, run_id, started, finished, duration_ms, outcome, detail),
        )
    conn.commit()