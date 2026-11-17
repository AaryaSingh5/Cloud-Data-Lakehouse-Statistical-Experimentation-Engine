"""Alerts (7.3). Stored in Postgres and logged. Optionally posted to a webhook.

One open alert per (kind, subject). A repeat bumps a counter instead of adding a row,
and only the first occurrence is posted. Set DRP_ALERT_WEBHOOK to enable posting.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import date

from common.config import load_config
from common.migrate import apply as migrate
from ingestion import run_log

logger = logging.getLogger("alerts")

SEVERITIES = ("info", "warning", "critical")


def in_range(d: date, start: date, end: date) -> bool:
    return start <= d <= end


def payload(kind: str, severity: str, subject: str, message: str) -> str:
    return json.dumps({"kind": kind, "severity": severity, "subject": subject, "message": message})


def _post(url: str, body: str) -> None:
    req = urllib.request.Request(
        url, data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5).close()
    except Exception:  # noqa: BLE001 - a failed webhook must not break the pipeline
        logger.exception("alert webhook post failed")


def raise_alert(conn, kind: str, severity: str, subject: str, message: str) -> bool:
    """Open or update an alert. Returns True when a new alert was opened."""
    if severity not in SEVERITIES:
        raise ValueError(f"unknown severity: {severity}")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alerts (kind, severity, subject, message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (kind, subject) WHERE resolved_at IS NULL
            DO UPDATE SET occurrences = alerts.occurrences + 1,
                          message = EXCLUDED.message,
                          severity = EXCLUDED.severity,
                          updated_at = now()
            RETURNING occurrences
            """,
            (kind, severity, subject, message),
        )
        occurrences = cur.fetchone()[0]
    conn.commit()

    is_new = occurrences == 1
    if is_new:
        logger.warning("%s %s: %s", kind, subject, message)
        url = os.getenv("DRP_ALERT_WEBHOOK", "")
        if url:
            _post(url, payload(kind, severity, subject, message))
    return is_new


def resolve_missing(conn, kind: str, active: set[str]) -> int:
    """Close open alerts of this kind whose subject is no longer active. Returns how many closed."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE alerts SET resolved_at = now(), updated_at = now() "
            "WHERE kind = %s AND resolved_at IS NULL AND NOT (subject = ANY(%s::text[]))",
            (kind, list(active)),
        )
        closed = cur.rowcount
    conn.commit()
    return closed


def task_failed(context) -> None:
    """Airflow on_failure_callback. Opens a critical alert for the failed task."""
    try:
        ti = context["task_instance"]
        exc = context.get("exception")
        conn = run_log.connect(load_config())
        try:
            migrate(conn)
            raise_alert(
                conn, "task_failed", "critical", f"{ti.dag_id}.{ti.task_id}",
                f"run {context.get('run_id')}: {str(exc)[:500]}",
            )
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - a callback must never crash the scheduler
        logger.exception("could not record task_failed alert")