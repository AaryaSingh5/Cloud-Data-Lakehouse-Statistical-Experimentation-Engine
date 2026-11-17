"""Alert checks: failed ingestion, blocked batches, held Gold days. Run after each daily pass.

Alerts clear automatically when their condition is gone. task_failed alerts from Airflow
are closed by hand with --resolve.

Usage:
    python -m orchestration.checks              # run the checks
    python -m orchestration.checks --list       # open alerts
    python -m orchestration.checks --resolve 12 # close one alert
"""
from __future__ import annotations

import argparse
import logging
import sys

from common.config import load_config
from common.migrate import apply as migrate
from ingestion import run_log
from orchestration.alerts import raise_alert, resolve_missing

FAILED_SQL = """
    SELECT r.file_path, COALESCE(r.error, 'ingest failed')
    FROM ingestion_runs r
    WHERE r.status = 'failed'
      AND r.started_at > now() - interval '1 day'
      AND NOT EXISTS (
          SELECT 1 FROM ingestion_runs ok
          WHERE ok.source = r.source AND ok.file_path = r.file_path AND ok.status = 'success'
      )
"""
BLOCKED_SQL = "SELECT source, batch_date FROM quality_verdicts WHERE verdict = 'BLOCK'"
HELD_SQL = "SELECT sale_date, sources_not_ok FROM gold_publications WHERE status = 'held'"
OPEN_SQL = """
    SELECT id, severity, kind, subject, occurrences, created_at
    FROM alerts WHERE resolved_at IS NULL ORDER BY created_at, id
"""


def run_checks(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(FAILED_SQL)
        failed = cur.fetchall()
        cur.execute(BLOCKED_SQL)
        blocked = cur.fetchall()
        cur.execute(HELD_SQL)
        held = cur.fetchall()

    active: dict[str, set[str]] = {"ingest_failed": set(), "batch_blocked": set(), "day_held": set()}
    for path, error in failed:
        active["ingest_failed"].add(path)
        raise_alert(conn, "ingest_failed", "critical", path, error[:500])
    for source, batch_date in blocked:
        subject = f"{source}/{batch_date}"
        active["batch_blocked"].add(subject)
        raise_alert(conn, "batch_blocked", "warning", subject, "batch excluded from Silver")
    for sale_date, not_ok in held:
        subject = sale_date.isoformat()
        active["day_held"].add(subject)
        raise_alert(conn, "day_held", "warning", subject, f"Gold held, waiting on: {not_ok}")

    closed = {kind: resolve_missing(conn, kind, subjects) for kind, subjects in active.items()}
    return {"active": {k: len(v) for k, v in active.items()}, "closed": closed}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert checks and alert list.")
    parser.add_argument("--list", action="store_true", help="show open alerts")
    parser.add_argument("--resolve", type=int, help="close one alert by id")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = run_log.connect(load_config())
    try:
        migrate(conn)
        if args.resolve is not None:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE alerts SET resolved_at = now(), updated_at = now() "
                    "WHERE id = %s AND resolved_at IS NULL",
                    (args.resolve,),
                )
                closed = cur.rowcount
            conn.commit()
            print(f"Resolved: {closed}")
            return 0
        if args.list:
            with conn.cursor() as cur:
                cur.execute(OPEN_SQL)
                rows = cur.fetchall()
            if not rows:
                print("No open alerts")
            for id_, severity, kind, subject, occurrences, created in rows:
                print(f"id={id_} {severity:8} {kind:14} {subject} x{occurrences} "
                      f"since={created:%Y-%m-%d %H:%M}")
            return 0
        print("Checks:", run_checks(conn))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())