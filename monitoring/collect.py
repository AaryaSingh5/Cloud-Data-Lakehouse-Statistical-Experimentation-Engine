"""Metrics collector (8.1). Writes one row per metric per source to pipeline_metrics.

Metrics:
  freshness_hours          hours since the last successful or empty load, per source
  ingest_seconds_avg_24h   average ingest duration over the last 24 hours, per source
  ingest_failures_24h      failed ingest runs over the last 24 hours, per source
  quality_pass_rate_14d    share of PASS verdicts over the last 14 batch dates, per source
  silver_rows              rows currently loaded in Silver, per source
  gold_days_published      Gold days currently published (source is empty)
  gold_days_held           Gold days currently held (source is empty)
  alerts_open              open alerts; the source column holds the severity

Usage:
    python -m monitoring.collect
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from psycopg2.extras import execute_values

from common.config import load_config
from common.migrate import apply as migrate
from ingestion import run_log
from monitoring.metrics import freshness_hours, pass_rate

logger = logging.getLogger("metrics")

FRESHNESS_SQL = """
    SELECT source, MAX(finished_at)
    FROM ingestion_runs
    WHERE status IN ('success', 'empty')
    GROUP BY source
"""
DURATION_SQL = """
    SELECT source, AVG(EXTRACT(EPOCH FROM (finished_at - started_at)))
    FROM ingestion_runs
    WHERE started_at > now() - interval '1 day'
    GROUP BY source
"""
FAILURES_SQL = """
    SELECT source, COUNT(*)
    FROM ingestion_runs
    WHERE status = 'failed' AND started_at > now() - interval '1 day'
    GROUP BY source
"""
PASS_SQL = """
    SELECT r.source, COUNT(*) FILTER (WHERE q.verdict = 'PASS'), COUNT(*)
    FROM quality_verdicts q
    JOIN ingestion_runs r ON r.run_id = q.run_id
    WHERE r.batch_date >= (SELECT MAX(batch_date) FROM quality_verdicts) - 13
    GROUP BY r.source
"""
SILVER_SQL = """
    SELECT source, SUM(rows_kept)
    FROM silver_loads
    WHERE status = 'loaded'
    GROUP BY source
"""
GOLD_SQL = "SELECT status, COUNT(*) FROM gold_publications GROUP BY status"
ALERTS_SQL = """
    SELECT severity, COUNT(*)
    FROM alerts
    WHERE resolved_at IS NULL
    GROUP BY severity
"""


def collect(conn, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    rows: list[tuple[str, str | None, float]] = []

    with conn.cursor() as cur:
        cur.execute(FRESHNESS_SQL)
        for source, last in cur.fetchall():
            hours = freshness_hours(now, last)
            if hours is not None:
                rows.append(("freshness_hours", source, round(hours, 2)))

        cur.execute(DURATION_SQL)
        rows += [
            ("ingest_seconds_avg_24h", s, round(float(v), 2))
            for s, v in cur.fetchall() if v is not None
        ]

        cur.execute(FAILURES_SQL)
        rows += [("ingest_failures_24h", s, float(n)) for s, n in cur.fetchall()]

        cur.execute(PASS_SQL)
        for source, passed, total in cur.fetchall():
            rate = pass_rate(passed, total)
            if rate is not None:
                rows.append(("quality_pass_rate_14d", source, round(rate, 4)))

        cur.execute(SILVER_SQL)
        rows += [("silver_rows", s, float(n)) for s, n in cur.fetchall()]

        cur.execute(GOLD_SQL)
        rows += [(f"gold_days_{status}", None, float(n)) for status, n in cur.fetchall()]

        cur.execute(ALERTS_SQL)
        rows += [("alerts_open", severity, float(n)) for severity, n in cur.fetchall()]

        if rows:
            execute_values(
                cur,
                "INSERT INTO pipeline_metrics (captured_at, metric, source, value) VALUES %s",
                [(now, metric, source, value) for metric, source, value in rows],
            )
    conn.commit()
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = run_log.connect(load_config())
    try:
        migrate(conn)
        written = collect(conn)
    finally:
        conn.close()
    print(f"Metrics: {written} rows written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
