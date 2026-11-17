"""Lineage queries (7.2). Postgres only, no Spark.

Usage:
    python -m lineage.query feeds --date 2025-01-03
    python -m lineage.query impact --run <run_id>
    python -m lineage.query impact --source pos --date 2025-01-01   # latest successful run
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

from common.config import load_config
from ingestion import run_log

# Mirrors transform.gold.GOLD_TABLES. Kept here so queries don't import Spark.
GOLD_TABLES = ("daily_revenue", "product_daily", "location_daily", "customer_daily")

FEEDS_SQL = """
    SELECT r.source, r.run_id::text, r.file_path, q.verdict, le.rows_loaded
    FROM lineage_edges le
    JOIN ingestion_runs r ON r.run_id = le.run_id
    JOIN quality_verdicts q ON q.run_id = le.run_id
    WHERE le.sale_date = %s
    ORDER BY r.source, r.batch_date
"""
GOLD_STATUS_SQL = """
    SELECT status, sources_ok, sources_not_ok
    FROM gold_publications WHERE sale_date = %s
"""
RUN_INFO_SQL = """
    SELECT r.run_id::text, r.source, r.batch_date, r.file_path, COALESCE(q.verdict, '-'),
           (SELECT count(*) FROM quarantine_items
             WHERE run_id = r.run_id AND status = 'held')
    FROM ingestion_runs r
    LEFT JOIN quality_verdicts q ON q.run_id = r.run_id
    WHERE r.run_id = %s
"""
IMPACT_SQL = """
    SELECT le.sale_date, le.rows_loaded, COALESCE(g.status, 'not published')
    FROM lineage_edges le
    LEFT JOIN gold_publications g ON g.sale_date = le.sale_date
    WHERE le.run_id = %s
    ORDER BY le.sale_date
"""
LATEST_RUN_SQL = """
    SELECT r.run_id::text
    FROM ingestion_runs r
    JOIN quality_verdicts q ON q.run_id = r.run_id
    WHERE r.source = %s AND r.batch_date = %s AND r.status = 'success'
    ORDER BY r.started_at DESC
    LIMIT 1
"""


def feeds(conn, day: date) -> None:
    with conn.cursor() as cur:
        cur.execute(FEEDS_SQL, (day,))
        rows = cur.fetchall()
        cur.execute(GOLD_STATUS_SQL, (day,))
        gold = cur.fetchone()

    print(f"Gold {day}: {gold[0] if gold else 'no publication record'}")
    if gold:
        print(f"  sources ok: {gold[1]}   sources not ok: {gold[2]}")
    if not rows:
        print("  no loaded Silver rows for this date")
    for source, run_id, path, verdict, rows_loaded in rows:
        print(f"  {source:8} run={run_id[:8]} verdict={verdict:5} rows={rows_loaded:5} file={path}")


def impact(conn, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(RUN_INFO_SQL, (run_id,))
        info = cur.fetchone()
        if info is None:
            print("run not found")
            return
        cur.execute(IMPACT_SQL, (run_id,))
        dates = cur.fetchall()

    rid, source, batch_date, path, verdict, held = info
    print(f"run {rid[:8]} source={source} batch={batch_date} verdict={verdict} quarantined_held={held}")
    print(f"file: {path}")
    if not dates:
        print("no Silver rows loaded from this run (excluded, superseded, or fully held)")
        return
    for sale_date, rows_loaded, status in dates:
        print(f"  sale_date={sale_date} rows={rows_loaded} gold={status}")
    published = [d for d, _, status in dates if status == "published"]
    if published:
        print(f"Gold tables affected: {', '.join(GOLD_TABLES)} for {len(published)} published date(s)")


def latest_run(conn, source: str, day: date) -> str | None:
    with conn.cursor() as cur:
        cur.execute(LATEST_RUN_SQL, (source, day))
        row = cur.fetchone()
    return row[0] if row else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lineage queries.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("feeds", help="sources that feed a Gold day")
    f.add_argument("--date", required=True, type=date.fromisoformat)
    i = sub.add_parser("impact", help="what a run affects")
    i.add_argument("--run")
    i.add_argument("--source")
    i.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args(argv)

    conn = run_log.connect(load_config())
    try:
        if args.cmd == "feeds":
            feeds(conn, args.date)
            return 0
        run_id = args.run
        if run_id is None:
            if not (args.source and args.date):
                parser.error("impact needs --run, or --source with --date")
            run_id = latest_run(conn, args.source, args.date)
            if run_id is None:
                print("no successful run found for that source and date")
                return 1
        impact(conn, run_id)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())