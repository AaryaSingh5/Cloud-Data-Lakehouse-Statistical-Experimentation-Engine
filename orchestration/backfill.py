"""Backfill pipeline (7.4): re-run ingestion, quality, and transform for a date range.

Usage:
    python -m orchestration.backfill --start 2025-01-01 --end 2025-01-07
"""
from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import sys

from common.config import load_config
from common.spark import get_spark
from ingestion import pipeline, run_log
from ingestion.landing import discover
from orchestration.alerts import in_range
from quality import runner as quality_runner
from transform import runner as transform_runner


def _counts(items: list, key: str) -> dict:
    out: dict = {}
    for item in items:
        value = getattr(item, key) if not isinstance(item, dict) else item[key]
        out[value] = out.get(value, 0) + 1
    return out


def run(start: date, end: date, source: str | None = None, cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    landing_root = Path(cfg["paths"]["landing"])
    all_files = discover(landing_root, source=source)
    files = [f for f in all_files if in_range(f.batch_date, start, end)]

    conn = run_log.connect(cfg)
    spark = None
    ingested = []
    try:
        run_log.ensure_schema(conn)
        if files:
            spark = get_spark("backfill-ingest", use_s3=True, cfg=cfg)
            ingested = [pipeline.ingest_file(spark, conn, lf, cfg) for lf in files]
    finally:
        if spark is not None:
            spark.stop()
        conn.close()

    checked = quality_runner.run(cfg=cfg, source=source)
    transformed = transform_runner.run(cfg=cfg)

    ingested_counts = _counts(ingested, "status")
    quality_counts = _counts(checked, "verdict")

    return {
        "files": len(files),
        "ingested": ingested_counts,
        "quality": quality_counts,
        "transform": {
            "silver_runs": transformed["silver_runs"],
            "gold_days": len(transformed["gold_days"]),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill a date range.")
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="end date (YYYY-MM-DD)")
    parser.add_argument("--source", help="only backfill this source (pos, web, partner)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    summary = run(start=args.start, end=args.end, source=args.source)
    print("Backfill:", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
