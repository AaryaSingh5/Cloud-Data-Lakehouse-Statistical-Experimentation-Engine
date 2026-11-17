"""Bronze to Silver, supersede older runs, then Silver to Gold for touched dates.

Usage:
    python -m transform.runner              # incremental
    python -m transform.runner --all-dates  # recompute every date
"""
from __future__ import annotations

import argparse
import logging
import sys

from common.buckets import ensure_buckets
from common.config import load_config
from common.migrate import apply as migrate
from common.spark import get_spark
from ingestion import run_log
from quality.config import load_registry, load_rules
from recovery import reconcile
from transform import gold, silver


def run(cfg: dict | None = None, all_dates: bool = False) -> dict:
    cfg = cfg or load_config()
    ensure_buckets(cfg)
    registry = load_registry()
    rules = load_rules()
    conn = run_log.connect(cfg)
    spark = None
    try:
        migrate(conn)
        spark = get_spark("transform", use_s3=True, cfg=cfg)
        processed, dates = silver.run(conn, spark, cfg, registry, rules)
        dates |= reconcile.reconcile(conn, spark, cfg)

        if all_dates:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT batch_date FROM quality_verdicts")
                dates = {row[0] for row in cur.fetchall()}

        gold_days = gold.refresh(conn, spark, cfg, list(registry.sources), dates)
        return {"silver_runs": processed, "gold_days": gold_days}
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bronze to Silver to Gold.")
    parser.add_argument("--all-dates", action="store_true", help="recompute every sale date")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run(all_dates=args.all_dates)
    published = sum(1 for d in result["gold_days"] if d["status"] == "published")
    held = len(result["gold_days"]) - published
    print(f"Transform summary: silver_runs={result['silver_runs']} "
          f"gold_published={published} gold_held={held}")
    return 0


if __name__ == "__main__":
    sys.exit(main())