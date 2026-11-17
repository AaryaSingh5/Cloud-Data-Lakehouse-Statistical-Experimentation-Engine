"""Development reset: wipe Bronze, Silver, Gold, and the run, quality, and transform tables.

Landing files are untouched. Re-running ingestion, quality, and transform rebuilds everything.
Destructive. Requires --yes.

Usage:
    python -m common.reset --yes
"""
from __future__ import annotations

import argparse
import logging
import sys

import boto3

from common.config import load_config
from ingestion import run_log

logger = logging.getLogger("reset")

TABLES = (
    "ingestion_runs",
    "quality_verdicts",
    "quality_rule_results",
    "quality_row_findings",
    "silver_loads",
    "gold_publications",
)
GOLD_PREFIXES = ("daily_revenue/", "product_daily/", "location_daily/", "customer_daily/")


def _wipe_prefix(s3, bucket: str, prefix: str) -> int:
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            deleted += len(objects)
    return deleted


def reset(cfg: dict) -> dict:
    s = cfg["storage"]
    buckets = s["buckets"]
    s3 = boto3.client(
        "s3",
        endpoint_url=s["endpoint"],
        aws_access_key_id=s["access_key"],
        aws_secret_access_key=s["secret_key"],
    )
    objects = _wipe_prefix(s3, buckets["bronze"], "sales_raw/")
    objects += _wipe_prefix(s3, buckets["silver"], "sales/")
    for prefix in GOLD_PREFIXES:
        objects += _wipe_prefix(s3, buckets["gold"], prefix)

    conn = run_log.connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
        conn.commit()
    finally:
        conn.close()
    return {"objects_deleted": objects, "tables_truncated": len(TABLES)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Wipe derived data. Landing files are kept.")
    parser.add_argument("--yes", action="store_true", help="required to confirm the wipe")
    args = parser.parse_args(argv)
    if not args.yes:
        print("Refusing to wipe without --yes")
        return 1
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("Reset:", reset(load_config()))
    return 0


if __name__ == "__main__":
    sys.exit(main())