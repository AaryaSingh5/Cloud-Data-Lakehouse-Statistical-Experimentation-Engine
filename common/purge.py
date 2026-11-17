"""Remove rows from Postgres and Bronze for runs whose file path starts with a prefix.

Used by the integration test teardown, and once to clean up earlier test runs.

Usage:
    python -m common.purge --prefix /tmp/pytest-
"""
from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import functions as F

from common.config import load_config
from common.spark import get_spark
from ingestion import run_log
from ingestion.bronze import bronze_path

logger = logging.getLogger("purge")


def purge(prefix: str, cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    conn = run_log.connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT batch_id::text FROM ingestion_runs "
                "WHERE file_path LIKE %s AND batch_id IS NOT NULL",
                (prefix + "%",),
            )
            batch_ids = [row[0] for row in cur.fetchall()]
            # quality_verdicts references ingestion_runs, so it must go first
            cur.execute(
                "DELETE FROM quality_verdicts WHERE run_id IN "
                "(SELECT run_id FROM ingestion_runs WHERE file_path LIKE %s)",
                (prefix + "%",),
            )
            cur.execute("DELETE FROM ingestion_runs WHERE file_path LIKE %s", (prefix + "%",))
            runs_deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    if batch_ids:
        from delta.tables import DeltaTable

        spark = get_spark("purge", use_s3=True, cfg=cfg)
        try:
            DeltaTable.forPath(spark, bronze_path(cfg)).delete(F.col("batch_id").isin(batch_ids))
        finally:
            spark.stop()

    result = {"runs_deleted": runs_deleted, "bronze_batches_deleted": len(batch_ids)}
    logger.info("purged %s", result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Purge runs by file path prefix.")
    parser.add_argument("--prefix", required=True, help="file path prefix, e.g. /tmp/pytest-")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("Purge:", purge(args.prefix))
    return 0


if __name__ == "__main__":
    sys.exit(main())