"""Rollback (6.4): restore Silver to an earlier Delta version, then rebuild what was undone.

After the restore, rows from superseded runs are removed again, so a rollback never
brings back data that a newer file replaced. Runs whose rows were removed by the restore
are reloaded from their checked data. Gold is recomputed from Silver.

Usage:
    python -m recovery.rollback --list
    python -m recovery.rollback --version N
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common.config import load_config
from common.retry import with_retry
from common.spark import get_spark
from ingestion import run_log
from recovery.log import record
from transform import runner as transform_runner
from transform.silver import silver_path

logger = logging.getLogger("rollback")


def list_versions(cfg: dict) -> list[tuple]:
    spark = get_spark("rollback-list", use_s3=True, cfg=cfg)
    try:
        rows = (
            DeltaTable.forPath(spark, silver_path(cfg))
            .history()
            .select("version", "timestamp", "operation")
            .collect()
        )
        return [tuple(r) for r in rows]
    finally:
        spark.stop()


def rollback(cfg: dict, version: int) -> dict:
    started = datetime.now(timezone.utc)
    path = silver_path(cfg)
    conn = run_log.connect(cfg)
    spark = get_spark("rollback", use_s3=True, cfg=cfg)
    try:
        with_retry(
            lambda: DeltaTable.forPath(spark, path).restoreToVersion(version),
            label=f"restore silver v{version}",
        )

        # The restore can bring back rows of runs that a newer file superseded. Remove them again.
        with conn.cursor() as cur:
            cur.execute("SELECT run_id::text FROM silver_loads WHERE status = 'superseded'")
            superseded = [r[0] for r in cur.fetchall()]
        if superseded:
            DeltaTable.forPath(spark, path).delete(F.col("run_id").isin(superseded))

        present = {
            r[0]
            for r in spark.read.format("delta").load(path).select("run_id").distinct().collect()
        }
        with conn.cursor() as cur:
            cur.execute("SELECT run_id::text, rows_kept FROM silver_loads WHERE status = 'loaded'")
            undone = [rid for rid, kept in cur.fetchall() if kept and rid not in present]
            if undone:
                cur.execute("DELETE FROM silver_loads WHERE run_id::text = ANY(%s)", (undone,))
        conn.commit()
        record(conn, "rollback", f"silver@v{version}", started, "success",
               f"undone runs={len(undone)} superseded_removed={len(superseded)}")
    finally:
        spark.stop()
        conn.close()

    rebuilt = transform_runner.run(cfg=cfg, all_dates=True)
    return {"undone_runs": len(undone), "superseded_removed": len(superseded), **rebuilt}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Restore Silver to an earlier Delta version.")
    parser.add_argument("--list", action="store_true", help="show Silver version history")
    parser.add_argument("--version", type=int, help="Delta version to restore")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    if args.list:
        for version, timestamp, operation in list_versions(cfg):
            print(f"version={version} time={timestamp} op={operation}")
        return 0
    if args.version is None:
        parser.error("pass --list or --version N")
    print("Rollback:", rollback(cfg, args.version))
    return 0


if __name__ == "__main__":
    sys.exit(main())