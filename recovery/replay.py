"""Replay (6.3): re-check runs after a rule change, using the original file.

For each run: remove its Silver rows and ledger entry, re-run quality (which updates
quarantine, including releasing rows that now pass), reload Silver, then reconcile and
refresh Gold for the affected dates.

Superseded runs are never replayed. A newer corrected file replaced them, and reloading
an old file would overwrite newer values.

Usage:
    python -m recovery.replay --run <run_id>
    python -m recovery.replay --all        # every WARN or BLOCK run that is not superseded
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common.config import load_config
from common.migrate import apply as migrate
from common.spark import get_spark
from ingestion import run_log
from quality import runner as quality_runner
from quality.config import load_registry, load_rules
from recovery import reconcile
from recovery.log import record
from transform import gold, silver
from transform.silver import silver_path

logger = logging.getLogger("replay")

ALL_FLAGGED_SQL = """
    SELECT q.run_id::text
    FROM quality_verdicts q
    JOIN ingestion_runs r ON r.run_id = q.run_id
    LEFT JOIN silver_loads s ON s.run_id = q.run_id
    WHERE q.verdict IN ('WARN', 'BLOCK')
      AND (s.status IS NULL OR s.status <> 'superseded')
    ORDER BY r.batch_date, r.source, r.started_at
"""


def _delete_silver_run(spark, cfg: dict, run_id: str) -> set:
    path = silver_path(cfg)
    if not DeltaTable.isDeltaTable(spark, path):
        return set()
    touched = {
        r[0]
        for r in spark.read.format("delta").load(path)
        .where(F.col("run_id") == run_id)
        .select("sale_date").distinct().collect()
    }
    DeltaTable.forPath(spark, path).delete(F.col("run_id") == run_id)
    return touched


def _is_superseded(conn, run_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM silver_loads WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    return bool(row) and row[0] == "superseded"


def replay_runs(conn, spark, cfg, registry, rules, run_ids: list[str]) -> set:
    dates: set = set()
    for run_id in run_ids:
        if _is_superseded(conn, run_id):
            logger.warning("run %s was superseded by a newer file; skipping", run_id[:8])
            continue

        started = datetime.now(timezone.utc)
        run = quality_runner.fetch_run(conn, run_id)
        if run is None:
            logger.warning("run %s not found, skipping", run_id)
            continue
        _, source, _, batch_date, _, _ = run

        dates.add(batch_date)
        dates |= _delete_silver_run(spark, cfg, run_id)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM silver_loads WHERE run_id = %s", (run_id,))
        conn.commit()

        quality_runner.process(conn, spark, cfg, registry, rules, run)
        silver_run = silver.run_tuple(conn, run_id)
        if silver_run is not None:
            dates |= silver.load_run(conn, spark, cfg, registry, rules, silver_run)

        record(conn, "replay", f"{source}/{batch_date}", started, "success",
               "re-checked and reloaded", run_id=run_id)
        logger.info("replayed %s %s run=%s", source, batch_date, run_id[:8])
    return dates


def replay(run_ids: list[str] | None = None, all_flagged: bool = False, cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    registry = load_registry()
    rules = load_rules()
    conn = run_log.connect(cfg)
    spark = None
    try:
        migrate(conn)
        if all_flagged:
            with conn.cursor() as cur:
                cur.execute(ALL_FLAGGED_SQL)
                run_ids = [r[0] for r in cur.fetchall()]
        if not run_ids:
            return {"runs": 0, "gold_days": []}

        spark = get_spark("replay", use_s3=True, cfg=cfg)
        dates = replay_runs(conn, spark, cfg, registry, rules, run_ids)
        dates |= reconcile.reconcile(conn, spark, cfg)
        gold_days = gold.refresh(conn, spark, cfg, list(registry.sources), dates)
        return {"runs": len(run_ids), "gold_days": gold_days}
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Re-check runs after a rule change.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run", help="run_id to replay")
    group.add_argument("--all", action="store_true", help="every WARN or BLOCK run that is not superseded")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = replay(run_ids=[args.run] if args.run else None, all_flagged=args.all)
    print(f"Replay summary: runs={result['runs']} gold_days={len(result['gold_days'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())