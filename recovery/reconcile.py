"""Automatic supersede (6.3).

When a newer non-BLOCK run exists for the same source and batch date, older loaded runs
are removed from Silver, their ledger rows are marked superseded, and their held quarantine
items are marked superseded.

BLOCK runs never supersede. A corrected file that is still bad keeps the earlier good data
loaded. The Gold gate still holds the day, because the latest verdict is BLOCK.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from recovery.log import record
from recovery.quarantine import mark_superseded
from transform.silver import silver_path

logger = logging.getLogger("reconcile")

SUPERSEDED_SQL = """
WITH checked AS (
    SELECT r.run_id, r.source, r.batch_date, r.started_at, q.verdict
    FROM ingestion_runs r
    JOIN quality_verdicts q ON q.run_id = r.run_id
),
latest_ok AS (
    SELECT DISTINCT ON (source, batch_date) source, batch_date, run_id
    FROM checked
    WHERE verdict <> 'BLOCK'
    ORDER BY source, batch_date, started_at DESC
)
SELECT s.run_id::text, s.source, s.batch_date, c.run_id::text
FROM silver_loads s
JOIN latest_ok c ON c.source = s.source AND c.batch_date = s.batch_date
WHERE s.status = 'loaded' AND s.run_id <> c.run_id
"""


def reconcile(conn, spark, cfg: dict) -> set:
    """Supersede older loaded runs. Returns sale dates whose Silver rows changed."""
    with conn.cursor() as cur:
        cur.execute(SUPERSEDED_SQL)
        superseded = cur.fetchall()

    dates: set = set()
    path = silver_path(cfg)
    for old_run, source, batch_date, current_run in superseded:
        started = datetime.now(timezone.utc)
        touched = {
            r[0]
            for r in spark.read.format("delta").load(path)
            .where(F.col("run_id") == old_run)
            .select("sale_date").distinct().collect()
        }
        dates |= touched
        dates.add(batch_date)

        DeltaTable.forPath(spark, path).delete(F.col("run_id") == old_run)
        with conn.cursor() as cur:
            cur.execute("UPDATE silver_loads SET status = 'superseded' WHERE run_id = %s", (old_run,))
        conn.commit()
        mark_superseded(conn, old_run)

        record(conn, "reconcile", f"{source}/{batch_date}", started, "success",
               f"superseded by run {current_run[:8]}", run_id=old_run)
        logger.info("superseded %s %s run=%s by %s", source, batch_date, old_run[:8], current_run[:8])
    return dates