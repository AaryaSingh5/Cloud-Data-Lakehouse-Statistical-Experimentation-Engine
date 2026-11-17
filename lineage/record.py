"""Lineage (7.1): which Silver run loaded how many rows into each sale date.

The source file to Bronze batch to Silver run chain lives in ingestion_runs
(file_path, batch_id, run_id). The Silver run to sale date link is stored here,
rebuilt from Silver on every sync so it matches what is loaded now. Excluded and
superseded runs have no edges.

Usage:
    python -m lineage.record
"""
from __future__ import annotations

import logging
import sys

from delta.tables import DeltaTable
from psycopg2.extras import execute_values

from common.config import load_config
from common.migrate import apply as migrate
from common.spark import get_spark
from ingestion import run_log
from transform.silver import silver_path

logger = logging.getLogger("lineage")

PRUNE_SQL = (
    "DELETE FROM lineage_edges "
    "WHERE run_id NOT IN (SELECT run_id FROM silver_loads WHERE status = 'loaded')"
)
UPSERT_SQL = """
    INSERT INTO lineage_edges (run_id, sale_date, rows_loaded) VALUES %s
    ON CONFLICT (run_id, sale_date)
    DO UPDATE SET rows_loaded = EXCLUDED.rows_loaded, recorded_at = now()
"""


def sync(conn, spark, cfg: dict) -> int:
    path = silver_path(cfg)
    counts = []
    if DeltaTable.isDeltaTable(spark, path):
        counts = (
            spark.read.format("delta").load(path)
            .groupBy("run_id", "sale_date").count().collect()
        )
    rows = [(r["run_id"], r["sale_date"], r["count"]) for r in counts]
    with conn.cursor() as cur:
        cur.execute(PRUNE_SQL)
        if rows:
            execute_values(cur, UPSERT_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    conn = run_log.connect(cfg)
    spark = None
    try:
        migrate(conn)
        spark = get_spark("lineage", use_s3=True, cfg=cfg)
        edges = sync(conn, spark, cfg)
    finally:
        if spark is not None:
            spark.stop()
        conn.close()
    print(f"Lineage edges: {edges}")
    return 0


if __name__ == "__main__":
    sys.exit(main())