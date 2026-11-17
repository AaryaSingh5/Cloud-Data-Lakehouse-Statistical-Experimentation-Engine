"""Quality runner: checks ingestion runs that have no verdict yet, or a run passed in by id.

Usage:
    python -m quality.runner                 # all pending runs
    python -m quality.runner --source pos    # one source

Row positions in findings follow Bronze line order. Quarantine maps positions to line numbers.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from common.config import load_config
from common.migrate import apply as migrate
from common.spark import get_spark
from ingestion import run_log
from ingestion.bronze_read import read_batch
from quality import store
from quality.config import Registry, RulesConfig, load_registry, load_rules
from quality.engine import evaluate
from quality.types import BatchInput, RawRecord
from recovery import quarantine

logger = logging.getLogger("quality")

PENDING_SQL = """
    SELECT r.run_id::text, r.source, r.batch_id::text, r.batch_date, r.file_path, r.status
    FROM ingestion_runs r
    LEFT JOIN quality_verdicts q ON q.run_id = r.run_id
    WHERE r.status IN ('success', 'empty')
      AND q.run_id IS NULL
      AND (%(source)s IS NULL OR r.source = %(source)s)
    ORDER BY r.batch_date, r.source, r.started_at
"""

RUN_SQL = """
    SELECT r.run_id::text, r.source, r.batch_id::text, r.batch_date, r.file_path, r.status
    FROM ingestion_runs r
    WHERE r.run_id = %s
"""


def fetch_run(conn, run_id: str):
    with conn.cursor() as cur:
        cur.execute(RUN_SQL, (run_id,))
        return cur.fetchone()


def _csv_header(file_path: str) -> list[str]:
    p = Path(file_path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    with p.open(encoding="utf-8-sig", newline="") as fh:
        return next(csv.reader(fh), [])


def _union_keys(payloads: list[dict | None]) -> list[str]:
    seen: dict[str, None] = {}
    for p in payloads:
        if p:
            for k in p:
                seen.setdefault(k, None)
    return list(seen)


def _parse(raws: list[str]) -> list[dict | None]:
    out: list[dict | None] = []
    for raw in raws:
        try:
            obj = json.loads(raw)
        except (TypeError, ValueError):
            obj = None
        out.append(obj if isinstance(obj, dict) else None)
    return out


def process(conn, spark, cfg: dict, registry: Registry, rules: RulesConfig, run: tuple) -> dict:
    run_id, source, batch_id, batch_date, file_path, status = run
    schema = registry.sources.get(source)
    if schema is None:
        logger.warning("no schema registered for source %s; skipping run %s", source, run_id)
        return {"run_id": run_id, "verdict": "SKIPPED"}

    line_nos, raws = (
        read_batch(spark, cfg, batch_id) if (status == "success" and batch_id) else ([], [])
    )
    payloads = _parse(raws)
    observed = _csv_header(file_path) if schema.fmt == "csv" else _union_keys(payloads)
    history = store.history(conn, source, batch_date, int(rules.defaults["history_window"]))

    batch = BatchInput(
        source=source,
        batch_id=batch_id,
        batch_date=batch_date,
        observed_columns=observed,
        records=[RawRecord(i, p) for i, p in enumerate(payloads)],
    )
    result = evaluate(batch, schema, rules, registry.ts_format, history)
    store.save(conn, run_id, source, batch_date, batch_id, result, rules.severity)
    quarantine.sync(conn, run_id, source, batch_date, result, raws, line_nos)

    logger.info("%s %s run=%s verdict=%s bad_rows=%d",
                source, batch_date, run_id[:8], result.verdict, result.bad_rows)
    return {"run_id": run_id, "verdict": result.verdict, "bad_rows": result.bad_rows}


def run(cfg: dict | None = None, source: str | None = None) -> list[dict]:
    cfg = cfg or load_config()
    registry = load_registry()
    rules = load_rules()
    conn = run_log.connect(cfg)
    spark = None
    try:
        migrate(conn)
        with conn.cursor() as cur:
            cur.execute(PENDING_SQL, {"source": source})
            pending = cur.fetchall()
        if not pending:
            logger.info("no pending runs to check")
            return []
        spark = get_spark("quality", use_s3=True, cfg=cfg)
        return [process(conn, spark, cfg, registry, rules, p) for p in pending]
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run data quality checks on pending ingestion runs.")
    parser.add_argument("--source", help="only check this source (pos, web, partner)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run(source=args.source)

    counts: dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("Quality summary:", counts or "nothing pending")
    return 0


if __name__ == "__main__":
    sys.exit(main())