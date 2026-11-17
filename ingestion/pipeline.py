"""Ingestion entry point: discover landing files, skip duplicates, write Bronze, log every run.

Usage:
    python -m ingestion.pipeline                 # all sources
    python -m ingestion.pipeline --source pos    # one source
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from common.config import load_config
from common.spark import get_spark
from ingestion import run_log
from ingestion.bronze import write_bronze
from ingestion.connectors import CONNECTORS
from ingestion.landing import LandingFile, discover, sha256_file
from ingestion.run_log import RunRecord

logger = logging.getLogger("ingestion")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _finish(conn, lf: LandingFile, file_hash: str, started: datetime, **kwargs) -> RunRecord:
    rec = RunRecord(
        source=lf.source,
        file_path=str(lf.path),
        file_hash=file_hash,
        batch_date=lf.batch_date,
        started_at=started,
        finished_at=_now(),
        **kwargs,
    )
    run_log.record_run(conn, rec)
    logger.info("%s %s status=%s rows=%d", lf.source, lf.path.name, rec.status, rec.rows_written)
    return rec


def ingest_file(spark: SparkSession, conn, lf: LandingFile, cfg: dict) -> RunRecord:
    """Ingest one landing file. Never raises: failures are recorded as runs."""
    started = _now()
    file_hash = sha256_file(lf.path)

    if run_log.already_ingested(conn, lf.source, file_hash):
        return _finish(conn, lf, file_hash, started, status="skipped_duplicate")

    if lf.path.stat().st_size == 0:
        return _finish(conn, lf, file_hash, started, status="empty")

    batch_id = str(uuid.uuid4())
    try:
        raw = CONNECTORS[lf.fmt].read_raw(spark, str(lf.path))
        rows = write_bronze(raw, lf, batch_id, file_hash, cfg)
    except Exception as exc:  # noqa: BLE001 - one bad file must not stop the batch
        logger.exception("failed to ingest %s", lf.path)
        return _finish(conn, lf, file_hash, started, status="failed", error=str(exc)[:2000])

    if rows == 0:
        return _finish(conn, lf, file_hash, started, status="empty")
    return _finish(
        conn,
        lf,
        file_hash,
        started,
        status="success",
        batch_id=batch_id,
        rows_written=rows,
    )


def run(
    cfg: dict | None = None,
    source: str | None = None,
    root: Path | None = None,
) -> list[RunRecord]:
    cfg = cfg or load_config()
    landing_root = root or Path(cfg["paths"]["landing"])
    files = discover(landing_root, source)
    if not files:
        logger.info("no landing files found under %s", landing_root)
        return []

    conn = run_log.connect(cfg)
    spark: SparkSession | None = None
    try:
        run_log.ensure_schema(conn)
        spark = get_spark("ingestion-bronze", use_s3=True, cfg=cfg)
        return [ingest_file(spark, conn, lf, cfg) for lf in files]
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest landing files into Bronze.")
    parser.add_argument("--source", help="only ingest this source (pos, web, partner)")
    parser.add_argument("--landing-root", type=Path, help="override paths.landing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run(source=args.source, root=args.landing_root)

    counts: dict[str, int] = {}
    for rec in results:
        counts[rec.status] = counts.get(rec.status, 0) + 1
    print("Ingestion summary:", counts or "no files")

    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())