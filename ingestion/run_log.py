"""Postgres ingestion run log. Also the source of truth for idempotency."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import psycopg2

from common.migrate import apply as migrate


@dataclass
class RunRecord:
    source: str
    file_path: str
    file_hash: str
    batch_date: date
    status: str
    started_at: datetime
    finished_at: datetime
    batch_id: str | None = None
    rows_written: int = 0
    error: str | None = None


def connect(cfg: dict):
    pg = cfg["postgres"]
    return psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["db"],
        user=pg["user"],
        password=pg["password"],
    )


def ensure_schema(conn) -> None:
    migrate(conn)

def already_ingested(conn, source: str, file_hash: str) -> bool:
    """True if this exact file was ingested, or already recorded as empty.

    Empty files are seen on every run, so after the first record they count as duplicates.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ingestion_runs "
            "WHERE source = %s AND file_hash = %s AND status IN ('success', 'empty') LIMIT 1",
            (source, file_hash),
        )
        return cur.fetchone() is not None


def record_run(conn, rec: RunRecord) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_runs
                (source, batch_id, file_path, file_hash, batch_date, status,
                 rows_written, error, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                rec.source,
                rec.batch_id,
                rec.file_path,
                rec.file_hash,
                rec.batch_date,
                rec.status,
                rec.rows_written,
                rec.error,
                rec.started_at,
                rec.finished_at,
            ),
        )
    conn.commit()