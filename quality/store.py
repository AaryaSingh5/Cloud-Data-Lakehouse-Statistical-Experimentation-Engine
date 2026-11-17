"""Quality persistence (4.9): one verdict per ingestion run, plus per-rule results and row findings."""
from __future__ import annotations

from collections.abc import Callable

from psycopg2.extras import execute_values

from quality.types import BatchMetrics, BatchResult


def history(conn, source: str, before, window: int) -> list[BatchMetrics]:
    """Prior batches for this source that were not blocked, newest first."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT row_count, mean_unit_price, mean_quantity
            FROM quality_verdicts
            WHERE source = %s AND batch_date < %s AND verdict <> 'BLOCK'
            ORDER BY batch_date DESC
            LIMIT %s
            """,
            (source, before, window),
        )
        return [
            BatchMetrics(row_count=int(r[0]), mean_unit_price=r[1], mean_quantity=r[2])
            for r in cur.fetchall()
        ]


def save(
    conn,
    run_id: str,
    source: str,
    batch_date,
    batch_id: str | None,
    result: BatchResult,
    severity_of: Callable[[str], str],
) -> None:
    m = result.metrics
    with conn.cursor() as cur:
        # re-running a check replaces its earlier results
        cur.execute("DELETE FROM quality_verdicts WHERE run_id = %s", (run_id,))
        cur.execute(
            """
            INSERT INTO quality_verdicts
                (run_id, source, batch_id, batch_date, schema_version, schema_hash,
                 row_count, bad_rows, mean_unit_price, mean_quantity, verdict)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, source, batch_id, batch_date, result.schema_version, result.schema_hash,
                m.row_count, result.bad_rows, m.mean_unit_price, m.mean_quantity, result.verdict,
            ),
        )
        execute_values(
            cur,
            "INSERT INTO quality_rule_results "
            "(run_id, rule_id, severity, triggered, observed, threshold, detail) VALUES %s",
            [
                (run_id, o.rule_id, severity_of(o.rule_id), o.triggered, o.observed, o.threshold, o.detail)
                for o in result.outcomes
            ],
        )
        if result.findings:
            execute_values(
                cur,
                "INSERT INTO quality_row_findings (run_id, row_index, rule_id, field, detail) VALUES %s",
                [(run_id, f.row_index, f.rule_id, f.field, f.detail) for f in result.findings],
                page_size=1000,
            )
    conn.commit()