"""Quarantine zone (6.1), kept in Postgres so status changes are transactional.

PASS: nothing quarantined.
WARN: flagged rows only, with their reason codes.
BLOCK: every row, with the batch-level reasons the batch was blocked.

Raw payloads are copied here. Bronze keeps the originals.
A row that a later check no longer flags is marked released.
Items from a superseded run are marked superseded.
"""
from __future__ import annotations

from collections import defaultdict

from psycopg2.extras import execute_values

from quality.rules import ROW_RULES


def build_items(result, raws: list[str], line_nos: list[int]) -> list[dict]:
    if result.verdict == "PASS":
        return []

    row_reasons: dict[int, set[str]] = defaultdict(set)
    for f in result.findings:
        row_reasons[f.row_index].add(f.rule_id)
    batch_reasons = {
        o.rule_id for o in result.outcomes if o.triggered and o.rule_id not in ROW_RULES
    }

    items: list[dict] = []
    for i, raw in enumerate(raws):
        reasons = set(row_reasons.get(i, set()))
        if result.verdict == "BLOCK":
            reasons = (reasons | batch_reasons) or {"batch_blocked"}
        if reasons:
            items.append({
                "line_no": line_nos[i],
                "reason_codes": ",".join(sorted(reasons)),
                "raw_payload": raw,
            })
    return items


def sync(conn, run_id: str, source: str, batch_date, result, raws: list[str], line_nos: list[int]) -> int:
    """Make quarantine match the latest check of this run. Returns the number of held items."""
    items = build_items(result, raws, line_nos)
    held = [it["line_no"] for it in items]
    with conn.cursor() as cur:
        if items:
            execute_values(
                cur,
                """
                INSERT INTO quarantine_items
                    (run_id, source, batch_date, line_no, reason_codes, raw_payload)
                VALUES %s
                ON CONFLICT (run_id, line_no) DO UPDATE SET
                    reason_codes = EXCLUDED.reason_codes,
                    raw_payload = EXCLUDED.raw_payload,
                    status = 'held',
                    updated_at = now()
                """,
                [(run_id, source, batch_date, it["line_no"], it["reason_codes"], it["raw_payload"])
                 for it in items],
                page_size=1000,
            )
        if held:
            cur.execute(
                """
                UPDATE quarantine_items SET status = 'released', updated_at = now()
                WHERE run_id = %s AND status = 'held' AND NOT (line_no = ANY(%s))
                """,
                (run_id, held),
            )
        else:
            cur.execute(
                """
                UPDATE quarantine_items SET status = 'released', updated_at = now()
                WHERE run_id = %s AND status = 'held'
                """,
                (run_id,),
            )
    conn.commit()
    return len(items)


def mark_superseded(conn, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE quarantine_items SET status = 'superseded', updated_at = now() "
            "WHERE run_id = %s AND status = 'held'",
            (run_id,),
        )
    conn.commit()