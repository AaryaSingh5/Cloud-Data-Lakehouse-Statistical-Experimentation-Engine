"""Pipeline dashboard (8.2) with alert acknowledge and close (8.3).

Reads Postgres. The only writes are alert acknowledge and close actions.
Start with: docker compose up -d dashboard
Then open http://localhost:8501
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from common.config import load_config
from ingestion import run_log

st.set_page_config(page_title="Data reliability platform", layout="wide")


def _conn():
    conn = st.session_state.get("conn")
    if conn is None or conn.closed:
        conn = run_log.connect(load_config())
        st.session_state["conn"] = conn
    return conn


def query(sql: str, params=None) -> pd.DataFrame:
    conn = _conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception:
        conn.rollback()
        raise


def execute(sql: str, params=None) -> None:
    conn = _conn()
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


st.title("Data reliability platform")
health, quality, gold, lineage, quarantine, alerts, metrics = st.tabs(
    ["Health", "Quality", "Gold", "Lineage", "Quarantine", "Alerts", "Metrics"]
)

with health:
    st.subheader("Freshness by source")
    st.dataframe(query("""
        SELECT source,
               MAX(finished_at) AS last_loaded,
               ROUND((EXTRACT(EPOCH FROM (now() - MAX(finished_at))) / 3600.0)::numeric, 1)
                   AS hours_since
        FROM ingestion_runs
        WHERE status IN ('success', 'empty')
        GROUP BY source
        ORDER BY source
    """), hide_index=True)

    st.subheader("Recent ingest runs")
    st.dataframe(query("""
        SELECT source, batch_date, status, rows_written,
               ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS seconds,
               error
        FROM ingestion_runs
        ORDER BY started_at DESC
        LIMIT 50
    """), hide_index=True)

with quality:
    verdicts = query("""
        SELECT batch_date, verdict, COUNT(*) AS runs
        FROM quality_verdicts
        GROUP BY batch_date, verdict
        ORDER BY batch_date
    """)
    st.subheader("Verdicts by batch date")
    if verdicts.empty:
        st.info("No quality results yet.")
    else:
        verdicts["batch_date"] = verdicts["batch_date"].astype(str)
        st.bar_chart(verdicts.pivot_table(
            index="batch_date", columns="verdict", values="runs", fill_value=0,
        ))
    st.subheader("Rules that fired")
    st.dataframe(query("""
        SELECT rule_id, severity, COUNT(*) FILTER (WHERE triggered) AS runs_triggered
        FROM quality_rule_results
        GROUP BY rule_id, severity
        ORDER BY runs_triggered DESC
    """), hide_index=True)

with gold:
    pubs = query("""
        SELECT sale_date, status, sources_ok, sources_not_ok, orders, revenue, updated_at
        FROM gold_publications
        ORDER BY sale_date
    """)
    published = int((pubs["status"] == "published").sum()) if not pubs.empty else 0
    held = int((pubs["status"] == "held").sum()) if not pubs.empty else 0
    c1, c2 = st.columns(2)
    c1.metric("Published days", published)
    c2.metric("Held days", held)
    if not pubs.empty:
        st.subheader("Revenue on published days")
        st.line_chart(pubs.set_index("sale_date")["revenue"])
    st.dataframe(pubs, hide_index=True)

with lineage:
    days = query("SELECT sale_date FROM gold_publications ORDER BY sale_date")
    if days.empty:
        st.info("No Gold days yet.")
    else:
        day = st.selectbox("Gold date", days["sale_date"].astype(str).tolist())
        st.subheader("Runs feeding this date")
        st.dataframe(query("""
            SELECT r.source, r.batch_date, r.run_id::text AS run_id,
                   q.verdict, le.rows_loaded, r.file_path
            FROM lineage_edges le
            JOIN ingestion_runs r ON r.run_id = le.run_id
            JOIN quality_verdicts q ON q.run_id = le.run_id
            WHERE le.sale_date = %s
            ORDER BY r.source
        """, (day,)), hide_index=True)
        st.subheader("Publication status")
        st.dataframe(query(
            "SELECT status, sources_ok, sources_not_ok FROM gold_publications WHERE sale_date = %s",
            (day,),
        ), hide_index=True)

with quarantine:
    st.subheader("Items by status and source")
    st.dataframe(query("""
        SELECT status, source, COUNT(*) AS items
        FROM quarantine_items
        GROUP BY status, source
        ORDER BY status, source
    """), hide_index=True)
    held_items = query("SELECT reason_codes FROM quarantine_items WHERE status = 'held'")
    if not held_items.empty:
        reasons = (
            held_items["reason_codes"].str.split(",").explode()
            .value_counts().rename_axis("reason").reset_index(name="held_items")
        )
        st.subheader("Held items by reason")
        st.bar_chart(reasons.set_index("reason"))

with alerts:
    open_alerts = query("""
        SELECT id, severity, kind, subject, occurrences, message,
               created_at, acknowledged_at
        FROM alerts
        WHERE resolved_at IS NULL
        ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                 created_at
    """)
    st.subheader("Open alerts")
    st.dataframe(open_alerts, hide_index=True)
    if not open_alerts.empty:
        alert_id = int(st.selectbox("Alert id", open_alerts["id"].tolist()))
        col1, col2 = st.columns(2)
        if col1.button("Acknowledge"):
            execute(
                "UPDATE alerts SET acknowledged_at = COALESCE(acknowledged_at, now()), "
                "updated_at = now() WHERE id = %s",
                (alert_id,),
            )
            st.rerun()
        if col2.button("Close"):
            execute(
                "UPDATE alerts SET resolved_at = now(), updated_at = now() "
                "WHERE id = %s AND resolved_at IS NULL",
                (alert_id,),
            )
            st.rerun()
    st.subheader("Recently closed")
    st.dataframe(query("""
        SELECT id, severity, kind, subject, resolved_at
        FROM alerts
        WHERE resolved_at IS NOT NULL
        ORDER BY resolved_at DESC
        LIMIT 20
    """), hide_index=True)

with metrics:
    series = query("""
        SELECT captured_at, metric, COALESCE(source, 'all') AS source, value
        FROM pipeline_metrics
        WHERE captured_at > now() - interval '14 days'
        ORDER BY captured_at
    """)
    if series.empty:
        st.info("No metrics yet. Run: make metrics")
    else:
        choice = st.selectbox("Metric", sorted(series["metric"].unique()))
        sub = series[series["metric"] == choice]
        st.line_chart(sub.pivot_table(
            index="captured_at", columns="source", values="value", aggfunc="last",
        ))
        st.dataframe(sub.tail(50), hide_index=True)