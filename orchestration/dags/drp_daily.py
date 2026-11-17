"""Daily pipeline (7.3, 8.1): ingest, quality, transform, lineage, metrics, alert checks.

Each step is its own task, so a failure shows exactly where the chain stopped.
Tasks retry twice with a 5 minute delay. A task that still fails opens a critical alert.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from orchestration.alerts import task_failed

RUN = "cd /opt/airflow/project && python -m "

default_args = {
    "owner": "drp",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failed,
}

with DAG(
    dag_id="drp_daily",
    description="Ingest, check, transform, record lineage, collect metrics, raise alerts",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["drp"],
) as dag:
    ingest = BashOperator(task_id="ingest", bash_command=RUN + "ingestion.pipeline")
    quality = BashOperator(task_id="quality", bash_command=RUN + "quality.runner")
    transform = BashOperator(task_id="transform", bash_command=RUN + "transform.runner")
    lineage = BashOperator(task_id="lineage", bash_command=RUN + "lineage.record")
    metrics = BashOperator(task_id="metrics", bash_command=RUN + "monitoring.collect")
    alerts = BashOperator(task_id="alerts", bash_command=RUN + "orchestration.checks")

    ingest >> quality >> transform >> lineage >> metrics >> alerts