"""Bronze to Silver (5.1, 5.4). Incremental MERGE on (source, order_key).

PASS and WARN runs contribute only rows with no quality findings.
BLOCK runs contribute nothing. Every run is recorded in silver_loads.
The MERGE is retried on transient errors (6.2).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.retry import with_retry
from ingestion.bronze_read import read_batch
from quality.config import Registry, RulesConfig
from quality.engine import evaluate
from quality.runner import _csv_header, _parse, _union_keys
from quality.schema_check import check_schema
from quality.types import BatchInput, RawRecord
from recovery.log import record
from transform.conform import SILVER_COLUMNS, conform_row

logger = logging.getLogger("silver")

SILVER_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("order_key", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("batch_date", DateType(), True),
    StructField("run_id", StringType(), True),
    StructField("order_ts", TimestampType(), True),
    StructField("sale_date", DateType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("loaded_at", TimestampType(), True),
])

RUN_SELECT = """
    SELECT r.run_id::text, r.source, r.batch_id::text, r.batch_date, r.file_path,
           r.rows_written, q.verdict
    FROM quality_verdicts q
    JOIN ingestion_runs r ON r.run_id = q.run_id
"""

PENDING_SQL = RUN_SELECT + """
    LEFT JOIN silver_loads s ON s.run_id = q.run_id
    WHERE s.run_id IS NULL
    ORDER BY r.batch_date, r.source, r.started_at
"""

RUN_TUPLE_SQL = RUN_SELECT + " WHERE q.run_id = %s"


def silver_path(cfg: dict) -> str:
    return f"s3a://{cfg['storage']['buckets']['silver']}/sales"


def run_tuple(conn, run_id: str):
    with conn.cursor() as cur:
        cur.execute(RUN_TUPLE_SQL, (run_id,))
        return cur.fetchone()


def _ledger(conn, run_id, batch_id, source, batch_date, verdict, status, rows_in, kept, held) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO silver_loads
                (run_id, batch_id, source, batch_date, verdict, status, rows_in, rows_kept, rows_held)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, batch_id, source, batch_date, verdict, status, rows_in, kept, held),
        )
    conn.commit()


def _kept_rows(spark, cfg, registry: Registry, rules: RulesConfig, run: tuple):
    run_id, source, batch_id, batch_date, file_path, _rows_written, _verdict = run
    schema = registry.sources[source]

    line_nos, raws = read_batch(spark, cfg, batch_id) if batch_id else ([], [])
    payloads = _parse(raws)
    observed = _csv_header(file_path) if schema.fmt == "csv" else _union_keys(payloads)

    batch = BatchInput(
        source=source,
        batch_id=batch_id,
        batch_date=batch_date,
        observed_columns=observed,
        records=[RawRecord(i, p) for i, p in enumerate(payloads)],
    )
    flagged = {f.row_index for f in evaluate(batch, schema, rules, registry.ts_format, []).findings}
    mapping = check_schema(schema, observed, payloads, registry.ts_format, rules.unknown_column).mapping

    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i, p in enumerate(payloads):
        if p is None or i in flagged:
            continue
        canon = {mapping.get(k, k): v for k, v in p.items()}
        rows.append(conform_row(source, canon, registry.ts_format, batch_id, batch_date, run_id, loaded_at))
    return rows, len(payloads), len(payloads) - len(rows)


def _merge(spark, rows: list[dict], path: str) -> None:
    if not rows:
        return
    df = spark.createDataFrame(
        [tuple(r[c] for c in SILVER_COLUMNS) for r in rows],
        schema=SILVER_SCHEMA,
    ).dropDuplicates(["source", "order_key"])

    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("append").partitionBy("sale_date").save(path)
        return

    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), "t.source = s.source AND t.order_key = s.order_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_run(conn, spark, cfg, registry: Registry, rules: RulesConfig, run: tuple) -> set:
    """Load one checked run into Silver. Returns the sale dates it touched."""
    run_id, source, batch_id, batch_date, _file_path, rows_written, verdict = run

    if verdict == "BLOCK":
        _ledger(conn, run_id, batch_id, source, batch_date, verdict,
                "excluded", rows_written, 0, rows_written)
        logger.info("excluded %s %s run=%s (BLOCK)", source, batch_date, run_id[:8])
        return {batch_date}

    started = datetime.now(timezone.utc)
    rows, rows_in, held = _kept_rows(spark, cfg, registry, rules, run)
    path = silver_path(cfg)
    _, attempts = with_retry(lambda: _merge(spark, rows, path), label=f"silver merge {source} {batch_date}")
    if attempts > 1:
        record(conn, "retry", f"silver merge {source}/{batch_date}", started,
               "recovered", f"attempts={attempts}", run_id=run_id)

    _ledger(conn, run_id, batch_id, source, batch_date, verdict, "loaded", rows_in, len(rows), held)
    logger.info("loaded %s %s run=%s kept=%d held=%d", source, batch_date, run_id[:8], len(rows), held)
    return {batch_date} | {r["sale_date"] for r in rows}


def run(conn, spark, cfg, registry: Registry, rules: RulesConfig) -> tuple[int, set]:
    """Load every checked run not yet in silver_loads. Returns (runs processed, sale dates touched)."""
    with conn.cursor() as cur:
        cur.execute(PENDING_SQL)
        pending = cur.fetchall()

    dates: set = set()
    for r in pending:
        dates |= load_run(conn, spark, cfg, registry, rules, r)
    return len(pending), dates