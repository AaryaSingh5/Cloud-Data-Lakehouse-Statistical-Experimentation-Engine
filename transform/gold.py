"""Silver to Gold (5.2) with the publication gate (5.3). Writes are retried on transient errors (6.2)."""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.retry import with_retry
from transform.gate import decide
from transform.silver import SILVER_SCHEMA, silver_path

logger = logging.getLogger("gold")

GOLD_TABLES = ("daily_revenue", "product_daily", "location_daily", "customer_daily")

LATEST_VERDICTS_SQL = """
    SELECT DISTINCT ON (r.source) r.source, q.verdict
    FROM quality_verdicts q
    JOIN ingestion_runs r ON r.run_id = q.run_id
    WHERE r.batch_date = %s
    ORDER BY r.source, r.started_at DESC
"""


def gold_path(cfg: dict, name: str) -> str:
    return f"s3a://{cfg['storage']['buckets']['gold']}/{name}"


def _metrics():
    return [
        F.count(F.lit(1)).alias("order_lines"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    ]


def build_aggregates(day: DataFrame) -> dict[str, DataFrame]:
    located = day.withColumn(
        "location", F.coalesce(F.col("store_id"), F.col("country"), F.col("region"))
    )
    return {
        "daily_revenue": day.groupBy("sale_date", "source").agg(*_metrics()),
        "product_daily": day.groupBy("sale_date", "product_id").agg(*_metrics()),
        "location_daily": located.groupBy("sale_date", "source", "location").agg(*_metrics()),
        "customer_daily": (
            day.where(F.col("customer_id").isNotNull())
            .groupBy("sale_date", "customer_id")
            .agg(*_metrics())
        ),
    }


def _silver_day(spark: SparkSession, silver: str, day):
    if not DeltaTable.isDeltaTable(spark, silver):
        return spark.createDataFrame([], SILVER_SCHEMA)
    return spark.read.format("delta").load(silver).where(F.col("sale_date") == F.lit(day))


def _replace_day(spark: SparkSession, df: DataFrame, path: str, day) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"sale_date = '{day.isoformat()}'")
            .save(path)
        )
    else:
        df.write.format("delta").mode("append").partitionBy("sale_date").save(path)


def _clear_day(spark: SparkSession, path: str, day) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete(F.col("sale_date") == F.lit(day))


def _latest_verdicts(conn, day) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(LATEST_VERDICTS_SQL, (day,))
        return {source: verdict for source, verdict in cur.fetchall()}


def _record(conn, day, status, ok, not_ok, orders, revenue) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold_publications
                (sale_date, status, sources_ok, sources_not_ok, orders, revenue, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (sale_date) DO UPDATE SET
                status = EXCLUDED.status,
                sources_ok = EXCLUDED.sources_ok,
                sources_not_ok = EXCLUDED.sources_not_ok,
                orders = EXCLUDED.orders,
                revenue = EXCLUDED.revenue,
                updated_at = now()
            """,
            (day, status, ",".join(ok) or "-", ",".join(not_ok) or "-", orders, revenue),
        )
    conn.commit()


def refresh(conn, spark: SparkSession, cfg: dict, expected: list[str], dates) -> list[dict]:
    if not dates:
        return []
    silver = silver_path(cfg)
    results = []
    for day in sorted(dates):
        status, ok, not_ok = decide(expected, _latest_verdicts(conn, day))
        orders = revenue = None

        if status == "published":
            day_df = _silver_day(spark, silver, day)
            for name, df in build_aggregates(day_df).items():
                with_retry(
                    lambda df=df, name=name: _replace_day(spark, df, gold_path(cfg, name), day),
                    label=f"gold {name} {day}",
                )
            totals = day_df.agg(
                F.count(F.lit(1)).alias("orders"),
                F.round(F.sum("revenue"), 2).alias("revenue"),
            ).first()
            orders, revenue = totals["orders"], totals["revenue"]
        else:
            for name in GOLD_TABLES:
                with_retry(
                    lambda name=name: _clear_day(spark, gold_path(cfg, name), day),
                    label=f"clear gold {name} {day}",
                )

        _record(conn, day, status, ok, not_ok, orders, revenue)
        logger.info("gold %s %s not_ok=%s", day, status, not_ok or "-")
        results.append({"sale_date": day, "status": status, "not_ok": not_ok})
    return results