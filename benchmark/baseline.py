"""The plain pipeline (9.1): loads every row, runs no checks, handles no schema drift.

Fields are read by their canonical names only. A renamed column comes through as null,
and a malformed record is kept as a row of nulls. Duplicates are loaded as they arrive.
That is the behaviour the platform is measured against.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.spark import get_spark
from ingestion.connectors import CONNECTORS
from ingestion.landing import discover
from quality.config import Registry
from transform.conform import CONFORM

# Spark's pattern for the registry's "%Y-%m-%d %H:%M:%S".
JAVA_TS = "yyyy-MM-dd HH:mm:ss"


def compute(cfg: dict, registry: Registry) -> dict:
    spark = get_spark("baseline", use_s3=False, cfg=cfg)
    try:
        parts = []
        for source, schema in registry.sources.items():
            columns = list(schema.versions[-1].columns)
            struct = StructType([StructField(c, StringType(), True) for c in columns])
            f = CONFORM[source]
            for lf in discover(Path(cfg["paths"]["landing"]), source):
                raw = CONNECTORS[lf.fmt].read_raw(spark, str(lf.path))
                parsed = raw.select(F.from_json("raw_payload", struct).alias("p")).select("p.*")
                parts.append(parsed.select(
                    F.lit(source).alias("source"),
                    F.col(f["order_key"]).alias("order_key"),
                    F.to_date(F.to_timestamp(F.col(f["order_ts"]), JAVA_TS)).alias("sale_date"),
                    F.expr(f"try_cast({f['quantity']} as int)").alias("qty"),
                    F.expr(f"try_cast({f['unit_price']} as double)").alias("price"),
                ))

        if not parts:
            return {"days": {}, "rows": 0, "duplicate_rows": 0, "undated_rows": 0}

        fact = parts[0]
        for part in parts[1:]:
            fact = fact.unionByName(part)
        fact = fact.withColumn("revenue", F.col("qty") * F.col("price"))

        rows = fact.count()
        duplicate_rows = rows - fact.select("source", "order_key").distinct().count()
        undated_rows = fact.where(F.col("sale_date").isNull()).count()
        per_day = (
            fact.where(F.col("sale_date").isNotNull())
            .groupBy("sale_date")
            .agg(F.count(F.lit(1)).alias("orders"),
                 F.round(F.sum("revenue"), 2).alias("revenue"))
        )
        days = {
            r["sale_date"]: (int(r["orders"]), float(r["revenue"] or 0.0))
            for r in per_day.collect()
        }
        return {"days": days, "rows": rows,
                "duplicate_rows": duplicate_rows, "undated_rows": undated_rows}
    finally:
        spark.stop()