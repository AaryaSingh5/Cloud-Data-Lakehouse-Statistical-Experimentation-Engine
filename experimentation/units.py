"""Per-customer totals (revenue, orders) for each data source over an experiment window.

clean_* reads the fault-free files. platform_units reads Gold. baseline_units reads Bronze with
no checks, so duplicates and malformed rows count. The Spark functions import their dependencies
inside the function, so this module loads on the host.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path


def _clean_records(clean_root):
    for path in sorted(Path(clean_root, "web").glob("*")):
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip():
                yield json.loads(line)


def clean_population(clean_root) -> set[str]:
    """Every web customer in the fault-free data, on any date."""
    return {str(rec["customer_id"]) for rec in _clean_records(clean_root)}


def clean_units(clean_root, start: date, end: date, ts_format: str) -> dict[str, tuple[float, int]]:
    totals: dict[str, tuple[float, int]] = {}
    for rec in _clean_records(clean_root):
        when = datetime.strptime(str(rec["order_ts"]), ts_format).date()
        if not (start <= when <= end):
            continue
        revenue = int(str(rec["quantity"])) * float(str(rec["unit_price"]))
        customer = str(rec["customer_id"])
        r, n = totals.get(customer, (0.0, 0))
        totals[customer] = (r + revenue, n + 1)
    return totals


def platform_units(spark, cfg: dict, start: date, end: date) -> dict[str, tuple[float, int]]:
    from pyspark.sql import functions as F

    from transform.gold import gold_path

    df = (
        spark.read.format("delta").load(gold_path(cfg, "customer_daily"))
        .where((F.col("sale_date") >= F.lit(start)) & (F.col("sale_date") <= F.lit(end)))
    )
    agg = df.groupBy("customer_id").agg(
        F.sum("revenue").alias("revenue"),
        F.sum("order_lines").alias("orders"),
    )
    return {
        r["customer_id"]: (float(r["revenue"] or 0.0), int(r["orders"] or 0))
        for r in agg.collect()
        if r["customer_id"] is not None
    }


def baseline_units(spark, cfg: dict, start: date, end: date) -> dict[str, tuple[float, int]]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    from ingestion.bronze import bronze_path

    schema = StructType([StructField(n, StringType(), True)
                         for n in ("customer_id", "order_ts", "quantity", "unit_price")])
    df = (
        spark.read.format("delta").load(bronze_path(cfg))
        .where(F.col("source") == "web")
        .select(F.from_json("raw_payload", schema).alias("p"))
        .select("p.*")
    )
    sale = F.to_date(F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"))
    df = df.where(sale.between(F.lit(start), F.lit(end)))
    agg = df.groupBy("customer_id").agg(
        F.sum(F.expr("try_cast(quantity as double)") * F.expr("try_cast(unit_price as double)"))
        .alias("revenue"),
        F.count(F.lit(1)).alias("orders"),
    )
    return {
        r["customer_id"]: (float(r["revenue"] or 0.0), int(r["orders"]))
        for r in agg.collect()
        if r["customer_id"] is not None
    }