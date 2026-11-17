"""Bronze writer: raw, append-only Delta table with metadata and line numbers.

Location: s3a://<storage.buckets.bronze>/sales_raw
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.landing import LandingFile

BRONZE_TABLE = "sales_raw"


def bronze_path(cfg: dict) -> str:
    return f"s3a://{cfg['storage']['buckets']['bronze']}/{BRONZE_TABLE}"


def write_bronze(
    raw: DataFrame,
    lf: LandingFile,
    batch_id: str,
    file_hash: str,
    cfg: dict,
) -> int:
    """Append one batch to Bronze. Returns the number of rows written."""
    out = raw.select(
        F.lit(lf.source).alias("source"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(file_hash).alias("file_hash"),
        F.lit(lf.path.name).alias("source_file"),
        F.current_timestamp().alias("ingest_ts"),
        F.lit(lf.delivered_date.isoformat()).cast("date").alias("delivered_date"),
        F.lit(lf.batch_date.isoformat()).cast("date").alias("batch_date"),
        F.col("raw_payload"),
        F.col("line_no"),
    )
    n = out.count()
    if n == 0:
        return 0

    (
        out.write.format("delta")
        .mode("append")
        .partitionBy("source", "batch_date")
        .save(bronze_path(cfg))
    )
    return n