"""Read one Bronze batch in file line order. Shared by quality, transform, and recovery."""
from __future__ import annotations

from pyspark.sql import functions as F

from ingestion.bronze import bronze_path


def read_batch(spark, cfg: dict, batch_id: str) -> tuple[list[int], list[str]]:
    rows = (
        spark.read.format("delta")
        .load(bronze_path(cfg))
        .where(F.col("batch_id") == batch_id)
        .orderBy("line_no")
        .select("line_no", "raw_payload")
        .collect()
    )
    return [r["line_no"] for r in rows], [r["raw_payload"] for r in rows]