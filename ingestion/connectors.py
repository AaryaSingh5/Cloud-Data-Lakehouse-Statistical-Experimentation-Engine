"""Spark connectors: landing file to a DataFrame with raw_payload and line_no."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ingestion.landing_rows import landing_rows

RAW_SCHEMA = StructType([
    StructField("raw_payload", StringType(), False),
    StructField("line_no", IntegerType(), False),
])


class LandingConnector:
    def __init__(self, fmt: str) -> None:
        self.fmt = fmt

    def read_raw(self, spark: SparkSession, path: str) -> DataFrame:
        # Tuples are (raw_payload, line_no) to match RAW_SCHEMA.
        rows = [(raw, n) for n, raw in landing_rows(Path(path), self.fmt)]
        return spark.createDataFrame(rows, schema=RAW_SCHEMA)


CONNECTORS = {fmt: LandingConnector(fmt) for fmt in ("csv", "jsonl")}