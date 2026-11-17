"""Format connectors. Each one reads a landing file into a DataFrame with a
single string column, `raw_payload`, holding the record as JSON.

Bronze keeps the raw payload, so schema problems stay visible and nothing is
lost before Phase 4 validation runs.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class Connector(ABC):
    fmt: str

    @abstractmethod
    def read_raw(self, spark: SparkSession, path: str) -> DataFrame:
        """Return a DataFrame with one string column, raw_payload."""


class CsvConnector(Connector):
    fmt = "csv"

    def read_raw(self, spark: SparkSession, path: str) -> DataFrame:
        # Every column is read as string: no type inference, so type-change faults survive into Bronze.
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(path)
        )
        return df.select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("raw_payload"))


class JsonlConnector(Connector):
    fmt = "jsonl"

    def read_raw(self, spark: SparkSession, path: str) -> DataFrame:
        # Each line is kept untouched. Corrupt JSON is passed through for the quality layer.
        return (
            spark.read.text(path)
            .where(F.trim(F.col("value")) != "")
            .select(F.col("value").alias("raw_payload"))
        )


CONNECTORS: dict[str, Connector] = {c.fmt: c for c in (CsvConnector(), JsonlConnector())}