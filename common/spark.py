"""SparkSession factory: Delta Lake + S3A (MinIO / AWS S3)."""
from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from common.config import load_config

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"  # matches Hadoop bundled with Spark 3.5


def get_spark(app_name: str = "drp", use_s3: bool = True, cfg: dict | None = None) -> SparkSession:
    cfg = cfg or load_config()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if use_s3:
        s = cfg["storage"]
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", s["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", s["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", s["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    extra = [HADOOP_AWS] if use_s3 else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
