"""Smoke test: Spark + Delta (+ MinIO). Run `python scripts/smoke_spark_delta.py --local` to skip MinIO.

First run is slow: Spark downloads the Delta / hadoop-aws jars once.
"""
import argparse

from common.spark import get_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="write to /tmp instead of MinIO")
    args = ap.parse_args()

    spark = get_spark("smoke", use_s3=not args.local)
    path = "/tmp/drp_smoke_delta" if args.local else "s3a://bronze/_smoke"

    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]).write.format("delta").mode("overwrite").save(path)
    df = spark.read.format("delta").load(path)
    assert df.count() == 2, "row count mismatch"
    df.show()
    print(f"OK: Spark + Delta working ({path})")
    spark.stop()


if __name__ == "__main__":
    main()
