"""Create the MinIO buckets listed in storage.buckets if they are missing. Safe to run repeatedly."""
from __future__ import annotations

import logging
import sys

import boto3

from common.config import load_config

logger = logging.getLogger("buckets")


def ensure_buckets(cfg: dict) -> list[str]:
    s = cfg["storage"]
    s3 = boto3.client(
        "s3",
        endpoint_url=s["endpoint"],
        aws_access_key_id=s["access_key"],
        aws_secret_access_key=s["secret_key"],
    )
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    created: list[str] = []
    for name in s["buckets"].values():
        if name not in existing:
            s3.create_bucket(Bucket=name)
            created.append(name)
    logger.info("buckets created: %s", created or "none")
    return created


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    created = ensure_buckets(load_config())
    print("Created:", created or "nothing, all buckets exist")
    return 0


if __name__ == "__main__":
    sys.exit(main())