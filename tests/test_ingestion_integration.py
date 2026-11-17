"""End-to-end Bronze ingestion against the real Compose stack (MinIO + Postgres + Spark).

Each test removes the rows it wrote, so no test batch stays in Bronze or the run log.
Run with:  RUN_SPARK_TESTS=1 pytest tests/test_ingestion_integration.py
"""
import os
import uuid
from pathlib import Path

import pytest

from common.config import load_config
from common.purge import purge
from ingestion import pipeline

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_SPARK_TESTS") != "1",
    reason="needs MinIO, Postgres and Spark",
)


@pytest.fixture
def landing_root(tmp_path):
    yield tmp_path
    purge(str(tmp_path))


def _write_landing(root: Path, tag: str) -> None:
    # The tag makes file contents unique per test run, so hashes never collide with earlier runs.
    pos = root / "pos" / "2024-03-02"
    pos.mkdir(parents=True)
    (pos / "pos_2024-03-01.csv").write_text(
        "order_id,order_ts,store_id,product_id,quantity,unit_price,payment_method\n"
        f"o1-{tag},2024-03-01 10:00:00,S01,P0001,2,9.99,card\n"
        f"o2-{tag},2024-03-01 11:00:00,S01,P0002,1,4.50,cash\n"
    )
    web = root / "web" / "2024-03-02"
    web.mkdir(parents=True)
    (web / "web_2024-03-01.jsonl").write_text(
        f'{{"order_id":"w1-{tag}","order_ts":"2024-03-01 12:00:00","customer_id":"C00001",'
        '"product_id":"P0001","quantity":1,"unit_price":9.99,"country":"IN"}\n'
    )


def test_ingest_is_idempotent(landing_root):
    _write_landing(landing_root, tag=uuid.uuid4().hex[:8])
    cfg = load_config()

    first = pipeline.run(cfg=cfg, root=landing_root)
    assert sorted(r.status for r in first) == ["success", "success"]
    assert sum(r.rows_written for r in first) == 3

    second = pipeline.run(cfg=cfg, root=landing_root)
    assert [r.status for r in second] == ["skipped_duplicate", "skipped_duplicate"]