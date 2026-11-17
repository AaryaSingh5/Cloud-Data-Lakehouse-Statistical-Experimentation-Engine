"""Automatic refresh (6.3): ingest new landing files, check them, load Silver,
supersede older runs, and refresh Gold. Run this after files land. Phase 7 schedules it.

Usage:
    python -m recovery.refresh
"""
from __future__ import annotations

import logging
import sys

from common.config import load_config
from ingestion import pipeline
from quality import runner as quality_runner
from transform import runner as transform_runner


def _counts(items: list, key: str) -> dict:
    out: dict = {}
    for item in items:
        value = getattr(item, key) if not isinstance(item, dict) else item[key]
        out[value] = out.get(value, 0) + 1
    return out


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()

    ingested = pipeline.run(cfg=cfg)
    print("Ingest:", _counts(ingested, "status") or "no new files")

    checked = quality_runner.run(cfg=cfg)
    print("Quality:", _counts(checked, "verdict") or "nothing pending")

    transformed = transform_runner.run(cfg=cfg)
    print(f"Transform: silver_runs={transformed['silver_runs']} "
          f"gold_days={len(transformed['gold_days'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())