"""Benchmark runner (9.1, 9.3). Both arms run on identical generated data, across seeds and profiles.

Per-day results are stored in benchmark_days, so the report can show the held-day penalty sensitivity.

WARNING: each run wipes derived data (common.reset) and deletes the landing, clean, and
ground_truth folders before regenerating them. The last run leaves its data in place.

Usage:
    python -m benchmark.run --yes                              # 3 seeds x low, medium, high
    python -m benchmark.run --yes --seeds 1 --profiles medium  # one run, for a first check
    python -m benchmark.run --report-only                      # rebuild docs/results.md
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from psycopg2.extras import execute_values

from benchmark import baseline, scoring, truth
from benchmark.report import write_report
from common.buckets import ensure_buckets
from common.config import load_config
from common.migrate import apply as migrate
from common.reset import reset
from common.spark import get_spark
from ingestion import pipeline, run_log
from quality import runner as quality_runner
from quality.config import load_registry
from transform import runner as transform_runner
from transform.silver import silver_path

DEFAULT_SEEDS = (1, 2, 3)
DEFAULT_PROFILES = ("low", "medium", "high")
GEN_LINE = re.compile(r"Generated .*")

logger = logging.getLogger("benchmark")


def generate(seed: int, profile: str, cfg: dict) -> str:
    for key in ("landing", "clean", "ground_truth"):
        shutil.rmtree(cfg["paths"][key], ignore_errors=True)
    env = {**os.environ,
           "DRP__GENERATOR__SEED": str(seed),
           "DRP__GENERATOR__PROFILE": profile}
    out = subprocess.run([sys.executable, "-m", "data_generator.generate"],
                         env=env, check=True, capture_output=True, text=True)
    match = GEN_LINE.search(out.stdout)
    return match.group(0) if match else out.stdout.strip()


def _proposed(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT sale_date, orders, revenue FROM gold_publications WHERE status = 'published'")
        days = {d: (int(o or 0), float(r or 0.0)) for d, o, r in cur.fetchall()}
        cur.execute("""
            SELECT r.source, r.batch_date, q.verdict
            FROM quality_verdicts q
            JOIN ingestion_runs r ON r.run_id = q.run_id
        """)
        flagged = {(s, d) for s, d, v in cur.fetchall() if v in ("WARN", "BLOCK")}
    return days, flagged


def _day_rows(seed: int, profile: str, arm: str, pred: dict, truth_days: dict) -> list[tuple]:
    rows = []
    for day, (t_orders, t_rev) in truth_days.items():
        p = pred.get(day)
        rows.append((seed, profile, arm, day, p is not None,
                     p[0] if p else None, p[1] if p else None,
                     t_orders, t_rev))
    return rows


def run_one(seed: int, profile: str, cfg: dict, registry, conn) -> tuple[dict, list[tuple]]:
    gen_line = generate(seed, profile, cfg)
    logger.info("seed=%d profile=%s: %s", seed, profile, gen_line)

    reset(cfg)
    ensure_buckets(cfg)

    start = time.perf_counter()
    pipeline.run(cfg=cfg)
    quality_runner.run(cfg=cfg)
    transform_runner.run(cfg=cfg)
    proposed_seconds = time.perf_counter() - start

    clean = Path(cfg["paths"]["clean"])
    truth_days = truth.clean_gold(clean, registry)
    universe = truth.clean_batches(clean)
    faulty = truth.faulty_batches(Path(cfg["paths"]["ground_truth"]) / "ground_truth.csv")
    pub_days, flagged = _proposed(conn)

    start = time.perf_counter()
    base = baseline.compute(cfg, registry)
    baseline_seconds = time.perf_counter() - start

    spark = get_spark("bench-check", use_s3=True, cfg=cfg)
    try:
        silver = spark.read.format("delta").load(silver_path(cfg))
        proposed_duplicates = silver.count() - silver.select("source", "order_key").distinct().count()
    finally:
        spark.stop()

    detect = scoring.detection(flagged, faulty, universe)
    outcome = {
        "proposed": {
            "detection_precision": detect["precision"],
            "detection_recall": detect["recall"],
            "detection_f1": detect["f1"],
            **scoring.gold_error(pub_days, truth_days),
            "duplicate_rows": float(proposed_duplicates),
            "run_seconds": round(proposed_seconds, 1),
        },
        "baseline": {
            **scoring.gold_error(base["days"], truth_days),
            "duplicate_rows": float(base["duplicate_rows"]),
            "run_seconds": round(baseline_seconds, 1),
        },
    }
    day_rows = (_day_rows(seed, profile, "proposed", pub_days, truth_days)
                + _day_rows(seed, profile, "baseline", base["days"], truth_days))
    return outcome, day_rows


def store(conn, seed: int, profile: str, outcome: dict) -> None:
    rows = [(seed, profile, arm, metric, value)
            for arm, metrics in outcome.items() for metric, value in metrics.items()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO benchmark_results (seed, profile, arm, metric, value) VALUES %s
            ON CONFLICT (seed, profile, arm, metric)
            DO UPDATE SET value = EXCLUDED.value, recorded_at = now()
            """,
            rows,
        )
    conn.commit()


def store_days(conn, rows: list[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO benchmark_days
                (seed, profile, arm, sale_date, published,
                 pred_orders, pred_revenue, truth_orders, truth_revenue) VALUES %s
            ON CONFLICT (seed, profile, arm, sale_date)
            DO UPDATE SET published = EXCLUDED.published,
                          pred_orders = EXCLUDED.pred_orders,
                          pred_revenue = EXCLUDED.pred_revenue,
                          truth_orders = EXCLUDED.truth_orders,
                          truth_revenue = EXCLUDED.truth_revenue
            """,
            rows,
        )
    conn.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the proposed-vs-baseline benchmark.")
    parser.add_argument("--yes", action="store_true", help="confirm the derived-data wipe")
    parser.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    parser.add_argument("--profiles", nargs="+", choices=DEFAULT_PROFILES, default=list(DEFAULT_PROFILES))
    parser.add_argument("--report-only", action="store_true", help="rebuild docs/results.md only")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    conn = run_log.connect(cfg)
    try:
        migrate(conn)
        if args.report_only:
            print("Report:", write_report(conn))
            return 0
        if not args.yes:
            print("Refusing to run: this wipes derived data. Pass --yes to confirm.")
            return 1

        registry = load_registry()
        for seed in args.seeds:
            for profile in args.profiles:
                outcome, day_rows = run_one(seed, profile, cfg, registry, conn)
                store(conn, seed, profile, outcome)
                store_days(conn, day_rows)
                logger.info("stored seed=%d profile=%s", seed, profile)
        print("Report:", write_report(conn))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())