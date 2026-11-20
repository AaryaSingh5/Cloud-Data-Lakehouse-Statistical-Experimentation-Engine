"""Experiment runner (10.1, 10.7). One experiment on three data sources:

  truth     fault-free clean data: what the business should have seen
  platform  Gold customer_daily: what the platform published
  baseline  plain pipeline Bronze: what loading everything would have shown

The same planted lift is applied to each source, so differences between the estimates
come from the data alone.

Usage:
    python -m experimentation.run --name checkout_test --start 2025-01-01 --end 2025-01-14 --lift 0.10
    python -m experimentation.run --name checkout_test --start 2025-01-01 --end 2025-01-14 --lift 0.10 --power
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from common.config import load_config
from common.migrate import apply as migrate
from common.spark import get_spark
from experimentation import design, report, stats, store, units
from ingestion import run_log
from quality.config import load_registry

SOURCES = ("truth", "platform", "baseline")
logger = logging.getLogger("experiment")


def _population_and_truth(cfg: dict, start: date, end: date):
    clean = Path(cfg["paths"]["clean"])
    ts_format = load_registry().ts_format
    return units.clean_population(clean), units.clean_units(clean, start, end, ts_format)


def power_check(name: str, start: date, end: date, lift: float, cfg: dict) -> int:
    population, truth_units = _population_and_truth(cfg, start, end)
    rows = design.outcomes(truth_units, population, name, 0.0)
    control = [r for v, r, _ in rows if v == "control"]
    return design.required_units_per_arm(control, lift)


def run_experiment(name: str, start: date, end: date, lift: float,
                   cfg: dict | None = None, sources=SOURCES) -> dict:
    cfg = cfg or load_config()
    population, truth_units = _population_and_truth(cfg, start, end)
    per_source = {"truth": truth_units}

    spark = None
    try:
        if {"platform", "baseline"} & set(sources):
            spark = get_spark("experiment", use_s3=True, cfg=cfg)
        if "platform" in sources:
            per_source["platform"] = units.platform_units(spark, cfg, start, end)
        if "baseline" in sources:
            per_source["baseline"] = units.baseline_units(spark, cfg, start, end)
    finally:
        if spark is not None:
            spark.stop()

    conn = run_log.connect(cfg)
    summary: dict = {}
    try:
        migrate(conn)
        store.upsert_experiment(conn, name, start, end, lift)
        for source in sources:
            rows = design.outcomes(per_source[source], population, name, lift)
            results = stats.analyze(rows)
            store.store_run(conn, name, source, len(rows), results)
            summary[source] = results
            logger.info("stored %s source=%s units=%d", name, source, len(rows))
        report.write_report(conn, name)
    finally:
        conn.close()
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run an A/B experiment on truth, platform, and baseline data.")
    parser.add_argument("--name", required=True)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--lift", required=True, type=float, help="planted relative lift, e.g. 0.10")
    parser.add_argument("--sources", nargs="+", choices=SOURCES, default=list(SOURCES))
    parser.add_argument("--power", action="store_true", help="print the sample size needed and exit")
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("--end must be on or after --start")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    if args.power:
        n = power_check(args.name, args.start, args.end, args.lift, cfg)
        print(f"Units needed per arm to detect a {args.lift:.0%} lift (alpha 0.05, power 0.8): {n}")
        return 0

    summary = run_experiment(args.name, args.start, args.end, args.lift, cfg, tuple(args.sources))
    for source, results in summary.items():
        rev = next(r for r in results if r["metric"] == "revenue" and r["test"] == "welch_t")
        print(f"{source:9} revenue lift={rev['effect']:+.1%} p_adj={rev['p_adjusted']:.4g}")
    print(f"Report: {report.RESULTS_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())