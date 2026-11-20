"""Experiment design, statistics, and unit totals. Pure Python plus scipy, runs on the host."""
import json
import random
from datetime import date

import pytest

from experimentation import design, stats, units


def test_assignment_is_deterministic_and_roughly_balanced():
    ids = [f"C{i:05d}" for i in range(2000)]
    first = [design.assign("checkout", c) for c in ids]
    assert first == [design.assign("checkout", c) for c in ids]
    assert 900 < sum(v == "treatment" for v in first) < 1100


def test_assignment_depends_on_experiment_name():
    ids = [f"C{i:05d}" for i in range(500)]
    assert [design.assign("a", c) for c in ids] != [design.assign("b", c) for c in ids]


def test_sample_size_matches_textbook_value():
    assert stats.sample_size(delta=1.0, sd=1.0) == 16


def test_holm_adjustment_matches_hand_calculation():
    assert stats.holm([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.06, 0.06])


def test_cohens_d_on_known_samples():
    assert stats.cohens_d([2.0, 3.0, 4.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_two_prop_z_detects_large_difference():
    _, p = stats.two_prop_z(80, 100, 50, 100)
    assert p < 0.001


def test_analyze_recovers_planted_revenue_lift():
    pytest.importorskip("scipy")
    rng = random.Random(7)
    rows = []
    for i in range(10000):
        variant = "treatment" if i % 2 else "control"
        base = rng.gammavariate(2.0, 10.0)
        revenue = base * (1.10 if variant == "treatment" else 1.0)
        rows.append((variant, revenue, rng.random() < 0.5))
    results = {(r["metric"], r["test"]): r for r in stats.analyze(rows)}
    rev = results[("revenue", "welch_t")]
    assert 0.05 < rev["effect"] < 0.15
    assert rev["ci_low"] < rev["effect"] < rev["ci_high"]
    assert rev["p_adjusted"] < 0.05


def test_clean_units_applies_window_and_sums_orders(tmp_path):
    web = tmp_path / "web"
    web.mkdir()
    (web / "2025-01-01.jsonl").write_text(
        json.dumps({"customer_id": "C1", "order_ts": "2025-01-01 10:00:00",
                    "quantity": 2, "unit_price": 5.0}) + "\n"
        + json.dumps({"customer_id": "C1", "order_ts": "2025-02-01 10:00:00",
                      "quantity": 9, "unit_price": 9.0}) + "\n",
        encoding="utf-8",
    )
    totals = units.clean_units(tmp_path, date(2025, 1, 1), date(2025, 1, 31), "%Y-%m-%d %H:%M:%S")
    assert totals == {"C1": (10.0, 1)}
    assert units.clean_population(tmp_path) == {"C1"}