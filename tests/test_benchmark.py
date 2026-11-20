"""Benchmark scoring and ground-truth parsing. Pure Python, runs on the host."""
import pytest
from datetime import date

from benchmark.scoring import detection, gold_error, mean_std
from benchmark.truth import clean_gold, faulty_batches
from quality.config import load_registry

REG = load_registry()


def test_detection_perfect_flags_score_one():
    faulty = {("pos", date(2025, 1, 1)), ("web", date(2025, 1, 1))}
    universe = faulty | {("partner", date(2025, 1, 1))}
    scores = detection(set(faulty), faulty, universe)
    assert scores == {"precision": 1.0, "recall": 1.0, "f1": 1.0}


def test_detection_counts_false_alarms_against_precision():
    faulty = {("pos", date(2025, 1, 1))}
    universe = {("pos", date(2025, 1, 1)), ("web", date(2025, 1, 1))}
    scores = detection({("pos", date(2025, 1, 1)), ("web", date(2025, 1, 1))}, faulty, universe)
    assert scores["precision"] == 0.5
    assert scores["recall"] == 1.0


def test_detection_recall_is_undefined_without_faults():
    scores = detection(set(), set(), {("pos", date(2025, 1, 1))})
    assert scores["recall"] is None


def test_gold_error_coverage_and_relative_error():
    truth_days = {date(2025, 1, 1): (10, 100.0), date(2025, 1, 2): (10, 100.0)}
    pred = {date(2025, 1, 1): (10, 110.0)}
    out = gold_error(pred, truth_days)
    assert out["coverage"] == 0.5
    assert out["revenue_error"] == pytest.approx(0.10)
    assert out["orders_error"] == 0.0


def test_mean_std_ignores_missing_values():
    mean, std = mean_std([1.0, None, 3.0])
    assert mean == 2.0
    assert std == pytest.approx(1.4142, rel=1e-3)


def test_faulty_batches_excludes_timing_faults(tmp_path):
    gt = tmp_path / "ground_truth.csv"
    gt.write_text(
        "source,batch_date,fault_type\n"
        "pos,2025-01-03,duplicate_key_conflict\n"
        "web,2025-01-04,late_arriving\n",
        encoding="utf-8",
    )
    assert faulty_batches(gt) == {("pos", date(2025, 1, 3))}


def test_clean_gold_sums_csv_and_jsonl(tmp_path):
    (tmp_path / "pos").mkdir()
    (tmp_path / "web").mkdir()
    (tmp_path / "pos" / "2025-01-01.csv").write_text(
        "order_id,order_ts,store_id,product_id,quantity,unit_price,payment_method\n"
        "o1,2025-01-01 10:00:00,S01,P0001,2,10.00,card\n",
        encoding="utf-8",
    )
    (tmp_path / "web" / "2025-01-01.jsonl").write_text(
        '{"order_id":"w1","order_ts":"2025-01-01 12:00:00","customer_id":"C00001",'
        '"product_id":"P0002","quantity":1,"unit_price":5.5,"country":"IN"}\n',
        encoding="utf-8",
    )
    gold = clean_gold(tmp_path, REG)
    assert gold == {date(2025, 1, 1): (2, 25.5)}


def test_paired_pvalues_need_scipy_and_three_pairs():
    pytest.importorskip("scipy")
    from benchmark.scoring import paired_pvalues

    assert paired_pvalues([1.0, 2.0], [1.0, 2.0]) == (None, None)
    t_p, _ = paired_pvalues([1.0, 2.0, 3.0, 4.0], [2.0, 3.1, 4.2, 5.0])
    assert 0.0 <= t_p <= 1.0

def test_wrong_published_days_counts_days_off_by_more_than_tolerance():
    truth_days = {date(2025, 1, 1): (10, 100.0), date(2025, 1, 2): (10, 100.0)}
    pred = {date(2025, 1, 1): (10, 100.5), date(2025, 1, 2): (10, 150.0)}
    assert gold_error(pred, truth_days)["wrong_published_days"] == 1.0


def test_revenue_error_all_days_counts_held_days_as_full_error():
    truth_days = {date(2025, 1, 1): (10, 100.0), date(2025, 1, 2): (10, 100.0)}
    pred = {date(2025, 1, 1): (10, 110.0)}
    out = gold_error(pred, truth_days)
    assert out["revenue_error_all_days"] == pytest.approx((0.10 + 1.0) / 2)

def test_penalty_error_applies_penalty_to_unpublished_days():
    from benchmark.scoring import penalty_error

    days = [
        {"published": True, "pred_revenue": 110.0, "truth_revenue": 100.0},
        {"published": False, "pred_revenue": None, "truth_revenue": 100.0},
    ]
    assert penalty_error(days, 0.5) == pytest.approx((0.10 + 0.5) / 2)
    assert penalty_error(days, 1.0) == pytest.approx((0.10 + 1.0) / 2)