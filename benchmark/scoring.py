"""Benchmark scoring (9.2) and significance tests (9.4). The statistics import is lazy."""
from __future__ import annotations

import statistics
from datetime import date

WRONG_TOLERANCE = 0.01  # a published day is "wrong" when revenue differs from truth by more than 1%


def detection(flagged: set, faulty: set, universe: set) -> dict:
    """Batch-level precision, recall, and F1. Batches outside the universe are ignored."""
    flagged = flagged & universe
    faulty = faulty & universe
    tp = len(flagged & faulty)
    fp = len(flagged - faulty)
    fn = len(faulty - flagged)
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    if precision is None or recall is None:
        f1 = None
    elif precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return {"precision": precision, "recall": recall, "f1": f1}


def _rel_error(pred_value: float, truth_value: float) -> float:
    return abs(pred_value - truth_value) / truth_value if truth_value else 0.0


def gold_error(pred: dict, truth: dict, tolerance: float = WRONG_TOLERANCE) -> dict:
    """Gold quality against fault-free truth.

    coverage                 share of truth days that were published
    revenue_error            mean relative revenue error, published days only
    orders_error             mean relative order-count error, published days only
    wrong_published_days     published days whose revenue differs by more than tolerance
    revenue_error_all_days   mean revenue error over every truth day; a held day counts as 1.0
    """
    days = set(truth)
    covered = [d for d in days if d in pred]

    rev = [_rel_error(pred[d][1], truth[d][1]) for d in covered if truth[d][1]]
    orders = [_rel_error(pred[d][0], truth[d][0]) for d in covered if truth[d][0]]
    wrong = sum(1 for d in covered if _rel_error(pred[d][1], truth[d][1]) > tolerance)
    wrong += sum(1 for d in pred if d not in truth)  # published days with no truth counterpart

    all_days = [
        1.0 if d not in pred else _rel_error(pred[d][1], truth[d][1])
        for d in days
    ]
    return {
        "coverage": len(covered) / len(days) if days else None,
        "revenue_error": statistics.fmean(rev) if rev else None,
        "orders_error": statistics.fmean(orders) if orders else None,
        "wrong_published_days": float(wrong),
        "revenue_error_all_days": statistics.fmean(all_days) if all_days else None,
    }


def mean_std(values) -> tuple[float | None, float | None]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.fmean(vals), statistics.stdev(vals)


def paired_pvalues(a, b) -> tuple[float | None, float | None]:
    """Paired t-test and Wilcoxon signed-rank p-values. None when there are fewer than 3 pairs."""
    pairs = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    if len(pairs) < 3:
        return None, None
    from scipy import stats

    xs, ys = zip(*pairs)
    t_p = float(stats.ttest_rel(xs, ys).pvalue)
    try:
        w_p = float(stats.wilcoxon(xs, ys).pvalue)
    except ValueError:  # every difference is zero
        w_p = None
    return t_p, w_p

SENSITIVITY_PENALTIES = (0.1, 0.5, 1.0)


def penalty_error(days: list[dict], penalty: float) -> float | None:
    """Mean revenue error over truth days. A day that was not published counts as penalty."""
    errs = []
    for d in days:
        if not d["published"]:
            errs.append(penalty)
        elif d["truth_revenue"]:
            errs.append(abs(d["pred_revenue"] - d["truth_revenue"]) / d["truth_revenue"])
    return statistics.fmean(errs) if errs else None