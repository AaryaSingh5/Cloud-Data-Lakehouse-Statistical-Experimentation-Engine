"""Statistical anomaly checks against a rolling baseline (4.7)."""
from __future__ import annotations

from statistics import median

from quality.types import BatchMetrics, RuleOutcome


def check_anomalies(
    current: BatchMetrics,
    history: list[BatchMetrics],
    defaults: dict,
) -> list[RuleOutcome]:
    min_hist = int(defaults["min_history_batches"])
    if len(history) < min_hist:
        note = f"skipped: {len(history)} prior batches, need {min_hist}"
        return [
            RuleOutcome("volume_anomaly", False, None, "", note),
            RuleOutcome("distribution_anomaly", False, None, "", note),
        ]

    lo_v, hi_v = (float(x) for x in defaults["volume_ratio"])
    lo_d, hi_d = (float(x) for x in defaults["distribution_ratio"])

    med_rows = median(h.row_count for h in history)
    vol_ratio = current.row_count / med_rows if med_rows else None
    vol_bad = vol_ratio is not None and not (lo_v <= vol_ratio <= hi_v)
    outcomes = [
        RuleOutcome(
            "volume_anomaly",
            vol_bad,
            round(vol_ratio, 3) if vol_ratio is not None else None,
            f"[{lo_v}, {hi_v}] x median",
            f"{current.row_count} rows vs median {med_rows}",
        )
    ]

    dist_bad = False
    worst: float | None = None
    details: list[str] = []
    for name in ("mean_unit_price", "mean_quantity"):
        cur_val = getattr(current, name)
        base = [getattr(h, name) for h in history if getattr(h, name) is not None]
        if cur_val is None or not base:
            continue
        med = median(base)
        if not med:
            continue
        ratio = cur_val / med
        if worst is None or abs(ratio - 1) > abs(worst - 1):
            worst = ratio
        if not (lo_d <= ratio <= hi_d):
            dist_bad = True
        details.append(f"{name} {cur_val:.2f} vs median {med:.2f}")
    outcomes.append(
        RuleOutcome(
            "distribution_anomaly",
            dist_bad,
            round(worst, 3) if worst is not None else None,
            f"[{lo_d}, {hi_d}] x median",
            "; ".join(details) or "no comparable metric",
        )
    )
    return outcomes
    