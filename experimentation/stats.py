"""Statistical tests (10.2), effect sizes and intervals (10.3), Holm correction, and sample size (10.4).

Conventions: effects are treatment minus control. scipy is imported inside the functions
that need it, so the pure helpers load without it.
"""
from __future__ import annotations

import math
from statistics import NormalDist, fmean, stdev

ALPHA = 0.05
POWER = 0.8


def _var(xs) -> float:
    return stdev(xs) ** 2


def welch_t(treatment, control) -> tuple[float, float]:
    from scipy import stats

    if _var(treatment) == 0 and _var(control) == 0:
        return 0.0, 1.0
    res = stats.ttest_ind(treatment, control, equal_var=False)
    return float(res.statistic), float(res.pvalue)


def welch_ci(treatment, control, alpha: float = ALPHA) -> tuple[float, float]:
    from scipy import stats

    diff = float(fmean(treatment) - fmean(control))
    na, nb = len(treatment), len(control)
    va, vb = _var(treatment) / na, _var(control) / nb
    if va + vb == 0:
        return diff, diff
    se = math.sqrt(va + vb)
    df = (va + vb) ** 2 / (va ** 2 / (na - 1) + vb ** 2 / (nb - 1))
    t = float(stats.t.ppf(1 - alpha / 2, df))
    return float(diff - t * se), float(diff + t * se)


def mann_whitney(treatment, control) -> float:
    from scipy import stats

    try:
        return float(stats.mannwhitneyu(treatment, control, alternative="two-sided").pvalue)
    except ValueError:
        return 1.0


def cohens_d(treatment, control) -> float:
    na, nb = len(treatment), len(control)
    pooled = math.sqrt(((na - 1) * _var(treatment) + (nb - 1) * _var(control)) / (na + nb - 2))
    return (fmean(treatment) - fmean(control)) / pooled if pooled else 0.0


def two_prop_z(conv_t: int, n_t: int, conv_c: int, n_c: int) -> tuple[float, float]:
    pooled = (conv_t + conv_c) / (n_t + n_c)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n_t + 1 / n_c))
    if se == 0:
        return 0.0, 1.0
    z = (conv_t / n_t - conv_c / n_c) / se
    return z, 2 * (1 - NormalDist().cdf(abs(z)))


def prop_diff_ci(conv_t: int, n_t: int, conv_c: int, n_c: int, alpha: float = ALPHA) -> tuple[float, float]:
    pt, pc = conv_t / n_t, conv_c / n_c
    se = math.sqrt(pt * (1 - pt) / n_t + pc * (1 - pc) / n_c)
    z = NormalDist().inv_cdf(1 - alpha / 2)
    return (pt - pc) - z * se, (pt - pc) + z * se


def chi2_2x2(conv_t: int, n_t: int, conv_c: int, n_c: int) -> float:
    from scipy import stats

    table = [[conv_t, n_t - conv_t], [conv_c, n_c - conv_c]]
    try:
        return float(stats.chi2_contingency(table, correction=False)[1])
    except ValueError:
        return 1.0


def holm(pvals: list[float]) -> list[float]:
    """Holm step-down adjusted p-values, returned in the original order."""
    m = len(pvals)
    order = sorted(range(m), key=lambda i: pvals[i])
    adjusted = [0.0] * m
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (m - rank) * pvals[i]))
        adjusted[i] = running
    return adjusted


def sample_size(delta: float, sd: float, alpha: float = ALPHA, power: float = POWER) -> int:
    """Units per arm to detect an absolute difference delta, two-sided, normal approximation."""
    if delta == 0 or sd == 0:
        raise ValueError("delta and sd must be non-zero")
    z_a = NormalDist().inv_cdf(1 - alpha / 2)
    z_b = NormalDist().inv_cdf(power)
    return math.ceil(2 * ((z_a + z_b) * sd / delta) ** 2)


def _row(metric, test, statistic, p_value, effect, ci, nc, nt, mc, mt) -> dict:
    lo, hi = ci if ci else (None, None)
    return {
        "metric": metric, "test": test, "statistic": statistic, "p_value": p_value,
        "effect": effect, "ci_low": lo, "ci_high": hi,
        "n_control": nc, "n_treatment": nt, "mean_control": mc, "mean_treatment": mt,
    }


def analyze(rows) -> list[dict]:
    """rows: (variant, revenue, converted) tuples. Returns one dict per test, Holm-adjusted."""
    rc = [r for v, r, _ in rows if v == "control"]
    rt = [r for v, r, _ in rows if v == "treatment"]
    cc = [int(c) for v, _, c in rows if v == "control"]
    ct = [int(c) for v, _, c in rows if v == "treatment"]
    nc, nt = len(rc), len(rt)
    if nc < 2 or nt < 2:
        raise ValueError("need at least 2 units per variant")

    mc, mt = fmean(rc), fmean(rt)
    rel = (mt - mc) / mc if mc else None
    rel_ci = None
    if mc:
        lo, hi = welch_ci(rt, rc)
        rel_ci = (lo / mc, hi / mc)
    t_stat, t_p = welch_t(rt, rc)

    pc, pt = sum(cc) / nc, sum(ct) / nt
    z, z_p = two_prop_z(sum(ct), nt, sum(cc), nc)
    results = [
        _row("revenue", "welch_t", t_stat, t_p, rel, rel_ci, nc, nt, mc, mt),
        _row("revenue", "mann_whitney", None, mann_whitney(rt, rc),
             cohens_d(rt, rc), None, nc, nt, mc, mt),
        _row("conversion", "two_prop_z", z, z_p, pt - pc,
             prop_diff_ci(sum(ct), nt, sum(cc), nc), nc, nt, pc, pt),
        _row("conversion", "chi2", None, chi2_2x2(sum(ct), nt, sum(cc), nc),
             pt - pc, None, nc, nt, pc, pt),
    ]
    for r, adj in zip(results, holm([r["p_value"] for r in results])):
        r["p_adjusted"] = adj
    return results