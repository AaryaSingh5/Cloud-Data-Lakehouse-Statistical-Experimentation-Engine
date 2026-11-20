"""Experiment report (10.7): the same experiment on truth, platform, and baseline data."""
from __future__ import annotations

from pathlib import Path

RESULTS_PATH = Path(__file__).resolve().parents[1] / "docs" / "experiments.md"
ORDER = ("truth", "platform", "baseline")

SQL = """
    SELECT e.planted_lift, r.data_source, r.n_units,
           res.metric, res.test, res.effect, res.ci_low, res.ci_high, res.p_adjusted
    FROM experiments e
    JOIN experiment_runs r ON r.experiment = e.name
    JOIN experiment_results res ON res.run_id = r.run_id
    WHERE e.name = %s
"""


def _fmt(x, pct: bool = False) -> str:
    if x is None:
        return "–"
    return f"{x:+.1%}" if pct else f"{x:.4g}"


def write_report(conn, name: str, path: Path = RESULTS_PATH) -> Path:
    with conn.cursor() as cur:
        cur.execute(SQL, (name,))
        rows = cur.fetchall()
    if not rows:
        raise ValueError(f"no results for experiment {name}")

    planted = rows[0][0]
    by: dict = {}
    for _lift, source, n, metric, test, effect, lo, hi, p_adj in rows:
        by.setdefault(source, {"n": n})[(metric, test)] = (effect, lo, hi, p_adj)

    lines = [
        f"# Experiment: {name}",
        "",
        f"Planted lift: {planted:.1%}. The same planted lift is applied to every data source.",
        "",
        "| source | units | revenue lift | 95% CI | p (Holm) | conversion difference | p (Holm) "
        "| revenue significant | bias vs planted |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for source in ORDER:
        if source not in by:
            continue
        d = by[source]
        eff, lo, hi, p_rev = d[("revenue", "welch_t")]
        c_eff, _, _, p_conv = d[("conversion", "two_prop_z")]
        sig = "yes" if p_rev < 0.05 else "no"
        bias = eff - planted if eff is not None else None
        lines.append(
            f"| {source} | {d['n']} | {_fmt(eff, True)} | [{_fmt(lo, True)}, {_fmt(hi, True)}] "
            f"| {_fmt(p_rev)} | {_fmt(c_eff)} | {_fmt(p_conv)} | {sig} | {_fmt(bias, True)} |"
        )
    lines += [
        "",
        "Read each row against the truth row. A source whose lift, interval, or significance "
        "differs from truth has had its conclusion changed by its data.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path