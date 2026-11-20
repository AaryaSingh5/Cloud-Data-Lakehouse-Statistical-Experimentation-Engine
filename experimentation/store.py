"""Experiment storage (10.1): experiments, one run per data source, and per-test results."""
from __future__ import annotations

from psycopg2.extras import execute_values


def upsert_experiment(conn, name: str, start, end, lift: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO experiments (name, start_date, end_date, planted_lift)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                planted_lift = EXCLUDED.planted_lift
            """,
            (name, start, end, lift),
        )
    conn.commit()


def store_run(conn, name: str, source: str, n_units: int, results: list[dict]) -> None:
    with conn.cursor() as cur:
        # re-running replaces the earlier run for this source
        cur.execute("DELETE FROM experiment_runs WHERE experiment = %s AND data_source = %s",
                    (name, source))
        cur.execute(
            "INSERT INTO experiment_runs (experiment, data_source, n_units) "
            "VALUES (%s, %s, %s) RETURNING run_id",
            (name, source, n_units),
        )
        run_id = cur.fetchone()[0]
        execute_values(
            cur,
            """
            INSERT INTO experiment_results
                (run_id, metric, test, statistic, p_value, p_adjusted, effect,
                 ci_low, ci_high, n_control, n_treatment, mean_control, mean_treatment)
            VALUES %s
            """,
            [(run_id, r["metric"], r["test"], r["statistic"], r["p_value"], r["p_adjusted"],
              r["effect"], r["ci_low"], r["ci_high"], r["n_control"], r["n_treatment"],
              r["mean_control"], r["mean_treatment"]) for r in results],
        )
    conn.commit()