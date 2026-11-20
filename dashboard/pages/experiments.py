"""Experiments page (10.6): one experiment's results on truth, platform, and baseline data."""
import pandas as pd
import streamlit as st

from common.config import load_config
from ingestion import run_log

st.set_page_config(page_title="Experiments", layout="wide")
st.title("Experiments")

conn = run_log.connect(load_config())
try:
    names = pd.read_sql("SELECT name, planted_lift FROM experiments ORDER BY name", conn)
    if names.empty:
        st.info("No experiments yet. Run: make experiment NAME=... START=... END=... LIFT=...")
    else:
        choice = st.selectbox("Experiment", names["name"].tolist())
        lift = float(names.loc[names["name"] == choice, "planted_lift"].iloc[0])
        st.write(f"Planted lift: {lift:.1%}")

        res = pd.read_sql(
            """
            SELECT r.data_source, res.metric, res.test, res.effect,
                   res.ci_low, res.ci_high, res.p_adjusted, res.n_control, res.n_treatment
            FROM experiment_runs r
            JOIN experiment_results res ON res.run_id = r.run_id
            WHERE r.experiment = %s
            ORDER BY r.data_source, res.metric, res.test
            """,
            conn,
            params=(choice,),
        )
        st.dataframe(res, hide_index=True)

        rev = res[(res["metric"] == "revenue") & (res["test"] == "welch_t")]
        if not rev.empty:
            st.subheader("Estimated revenue lift by data source")
            st.bar_chart(rev.set_index("data_source")["effect"])
            st.caption(
                f"Planted lift is {lift:.1%}. A source whose estimate or significance differs "
                "from truth has had its conclusion changed by its data."
            )
finally:
    conn.close()