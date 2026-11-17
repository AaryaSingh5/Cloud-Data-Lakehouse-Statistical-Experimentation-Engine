"""Quality engine tests: pure Python, no Spark or Postgres needed.

Clean rows come from the real generator, so validity rules are tested against
the same data shape the injector produces.
"""
import random
from datetime import date, datetime

import pytest

from data_generator.sources import build_catalog, make_clean_rows
from quality.config import load_registry, load_rules
from quality.engine import evaluate
from quality.types import BatchInput, BatchMetrics, RawRecord

REG = load_registry()
RULES = load_rules()


def clean(source: str, n: int = 50) -> list[dict]:
    return make_clean_rows(source, date(2025, 1, 2), n, random.Random(7), build_catalog(7))


def run(source: str, rows: list, history=()):
    observed: list[str] = []
    for r in rows:
        if r is not None:
            observed = list(r.keys())
            break
    batch = BatchInput(
        source=source,
        batch_id="test-batch",
        batch_date=date(2025, 1, 2),
        observed_columns=observed,
        records=[RawRecord(i, r) for i, r in enumerate(rows)],
    )
    return evaluate(batch, REG.sources[source], RULES, REG.ts_format, list(history))


def fired(res) -> set[str]:
    return {o.rule_id for o in res.outcomes if o.triggered}


@pytest.mark.parametrize("source", ["pos", "web", "partner"])
def test_clean_batch_passes_for_every_source(source):
    res = run(source, clean(source))
    assert res.verdict == "PASS", fired(res)


def test_known_rename_is_auto_mapped_with_warning():
    rows = [{("qty" if k == "quantity" else k): v for k, v in r.items()} for r in clean("pos")]
    res = run("pos", rows)
    assert res.verdict == "WARN"
    assert "schema_alias_mapped" in fired(res)
    assert "schema_breaking" not in fired(res)
    assert res.bad_rows == 0


def test_unknown_rename_blocks():
    rows = [{("store_id_v2" if k == "store_id" else k): v for k, v in r.items()} for r in clean("pos")]
    res = run("pos", rows)
    assert res.verdict == "BLOCK"
    assert "schema_breaking" in fired(res)


def test_extra_column_is_auto_merged_with_warning():
    rows = [{**r, "discount_code": "WELCOME10"} for r in clean("pos")]
    res = run("pos", rows)
    assert res.verdict == "WARN"
    assert "schema_unknown_column" in fired(res)
    assert "schema_breaking" not in fired(res)


def test_dropped_column_blocks():
    rows = [{k: v for k, v in r.items() if k != "store_id"} for r in clean("pos")]
    assert run("pos", rows).verdict == "BLOCK"


def test_currency_symbol_in_price_blocks():
    rows = [{**r, "unit_price": f"${r['unit_price']:.2f}"} for r in clean("pos")]
    res = run("pos", rows)
    assert res.verdict == "BLOCK"
    assert "schema_breaking" in fired(res)


def test_timestamp_format_change_blocks():
    rows = [
        {**r, "order_ts": datetime.strptime(r["order_ts"], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")}
        for r in clean("pos")
    ]
    assert run("pos", rows).verdict == "BLOCK"


def test_null_values_warn_and_are_row_flagged():
    rows = clean("pos")
    for i in range(3):
        rows[i]["product_id"] = None
    res = run("pos", rows)
    assert res.verdict == "WARN"
    assert "null_value" in fired(res)
    assert res.bad_rows == 3


def test_out_of_range_values_are_flagged():
    rows = clean("pos")
    rows[0]["quantity"] = -1
    rows[1]["unit_price"] = 999.0
    res = run("pos", rows)
    assert "invalid_value" in fired(res)
    assert res.bad_rows == 2
    assert res.verdict == "WARN"


def test_exact_duplicate_warns():
    rows = clean("pos")
    rows.append(dict(rows[0]))
    res = run("pos", rows)
    assert "duplicate_exact" in fired(res)
    assert res.verdict == "WARN"


def test_key_conflict_blocks():
    rows = clean("pos")
    dup = dict(rows[0])
    dup["quantity"] = rows[0]["quantity"] + 1
    rows.append(dup)
    res = run("pos", rows)
    assert "duplicate_key_conflict" in fired(res)
    assert res.verdict == "BLOCK"


def test_unparseable_record_is_flagged():
    rows = clean("web")
    rows[3] = None
    res = run("web", rows)
    assert "unparseable_record" in fired(res)
    assert res.verdict == "WARN"


def test_empty_batch_blocks():
    res = run("pos", [])
    assert res.verdict == "BLOCK"
    assert "empty_batch" in fired(res)


def test_bad_row_rate_over_threshold_blocks():
    rows = clean("pos")
    for i in range(10):  # 10 of 50 rows = 20%, above the 10% limit
        rows[i]["quantity"] = -1
    res = run("pos", rows)
    assert "bad_row_rate" in fired(res)
    assert res.verdict == "BLOCK"


def test_volume_anomaly_against_history_warns():
    res = run("pos", clean("pos", n=50), history=[BatchMetrics(row_count=500)] * 5)
    assert "volume_anomaly" in fired(res)
    assert res.verdict == "WARN"


def test_anomaly_checks_skipped_without_enough_history():
    res = run("pos", clean("pos"), history=[BatchMetrics(row_count=500)] * 2)
    assert res.verdict == "PASS"


def test_matching_history_passes():
    res = run("pos", clean("pos"), history=[BatchMetrics(row_count=50)] * 5)
    assert res.verdict == "PASS", fired(res)