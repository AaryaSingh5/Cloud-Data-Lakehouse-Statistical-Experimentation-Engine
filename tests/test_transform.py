"""Pure tests for conforming rows and the publication gate. No Spark or Postgres needed."""
from datetime import date, datetime

from quality.config import load_registry
from transform.conform import CONFORM, SILVER_COLUMNS, conform_row
from transform.gate import decide

REG = load_registry()
LOADED = datetime(2025, 1, 1)


def test_pos_row_keeps_store_and_payment():
    canon = {
        "order_id": "pos-1", "order_ts": "2025-01-03 10:00:00", "store_id": "S01",
        "product_id": "P0001", "quantity": "2", "unit_price": "9.99", "payment_method": "card",
    }
    row = conform_row("pos", canon, REG.ts_format, "b1", date(2025, 1, 3), "r1", LOADED)
    assert row["order_key"] == "pos-1"
    assert row["sale_date"] == date(2025, 1, 3)
    assert row["quantity"] == 2
    assert row["revenue"] == 19.98
    assert row["store_id"] == "S01"
    assert row["customer_id"] is None
    assert set(row) == set(SILVER_COLUMNS)


def test_web_row_keeps_customer_and_country():
    canon = {
        "order_id": "w-1", "order_ts": "2025-01-03 12:00:00", "customer_id": "C00001",
        "product_id": "P0002", "quantity": "1", "unit_price": "4.5", "country": "IN",
    }
    row = conform_row("web", canon, REG.ts_format, "b1", date(2025, 1, 3), "r1", LOADED)
    assert row["customer_id"] == "C00001"
    assert row["country"] == "IN"
    assert row["store_id"] is None


def test_partner_row_maps_renamed_fields_to_silver_names():
    canon = {
        "order_ref": "p-1", "sold_at": "2025-01-03 11:00:00", "sku": "P0002",
        "units": "3", "price_usd": "5.00", "region": "APAC",
    }
    row = conform_row("partner", canon, REG.ts_format, "b1", date(2025, 1, 3), "r1", LOADED)
    assert row["order_key"] == "p-1"
    assert row["product_id"] == "P0002"
    assert row["quantity"] == 3
    assert row["unit_price"] == 5.0
    assert row["region"] == "APAC"


def test_conform_fields_exist_in_registry():
    for source, fields in CONFORM.items():
        columns = REG.sources[source].versions[-1].columns
        for silver_col, canonical in fields.items():
            assert canonical in columns, f"{source}.{silver_col} -> {canonical} not in registry"


def test_gate_publishes_when_every_source_ok():
    status, ok, not_ok = decide(["pos", "web", "partner"], {"pos": "PASS", "web": "WARN", "partner": "PASS"})
    assert status == "published"
    assert not_ok == []


def test_gate_holds_when_a_source_is_blocked():
    status, _, not_ok = decide(["pos", "web", "partner"], {"pos": "PASS", "web": "BLOCK", "partner": "PASS"})
    assert status == "held"
    assert not_ok == ["web"]


def test_gate_holds_when_a_source_is_missing():
    status, _, not_ok = decide(["pos", "web", "partner"], {"pos": "PASS", "partner": "PASS"})
    assert status == "held"
    assert not_ok == ["web"]