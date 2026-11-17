"""Line numbers and payload shape for landing files. Pure Python, runs on the host."""
import json

from ingestion.landing_rows import landing_rows


def test_csv_line_numbers_start_at_two_and_skip_blank_lines(tmp_path):
    p = tmp_path / "pos_2025-01-01.csv"
    p.write_text("order_id,quantity\no1,2\n\no2,3\n", encoding="utf-8")
    assert [n for n, _ in landing_rows(p, "csv")] == [2, 4]


def test_csv_empty_values_are_omitted(tmp_path):
    p = tmp_path / "pos_2025-01-01.csv"
    p.write_text("order_id,quantity,unit_price\no1,,9.99\n", encoding="utf-8")
    (_, raw), = landing_rows(p, "csv")
    assert json.loads(raw) == {"order_id": "o1", "unit_price": "9.99"}


def test_csv_short_row_drops_missing_trailing_fields(tmp_path):
    p = tmp_path / "pos_2025-01-01.csv"
    p.write_text("order_id,quantity,unit_price\no1,2\n", encoding="utf-8")
    (_, raw), = landing_rows(p, "csv")
    assert json.loads(raw) == {"order_id": "o1", "quantity": "2"}


def test_csv_header_only_is_empty(tmp_path):
    p = tmp_path / "pos_2025-01-01.csv"
    p.write_text("order_id,quantity\n", encoding="utf-8")
    assert landing_rows(p, "csv") == []


def test_jsonl_keeps_raw_and_corrupt_lines(tmp_path):
    p = tmp_path / "web_2025-01-01.jsonl"
    p.write_text('{"a":1}\n{"a":\n', encoding="utf-8")
    assert landing_rows(p, "jsonl") == [(1, '{"a":1}'), (2, '{"a":')]


def test_zero_byte_file_is_empty(tmp_path):
    p = tmp_path / "web_2025-01-01.jsonl"
    p.write_bytes(b"")
    assert landing_rows(p, "jsonl") == []