"""Metric helpers and dashboard syntax. Pure Python, runs on the host."""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

from monitoring.metrics import freshness_hours, pass_rate

NOW = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)
APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_freshness_is_hours_since_last_load():
    assert freshness_hours(NOW, NOW - timedelta(hours=3)) == 3.0


def test_freshness_is_none_for_a_source_that_never_loaded():
    assert freshness_hours(NOW, None) is None


def test_pass_rate_is_share_of_passes():
    assert pass_rate(3, 4) == 0.75


def test_pass_rate_is_none_without_data():
    assert pass_rate(0, 0) is None


def test_dashboard_source_is_valid_python():
    ast.parse(APP.read_text(encoding="utf-8"))