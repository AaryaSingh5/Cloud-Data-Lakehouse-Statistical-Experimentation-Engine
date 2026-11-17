from common.config import load_config


def test_loads_defaults():
    cfg = load_config(env={})
    assert cfg["postgres"]["port"] == 5432
    assert set(cfg["storage"]["buckets"]) == {"landing", "bronze", "silver", "gold", "quarantine"}


def test_env_override_nested_and_typed():
    cfg = load_config(env={"DRP__POSTGRES__HOST": "localhost", "DRP__GENERATOR__DAYS": "3"})
    assert cfg["postgres"]["host"] == "localhost"
    assert cfg["generator"]["days"] == 3
