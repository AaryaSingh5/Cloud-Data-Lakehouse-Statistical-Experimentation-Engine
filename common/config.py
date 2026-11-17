"""YAML config loader with env overrides.

Override any nested key with DRP__SECTION__KEY, e.g. DRP__POSTGRES__HOST=localhost
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"


def _coerce(value: str):
    try:
        return yaml.safe_load(value)
    except yaml.YAMLError:
        return value


def load_config(path: str | Path | None = None, env: dict | None = None) -> dict:
    cfg = yaml.safe_load(Path(path or DEFAULT_PATH).read_text())
    env = os.environ if env is None else env
    for name, raw in env.items():
        if not name.startswith("DRP__"):
            continue
        keys = [k.lower() for k in name[5:].split("__")]
        node = cfg
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = _coerce(raw)
    return cfg
