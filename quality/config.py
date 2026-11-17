"""Loads the schema registry and the rules config from config/*.yaml."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_PATH = ROOT / "config" / "schemas.yaml"
RULES_PATH = ROOT / "config" / "quality_rules.yaml"


@dataclass(frozen=True)
class SchemaVersion:
    version: int
    columns: dict[str, str]   # canonical name -> type (string, int, float, timestamp)
    aliases: dict[str, str]   # observed name -> canonical name


@dataclass(frozen=True)
class SourceSchema:
    name: str
    fmt: str
    key: str
    price_field: str
    quantity_field: str
    versions: tuple[SchemaVersion, ...]


@dataclass(frozen=True)
class Registry:
    ts_format: str
    sources: dict[str, SourceSchema]


def load_registry(path: Path | None = None) -> Registry:
    raw = yaml.safe_load((path or SCHEMAS_PATH).read_text(encoding="utf-8"))
    sources: dict[str, SourceSchema] = {}
    for name, s in raw["sources"].items():
        versions = tuple(
            SchemaVersion(
                version=int(v["version"]),
                columns=dict(v["columns"]),
                aliases=dict(v.get("aliases") or {}),
            )
            for v in s["versions"]
        )
        sources[name] = SourceSchema(
            name=name,
            fmt=s["format"],
            key=s["key"],
            price_field=s["metrics"]["price"],
            quantity_field=s["metrics"]["quantity"],
            versions=versions,
        )
    return Registry(ts_format=raw["ts_format"], sources=sources)


@dataclass(frozen=True)
class RulesConfig:
    defaults: dict
    unknown_column: str
    rules: dict[str, dict]
    validity: dict[str, dict[str, dict]]

    def severity(self, rule_id: str) -> str:
        return self.rules.get(rule_id, {}).get("severity", "warn")


def load_rules(path: Path | None = None) -> RulesConfig:
    raw = yaml.safe_load((path or RULES_PATH).read_text(encoding="utf-8"))
    return RulesConfig(
        defaults=dict(raw["defaults"]),
        unknown_column=raw["schema"]["unknown_column"],
        rules=raw["rules"],
        validity=raw["validity"],
    )