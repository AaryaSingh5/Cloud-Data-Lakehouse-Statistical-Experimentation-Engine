"""Three sources, same business entity (a sales line), three different native formats and column names."""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

SOURCES = {
    "pos": {  # in-store point-of-sale export, CSV
        "fmt": "csv",
        "columns": ["order_id", "order_ts", "store_id", "product_id", "quantity", "unit_price", "payment_method"],
        "key": "order_id", "ts": "order_ts", "product": "product_id", "qty": "quantity", "price": "unit_price",
        "extras": {
            "store_id": [f"S{i:02d}" for i in range(1, 11)],
            "payment_method": ["card", "cash", "upi", "wallet"],
        },
    },
    "web": {  # e-commerce events, JSON Lines
        "fmt": "jsonl",
        "columns": ["order_id", "order_ts", "customer_id", "product_id", "quantity", "unit_price", "country"],
        "key": "order_id", "ts": "order_ts", "product": "product_id", "qty": "quantity", "price": "unit_price",
        "extras": {
            "customer_id": [f"C{i:05d}" for i in range(1, 2001)],
            "country": ["IN", "US", "GB", "DE", "SG"],
        },
    },
    "partner": {  # reseller feed, CSV with different naming
        "fmt": "csv",
        "columns": ["order_ref", "sold_at", "sku", "units", "price_usd", "region"],
        "key": "order_ref", "ts": "sold_at", "product": "sku", "qty": "units", "price": "price_usd",
        "extras": {"region": ["APAC", "EMEA", "AMER"]},
    },
}


def build_catalog(seed: int, size: int = 50) -> list[dict]:
    rng = random.Random(f"{seed}|catalog")
    return [{"sku": f"P{i:04d}", "price": round(rng.uniform(2, 200), 2)} for i in range(1, size + 1)]


def make_clean_rows(source: str, day: date, n: int, rng: random.Random, catalog: list[dict]) -> list[dict]:
    m = SOURCES[source]
    base = datetime.combine(day, datetime.min.time())
    rows = []
    for i in range(n):
        p = rng.choice(catalog)
        row = {
            m["key"]: f"{source}-{day.isoformat()}-{i:05d}",
            m["ts"]: (base + timedelta(seconds=rng.randint(0, 86399))).strftime("%Y-%m-%d %H:%M:%S"),
            m["product"]: p["sku"],
            m["qty"]: rng.randint(1, 5),
            m["price"]: p["price"],
        }
        for col, choices in m["extras"].items():
            row[col] = rng.choice(choices)
        rows.append({c: row[c] for c in m["columns"]})
    return rows
