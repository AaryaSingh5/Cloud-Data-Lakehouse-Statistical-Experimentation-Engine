"""Conform canonical payloads to the Silver sales schema (5.1).

Input is a payload after schema alias mapping. Output is one typed row.
Silver merges on (source, order_key).
"""
from __future__ import annotations

from datetime import date, datetime

SILVER_COLUMNS = (
    "source", "order_key", "batch_id", "batch_date", "run_id",
    "order_ts", "sale_date", "product_id", "quantity", "unit_price", "revenue",
    "store_id", "customer_id", "country", "region", "payment_method", "loaded_at",
)

# Silver column -> canonical field, per source. Channel fields are optional.
CONFORM: dict[str, dict[str, str]] = {
    "pos": {
        "order_key": "order_id", "order_ts": "order_ts", "product_id": "product_id",
        "quantity": "quantity", "unit_price": "unit_price",
        "store_id": "store_id", "payment_method": "payment_method",
    },
    "web": {
        "order_key": "order_id", "order_ts": "order_ts", "product_id": "product_id",
        "quantity": "quantity", "unit_price": "unit_price",
        "customer_id": "customer_id", "country": "country",
    },
    "partner": {
        "order_key": "order_ref", "order_ts": "sold_at", "product_id": "sku",
        "quantity": "units", "unit_price": "price_usd", "region": "region",
    },
}

CHANNEL_COLUMNS = ("store_id", "customer_id", "country", "region", "payment_method")


def conform_row(
    source: str,
    canon: dict,
    ts_format: str,
    batch_id: str | None,
    batch_date: date,
    run_id: str,
    loaded_at: datetime,
) -> dict:
    f = CONFORM[source]
    order_ts = datetime.strptime(str(canon[f["order_ts"]]), ts_format)
    quantity = int(str(canon[f["quantity"]]))
    unit_price = float(str(canon[f["unit_price"]]))

    row = {
        "source": source,
        "order_key": str(canon[f["order_key"]]),
        "batch_id": batch_id,
        "batch_date": batch_date,
        "run_id": run_id,
        "order_ts": order_ts,
        "sale_date": order_ts.date(),
        "product_id": str(canon[f["product_id"]]),
        "quantity": quantity,
        "unit_price": unit_price,
        "revenue": round(quantity * unit_price, 2),
        "loaded_at": loaded_at,
    }
    for col in CHANNEL_COLUMNS:
        field = f.get(col)
        value = canon.get(field) if field else None
        row[col] = None if value is None else str(value)
    return row