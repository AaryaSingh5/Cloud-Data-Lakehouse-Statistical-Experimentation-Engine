"""Synthetic multi-source sales data + fault injector with exact ground truth.

Outputs (under --out, default `data/`):
  clean/<source>/<batch_date>.<ext>          fault-free copy of every batch (used to measure downstream error)
  landing/<source>/<delivered_date>/<source>_<batch_date>.<ext>   what the pipeline actually receives
  ground_truth/ground_truth.csv              one row per injected fault (with affected row keys)
  ground_truth/batches.csv                   one row per batch (faulty or not)
Re-running wipes clean/, landing/ and ground_truth/ first, so output is fully reproducible from the seed.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import random
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from data_generator.profiles import PROFILES, SCHEMA_FAULTS
from data_generator.sources import SOURCES, build_catalog, make_clean_rows

RENAMES = {"quantity": "qty", "unit_price": "price", "product_id": "sku", "units": "quantity",
           "price_usd": "price", "sku": "product_code", "country": "country_code", "region": "sales_region"}


def serialize(rows: list[dict], columns: list[str], fmt: str) -> list[str]:
    if fmt == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        return buf.getvalue().splitlines()
    return [json.dumps({c: r.get(c) for c in columns}) for r in rows]


def _event(fault, level, keys, detail=None, affected=None):
    return {"fault_type": fault, "level": level, "affected_rows": len(keys) if affected is None else affected,
            "row_keys": list(keys), "detail": detail or {}}


def build_batch(source: str, batch_date: date, n: int, seed: int, profile: dict) -> dict:
    m = SOURCES[source]
    key, fmt = m["key"], m["fmt"]
    rng_data = random.Random(f"{seed}|data|{source}|{batch_date}")
    rng = random.Random(f"{seed}|fault|{source}|{batch_date}")

    clean = make_clean_rows(source, batch_date, n, rng_data, build_catalog(seed))
    clean_lines = serialize(clean, m["columns"], fmt)

    # decide which faults hit this batch (rng consumed per fault -> deterministic)
    chosen = [f for f, cfg in profile.items() if rng.random() < cfg["batch_prob"]]
    schema_pick = [f for f in chosen if f in SCHEMA_FAULTS][:1]  # at most one schema fault per batch
    chosen = [f for f in chosen if f not in SCHEMA_FAULTS] + schema_pick
    if "empty_file" in chosen:
        chosen = ["empty_file"] + (["late_arriving"] if "late_arriving" in chosen else [])

    rows = [dict(r) for r in clean]
    columns = list(m["columns"])
    events: list[dict] = []
    touched: set[int] = set()

    def avail(k: int) -> list[int]:
        free = [i for i in range(len(rows)) if i not in touched]
        sel = rng.sample(free, min(k, len(free)))
        touched.update(sel)
        return sel

    def k_for(f: str) -> int:
        return max(1, round(profile[f]["row_rate"] * n))

    # ---- row-level faults ----
    if "null_values" in chosen:
        idx = avail(k_for("null_values"))
        fields = [m["product"], m["qty"], m["price"]]
        used = []
        for i in idx:
            f = rng.choice(fields)
            rows[i][f] = None
            used.append(f)
        events.append(_event("null_values", "row", [rows[i][key] for i in idx], {"fields": sorted(set(used))}))

    if "out_of_range" in chosen:
        idx = avail(k_for("out_of_range"))
        modes = []
        for i in idx:
            mode = rng.choice(["negative_qty", "huge_qty", "price_x100", "negative_price"])
            if mode == "negative_qty":
                rows[i][m["qty"]] = -rng.randint(1, 5)
            elif mode == "huge_qty":
                rows[i][m["qty"]] = rng.randint(500, 1000)
            elif mode == "price_x100":
                rows[i][m["price"]] = round(rows[i][m["price"]] * 100, 2)
            else:
                rows[i][m["price"]] = -abs(rows[i][m["price"]])
            modes.append(mode)
        events.append(_event("out_of_range", "row", [rows[i][key] for i in idx], {"modes": sorted(set(modes))}))

    for fault in ("duplicate_exact", "duplicate_key_conflict"):
        if fault in chosen:
            idx = avail(k_for(fault))
            start = len(rows)
            for i in idx:
                dup = dict(rows[i])
                if fault == "duplicate_key_conflict":
                    dup[m["qty"]] = rows[i][m["qty"]] + rng.randint(1, 3)
                rows.append(dup)
            touched.update(range(start, len(rows)))
            events.append(_event(fault, "row", [rows[i][key] for i in idx]))

    # ---- schema faults (whole batch) ----
    non_key = [c for c in columns if c != key]
    if "column_rename" in chosen:
        col = rng.choice([c for c in non_key if c != m["ts"]])
        new = RENAMES.get(col, col + "_v2")
        for r in rows:
            r[new] = r.pop(col)
        columns[columns.index(col)] = new
        events.append(_event("column_rename", "batch", [], {"from": col, "to": new}, len(rows)))
    if "column_dropped" in chosen:
        col = rng.choice([m["product"], m["qty"], m["price"]] + [c for c in non_key if c in m["extras"]])
        for r in rows:
            r.pop(col, None)
        columns.remove(col)
        events.append(_event("column_dropped", "batch", [], {"column": col}, len(rows)))
    if "extra_column" in chosen:
        for r in rows:
            r["discount_code"] = rng.choice(["", "WELCOME10", "FESTIVE20"])
        columns.append("discount_code")
        events.append(_event("extra_column", "batch", [], {"column": "discount_code"}, len(rows)))
    if "type_change" in chosen:
        style = rng.choice(["currency_symbol", "comma_decimal"])
        for r in rows:
            v = r[m["price"]]
            if v is not None:
                r[m["price"]] = f"${v:.2f}" if style == "currency_symbol" else f"{v:.2f}".replace(".", ",")
        events.append(_event("type_change", "batch", [], {"column": m["price"], "style": style}, len(rows)))
    if "timestamp_format_change" in chosen:
        style = rng.choice(["dmy", "epoch"])
        for r in rows:
            dt = datetime.strptime(r[m["ts"]], "%Y-%m-%d %H:%M:%S")
            r[m["ts"]] = dt.strftime("%d/%m/%Y %H:%M") if style == "dmy" else int(dt.timestamp())
        events.append(_event("timestamp_format_change", "batch", [], {"column": m["ts"], "style": style}, len(rows)))

    lines = serialize(rows, columns, fmt)

    # ---- text-level corruption ----
    if "corrupt_rows" in chosen:
        idx = avail(k_for("corrupt_rows"))
        offset = 1 if fmt == "csv" else 0
        for i in idx:
            ln = lines[i + offset]
            if fmt == "csv":
                parts = ln.split(",")
                lines[i + offset] = ",".join(parts[: max(1, len(parts) // 2)]) + ',"#@!'
            else:
                lines[i + offset] = ln[: len(ln) // 2]
        events.append(_event("corrupt_rows", "row", [rows[i][key] for i in idx]))

    # ---- batch-level faults ----
    n_delivered = len(rows)
    if "empty_file" in chosen:
        lines = serialize([], columns, fmt)
        events.append(_event("empty_file", "batch", [], {}, len(rows)))
        n_delivered = 0
    delivered = batch_date
    if "late_arriving" in chosen:
        delay = rng.randint(1, 3)
        delivered = batch_date + timedelta(days=delay)
        events.append(_event("late_arriving", "batch", [], {"delay_days": delay}, n_delivered))

    return {"lines": lines, "clean_lines": clean_lines, "delivered": delivered, "events": events,
            "n_clean": n, "n_delivered": n_delivered}


def generate(out_dir: str | Path = "data", start: date = date(2025, 1, 1), days: int = 14,
             rows_per_batch: int = 500, seed: int = 42, profile: str = "medium") -> dict:
    out = Path(out_dir)
    for sub in ("clean", "landing", "ground_truth"):
        shutil.rmtree(out / sub, ignore_errors=True)
    (out / "ground_truth").mkdir(parents=True, exist_ok=True)
    prof = PROFILES[profile]

    fault_rows, batch_rows = [], []
    for d in range(days):
        day = start + timedelta(days=d)
        for source, m in SOURCES.items():
            b = build_batch(source, day, rows_per_batch, seed, prof)
            ext = "csv" if m["fmt"] == "csv" else "jsonl"
            batch_id = f"{source}-{day.isoformat()}"

            clean_path = out / "clean" / source / f"{day.isoformat()}.{ext}"
            clean_path.parent.mkdir(parents=True, exist_ok=True)
            clean_path.write_text("\n".join(b["clean_lines"]) + "\n", encoding="utf-8")

            land_path = out / "landing" / source / b["delivered"].isoformat() / f"{source}_{day.isoformat()}.{ext}"
            land_path.parent.mkdir(parents=True, exist_ok=True)
            land_path.write_text("\n".join(b["lines"]) + ("\n" if b["lines"] else ""), encoding="utf-8")

            batch_rows.append({"batch_id": batch_id, "source": source, "batch_date": day.isoformat(),
                               "delivered_date": b["delivered"].isoformat(),
                               "file_path": str(land_path.relative_to(out)),
                               "rows_clean": b["n_clean"], "rows_delivered": b["n_delivered"],
                               "n_faults": len(b["events"])})
            for e in b["events"]:
                fault_rows.append({"batch_id": batch_id, "source": source, "batch_date": day.isoformat(),
                                   "delivered_date": b["delivered"].isoformat(), "fault_type": e["fault_type"],
                                   "level": e["level"], "affected_rows": e["affected_rows"],
                                   "row_keys": json.dumps(e["row_keys"]), "detail": json.dumps(e["detail"])})

    def dump(name, rows, cols):
        with open(out / "ground_truth" / name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

    dump("ground_truth.csv", fault_rows, ["batch_id", "source", "batch_date", "delivered_date", "fault_type",
                                          "level", "affected_rows", "row_keys", "detail"])
    dump("batches.csv", batch_rows, ["batch_id", "source", "batch_date", "delivered_date", "file_path",
                                     "rows_clean", "rows_delivered", "n_faults"])
    return {"batches": len(batch_rows), "faults": len(fault_rows),
            "faulty_batches": sum(1 for b in batch_rows if b["n_faults"])}


def main() -> None:
    from common.config import load_config
    g = load_config()["generator"]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data")
    ap.add_argument("--start", default=g["start_date"])
    ap.add_argument("--days", type=int, default=g["days"])
    ap.add_argument("--rows", type=int, default=g["rows_per_batch"])
    ap.add_argument("--seed", type=int, default=g["seed"])
    ap.add_argument("--profile", choices=list(PROFILES), default=g["profile"])
    a = ap.parse_args()
    s = generate(a.out, date.fromisoformat(a.start), a.days, a.rows, a.seed, a.profile)
    print(f"Generated {s['batches']} batches ({s['faulty_batches']} faulty), {s['faults']} fault events -> {a.out}/")


if __name__ == "__main__":
    main()
