"""Backfill fuel prices from free Kaggle datasets (no paid acquisition).

Usage:
  python -m src.backfill_kaggle --input data/raw/soulforge.csv --out output/backfill.parquet --no-upload
  python -m src.backfill_kaggle --input data/raw/sudhirnl7_petrol.csv --fuel petrol --out output/backfill.parquet

Transforms common Kaggle schemas into the canonical fuel schema and merges into HF.
"""
from __future__ import annotations
import argparse, csv, os
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from .scraper import STATE_MAP, HF_REPO, HF_PATH, ethanol_pct_for, merge_with_hf
try:
    from huggingface_hub import HfApi, hf_hub_download
except ImportError:
    HfApi = None

CITY_STATE = {
    "Delhi": "DL", "New Delhi": "DL", "Mumbai": "MH", "Chennai": "TN",
    "Kolkata": "WB", "Bengaluru": "KA", "Bangalore": "KA", "Hyderabad": "TG",
    "Coimbatore": "TN", "Lucknow": "UP",
}

STATE_NAME_BY_CODE = {v: k for k, v in STATE_MAP.items()}


def parse_date(v):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(v.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"bad date {v}")


def transform_csv(path: Path, fuel_hint: str | None):
    rows = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fields = [x.lower() for x in (reader.fieldnames or [])]
        for raw in reader:
            r = {k.lower(): v for k, v in raw.items()}
            # detect wide format: petrol/diesel columns
            dval = None
            for key in ("date", "price_date", "day"):
                if key in r:
                    dval = r[key]
                    break
            if not dval:
                continue
            try:
                pd_ = parse_date(dval)
            except ValueError:
                continue
            # wide
            if "petrol" in r and r.get("petrol"):
                try:
                    price = float(str(r["petrol"]).replace(",", ""))
                    city = r.get("city") or r.get("location") or r.get("metro")
                    code = CITY_STATE.get(city, None)
                    if code:
                        rows.append(_row(code, city, "petrol", price, pd_, "kaggle"))
                except ValueError:
                    pass
            if "diesel" in r and r.get("diesel"):
                try:
                    price = float(str(r["diesel"]).replace(",", ""))
                    city = r.get("city") or r.get("location") or r.get("metro")
                    code = CITY_STATE.get(city, None)
                    if code:
                        rows.append(_row(code, city, "diesel", price, pd_, "kaggle"))
                except ValueError:
                    pass
            # long format: fuel_type + price
            if "fuel_type" in r or "fuel" in r:
                ft = (r.get("fuel_type") or r.get("fuel") or "").strip().lower()
                if ft in ("petrol", "regular", "ms"):
                    ft = "petrol"
                elif ft in ("diesel", "hsd"):
                    ft = "diesel"
                else:
                    ft = None
                price_key = None
                for key in ("price", "price_inr", "price_inr_per_litre", "value", "rate"):
                    if key in r and r.get(key):
                        price_key = key
                        break
                city = r.get("city") or r.get("location") or r.get("metro")
                code = CITY_STATE.get(city) if city else None
                if ft and code and price_key:
                    try:
                        price = float(str(r[price_key]).replace(",", "").replace("₹", ""))
                        rows.append(_row(code, city, ft, price, pd_, "kaggle"))
                    except ValueError:
                        pass
            # fuel hint mode (single-fuel csv)
            if fuel_hint and not (("petrol" in r and r.get("petrol")) or ("diesel" in r and r.get("diesel"))):
                price_key = None
                for key in ("price", "price_inr", "price_inr_per_litre", "value", "rate"):
                    if key in r and r.get(key):
                        price_key = key
                        break
                city = r.get("city") or r.get("location") or r.get("metro")
                code = CITY_STATE.get(city) if city else None
                if code and price_key:
                    try:
                        price = float(str(r[price_key]).replace(",", "").replace("₹", ""))
                        rows.append(_row(code, city, fuel_hint, price, pd_, "kaggle"))
                    except ValueError:
                        pass
    return rows


def _row(code, city, fuel_type, price, pd_, source):
    is_petrol = fuel_type == "petrol"
    return {
        "state_name": STATE_NAME_BY_CODE.get(code, code),
        "state_code": code,
        "city": city,
        "fuel_type": fuel_type,
        "price_inr": price,
        "price_date": pd_,
        "source": source,
        "ethanol_pct": (ethanol_pct_for(pd_) if is_petrol else None),
        "isobutanol_pct": 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--fuel", default=None, choices=["petrol", "diesel"])
    ap.add_argument("--out", default="output/backfill_kaggle.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()
    rows = transform_csv(Path(args.input), args.fuel)
    # fill ethanol_pct from policy timeline
    for r in rows:
        if r["fuel_type"] == "petrol" and r.get("ethanol_pct") is None:
            r["ethanol_pct"] = None  # caller can set; keep None for unknown historical
    print(f"Transformed {len(rows)} rows from {args.input}")
    if not rows:
        print("No rows; abort")
        return
    # merge into HF
    # merge_with_hf signature expects ppac, ndtv; reuse by passing rows as ndtv
    merged, existing = merge_with_hf([], rows, Path(args.out))
    print(f"Merged total {len(merged)}")
    if args.push and not args.no_upload:
        from huggingface_hub import HfApi
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        api = HfApi(token=token) if token else HfApi()
        api.upload_file(path_or_fileobj=str(Path(args.out)), path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset")
        print(f"Uploaded → hf://{HF_REPO}/{HF_PATH}")


if __name__ == "__main__":
    main()