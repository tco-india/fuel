"""Create india_fuel_annual config from the Global Fuel Prices 100 Years dataset (Kaggle CC0).

Reads fuel_prices/by_country/fuel_prices_india.csv, appends 2025 & 2026 (derived from
our daily fuel_prices.parquet + web-sourced crude/FX), writes parquet, optionally pushes
to HF as a separate config (india_fuel_annual).
"""
from __future__ import annotations
import argparse, csv, os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

HF_REPO = "bhavabhuthi/tco-india"
HF_PATH = "fuel/india_fuel_annual.parquet"

# Approximate annual values for 2025 & 2026 (web-sourced).
EXTRA = [
    # year, gasoline_usd_per_liter, diesel_usd_per_liter, gasoline_local_currency,
    # crude_oil_usd_per_barrel, crude_oil_usd_per_liter, tax_pct, fx, subsidy_regime
    (2025, 1.166, 1.049, 101.7, 70.0, 0.440, 48.0, 87.166, "partial"),
    (2026, 1.146, 1.038, 104.8, 95.0, 0.597, 48.0, 91.437, "partial"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to fuel_prices_india.csv")
    ap.add_argument("--out", default="output/india_fuel_annual.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    with Path(args.input).open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        base_rows = list(reader)
    print(f"Loaded {len(base_rows)} base rows")

    # Build pyarrow table from base csv (string values -> keep as-is, cast numerics)
    # We'll just pass through all columns as strings then let parquet infer; better cast.
    columns = list(base_rows[0].keys())
    data = {c: [r[c] for r in base_rows] for c in columns}

    # Append extra years using base schema; fill missing with None
    for (yr, g_usd, d_usd, g_local, crude, crude_ltr, tax, fx, regime) in EXTRA:
        row = {c: None for c in columns}
        row["year"] = str(yr)
        row["country"] = "India"
        row["iso3"] = "IND"
        row["currency_code"] = "INR"
        row["region"] = "Asia"
        row["decade"] = f"{(yr//10)*10}s"
        row["gasoline_usd_per_liter"] = str(g_usd)
        row["gasoline_usd_per_gallon"] = str(round(g_usd * 3.78541, 4))
        row["gasoline_real_2024usd"] = str(g_usd)
        row["diesel_usd_per_liter"] = str(d_usd)
        row["diesel_real_2024usd"] = str(d_usd)
        row["gasoline_local_currency"] = str(g_local)
        row["tax_pct_of_pump_price"] = str(tax)
        row["pretax_price_usd_ltr"] = str(round(g_usd * (1 - tax / 100.0), 4))
        row["subsidy_flag"] = "0"
        row["subsidy_regime"] = regime
        row["is_oil_producer"] = "1"
        row["crude_oil_usd_per_barrel"] = str(crude)
        row["crude_oil_usd_per_liter"] = str(crude_ltr)
        row["refinery_margin_usd_ltr"] = "0.05"
        row["oil_price_shock_idx"] = "1.0"
        row["us_cpi"] = ""
        row["inflation_deflator_2024"] = ""
        for c in columns:
            data[c].append(row[c])

    table = pa.Table.from_pydict(data)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out)
    print(f"Wrote {table.num_rows} rows to {out}")

    if args.push and not args.no_upload:
        from huggingface_hub import HfApi
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        api = HfApi(token=token) if token else HfApi()
        api.upload_file(path_or_fileobj=str(out), path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset")
        print(f"Uploaded → hf://{HF_REPO}/{HF_PATH}")


if __name__ == "__main__":
    main()