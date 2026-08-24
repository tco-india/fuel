"""
TCO India — Fuel scraper (tco-india/fuel) — single source PPAC, valid-only.

Source: PPAC metro PDF (govt) → regular petrol/diesel only (no premium variants for now)
Publishes to: hf://bhavabhuthi/tco-india/fuel/fuel_prices.parquet

Evaluated NDTV (https://www.ndtv.com/fuel-prices/...) → 403 blocked, secondary aggregate of PPAC — not useful vs direct PPAC.
"""
from __future__ import annotations
import argparse, asyncio, os, re
from datetime import date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

try:
    from huggingface_hub import HfApi, hf_hub_download
except ImportError:
    HfApi = None

HF_REPO = "bhavabhuthi/tco-india"
HF_PATH = "fuel/fuel_prices.parquet"
PPAC_PAGE = "https://ppac.gov.in/retail-selling-price-rsp-of-petrol-diesel-and-domestic-lpg/rsp-of-petrol-and-diesel-in-metro-cities-since-16-6-2017"

async def fetch_ppac(price_date: date) -> list[dict]:
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 (TCOIndia/1.0)"}, timeout=30, follow_redirects=True) as client:
        r = await client.get(PPAC_PAGE)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        pdf_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".pdf") and "DailyPriceMSHSD_Metro" in href:
                pdf_url = href if href.startswith("http") else "https://ppac.gov.in/" + href.lstrip("/")
                break
        if not pdf_url:
            print("[ppac] no PDF link found")
            return []
        print(f"[ppac] fetching {pdf_url}")
        pr = await client.get(pdf_url)
        pr.raise_for_status()
        import pdfplumber, io
        rows = []
        with pdfplumber.open(io.BytesIO(pr.content)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            # Metro cities: Delhi, Mumbai, Chennai, Kolkata
            city_map = {"Delhi":"DL","Mumbai":"MH","Chennai":"TN","Kolkata":"WB"}
            state_map = {"Delhi":"Delhi","Mumbai":"Maharashtra","Chennai":"Tamil Nadu","Kolkata":"West Bengal"}
            for line in text.splitlines():
                for city, code in city_map.items():
                    if city.lower() in line.lower():
                        nums = re.findall(r"\d+\.\d+", line)
                        if len(nums) >= 2:
                            rows.append({"state_name": state_map[city], "state_code": code, "city": city, "fuel_type": "regular_e20", "price_inr": float(nums[0]), "price_date": price_date, "source": "ppac"})
                            rows.append({"state_name": state_map[city], "state_code": code, "city": city, "fuel_type": "diesel", "price_inr": float(nums[1]), "price_date": price_date, "source": "ppac"})
        # dedupe
        seen = {(r["city"], r["fuel_type"]): r for r in rows}
        rows = list(seen.values())
        print(f"[ppac] parsed {len(rows)} valid rows")
        return rows

def rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table({k: [r[k] for r in rows] for k in ["state_name","state_code","city","fuel_type","price_inr","price_date","source"]}) if rows else pa.table({k: [] for k in ["state_name","state_code","city","fuel_type","price_inr","price_date","source"]})

def merge_with_hf(new_rows: list[dict], out_path: Path):
    try:
        hf_path = hf_hub_download(HF_REPO, HF_PATH, repo_type="dataset") if HfApi else None
        existing = pq.read_table(hf_path).to_pylist() if hf_path else []
        print(f"Downloaded {len(existing)} rows from hf://{HF_REPO}/{HF_PATH}")
    except Exception as e:
        print(f"No existing HF ({e})")
        existing = []
    if not new_rows:
        print("No new valid rows — keep existing")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tbl = rows_to_table(existing)
        con = duckdb.connect(); con.register("rows", tbl); con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)"); con.close()
        return existing
    seen = {(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"])): r for r in existing}
    for r in new_rows:
        seen[(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]))] = r
    merged = sorted(seen.values(), key=lambda x: (str(x["price_date"]), x["state_code"]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tbl = rows_to_table(merged)
    con = duckdb.connect(); con.register("rows", tbl); con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)"); con.close()
    print(f"Wrote {len(merged)} rows (added {len(merged)-len(existing)}) to {out_path}")
    return merged

async def main(argv=None):
    ap = argparse.ArgumentParser(description="PPAC single-source fuel scraper (no mock)")
    ap.add_argument("--out", default="output/fuel_prices.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--date", default=None)
    args = ap.parse_args(argv)
    price_date = date.fromisoformat(args.date) if args.date else date.today()
    print(f"Fetching PPAC for {price_date} (single source, regular_e20/diesel only)")
    rows = await fetch_ppac(price_date)
    if not rows:
        print("No valid rows — HF stays clean (no mock)")
        merge_with_hf([], Path(args.out))
        return 0
    print(f"Fetched {len(rows)} valid rows")
    merged = merge_with_hf(rows, Path(args.out))
    if args.push and not args.no_upload:
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        api = HfApi(token=token) if token else HfApi()
        try:
            print(f"HF auth as {api.whoami()['name']}")
        except Exception as e:
            print(f"HF auth failed {e}"); return 2
        api.upload_file(path_or_fileobj=str(Path(args.out)), path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset")
        print(f"Uploaded → hf://{HF_REPO}/{HF_PATH}")
    else:
        print("Skipped HF push")
    return 0

if __name__ == "__main__":
    import asyncio; raise SystemExit(asyncio.run(main()))
