"""
TCO India — Fuel scraper (tco-india/fuel) — PPAC + NDTV (firecrawl) merged, valid-only.

Sources: PPAC metro PDF (govt, 4 metros) + NDTV all-state (firecrawl, 33 states) → merged like cars
Grades: regular_e20 (petrol), diesel only (no variants for now); district later via state pages
Publishes to: hf://bhavabhuthi/tco-india/fuel/fuel_prices.parquet
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
NDTV_PETROL_ALL = "https://www.ndtv.com/fuel-prices/petrol-price-in-all-state"
NDTV_DIESEL_ALL = "https://www.ndtv.com/fuel-prices/diesel-price-in-all-state"

STATE_MAP = {
    'Andaman And Nicobar':'AN','Andaman and Nicobar':'AN','Andhra Pradesh':'AP','Arunachal Pradesh':'AR','Assam':'AS','Bihar':'BR',
    'Chandigarh':'CH','Chhattisgarh':'CG','Dadra And Nagar Haveli':'DD','Daman And Diu':'DD','Delhi':'DL','Goa':'GA','Gujarat':'GJ',
    'Haryana':'HR','Himachal Pradesh':'HP','Jammu And Kashmir':'JK','Jharkhand':'JH','Karnataka':'KA','Kerala':'KL',
    'Madhya Pradesh':'MP','Maharashtra':'MH','Manipur':'MN','Meghalaya':'ML','Mizoram':'MZ','Nagaland':'NL','Odisha':'OD',
    'Puducherry':'PY','Punjab':'PB','Rajasthan':'RJ','Sikkim':'SK','Tamil Nadu':'TN','Telangana':'TG','Tripura':'TR',
    'Uttar Pradesh':'UP','Uttarakhand':'UK','West Bengal':'WB',
}

async def fetch_ppac(price_date: date) -> list[dict]:
    try:
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
                print("[ppac] no PDF link")
                return []
            print(f"[ppac] fetching {pdf_url}")
            pr = await client.get(pdf_url)
            pr.raise_for_status()
            import pdfplumber, io
            rows = []
            with pdfplumber.open(io.BytesIO(pr.content)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                city_map = {"Delhi":"DL","Mumbai":"MH","Chennai":"TN","Kolkata":"WB"}
                state_map = {"Delhi":"Delhi","Mumbai":"Maharashtra","Chennai":"Tamil Nadu","Kolkata":"West Bengal"}
                for line in text.splitlines():
                    for city, code in city_map.items():
                        if city.lower() in line.lower():
                            nums = re.findall(r"\d+\.\d+", line)
                            if len(nums) >= 2:
                                rows.append({"state_name": state_map[city], "state_code": code, "city": city, "fuel_type": "regular_e20", "price_inr": float(nums[0]), "price_date": price_date, "source": "ppac"})
                                rows.append({"state_name": state_map[city], "state_code": code, "city": city, "fuel_type": "diesel", "price_inr": float(nums[1]), "price_date": price_date, "source": "ppac"})
            seen = {(r["city"], r["fuel_type"]): r for r in rows}
            rows = list(seen.values())
            print(f"[ppac] parsed {len(rows)} valid rows")
            return rows
    except Exception as e:
        print(f"[ppac] failed {e}")
        return []

def parse_ndtv_markdown(md: str, fuel_type: str, price_date: date) -> list[dict]:
    rows = []
    # table | State | Price | Change |
    for line in md.splitlines():
        # match | [State](...) | 88.66 ₹/L | 0.00 |
        m = re.search(r"\|\s*\[([^\]]+)\]\(https://www\.ndtv\.com/fuel-prices/[^)]+\)\s*\|\s*([\d.]+)", line)
        if m:
            state = m.group(1).strip()
            price = m.group(2).strip()
            code = STATE_MAP.get(state, state[:2].upper())
            try:
                rows.append({"state_name": state, "state_code": code, "city": None, "fuel_type": fuel_type, "price_inr": float(price), "price_date": price_date, "source": "ndtv"})
            except:
                continue
        # fallback: | State | 88.66 ₹/L | — without link
        m2 = re.search(r"\|\s*([A-Za-z &]+)\s*\|\s*([\d.]+)\s*₹/L", line)
        if m2 and not m:
            state = m2.group(1).strip()
            if state.lower() in ("state","price"): continue
            price = m2.group(2)
            if state in STATE_MAP:
                rows.append({"state_name": state, "state_code": STATE_MAP[state], "city": None, "fuel_type": fuel_type, "price_inr": float(price), "price_date": price_date, "source": "ndtv"})
    return rows

async def fetch_ndtv(price_date: date) -> list[dict]:
    key = os.environ.get("FIRECRAWL_API_KEY") or os.environ.get("FC_API_KEY")
    if not key:
        print("[ndtv] no FIRECRAWL_API_KEY — skip (use PPAC only)")
        return []
    out = []
    for url, fuel in [(NDTV_PETROL_ALL, "regular_e20"), (NDTV_DIESEL_ALL, "diesel")]:
        try:
            async with httpx.AsyncClient(timeout=40) as client:
                r = await client.post("https://api.firecrawl.dev/v1/scrape",
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={"url": url, "formats": ["markdown"], "onlyMainContent": True})
                r.raise_for_status()
                j = r.json()
                md = (j.get("data") or {}).get("markdown","")
                rows = parse_ndtv_markdown(md, fuel, price_date)
                print(f"[ndtv:{fuel}] parsed {len(rows)} rows from {url}")
                out.extend(rows)
        except Exception as e:
            print(f"[ndtv:{fuel}] failed {e}")
    return out

def rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table({k: [r[k] for r in rows] for k in ["state_name","state_code","city","fuel_type","price_inr","price_date","source"]}) if rows else pa.table({k: [] for k in ["state_name","state_code","city","fuel_type","price_inr","price_date","source"]})

def merge_with_hf(ppac_rows: list[dict], ndtv_rows: list[dict], out_path: Path):
    try:
        hf_path = hf_hub_download(HF_REPO, HF_PATH, repo_type="dataset") if HfApi else None
        existing = pq.read_table(hf_path).to_pylist() if hf_path else []
        print(f"Downloaded existing {len(existing)} rows from hf://{HF_REPO}/{HF_PATH}")
    except Exception as e:
        print(f"No existing HF ({e})")
        existing = []
    # union like merge_car.py: dedupe on (state_code,city,fuel_type,price_date,source)
    seen = {(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"]): r for r in existing}
    for r in ppac_rows + ndtv_rows:
        seen[(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"])] = r
    merged = sorted(seen.values(), key=lambda x: (str(x["price_date"]), x["state_code"], x["fuel_type"], x["source"]))
    # cross-source validate: same (state_code,fuel_type,price_date) across ppac vs ndtv >2% → WARN (like cars)
    try:
        by_key = {}
        for r in merged:
            k = (r["state_code"], r["fuel_type"], str(r["price_date"]))
            by_key.setdefault(k, {}).setdefault(r["source"], r["price_inr"])
        warns = 0
        for k, srcs in by_key.items():
            if len(srcs) > 1:
                vals = list(srcs.values())
                if max(vals)/min(vals)-1 > 0.02:
                    warns += 1
                    print(f"[validate] WARN {k} diff >2%: {srcs}")
        if warns:
            print(f"[validate] {warns} cross-source WARNs")
        else:
            print("[validate] cross-source OK (≤2% or single source)")
    except Exception as e:
        print(f"[validate] failed {e}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tbl = rows_to_table(merged)
    con = duckdb.connect(); con.register("rows", tbl); con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)"); con.close()
    print(f"Wrote {len(merged)} rows to {out_path} (added {len(merged)-len(existing)} new: ppac {len(ppac_rows)}, ndtv {len(ndtv_rows)})")
    return merged, len(existing)

async def main(argv=None):
    ap = argparse.ArgumentParser(description="PPAC + NDTV (firecrawl) merged fuel scraper")
    ap.add_argument("--out", default="output/fuel_prices.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--date", default=None)
    args = ap.parse_args(argv)
    price_date = date.fromisoformat(args.date) if args.date else date.today()
    print(f"Fetching PPAC + NDTV for {price_date}")
    ppac_rows = await fetch_ppac(price_date)
    ndtv_rows = await fetch_ndtv(price_date)
    new_rows = ppac_rows + ndtv_rows
    if not new_rows:
        print("No valid rows from either source — keep HF clean, no push")
        merge_with_hf([], [], Path(args.out))
        return 0
    print(f"Fetched ppac {len(ppac_rows)} + ndtv {len(ndtv_rows)} = {len(new_rows)} valid rows")
    merged, existing_len = merge_with_hf(ppac_rows, ndtv_rows, Path(args.out))
    if args.push and not args.no_upload:
        if len(merged) == existing_len:
            print("No new rows to upload — skip")
            return 0
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
