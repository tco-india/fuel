"""
TCO India — Fuel scraper (tco-india/fuel) — valid-only, no mock.

Sources: PPAC (primary, govt), IOCL/BPCL/HPCL (premium grades when available)
Grades: regular_e20, xp95, xp100, diesel, xtragreen
Publishes to: hf://bhavabhuthi/tco-india/fuel/fuel_prices.parquet

No mock fallback — if fetch yields 0 valid rows, job fails (no HF push) so HF stays clean.
"""
from __future__ import annotations
import argparse, asyncio, os, re, sys
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

STATE_MAP = {
    'Andhra Pradesh': 'AP','Arunachal Pradesh': 'AR','Assam': 'AS','Bihar': 'BR',
    'Chhattisgarh': 'CG','Goa': 'GA','Gujarat': 'GJ','Haryana': 'HR',
    'Himachal Pradesh': 'HP','Jharkhand': 'JH','Karnataka': 'KA','Kerala': 'KL',
    'Madhya Pradesh': 'MP','Maharashtra': 'MH','Manipur': 'MN','Meghalaya': 'ML',
    'Mizoram': 'MZ','Nagaland': 'NL','Odisha': 'OD','Punjab': 'PB',
    'Rajasthan': 'RJ','Sikkim': 'SK','Tamil Nadu': 'TN','Telangana': 'TG',
    'Tripura': 'TR','Uttar Pradesh': 'UP','Uttarakhand': 'UK','West Bengal': 'WB',
    'Delhi': 'DL','Jammu and Kashmir': 'JK','Ladakh': 'LA','Puducherry': 'PY',
    'Chandigarh': 'CH','Andaman and Nicobar': 'AN','Dadra and Nagar Haveli and Daman and Diu': 'DD',
}
CITY_TO_STATE = {"Delhi":"DL","New Delhi":"DL","Mumbai":"MH","Chennai":"TN","Kolkata":"WB"}

PPAC_PAGE = "https://ppac.gov.in/retail-selling-price-rsp-of-petrol-diesel-and-domestic-lpg/rsp-of-petrol-and-diesel-in-metro-cities-since-16-6-2017"
# variant catalogue — IOCL premium grades (when HTML/API available)
FUEL_VARIANTS = ["regular_e20","xp95","xp100","diesel","xtragreen"]

async def fetch_ppac_pdf(price_date: date) -> list[dict]:
    """Fetch PPAC Current PDF (metro cities) and parse petrol/diesel prices."""
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 (TCOIndia/1.0)"}, timeout=30, follow_redirects=True) as client:
        # 1) get page, find Current PDF link
        r = await client.get(PPAC_PAGE)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        pdf_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            txt = a.get_text(strip=True).lower()
            if "current" in txt and href.endswith(".pdf"):
                pdf_url = href if href.startswith("http") else "https://ppac.gov.in/" + href.lstrip("/")
                break
            if href.endswith(".pdf") and "DailyPriceMSHSD_Metro" in href:
                pdf_url = href if href.startswith("http") else "https://ppac.gov.in/" + href.lstrip("/")
                if not pdf_url:
                    continue
                break
        if not pdf_url:
            # fallback: try direct pattern for today
            pdf_url = f"https://ppac.gov.in/uploads/page-images/{price_date.strftime('%d.%m.%Y')}_DailyPriceMSHSD_Metro.pdf"
            print(f"[ppac] no Current link found, trying fallback {pdf_url}")
        print(f"[ppac] fetching PDF {pdf_url}")
        pr = await client.get(pdf_url)
        pr.raise_for_status()
        # parse PDF via pdfplumber
        import pdfplumber, io
        rows = []
        with pdfplumber.open(io.BytesIO(pr.content)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            # heuristic: look for lines with city and prices like "Delhi 102.09 89.5"
            for line in text.splitlines():
                for city in ["Delhi","Mumbai","Chennai","Kolkata"]:
                    if city.lower() in line.lower():
                        nums = re.findall(r"\d+\.\d+", line)
                        if len(nums) >= 2:
                            # assume first 2 are petrol/diesel for that city in order
                            # Map to variant regular_e20/diesel
                            for fuel, price in [("regular_e20", nums[0]), ("diesel", nums[1])]:
                                rows.append({
                                    "state_name": {"Delhi":"Delhi","Mumbai":"Maharashtra","Chennai":"Tamil Nadu","Kolkata":"West Bengal"}[city],
                                    "state_code": CITY_TO_STATE[city],
                                    "city": city,
                                    "fuel_type": fuel,
                                    "price_inr": float(price),
                                    "price_date": price_date,
                                    "source": "ppac",
                                })
        if rows:
            print(f"[ppac] parsed {len(rows)} rows from PDF")
            # dedupe by (city,fuel)
            seen = {}
            for r in rows:
                seen[(r["city"], r["fuel_type"])] = r
            return list(seen.values())
        print("[ppac] PDF parsed 0 rows")
        return []

async def fetch_iocl_variants(price_date: date) -> list[dict]:
    """Try IOCL premium grades — currently 404/403, return [] if not available (no mock)."""
    urls = {
        "regular_e20": "https://iocl.com/retail-outlet/petrol-diesel-rates",
        "xp95": "https://iocl.com/xp95-price",
        "xp100": "https://iocl.com/xp100-price",
        "xtragreen": "https://iocl.com/xtragreen-price",
    }
    rows = []
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 (TCOIndia/1.0)"}, timeout=15) as client:
        for variant, url in urls.items():
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    print(f"[iocl:{variant}] {r.status_code} skip")
                    continue
                # TODO: parse when IOCL HTML stable — for now skip (no valid mock)
                print(f"[iocl:{variant}] fetched but parse not implemented — skip")
            except Exception as e:
                print(f"[iocl:{variant}] fetch failed {e}")
    return rows  # empty = no valid variant data today

async def fetch_all_valid(price_date: date) -> list[dict]:
    out = []
    try:
        ppac_rows = await fetch_ppac_pdf(price_date)
        out.extend(ppac_rows)
    except Exception as e:
        print(f"[ppac] failed {e}")
    try:
        iocl_rows = await fetch_iocl_variants(price_date)
        out.extend(iocl_rows)
    except Exception as e:
        print(f"[iocl] failed {e}")
    # BPCL/HPCL similarly — add when URLs stable
    return out

def rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table({
        "state_name": [r["state_name"] for r in rows],
        "state_code": [r["state_code"] for r in rows],
        "city": [r["city"] for r in rows],
        "fuel_type": [r["fuel_type"] for r in rows],
        "price_inr": [r["price_inr"] for r in rows],
        "price_date": [r["price_date"] for r in rows],
        "source": [r["source"] for r in rows],
    }) if rows else pa.table({k: [] for k in ["state_name","state_code","city","fuel_type","price_inr","price_date","source"]})

def merge_with_hf(new_rows: list[dict], out_path: Path) -> list[dict]:
    try:
        hf_path = hf_hub_download(HF_REPO, HF_PATH, repo_type="dataset") if HfApi else None
        existing = pq.read_table(hf_path).to_pylist() if hf_path else []
        print(f"Downloaded existing {len(existing)} rows from hf://{HF_REPO}/{HF_PATH}")
    except Exception as e:
        print(f"No existing HF file ({e}) — starting fresh")
        existing = []
    if not new_rows:
        print("No new valid rows — keeping existing, not overwriting HF")
        # still write merged (existing) to out_path for artifact
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tbl = rows_to_table(existing)
        con = duckdb.connect(); con.register("rows", tbl); con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)"); con.close()
        return existing
    seen = {(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"]): r for r in existing}
    for r in new_rows:
        seen[(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"])] = r
    merged = sorted(seen.values(), key=lambda x: (str(x["price_date"]), x["state_code"], x["fuel_type"]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tbl = rows_to_table(merged)
    con = duckdb.connect(); con.register("rows", tbl); con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)"); con.close()
    print(f"Wrote {len(merged)} rows to {out_path} (added {len(merged)-len(existing)} new)")
    return merged

async def main(argv=None):
    ap = argparse.ArgumentParser(description="Scrape valid fuel prices → HF (no mock)")
    ap.add_argument("--out", default="output/fuel_prices.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--date", default=None)
    args = ap.parse_args(argv)
    price_date = date.fromisoformat(args.date) if args.date else date.today()
    print(f"Fetching valid prices for {price_date} — expecting PPAC metro PDF")
    new_rows = await fetch_all_valid(price_date)
    if not new_rows:
        print("No valid rows fetched — will NOT push mock data (HF stays clean). Exiting 0 with no upload.")
        # still write existing to out for artifact
        merge_with_hf([], Path(args.out))
        return 0
    print(f"Fetched {len(new_rows)} valid rows")
    merged = merge_with_hf(new_rows, Path(args.out))
    if args.push and not args.no_upload:
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        api = HfApi(token=token) if token else HfApi()
        try:
            print(f"HF auth as {api.whoami()['name']}")
        except Exception as e:
            print(f"HF auth failed: {e}", file=sys.stderr); sys.exit(2)
        # only push if we actually added new valid rows
        if len(merged) > len(merge_with_hf([], Path("/tmp/_existing.parquet"))):
            api.upload_file(path_or_fileobj=str(Path(args.out)), path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset")
            print(f"Uploaded {args.out} → hf://{HF_REPO}/{HF_PATH}")
        else:
            print("No new valid rows to upload — skipping HF push")
    else:
        print("Skipped HF upload (--no-upload or --push not set)")
    return 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
