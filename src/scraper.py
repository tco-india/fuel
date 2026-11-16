"""
TCO India — Fuel scraper (tco-india/fuel).

Sources (no Firecrawl):
  - PPAC metro PDF (govt) → 4 metros, daily-ish (latest PDF)
  - NDTV all-state pages (server-rendered HTML table) → 36 states/UTs, daily
Grades: petrol (E20 today; ethanol_pct from policy timeline) + diesel (isobutanol_pct future)
Publishes to: hf://bhavabhuthi/tco-india/fuel/fuel_prices.parquet
"""
from __future__ import annotations
import argparse, asyncio, os, re
from datetime import date, datetime
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
    'Andaman And Nicobar': 'AN', 'Andaman and Nicobar': 'AN', 'Andhra Pradesh': 'AP',
    'Arunachal Pradesh': 'AR', 'Assam': 'AS', 'Bihar': 'BR', 'Chandigarh': 'CH',
    'Chhattisgarh': 'CG', 'Dadra And Nagar Haveli': 'DD', 'Daman And Diu': 'DD',
    'Delhi': 'DL', 'Goa': 'GA', 'Gujarat': 'GJ', 'Haryana': 'HR',
    'Himachal Pradesh': 'HP', 'Jammu And Kashmir': 'JK', 'Jammu and Kashmir': 'JK',
    'Jharkhand': 'JH', 'Karnataka': 'KA', 'Kerala': 'KL', 'Ladakh': 'LA',
    'Madhya Pradesh': 'MP', 'Maharashtra': 'MH', 'Manipur': 'MN', 'Meghalaya': 'ML',
    'Mizoram': 'MZ', 'Nagaland': 'NL', 'Odisha': 'OD', 'Puducherry': 'PY',
    'Punjab': 'PB', 'Rajasthan': 'RJ', 'Sikkim': 'SK', 'Tamil Nadu': 'TN',
    'Telangana': 'TG', 'Tripura': 'TR', 'Uttar Pradesh': 'UP', 'Uttarakhand': 'UK',
    'West Bengal': 'WB',
}

COLS = ["state_name", "state_code", "city", "fuel_type", "price_inr", "price_date", "source", "ethanol_pct", "isobutanol_pct"]

# Ethanol blending policy timeline (petrol). isobutanol for diesel is proposal-stage (0 today).
ETHANOL_TIMELINE = [
    (date(2023, 4, 1), 20),   # E20 rollout
    (date(2020, 1, 1), 10),   # E10
    (date(2015, 1, 1), 5),    # E5
]


def ethanol_pct_for(d: date) -> int:
    for cutoff, pct in ETHANOL_TIMELINE:
        if d >= cutoff:
            return pct
    return 0


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
            cities = [("Delhi", "DL", "Delhi"), ("Mumbai", "MH", "Maharashtra"),
                      ("Chennai", "TN", "Tamil Nadu"), ("Kolkata", "WB", "West Bengal")]
            for line in text.splitlines():
                m = re.match(r"\s*(\d{1,2}-[A-Za-z]{3}-\d{2})\s+(.+)", line)
                if not m:
                    continue
                try:
                    row_date = datetime.strptime(m.group(1), "%d-%b-%y").date()
                except ValueError:
                    continue
                if row_date != price_date:
                    continue
                clean = re.sub(r"\d{1,2}-[A-Za-z]{3}-\d{2}", "", m.group(2))
                nums_raw = re.findall(r"\d\s*\d*\.\d{2}", clean)
                nums = [float(n.replace(" ", "")) for n in nums_raw]
                if len(nums) < 8:
                    continue
                petrols, diesels = nums[:4], nums[4:8]
                for i, (city, code, state) in enumerate(cities):
                    rows.append({"state_name": state, "state_code": code, "city": city,
                                 "fuel_type": "petrol", "price_inr": petrols[i],
                                 "price_date": price_date, "source": "ppac",
                                 "ethanol_pct": ethanol_pct_for(price_date), "isobutanol_pct": 0})
                    rows.append({"state_name": state, "state_code": code, "city": city,
                                 "fuel_type": "diesel", "price_inr": diesels[i],
                                 "price_date": price_date, "source": "ppac",
                                 "ethanol_pct": None, "isobutanol_pct": 0})
            seen = {(r["city"], r["fuel_type"]): r for r in rows}
            rows = list(seen.values())
            print(f"[ppac] parsed {len(rows)} valid rows")
            return rows
    except Exception as e:
        print(f"[ppac] failed {e}")
        return []


def parse_ndtv_table(soup: BeautifulSoup, fuel_type: str, price_date: date) -> list[dict]:
    rows = []
    tb = soup.find("table")
    if not tb:
        return rows
    for tr in tb.find_all("tr")[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cols) < 2:
            continue
        state = cols[0].strip()
        price_raw = cols[1].replace("₹", "").replace("/L", "").replace(",", "").strip()
        if state.lower() in ("state", "price", ""):
            continue
        if state not in STATE_MAP:
            continue
        try:
            price = float(price_raw)
        except ValueError:
            continue
        is_petrol = fuel_type == "petrol"
        rows.append({
            "state_name": state, "state_code": STATE_MAP[state], "city": None,
            "fuel_type": "petrol" if is_petrol else "diesel",
            "price_inr": price, "price_date": price_date, "source": "ndtv",
            "ethanol_pct": ethanol_pct_for(price_date) if is_petrol else None,
            "isobutanol_pct": 0,
        })
    return rows


async def fetch_ndtv(price_date: date) -> list[dict]:
    out = []
    for url, is_petrol in [(NDTV_PETROL_ALL, True), (NDTV_DIESEL_ALL, False)]:
        try:
            async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (TCOIndia/1.0)"})
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                rows = parse_ndtv_table(soup, "petrol" if is_petrol else "diesel", price_date)
                print(f"[ndtv:{'petrol' if is_petrol else 'diesel'}] parsed {len(rows)} rows from {url}")
                out.extend(rows)
        except Exception as e:
            print(f"[ndtv] failed {e}")
    return out


GOODRETURNS_FALLBACK = {"leh": ("Ladakh", "LA"), "agartala": ("Tripura", "TR")}


async def fetch_goodreturns_missing(price_date: date) -> list[dict]:
    out = []
    for city, (state_name, code) in GOODRETURNS_FALLBACK.items():
        for fuel in ("petrol", "diesel"):
            url = f"https://www.goodreturns.in/{fuel}-price-in-{city}.html"
            try:
                async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                    r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (TCOIndia/1.0)"})
                    r.raise_for_status()
                    import re as _re
                    m = _re.search(r"Rs\.\s*([\d.]+)", r.text)
                    if not m:
                        continue
                    price = float(m.group(1))
                    is_petrol = fuel == "petrol"
                    out.append({
                        "state_name": state_name, "state_code": code, "city": city.title(),
                        "fuel_type": fuel, "price_inr": price, "price_date": price_date,
                        "source": "goodreturns", "ethanol_pct": ethanol_pct_for(price_date) if is_petrol else None,
                        "isobutanol_pct": 0,
                    })
            except Exception as e:
                print(f"[goodreturns:{city}:{fuel}] failed {e}")
    print(f"[goodreturns] parsed {len(out)} fallback rows")
    return out


def rows_to_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({k: [] for k in COLS})
    return pa.table({k: [r.get(k) for r in rows] for k in COLS})


def merge_with_hf(ppac_rows, ndtv_rows, out_path: Path):
    try:
        hf_path = hf_hub_download(HF_REPO, HF_PATH, repo_type="dataset") if HfApi else None
        existing = pq.read_table(hf_path).to_pylist() if hf_path else []
        print(f"Downloaded existing {len(existing)} rows from hf://{HF_REPO}/{HF_PATH}")
    except Exception as e:
        print(f"No existing HF ({e})")
        existing = []
    # normalize legacy rows: regular_e20 -> petrol + ethanol_pct
    for r in existing:
        if r.get("fuel_type") == "regular_e20":
            r["fuel_type"] = "petrol"
            if r.get("ethanol_pct") is None:
                try:
                    r["ethanol_pct"] = ethanol_pct_for(r["price_date"])
                except Exception:
                    r["ethanol_pct"] = None
        if "isobutanol_pct" not in r or r.get("isobutanol_pct") is None:
            r["isobutanol_pct"] = 0 if r.get("fuel_type") == "diesel" else None
        for c in COLS:
            r.setdefault(c, None)
    seen = {(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"]): r for r in existing}
    for r in ppac_rows + ndtv_rows:
        for c in COLS:
            r.setdefault(c, None)
        seen[(r["state_code"], r["city"], r["fuel_type"], str(r["price_date"]), r["source"])] = r
    merged = sorted(seen.values(), key=lambda x: (str(x["price_date"]), x["state_code"], x["fuel_type"], x["source"]))
    # cross-source validate
    try:
        by_key = {}
        for r in merged:
            k = (r["state_code"], r["fuel_type"], str(r["price_date"]))
            by_key.setdefault(k, {})[r["source"]] = r["price_inr"]
        warns = 0
        for k, srcs in by_key.items():
            if len(srcs) > 1:
                vals = list(srcs.values())
                if max(vals) / min(vals) - 1 > 0.02:
                    warns += 1
                    print(f"[validate] WARN {k} diff >2%: {srcs}")
        print(f"[validate] {warns} cross-source WARNs" if warns else "[validate] cross-source OK")
    except Exception as e:
        print(f"[validate] failed {e}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tbl = rows_to_table(merged)
    con = duckdb.connect()
    con.register("rows", tbl)
    con.execute(f"COPY (SELECT * FROM rows) TO '{out_path}' (FORMAT PARQUET)")
    con.close()
    print(f"Wrote {len(merged)} rows to {out_path} (added {len(merged) - len(existing)} new)")
    return merged, len(existing)


async def main(argv=None):
    ap = argparse.ArgumentParser(description="PPAC + NDTV (direct) merged fuel scraper")
    ap.add_argument("--out", default="output/fuel_prices.parquet")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--date", default=None)
    ap.add_argument("--coverage", action="store_true", help="print coverage matrix")
    args = ap.parse_args(argv)
    price_date = date.fromisoformat(args.date) if args.date else date.today()
    print(f"Fetching PPAC + NDTV for {price_date}")
    ppac_rows = await fetch_ppac(price_date)
    ndtv_rows = await fetch_ndtv(price_date)
    ndtv_rows += await fetch_goodreturns_missing(price_date)
    new_rows = ppac_rows + ndtv_rows
    if not new_rows:
        print("No valid rows from either source — keep HF clean, no push")
        merge_with_hf([], [], Path(args.out))
        return 0
    print(f"Fetched ppac {len(ppac_rows)} + ndtv {len(ndtv_rows)} = {len(new_rows)} valid rows")
    merged, existing_len = merge_with_hf(ppac_rows, ndtv_rows, Path(args.out))
    if args.coverage:
        print_coverage(merged)
    if args.push and not args.no_upload:
        if len(merged) == existing_len:
            print("No new rows to upload — skip")
            return 0
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
        api = HfApi(token=token) if token else HfApi()
        try:
            print(f"HF auth as {api.whoami()['name']}")
        except Exception as e:
            print(f"HF auth failed {e}")
            return 2
        api.upload_file(path_or_fileobj=str(Path(args.out)), path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset")
        print(f"Uploaded → hf://{HF_REPO}/{HF_PATH}")
    else:
        print("Skipped HF push")
    return 0


def print_coverage(rows):
    import collections
    by = collections.defaultdict(set)
    for r in rows:
        by[(r["state_code"], r.get("city") or "*")].add(str(r["price_date"])[:4])
    print("\nCoverage (state/city -> years):")
    for k in sorted(by):
        print(k, sorted(by[k]))


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main()))