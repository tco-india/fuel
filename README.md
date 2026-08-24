# tco-fuel

Daily fuel price scraper for [TCO India](https://github.com/tco-india) — publishes to `bhavabhuthi/tco-india` **fuel module** (`fuel/fuel_prices.parquet`, config `fuel_prices`).

Part of the [tco-india](https://github.com/tco-india) org. Data module is separate from the app (which lives on GitLab) — see [TASKS.md](https://github.com/tco-india/fuel/../TASKS.md) in the root `tco` planning repo.

## What it does

- **Source:** [IOCL](https://iocl.com/retail-outlet/petrol-diesel-rates) (fallback: PPAC/HPCL if IOCL HTML changes)
- **Schedule:** GitHub Actions `daily.yml` @ 01:30 UTC (~07:00 IST) + manual `workflow_dispatch`
- **Output:** `fuel/fuel_prices.parquet` on HF with cols `state_name, state_code, city, fuel_type, price_inr, price_date, source` — one row per `(state, fuel_type, price_date)` (`price_date` is the TCO history key)
- **Merge:** downloads existing `hf://bhavabhuthi/tco-india/fuel/fuel_prices.parquet`, dedups on `(state_code, city, fuel_type, price_date)`, `COPY` via DuckDB, re-uploads

## Usage

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
# scrape + merge locally (no HF push)
python -m src.scraper --out output/fuel.parquet --no-upload
# scrape + publish to HF (needs HF_TOKEN)
HF_TOKEN=hf_xxx python -m src.scraper --push
```

## HF Consumption

```python
import duckdb
con = duckdb.connect()
con.execute("SELECT * FROM 'hf://datasets/bhavabhuthi/tco-india/fuel/fuel_prices.parquet' WHERE state_code='MH' ORDER BY price_date DESC LIMIT 5").fetchall()
```

Or via `datasets`: `load_dataset("bhavabhuthi/tco-india", "fuel_prices")`.

## License

**AGPL-3.0** — see [LICENSE](LICENSE). Data compilation is `CC-BY-4.0` on HF (facts are public; presentation is CC-BY).

## Related

- Unified dataset: `bhavabhuthi/tco-india` (car + fuel modules, `README` declares `fuel_prices` config)
- Old standalone `bhavabhuthi/tco-fuel` is deprecated (redirects here)
- Scraper source (forked from `total-cost-of-ownership/packages/scrapers/adapters/html/iocl.py`)
