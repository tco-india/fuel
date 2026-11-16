# tco-fuel

Daily fuel price scraper for [TCO India](https://github.com/tco-india) — publishes to `bhavabhuthi/tco-india` **fuel module** (`fuel/fuel_prices.parquet`, config `fuel_prices`).

Part of the [tco-india](https://github.com/tco-india) org. Data module is separate from the app (which lives on GitLab).

## What it does

- **Sources:** PPAC metro PDF + NDTV all-state pages (direct httpx, no Firecrawl)
- **Schedule:** GitHub Actions `daily.yml` @ 01:30 UTC (~07:00 IST) + manual `workflow_dispatch`
- **Output:** `fuel/fuel_prices.parquet` on HF with cols `state_name, state_code, city, fuel_type, price_inr, price_date, source, ethanol_pct, isobutanol_pct`
- **Fuel types:** `petrol` (E20 today; ethanol_pct from policy timeline) + `diesel` (isobutanol_pct future)
- **Tooling:** [Astral uv](https://docs.astral.sh/uv/) — `uv sync` + `uv run`

## Usage (uv)

```bash
# install uv: curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync                          # creates .venv + uv.lock (frozen in CI)
uv run python -m src.scraper --out output/fuel.parquet --no-upload
uv run python -m src.scraper --source ppac --no-upload --date 2026-08-24
HF_TOKEN=hf_xxx uv run python -m src.scraper --push
```

Inspect locally:

```bash
uv run python -c "import pyarrow.parquet as pq, duckdb; print(pq.ParquetFile('output/fuel.parquet').metadata.num_rows)"
uv run python -c "import duckdb; print(duckdb.connect().execute(\"SELECT fuel_type, source, COUNT(*) FROM 'output/fuel.parquet' GROUP BY 1,2\").fetchall())"
```

## HF Consumption

```python
import duckdb
duckdb.connect().execute("SELECT * FROM 'hf://datasets/bhavabhuthi/tco-india/fuel/fuel_prices.parquet' WHERE state_code='MH' ORDER BY price_date DESC LIMIT 5").fetchall()
```

## License

**AGPL-3.0** — see [LICENSE](LICENSE). Data compilation is `CC-BY-4.0` on HF.
