
# webharvest

![CI](https://github.com/Hbollas/webharvest/actions/workflows/ci.yml/badge.svg)

An async, polite web scraper built to demonstrate intermediate Python skills:
- **Click CLI** with subcommands
- **httpx** with retries/backoff
- **robots.txt** awareness
- **BeautifulSoup + lxml** HTML parsing
- **SQLite** persistence + small analytics
- **Tests + GitHub Actions CI**
- **CSV export**

> Demo target: `quotes.toscrape.com` (safe practice site). Easily add more spiders.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
webharvest scrape-quotes --max-pages 10 --db data/quotes.db
webharvest stats --db data/quotes.db
webharvest top-authors --db data/quotes.db --k 5
webharvest export-csv --db data/quotes.db --out data/quotes.csv
```
## Commands
webharvest fetch --url URL – fetch a page (sanity check)

webharvest parse-quotes --page N – parse one page (preview)

webharvest scrape-quotes --max-pages N [--db PATH] – scrape & store

webharvest stats [--db PATH] – show row count

webharvest top-authors [--db PATH] [--k K] – small analytics

webharvest export-csv [--db PATH] [--out PATH] – export to CSV

## Project Structure 
src/webharvest/
  cli.py              # Click CLI (commands)
  http.py             # httpx client with retries/backoff
  robots.py           # tiny robots.txt helper
  spiders/quotes.py   # parser & URL helpers for demo site
  storage/sqlite.py   # SQLite schema + CRUD
tests/                # parser + storage tests (offline sample HTML)
.github/workflows/    # CI (ruff, black, pytest)

