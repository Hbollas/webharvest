import click
import csv
import asyncio
import time
from rich.console import Console

from .http import fetch_text_retry
from .spiders.quotes import parse_quotes, page_url, BASE
from .storage.sqlite import SqliteStore
from .robots import fetch_disallows, is_allowed
from typing import List, Dict
from pathlib import Path

console = Console()


@click.group(help="webharvest - learn scraping step by step")
def app():
    pass


@app.command("hello")
@click.option("--name", "-n", default="world", help="Who to greet")
def hello(name: str):
    console.print(f"[bold green]Hello, {name}![/]")


@app.command("fetch")
@click.option("--url", default="https://quotes.toscrape.com/", show_default=True)
def fetch(url: str):
    """Fetch a page and print a short snippet (sanity check)."""
    status, html = asyncio.run(fetch_text_retry(url))
    console.print(f"Status: {status}")
    snippet = (html[:200] if html else "").replace("\n", " ")
    console.print(f"HTML snippet: {snippet}...")


@app.command("parse-quotes")
@click.option("--page", default=1, show_default=True, type=int)
def parse_quotes_cmd(page: int):
    """Fetch one page and parse quotes (prints a small preview)."""
    url = page_url(page)
    status, html = asyncio.run(fetch_text_retry(url))
    if status != 200:
        console.print(f"[red]HTTP {status}[/] {url}")
        raise SystemExit(1)
    rows = parse_quotes(html, url)
    console.print(f"[bold]Parsed {len(rows)} quotes from {url}[/]")
    for r in rows[:3]:
        console.print(f"- {r['text']} — {r['author']} [{', '.join(r['tags'])}]")


@app.command("scrape-quotes")
@click.option(
    "--max-pages", default=3, show_default=True, type=int, help="How many pages to scrape"
)
@click.option(
    "--db", default="data/quotes.db", show_default=True, type=str, help="SQLite database path"
)
@click.option(
    "--delay",
    default=0.5,
    show_default=True,
    type=float,
    help="Polite pause between requests (seconds)",
)
@click.option("--ignore-robots", is_flag=True, help="Ignore robots.txt (not recommended)")
def scrape_quotes(max_pages: int, db: str, delay: float, ignore_robots: bool):
    """Fetch N pages, parse, and store in SQLite."""
    store = SqliteStore(db)
    inserted_before = store.count()
    total_parsed = 0

    disallows = [] if ignore_robots else fetch_disallows(BASE)

    try:
        for page in range(1, max_pages + 1):
            url = page_url(page)
            if not ignore_robots and not is_allowed(url, disallows):
                console.print(f"[yellow]Skipping disallowed[/] {url}")
                continue
            try:
                status, html = asyncio.run(fetch_text_retry(url))
            except Exception as e:
                console.print(f"[red]Fetch failed:[/] {e}")
                continue
            if status != 200:
                console.print(f"[red]HTTP {status}[/] {url}")
                continue
            rows = parse_quotes(html, url)
            total_parsed += len(rows)
            store.insert_quotes(rows)
            console.print(f"Page {page}: parsed {len(rows)}")
            time.sleep(delay)
    finally:
        inserted_after = store.count()
        delta = inserted_after - inserted_before
        console.print(
            f"[bold green]Done[/]. Parsed {total_parsed} rows. Inserted {delta} new rows. Total in DB: {inserted_after}"
        )
        store.close()


@app.command("stats")
@click.option(
    "--db", default="data/quotes.db", show_default=True, type=str, help="SQLite database path"
)
def stats(db: str):
    """Show simple dataset stats."""
    store = SqliteStore(db)
    console.print(f"[bold]Rows:[/] {store.count()}  (db: {db})")
    store.close()


@app.command("export-csv")
@click.option("--db", default="data/quotes.db", show_default=True)
@click.option("--out", default="data/quotes.csv", show_default=True)
def export_csv(db: str, out: str):
    """Export all quotes to a CSV file."""
    store = SqliteStore(db)
    rows: List[Dict] = store.all_quotes()
    store.close()

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["text", "author", "tags", "source_url"])
        w.writeheader()
        for r in rows:
            w.writerow({**r, "tags": ",".join(r["tags"])})
    console.print(f"[bold green]Wrote[/] {len(rows)} rows to {out}")


@app.command("top-authors")
@click.option("--db", default="data/quotes.db", show_default=True)
@click.option("--k", default=5, show_default=True, type=int)
def top_authors(db: str, k: int):
    """Show the top-K authors by quote count."""
    store = SqliteStore(db)
    pairs = store.top_authors(k)
    store.close()
    if not pairs:
        console.print("[yellow]No data yet. Run scrape first.[/]")
        raise SystemExit(0)
    console.print("[bold]Top authors[/]:")
    for author, n in pairs:
        console.print(f"- {author}: {n}")


if __name__ == "__main__":
    app()
