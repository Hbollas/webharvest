import click
import time 
import asyncio
from pathlib import Path
from .http import fetch_text
from rich.console import Console
from .spiders.quotes import parse_quotes, page_url
from .storage.sqlite import SqliteStore

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
    status, html = asyncio.run(fetch_text(url))
    console.print(f"Status: {status}")
    console.print(f"HTML snippet: {html[:200].replace('\n',' ')}...")

@app.command("parse-quotes")
@click.option("--page", default=1, show_default=True, type=int)
def parse_quotes_cmd(page: int):
    """Fetch one page and parse quotes (prints a small preview)."""
    url = page_url(page)
    status, html = asyncio.run(fetch_text(url))
    if status != 200:
        console.print(f"[red]HTTP {status}[/] {url}")
        raise SystemExit(1)
    rows = parse_quotes(html, url)
    console.print(f"[bold]Parsed {len(rows)} quotes from {url}[/]")
    for r in rows[:3]:
        console.print(f"- {r['text']} — {r['author']} [{', '.join(r['tags'])}]")

@app.command("scrape-quotes")
@click.option("--max-pages", default=3, show_default=True, type=int, help="How many pages to scrape")
@click.option("--db", default="data/quotes.db", show_default=True, type=str, help="SQLite database path")
def scrape_quotes(max_pages: int, db: str):
    """Fetch N pages, parse, and store in SQLite."""
    store = SqliteStore(db)
    inserted_before = store.count()
    total_parsed = 0

    try:
        for page in range(1, max_pages + 1):
            url = page_url(page)
            status, html = asyncio.run(fetch_text(url))
            if status != 200:
                console.print(f"[red]HTTP {status}[/] {url}")
                continue
            rows = parse_quotes(html, url)
            total_parsed += len(rows)
            store.insert_quotes(rows)
            console.print(f"Page {page}: parsed {len(rows)}")
            time.sleep(0.5)  # polite pause
    finally:
        inserted_after = store.count()
        delta = inserted_after - inserted_before
        console.print(f"[bold green]Done[/]. Parsed {total_parsed} rows. Inserted {delta} new rows. Total in DB: {inserted_after}")
        store.close()

@app.command("stats")
@click.option("--db", default="data/quotes.db", show_default=True, type=str, help="SQLite database path")
def stats(db: str):
    """Show simple dataset stats."""
    store = SqliteStore(db)
    console.print(f"[bold]Rows:[/] {store.count()}  (db: {db})")
    store.close()

if __name__ == "__main__":
    app()
