import click
import csv
import asyncio
from rich.console import Console

from .http import fetch_text_retry, fetch_many
from .spiders.quotes import parse_quotes, page_url, BASE
from .storage.sqlite import SqliteStore
from .spiders import books as books_spider
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
@click.option("--max-pages", default=3, show_default=True, type=int)
@click.option("--db", default="data/quotes.db", show_default=True, type=str)
@click.option("--delay", default=0.5, show_default=True, type=float)
@click.option("--concurrency", default=5, show_default=True, type=int)
@click.option("--ignore-robots", is_flag=True)
def scrape_quotes(max_pages: int, db: str, delay: float, concurrency: int, ignore_robots: bool):
    """Fetch N pages, parse, and store in SQLite."""
    store = SqliteStore(db)
    inserted_before = store.count()
    total_parsed = 0

    disallows = [] if ignore_robots else fetch_disallows(BASE)
    urls = [page_url(p) for p in range(1, max_pages + 1)]
    if not ignore_robots:
        urls = [u for u in urls if is_allowed(u, disallows)]

    # Fetch concurrently
    results = asyncio.run(fetch_many(urls, concurrency=concurrency, delay=delay))

    try:
        for u in urls:
            status, html = results.get(u, (0, ""))
            if status != 200 or not html:
                console.print(f"[red]HTTP {status}[/] {u}")
                continue
            rows = parse_quotes(html, u)
            total_parsed += len(rows)
            store.insert_quotes(rows)
            console.print(f"{u} -> parsed {len(rows)}")
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


@app.command("parse-books")
@click.option("--page", default=1, show_default=True, type=int)
def parse_books_cmd(page: int):
    """Fetch one book listing page and preview a few entries."""
    url = books_spider.page_url(page)
    status, html = asyncio.run(fetch_text_retry(url))
    if status != 200:
        console.print(f"[red]HTTP {status}[/] {url}")
        raise SystemExit(1)
    rows = books_spider.parse_books(html, url)
    console.print(f"[bold]Parsed {len(rows)} books from {url}[/]")
    for r in rows[:3]:
        console.print(
            f"- {r['title']} | £{r['price_gbp']} | rating={r['rating']} | in_stock={r['in_stock']}"
        )


@app.command("scrape-books")
@click.option("--max-pages", default=3, show_default=True, type=int)
@click.option("--db", default="data/quotes.db", show_default=True, type=str)
@click.option("--delay", default=0.5, show_default=True, type=float)
@click.option("--concurrency", default=5, show_default=True, type=int)
@click.option("--ignore-robots", is_flag=True)
def scrape_books(max_pages: int, db: str, delay: float, concurrency: int, ignore_robots: bool):
    """Scrape book listings and store them."""
    store = SqliteStore(db)
    before = store.count_books()
    total_parsed = 0

    disallows = [] if ignore_robots else fetch_disallows(books_spider.BASE)
    urls = [books_spider.page_url(p) for p in range(1, max_pages + 1)]
    if not ignore_robots:
        urls = [u for u in urls if is_allowed(u, disallows)]

    results = asyncio.run(fetch_many(urls, concurrency=concurrency, delay=delay))

    try:
        for u in urls:
            status, html = results.get(u, (0, ""))
            if status != 200 or not html:
                console.print(f"[red]HTTP {status}[/] {u}")
                continue
            rows = books_spider.parse_books(html, u)
            total_parsed += len(rows)
            store.insert_books(rows)
            console.print(f"{u} -> parsed {len(rows)}")
    finally:
        after = store.count_books()
        console.print(
            f"[bold green]Done[/]. Parsed {total_parsed} rows. Inserted {after - before} new rows. Total books: {after}"
        )
        store.close()


@app.command("book-stats")
@click.option("--db", default="data/quotes.db", show_default=True, type=str)
@click.option("--k", default=5, show_default=True, type=int)
def book_stats(db: str, k: int):
    """Show top-rated books."""
    store = SqliteStore(db)
    pairs = store.top_rated_books(k)
    store.close()
    if not pairs:
        console.print("[yellow]No books yet. Run scrape-books first.[/]")
        return
    console.print("[bold]Top-rated books[/]:")
    for title, price, rating in pairs:
        console.print(f"- {title} (rating {rating}, £{price:.2f})")


@app.command("report")
@click.option("--db", default="data/quotes.db", show_default=True)
@click.option("--k", default=5, show_default=True, help="Top-K items to display")
def report(db: str, k: int):
    """Print a quick, portfolio-friendly project report."""
    store = SqliteStore(db)

    # Totals
    q_total = store.count()
    b_total = store.count_books()
    console.print("[bold]webharvest report[/]")
    console.print(f"- Quotes in DB: {q_total}")
    console.print(f"- Books in DB:  {b_total}")

    # Quotes: top authors & tags
    if q_total:
        console.print("\n[bold]Top authors[/]:")
        for author, n in store.top_authors(k):
            console.print(f"- {author}: {n}")

        console.print("\n[bold]Top tags[/]:")
        for tag, n in store.tag_counts(k):
            console.print(f"- {tag}: {n}")

    # Books: stock + avg price by rating
    if b_total:
        in_stock, total = store.stock_counts()
        console.print(f"\n[bold]Books stock[/]: {in_stock}/{total} in stock")
        console.print("[bold]Average price by rating[/]:")
        rows = store.avg_price_by_rating()
        if rows:
            for rating, avg in rows:
                console.print(f"- {rating}★: £{avg:.2f}")
        else:
            console.print("- no price/rating data")

    store.close()


if __name__ == "__main__":
    app()
