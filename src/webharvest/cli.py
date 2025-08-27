import click
import asyncio
from .http import fetch_text
from rich.console import Console
from .spiders.quotes import parse_quotes, page_url

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
        
if __name__ == "__main__":
    app()
