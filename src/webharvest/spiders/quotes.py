from bs4 import BeautifulSoup
from typing import List, Dict
from urllib.parse import urljoin

BASE = "https://quotes.toscrape.com/"

def page_url(page: int) -> str:
    return BASE if page <= 1 else urljoin(BASE, f"/page/{page}/")

def parse_quotes(html: str, source_url: str) -> List[Dict]:
    """
    Extract quotes from a single page.

    CSS map:
      - One quote block: div.quote
      - Quote text:      span.text   (e.g., “...”)  -> strip the curly quotes
      - Author:          small.author
      - Tags:            div.tags a.tag
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []

    for q in soup.select("div.quote"):
        text_el = q.select_one("span.text")
        author_el = q.select_one("small.author")
        tag_els = q.select("div.tags a.tag")

        if not (text_el and author_el):
            continue

        rows.append({
            "text": text_el.get_text(strip=True).strip("“”"),
            "author": author_el.get_text(strip=True),
            "tags": [t.get_text(strip=True) for t in tag_els],
            "source_url": source_url,
        })

    return rows
