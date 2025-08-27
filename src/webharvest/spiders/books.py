from bs4 import BeautifulSoup
from typing import List, Dict
from urllib.parse import urljoin

BASE = "https://books.toscrape.com/"


def page_url(page: int) -> str:
    # Listing pages live under /catalogue/page-N.html
    if page <= 1:
        return urljoin(BASE, "catalogue/page-1.html")
    return urljoin(BASE, f"catalogue/page-{page}.html")


_RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


def parse_books(html: str, source_url: str) -> List[Dict]:
    """
    Extract book cards from a listing page.
    Each card: article.product_pod
      - title: h3 a[title]
      - link:  h3 a[href] (relative) -> urljoin with current page
      - price: p.price_color, e.g. '£51.77'
      - rating: p.star-rating.<Word> -> map to 1..5
      - stock (bool): p.instock.availability contains 'In stock'
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []
    for card in soup.select("article.product_pod"):
        a = card.select_one("h3 a")
        price_el = card.select_one("p.price_color")
        rating_el = card.select_one("p.star-rating")
        stock_el = card.select_one("p.instock.availability")

        if not (a and price_el and rating_el):
            continue

        title = a.get("title", "").strip()
        href = a.get("href", "")
        product_url = urljoin(source_url, href)

        price_text = price_el.get_text(strip=True).replace("£", "").strip()
        try:
            price_gbp = float(price_text)
        except ValueError:
            price_gbp = None  # rare, but be safe

        # rating appears as classes: ["star-rating", "Three"] etc.
        rating_word = next((cls for cls in rating_el.get("class", []) if cls != "star-rating"), "")
        rating = _RATING_MAP.get(rating_word, None)

        in_stock = False
        if stock_el:
            in_stock = "in stock" in stock_el.get_text(" ", strip=True).lower()

        rows.append(
            {
                "title": title,
                "price_gbp": price_gbp,
                "rating": rating,
                "in_stock": in_stock,
                "product_url": product_url,
                "source_url": source_url,
            }
        )
    return rows
