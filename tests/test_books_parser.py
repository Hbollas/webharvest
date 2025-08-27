from pathlib import Path
from webharvest.spiders.books import parse_books


def test_parse_books_sample():
    html = Path(__file__).with_name("sample_books.html").read_text(encoding="utf-8")
    rows = parse_books(html, "https://books.toscrape.com/catalogue/page-1.html")
    assert len(rows) >= 20  # books.toscrape pages typically list 20 items
    first = rows[0]
    assert {"title", "price_gbp", "rating", "in_stock", "product_url", "source_url"} <= set(
        first.keys()
    )
    assert isinstance(first["in_stock"], bool)
    if first["price_gbp"] is not None:
        assert first["price_gbp"] >= 0.0
    if first["rating"] is not None:
        assert 1 <= first["rating"] <= 5
