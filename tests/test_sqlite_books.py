from webharvest.storage.sqlite import SqliteStore


def test_books_roundtrip(tmp_path):
    db = tmp_path / "t.db"
    s = SqliteStore(str(db))
    before = s.count_books()
    s.insert_books(
        [
            {
                "title": "Example Book",
                "price_gbp": 12.34,
                "rating": 4,
                "in_stock": True,
                "product_url": "https://books.toscrape.com/catalogue/example_1/index.html",
                "source_url": "https://books.toscrape.com/catalogue/page-1.html",
            }
        ]
    )
    assert s.count_books() == before + 1
    # idempotent insert
    s.insert_books(
        [
            {
                "title": "Example Book",
                "price_gbp": 12.34,
                "rating": 4,
                "in_stock": True,
                "product_url": "https://books.toscrape.com/catalogue/example_1/index.html",
                "source_url": "https://books.toscrape.com/catalogue/page-1.html",
            }
        ]
    )
    assert s.count_books() == before + 1
    s.close()
