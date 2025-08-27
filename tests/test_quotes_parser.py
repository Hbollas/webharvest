from pathlib import Path
from webharvest.spiders.quotes import parse_quotes

def test_parse_quotes_sample():
    html = Path(__file__).with_name("sample_quotes.html").read_text(encoding="utf-8")
    rows = parse_quotes(html, "https://quotes.toscrape.com/")
    assert len(rows) >= 10  # site shows 10 quotes on page 1
    first = rows[0]
    assert {"text", "author", "tags", "source_url"} <= set(first.keys())
    assert isinstance(first["tags"], list)
