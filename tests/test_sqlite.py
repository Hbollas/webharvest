from webharvest.storage.sqlite import SqliteStore

def test_sqlite_roundtrip(tmp_path):
    db = tmp_path / "t.db"
    s = SqliteStore(str(db))
    before = s.count()
    # insert one
    s.insert_quotes([{
        "text": "Hello world",
        "author": "Me",
        "tags": ["x","y"],
        "source_url": "https://example.com"
    }])
    assert s.count() == before + 1
    # idempotent (UNIQUE)
    s.insert_quotes([{
        "text": "Hello world",
        "author": "Me",
        "tags": ["x","y"],
        "source_url": "https://example.com"
    }])
    assert s.count() == before + 1
    s.close()
