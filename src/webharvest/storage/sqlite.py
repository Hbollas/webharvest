from pathlib import Path
import sqlite3
from typing import Iterable, Dict, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT NOT NULL,
  author TEXT NOT NULL,
  tags TEXT NOT NULL,
  source_url TEXT NOT NULL,
  UNIQUE(text, author, source_url) ON CONFLICT IGNORE
);
"""

class SqliteStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute(SCHEMA)
        self.conn.commit()

    def insert_quotes(self, rows: Iterable[Dict]) -> int:
        cur = self.conn.executemany(
            "INSERT OR IGNORE INTO quotes (text, author, tags, source_url) VALUES (?, ?, ?, ?)",
            ((r["text"], r["author"], ",".join(r["tags"]), r["source_url"]) for r in rows),
        )
        self.conn.commit()
        # sqlite3's executemany doesn't always give inserted count reliably; recompute via changes()
        return self.conn.total_changes

    def count(self) -> int:
        (n,) = self.conn.execute("SELECT COUNT(*) FROM quotes").fetchone()
        return int(n)

    def close(self) -> None:
        self.conn.close()
