from pathlib import Path
import sqlite3
from typing import Iterable, Dict, List, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT NOT NULL,
  author TEXT NOT NULL,
  tags TEXT NOT NULL,
  source_url TEXT NOT NULL,
  UNIQUE(text, author, source_url) ON CONFLICT IGNORE
);

CREATE TABLE IF NOT EXISTS books (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  price_gbp REAL,
  rating INTEGER,
  in_stock INTEGER NOT NULL,
  product_url TEXT NOT NULL,
  source_url TEXT NOT NULL,
  UNIQUE(title, product_url) ON CONFLICT IGNORE
);
"""


class SqliteStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---------- QUOTES ----------
    def insert_quotes(self, rows: Iterable[Dict]) -> int:
        self.conn.executemany(
            "INSERT OR IGNORE INTO quotes (text, author, tags, source_url) VALUES (?, ?, ?, ?)",
            ((r["text"], r["author"], ",".join(r["tags"]), r["source_url"]) for r in rows),
        )
        self.conn.commit()
        return self.conn.total_changes

    def count(self) -> int:
        (n,) = self.conn.execute("SELECT COUNT(*) FROM quotes").fetchone()
        return int(n)

    def all_quotes(self) -> List[Dict]:
        cur = self.conn.execute("SELECT text, author, tags, source_url FROM quotes ORDER BY id")
        rows = []
        for text, author, tags, source_url in cur.fetchall():
            rows.append(
                {
                    "text": text,
                    "author": author,
                    "tags": tags.split(",") if tags else [],
                    "source_url": source_url,
                }
            )
        return rows

    def top_authors(self, k: int = 5) -> List[Tuple[str, int]]:
        cur = self.conn.execute(
            "SELECT author, COUNT(*) as n FROM quotes GROUP BY author ORDER BY n DESC, author ASC LIMIT ?",
            (k,),
        )
        return cur.fetchall()

    # ---------- BOOKS ----------
    def insert_books(self, rows: Iterable[Dict]) -> int:
        self.conn.executemany(
            """INSERT OR IGNORE INTO books
               (title, price_gbp, rating, in_stock, product_url, source_url)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                (
                    r["title"],
                    r.get("price_gbp"),
                    r.get("rating"),
                    1 if r.get("in_stock") else 0,
                    r["product_url"],
                    r["source_url"],
                )
                for r in rows
            ),
        )
        self.conn.commit()
        return self.conn.total_changes

    def count_books(self) -> int:
        (n,) = self.conn.execute("SELECT COUNT(*) FROM books").fetchone()
        return int(n)

    def top_rated_books(self, k: int = 5) -> List[Tuple[str, float, int]]:
        cur = self.conn.execute(
            """SELECT title, COALESCE(price_gbp, 0.0) as price_gbp, COALESCE(rating, 0) as rating
               FROM books WHERE rating IS NOT NULL
               ORDER BY rating DESC, price_gbp DESC, title ASC
               LIMIT ?""",
            (k,),
        )
        return cur.fetchall()

    def close(self) -> None:
        self.conn.close()
