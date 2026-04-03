import os
import sqlite3
import threading
import logging
from pathlib import Path

log = logging.getLogger("resiprice.database")

_DB_PATH = os.environ.get(
    "RESIPRICE_DB_PATH",
    str(Path(__file__).parent.parent / "data" / "resiprice.db"),
)

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Return a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(_DB_PATH)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


def init_db() -> None:
    """Create the database file and tables if they don't exist."""
    Path(_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS communities (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            url             TEXT NOT NULL UNIQUE,
            platform        TEXT,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            last_scraped_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_communities_name
            ON communities(name COLLATE NOCASE);
    """)
    conn.commit()
    log.info(f"Database initialized at {_DB_PATH}")


def search_communities(query: str, limit: int = 10) -> list[dict]:
    """Search communities by name (case-insensitive substring match)."""
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT id, name, url, platform
        FROM communities
        WHERE name LIKE ?
        ORDER BY last_scraped_at DESC NULLS LAST, name ASC
        LIMIT ?
        """,
        (f"%{query}%", limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_communities() -> list[dict]:
    """Return all communities."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, name, url, platform FROM communities ORDER BY name ASC"
    ).fetchall()
    return [dict(r) for r in rows]


def upsert_community(name: str, url: str, platform: str | None = None) -> int:
    """Insert or update a community. Returns the row id."""
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO communities (name, url, platform, last_scraped_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(url) DO UPDATE SET
            name = excluded.name,
            platform = excluded.platform,
            last_scraped_at = datetime('now')
        """,
        (name, url, platform),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM communities WHERE url = ?", (url,)).fetchone()
    return row["id"]


def import_communities(rows: list[dict]) -> int:
    """Bulk-import communities. Skips duplicates. Returns count of new rows."""
    conn = _get_conn()
    inserted = 0
    for row in rows:
        name = row.get("name", "").strip()
        url = row.get("url", "").strip()
        if not name or not url:
            continue
        platform = row.get("platform", "").strip() or None
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO communities (name, url, platform)
                VALUES (?, ?, ?)
                """,
                (name, url, platform),
            )
            inserted += conn.total_changes  # approximation within transaction
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def delete_community(community_id: int) -> bool:
    """Delete a community by ID. Returns True if a row was deleted."""
    conn = _get_conn()
    cursor = conn.execute("DELETE FROM communities WHERE id = ?", (community_id,))
    conn.commit()
    return cursor.rowcount > 0
