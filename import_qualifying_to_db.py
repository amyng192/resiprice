#!/usr/bin/env python3
"""
import_qualifying_to_db.py - Push qualifying_communities.csv into the app's
SQLite DB, with a couple of safety features beyond the raw
import_communities() helper:

  * Updates rows whose name matches but URL has changed (i.e. we remediated
    the URL). The default import uses INSERT OR IGNORE on url which would
    leave the stale junk URL in place.
  * Dedupes rows within the CSV by url before inserting.
  * Prints a summary (new/updated/skipped) so we can sanity check.

Run from the scraper/ directory:
    python import_qualifying_to_db.py
"""

import csv
from pathlib import Path

from backend.database import _get_conn, init_db

CSV_PATH = Path(__file__).parent / "qualifying_communities.csv"


def main() -> None:
    init_db()
    conn = _get_conn()

    # Load existing DB state
    existing_by_url = {
        r["url"]: dict(r)
        for r in conn.execute("SELECT id, name, url, platform FROM communities").fetchall()
    }
    existing_by_name = {
        r["name"]: dict(r)
        for r in conn.execute("SELECT id, name, url, platform FROM communities").fetchall()
    }

    # Load CSV, dedupe by url (first occurrence wins)
    seen_urls: set[str] = set()
    rows: list[dict] = []
    csv_dup_rows = 0
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            name = (row.get("name") or "").strip()
            url = (row.get("url") or "").strip()
            platform = (row.get("platform") or "").strip() or None
            if not name or not url:
                continue
            if url in seen_urls:
                csv_dup_rows += 1
                continue
            seen_urls.add(url)
            rows.append({"name": name, "url": url, "platform": platform})

    inserted = updated = skipped = 0
    for r in rows:
        if r["url"] in existing_by_url:
            skipped += 1
            continue
        # URL is new — check if a row with the same name (and a different URL)
        # already exists and should be updated in place
        existing = existing_by_name.get(r["name"])
        if existing and existing["url"] != r["url"]:
            conn.execute(
                "UPDATE communities SET url = ?, platform = ? WHERE id = ?",
                (r["url"], r["platform"], existing["id"]),
            )
            updated += 1
            continue
        conn.execute(
            "INSERT INTO communities (name, url, platform) VALUES (?, ?, ?)",
            (r["name"], r["url"], r["platform"]),
        )
        inserted += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM communities").fetchone()[0]
    print(f"CSV rows (deduped): {len(rows)} (+{csv_dup_rows} duplicate urls in CSV)")
    print(f"  inserted: {inserted}")
    print(f"  updated:  {updated}")
    print(f"  skipped:  {skipped}  (URL already present)")
    print(f"DB total: {total} communities")


if __name__ == "__main__":
    main()
