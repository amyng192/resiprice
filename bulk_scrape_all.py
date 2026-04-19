#!/usr/bin/env python3
"""
bulk_scrape_all.py - Run the PlaywrightScraper against every community in
the app's SQLite DB. Runs 4 in parallel (matches backend/api.py). Streams
results to bulk_scrape_results.jsonl so we can resume on interruption and
analyze afterward.

After the run finishes:
  * bulk_scrape_qualified.csv     — communities where scraper returned >=1
                                    unit with rent_min > 0
  * bulk_scrape_failed.csv        — everything else, with error/reason

Usage:
    python bulk_scrape_all.py                  # scrape everything not yet done
    python bulk_scrape_all.py --limit 100      # just the first 100 pending
    python bulk_scrape_all.py --reset          # wipe results and start over
    python bulk_scrape_all.py --workers 4      # parallelism (default 4)
    python bulk_scrape_all.py --export-only    # rebuild the summary CSVs
"""

import argparse
import csv
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from apartment_scraper import PlaywrightScraper
from backend.database import _get_conn, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("bulk_scrape")
logging.getLogger("ApartmentScraper").setLevel(logging.WARNING)

ROOT = Path(__file__).parent
RESULTS_PATH = ROOT / "bulk_scrape_results.jsonl"
QUALIFIED_CSV = ROOT / "bulk_scrape_qualified.csv"
FAILED_CSV = ROOT / "bulk_scrape_failed.csv"

SCRAPER_TIMEOUT_MS = 20000  # per-action timeout inside the scraper
FILE_LOCK = threading.Lock()


def load_done_urls() -> set[str]:
    """URLs we've already scraped (resume support)."""
    if not RESULTS_PATH.exists():
        return set()
    done = set()
    with open(RESULTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                done.add(rec["url"])
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def append_result(record: dict) -> None:
    with FILE_LOCK:
        with open(RESULTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def worker(community: dict) -> dict:
    """Scrape one community in its own thread with its own scraper instance."""
    name = community["name"]
    url = community["url"]
    started = time.time()
    record = {
        "id": community["id"],
        "name": name,
        "url": url,
        "platform_hint": community.get("platform") or "",
        "status": "error",
        "units": 0,
        "priced": 0,
        "platform_detected": None,
        "error": "",
        "elapsed_s": 0.0,
        "scraped_at": datetime.utcnow().isoformat(),
    }
    scraper = PlaywrightScraper(headless=True, timeout_ms=SCRAPER_TIMEOUT_MS)
    try:
        prop = scraper.scrape(
            url,
            tab_labels=["0", "1", "2", "3", "4", "5"],
            tab_type="floor",
        )
        units = len(prop.units)
        priced = sum(1 for u in prop.units if u.rent_min and u.rent_min > 0)
        record["units"] = units
        record["priced"] = priced
        record["platform_detected"] = getattr(prop, "platform", None)
        if priced >= 1:
            record["status"] = "qualified"
        elif units >= 1:
            record["status"] = "no_pricing"
        else:
            record["status"] = "no_units"
    except Exception as e:
        record["error"] = str(e)[:300]
        record["status"] = "error"
    record["elapsed_s"] = round(time.time() - started, 1)
    return record


def load_pending(limit: int = 0) -> list[dict]:
    init_db()
    conn = _get_conn()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, name, url, platform FROM communities ORDER BY id ASC"
    ).fetchall()]
    done = load_done_urls()
    pending = [r for r in rows if r["url"] not in done]
    if limit > 0:
        pending = pending[:limit]
    return pending


def export_summary() -> None:
    if not RESULTS_PATH.exists():
        print("No results yet — run scraping first.")
        return

    # Load + dedupe by url (last wins)
    records: dict[str, dict] = {}
    with open(RESULTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records[rec["url"]] = rec
            except (json.JSONDecodeError, KeyError):
                continue

    qualified = [r for r in records.values() if r["status"] == "qualified"]
    failed = [r for r in records.values() if r["status"] != "qualified"]

    qualified.sort(key=lambda r: r["name"])
    failed.sort(key=lambda r: (r["status"], r["name"]))

    with open(QUALIFIED_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name", "url", "platform", "units", "priced", "elapsed_s"])
        for r in qualified:
            w.writerow([
                r["name"], r["url"],
                r.get("platform_detected") or r.get("platform_hint") or "",
                r["units"], r["priced"], r.get("elapsed_s", ""),
            ])

    with open(FAILED_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name", "url", "platform_hint", "status", "units", "priced", "error", "elapsed_s"])
        for r in failed:
            w.writerow([
                r["name"], r["url"], r.get("platform_hint") or "",
                r["status"], r["units"], r["priced"],
                r.get("error", "")[:300], r.get("elapsed_s", ""),
            ])

    by_status: dict[str, int] = {}
    for r in records.values():
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1

    print(f"\nTotal scraped: {len(records)}")
    for s, c in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {s:12s}  {c:5d}")
    print(f"\n  {QUALIFIED_CSV.name}: {len(qualified)}")
    print(f"  {FAILED_CSV.name}:    {len(failed)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Max communities to scrape (0=all)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers")
    ap.add_argument("--reset", action="store_true", help="Wipe results file and start over")
    ap.add_argument("--export-only", action="store_true", help="Just rebuild summary CSVs")
    args = ap.parse_args()

    if args.export_only:
        export_summary()
        return

    if args.reset and RESULTS_PATH.exists():
        RESULTS_PATH.unlink()
        log.info(f"Removed {RESULTS_PATH.name}")

    pending = load_pending(args.limit)
    total = len(pending)
    if total == 0:
        log.info("Nothing to scrape — all communities already have results")
        export_summary()
        return

    log.info(f"Starting bulk scrape: {total} communities, {args.workers} workers, "
             f"{SCRAPER_TIMEOUT_MS}ms per-action timeout")

    counters = {"qualified": 0, "no_pricing": 0, "no_units": 0, "error": 0}
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, c): c for c in pending}
        for i, fut in enumerate(as_completed(futures), 1):
            community = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                rec = {
                    "id": community["id"],
                    "name": community["name"],
                    "url": community["url"],
                    "platform_hint": community.get("platform") or "",
                    "status": "error",
                    "units": 0,
                    "priced": 0,
                    "error": f"future crash: {str(e)[:250]}",
                    "elapsed_s": 0.0,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
            append_result(rec)
            counters[rec["status"]] = counters.get(rec["status"], 0) + 1

            if i % 25 == 0 or i == total:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                eta_s = (total - i) / rate if rate > 0 else 0
                log.info(
                    f"[{i}/{total}] q={counters['qualified']} "
                    f"np={counters['no_pricing']} nu={counters['no_units']} "
                    f"err={counters['error']} | "
                    f"{rate*60:.1f}/min, ETA {eta_s/60:.0f} min"
                )

    log.info(f"Done in {(time.time()-start)/60:.1f} min")
    export_summary()


if __name__ == "__main__":
    main()
