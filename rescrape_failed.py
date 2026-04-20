#!/usr/bin/env python3
"""
rescrape_failed.py - Re-scrape every entry in bulk_scrape_results.jsonl
whose latest record isn't 'qualified'. Appends new records (last-write-wins
on URL), so the next export picks up the updated state.

The scraper was just taught to parse G5 Marketing Cloud prices arrays, so
this is the cheap way to find out how many of the ~520 failed entries use
that format (or any other platform that now works thanks to scraper fixes).

Usage:
    python rescrape_failed.py                    # re-scrape everything failed
    python rescrape_failed.py --limit 50         # just the first 50
    python rescrape_failed.py --status no_units  # only no_units
    python rescrape_failed.py --workers 4
"""

import argparse
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rescrape")
logging.getLogger("ApartmentScraper").setLevel(logging.WARNING)

ROOT = Path(__file__).parent
RESULTS_PATH = ROOT / "bulk_scrape_results.jsonl"
FILE_LOCK = threading.Lock()


def load_latest_by_url() -> dict[str, dict]:
    by_url: dict[str, dict] = {}
    with open(RESULTS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                by_url[rec["url"]] = rec
            except (json.JSONDecodeError, KeyError):
                continue
    return by_url


def append_result(record: dict) -> None:
    with FILE_LOCK:
        with open(RESULTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def worker(community: dict) -> dict:
    name = community["name"]
    url = community["url"]
    started = time.time()
    record = {
        "id": community.get("id"),
        "name": name,
        "url": url,
        "platform_hint": community.get("platform_hint") or "",
        "status": "error",
        "units": 0,
        "priced": 0,
        "platform_detected": None,
        "error": "",
        "elapsed_s": 0.0,
        "scraped_at": datetime.utcnow().isoformat(),
    }
    scraper = PlaywrightScraper(headless=True, timeout_ms=20000)
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument(
        "--status",
        default="all",
        choices=["all", "no_units", "no_pricing", "error"],
        help="Which failed status to re-scrape (default: all failed)",
    )
    args = ap.parse_args()

    by_url = load_latest_by_url()
    pending = [
        r for r in by_url.values()
        if r["status"] != "qualified"
        and (args.status == "all" or r["status"] == args.status)
    ]
    pending.sort(key=lambda r: r["name"])
    if args.limit > 0:
        pending = pending[: args.limit]
    total = len(pending)
    log.info(f"Re-scraping {total} failed entries (status filter: {args.status})")

    counters = {"qualified": 0, "no_pricing": 0, "no_units": 0, "error": 0}
    moved_to_qualified: list[str] = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, c): c for c in pending}
        for i, fut in enumerate(as_completed(futures), 1):
            community = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                rec = {
                    "id": community.get("id"),
                    "name": community["name"],
                    "url": community["url"],
                    "platform_hint": community.get("platform_hint") or "",
                    "status": "error",
                    "units": 0,
                    "priced": 0,
                    "error": f"future crash: {str(e)[:250]}",
                    "elapsed_s": 0.0,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
            append_result(rec)
            counters[rec["status"]] = counters.get(rec["status"], 0) + 1
            # Track the wins
            old_status = community.get("status")
            if rec["status"] == "qualified" and old_status != "qualified":
                moved_to_qualified.append(f"{rec['name']}  ({old_status}->qualified, {rec['priced']} priced)")

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
    print(f"\n{len(moved_to_qualified)} rows moved to QUALIFIED:")
    for m in moved_to_qualified[:50]:
        print(f"  + {m}")
    if len(moved_to_qualified) > 50:
        print(f"  ... and {len(moved_to_qualified) - 50} more")


if __name__ == "__main__":
    main()
