#!/usr/bin/env python3
"""
fix_g5_urls.py - Remediate every DB URL that follows the G5 Marketing Cloud
shape `/apartments/<state>/<city>/<route>` but points at a non-pricing route
(/apply, /contact-us, /schedule-a-tour, /amenities, /residents, …). Swap the
last segment to /floor-plans — that's where the G5 graphql units call fires,
and the scraper now parses those prices after commit 9006484.

Also re-runs 2 properties already on /floor-plans (Gwinnett Station, Hundred
Exchange) that failed the overnight rescrape — they may succeed now that
the retry goes through the full scrape path cleanly.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))
from apartment_scraper import PlaywrightScraper
from audit_properties import check_pricing_on_page
from backend.database import _get_conn, init_db
from pipeline_atlanta import save_progress

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fix_g5_urls")

ROOT = Path(__file__).parent
PROGRESS_PATH = ROOT / "pipeline_progress.jsonl"
BULK_JSONL = ROOT / "bulk_scrape_results.jsonl"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

G5_RE = re.compile(r"^(https?://[^/]+/apartments/[a-z]{2}/[^/]+/)[^/]+/?$")


def collect_g5_targets() -> list[tuple[str, str, str]]:
    """Return (name, old_url, new_url) tuples for every non-qualified G5 entry."""
    by_url: dict[str, dict] = {}
    with open(BULK_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            by_url[rec["url"]] = rec

    out = []
    for r in by_url.values():
        if r["status"] == "qualified":
            continue
        m = G5_RE.match(r["url"])
        if not m:
            continue
        new_url = m.group(1) + "floor-plans"
        out.append((r["name"], r["url"], new_url))
    return out


def run_pricing_check(url: str) -> dict:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(user_agent=UA, viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()
        try:
            from playwright_stealth import Stealth
            Stealth().apply_stealth_sync(page)
        except ImportError:
            pass
        try:
            info = check_pricing_on_page(page, url)
        except Exception as e:
            info = {"notes": f"pricing check failed: {e}", "platforms": [], "price_count": 0}
        browser.close()
        return info


def update_db(name: str, url: str, platform: str | None) -> None:
    conn = _get_conn()
    conn.execute(
        "UPDATE communities SET url = ?, platform = ? WHERE name = ?",
        (url, platform, name),
    )
    conn.commit()


def main() -> None:
    init_db()
    scraper = PlaywrightScraper(headless=True, timeout_ms=30000)

    targets = collect_g5_targets()
    log.info(f"Found {len(targets)} G5-pattern targets to remediate")

    results: list[dict] = []
    for name, old_url, new_url in targets:
        log.info(f"[{name}] {old_url} -> {new_url}")
        pricing_info = run_pricing_check(new_url)
        platforms = pricing_info.get("platforms", [])

        units = priced = 0
        scraper_platform = None
        scraper_note = ""
        try:
            prop = scraper.scrape(
                new_url,
                tab_labels=["0", "1", "2", "3", "4", "5"],
                tab_type="floor",
            )
            units = len(prop.units)
            priced = sum(1 for u in prop.units if u.rent_min and u.rent_min > 0)
            scraper_platform = getattr(prop, "platform", None)
            scraper_note = f"scraper: {units}u ({priced} priced)"
        except Exception as e:
            scraper_note = f"scraper error: {str(e)[:200]}"
        log.info(f"  {scraper_note}")

        qualified = priced >= 1
        platform = (platforms[0].lower() if platforms else None) or scraper_platform

        if qualified:
            update_db(name, new_url, platform)

        progress_record = {
            "property_name": name, "address": "", "city": "", "state": "GA", "zip": "",
            "website_url": new_url, "availability_url": new_url,
            "platform": platform if qualified else None,
            "status": "qualified" if qualified else "no_pricing",
            "notes": f"G5 URL remediation: {pricing_info.get('notes', '')}; {scraper_note}",
            "processed_at": datetime.now().isoformat(),
        }
        save_progress(str(PROGRESS_PATH), progress_record)

        bulk_record = {
            "id": None, "name": name, "url": new_url, "platform_hint": platform or "",
            "status": "qualified" if priced >= 1 else ("no_pricing" if units >= 1 else "no_units"),
            "units": units, "priced": priced, "platform_detected": scraper_platform,
            "error": "" if (units or priced) else scraper_note,
            "elapsed_s": 0.0, "scraped_at": datetime.utcnow().isoformat(),
        }
        with open(BULK_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(bulk_record, ensure_ascii=False) + "\n")
        results.append(bulk_record)
        time.sleep(1)

    print("\n=== G5 URL Remediation Summary ===")
    for r in results:
        print(f"  {r['status']:12s} | {r['name']:34s} | {r['units']:3d}u/{r['priced']:3d}p | {r['url']}")
    total_q = sum(1 for r in results if r["status"] == "qualified")
    total_p = sum(r["priced"] for r in results)
    print(f"\n{total_q}/{len(results)} qualified, {total_p} priced units added")


if __name__ == "__main__":
    main()
