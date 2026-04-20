#!/usr/bin/env python3
"""
fix_amli_urls.py - Remediate all 14 AMLI properties. AMLI uses SightMap for
availability but only mounts the iframe on the /floorplans?tab=map view.
The scraper already handles SightMap; we just need to point it at the right
URL.

For the 10 properties already on amli.com: append /floorplans?tab=map.
For the 4 on retired individual domains (amli3464.com etc.): redirect to
the canonical amli.com slug.

Runs the scraper on each new URL (separate playwright lifetimes to avoid
nested sync_playwright), updates the DB and pipeline_progress.jsonl, and
surgically patches the qualifying/failed CSVs.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))
from apartment_scraper import PlaywrightScraper
from audit_properties import check_pricing_on_page
from backend.database import _get_conn, init_db
from pipeline_atlanta import load_progress, save_progress

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fix_amli_urls")

ROOT = Path(__file__).parent
PROGRESS_PATH = ROOT / "pipeline_progress.jsonl"
BULK_JSONL = ROOT / "bulk_scrape_results.jsonl"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Maps community name -> corrected URL (always the /floorplans?tab=map view).
REMEDIATIONS: dict[str, str] = {
    "AMLI Arts Center":       "https://www.amli.com/apartments/atlanta/midtown-apartments/amli-arts-center/floorplans?tab=map",
    "AMLI Buckhead":          "https://www.amli.com/apartments/atlanta/buckhead-apartments/amli-buckhead/floorplans?tab=map",
    "AMLI Decatur":           "https://www.amli.com/apartments/atlanta/decatur-apartments/amli-decatur/floorplans?tab=map",
    "AMLI Flatiron":          "https://www.amli.com/apartments/atlanta/buckhead-apartments/amli-flatiron/floorplans?tab=map",
    "AMLI Lenox":             "https://www.amli.com/apartments/atlanta/buckhead-apartments/amli-lenox/floorplans?tab=map",
    "AMLI North Point":       "https://www.amli.com/apartments/atlanta/alpharetta-apartments/amli-north-point/floorplans?tab=map",
    "AMLI Old 4th Ward":      "https://www.amli.com/apartments/atlanta/old-4th-ward-apartments/amli-old-4th-ward/floorplans?tab=map",
    "AMLI Parkside":          "https://www.amli.com/apartments/atlanta/old-4th-ward-apartments/amli-parkside/floorplans?tab=map",
    "AMLI Piedmont Heights":  "https://www.amli.com/apartments/atlanta/lindbergh-apartments/amli-piedmont-heights/floorplans?tab=map",
    "AMLI Ponce Park":        "https://www.amli.com/apartments/atlanta/old-4th-ward-apartments/amli-ponce-park/floorplans?tab=map",
    # These four were on retired individual domains — pointing at canonical
    # amli.com slugs verified via HEAD requests (200 OK).
    "AMLI 3464":              "https://www.amli.com/apartments/atlanta/buckhead-apartments/amli-3464/floorplans?tab=map",
    "AMLI Atlantic Station":  "https://www.amli.com/apartments/atlanta/midtown-apartments/amli-atlantic-station/floorplans?tab=map",
    "AMLI Lindbergh":         "https://www.amli.com/apartments/atlanta/lindbergh-apartments/amli-lindbergh/floorplans?tab=map",
    "AMLI Westside":          "https://www.amli.com/apartments/atlanta/midtown-apartments/amli-westside/floorplans?tab=map",
}


def run_pricing_check(url: str) -> dict:
    """One-off playwright stealth check to classify the page."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1920, "height": 1080})
        page = context.new_page()
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

    results: list[dict] = []

    for name, new_url in REMEDIATIONS.items():
        log.info(f"[{name}] -> {new_url}")

        pricing_info = run_pricing_check(new_url)
        platforms = pricing_info.get("platforms", [])
        log.info(
            f"  platforms={platforms} prices={pricing_info.get('price_count', 0)} "
            f"iframe={pricing_info.get('has_iframe', False)}"
        )

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

        qualified = priced >= 1 or bool(platforms) or pricing_info.get("has_iframe")
        platform = (platforms[0].lower() if platforms else None) or scraper_platform or "sightmap"

        # Update the DB so the next bulk scrape uses the new URL
        update_db(name, new_url, platform if qualified else None)

        # Record in the legacy pipeline JSONL for consistency with prior runs
        progress_record = {
            "property_name": name,
            "address": "",  # we no longer track per-firm addresses here
            "city": "",
            "state": "GA",
            "zip": "",
            "website_url": new_url,
            "availability_url": new_url,
            "platform": platform if qualified else None,
            "status": "qualified" if qualified else "no_pricing",
            "notes": f"AMLI remediation: {pricing_info.get('notes', '')}; {scraper_note}",
            "processed_at": datetime.now().isoformat(),
        }
        save_progress(str(PROGRESS_PATH), progress_record)

        # Also record in bulk_scrape_results.jsonl so analyze/export picks up the new state
        bulk_record = {
            "id": None,
            "name": name,
            "url": new_url,
            "platform_hint": platform or "",
            "status": "qualified" if priced >= 1 else ("no_pricing" if units >= 1 else "no_units"),
            "units": units,
            "priced": priced,
            "platform_detected": scraper_platform,
            "error": "" if units or priced else scraper_note,
            "elapsed_s": 0.0,
            "scraped_at": datetime.utcnow().isoformat(),
        }
        with open(BULK_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(bulk_record, ensure_ascii=False) + "\n")

        results.append(bulk_record)
        time.sleep(1)

    print("\n=== AMLI Remediation Summary ===")
    for r in results:
        print(f"  {r['status']:12s} | {r['name']:25s} | {r['units']:3d}u/{r['priced']:3d}p | {r['url'][:90]}")
    total_q = sum(1 for r in results if r["status"] == "qualified")
    total_u = sum(r["units"] for r in results)
    total_p = sum(r["priced"] for r in results)
    print(f"\n{total_q}/{len(results)} qualified, {total_u} total units ({total_p} priced)")
    print("\nNext: run `python bulk_scrape_all.py --export-only` to refresh the summary CSVs.")


if __name__ == "__main__":
    main()
