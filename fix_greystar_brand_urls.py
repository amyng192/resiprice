#!/usr/bin/env python3
"""
fix_greystar_brand_urls.py - Remediate 6 properties whose DB URL pointed at
greystar.com (the Greystar brand portal), not the community's own leasing
site. Each property has an individual Greystar-managed domain found via
search — mostly SightMap-based.
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
from pipeline_atlanta import save_progress

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fix_greystar_brand_urls")

ROOT = Path(__file__).parent
PROGRESS_PATH = ROOT / "pipeline_progress.jsonl"
BULK_JSONL = ROOT / "bulk_scrape_results.jsonl"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REMEDIATIONS: dict[str, str] = {
    "400 Belmont":                "https://400belmont.com/floorplans/",
    "Trellis":                    "https://www.livetrellis.com/floorplans/",
    "Phipps Place":               "https://www.phipps-place.com/floorplans",
    "Avana Uptown":               "https://avanauptown.com/floorplans/",
    "Overlook at Kennerly Lake":  "https://www.overlookatkennerlylake.com/floorplans",
    "Weldon by Broadstone, The":  "https://theweldonbybroadstone.com/floorplans/",
}


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
    scraper = PlaywrightScraper(headless=True, timeout_ms=45000)

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
        platform = (platforms[0].lower() if platforms else None) or scraper_platform

        update_db(name, new_url, platform if qualified else None)

        progress_record = {
            "property_name": name,
            "address": "",
            "city": "",
            "state": "GA",
            "zip": "",
            "website_url": new_url,
            "availability_url": new_url,
            "platform": platform if qualified else None,
            "status": "qualified" if qualified else "no_pricing",
            "notes": f"Greystar brand remediation: {pricing_info.get('notes', '')}; {scraper_note}",
            "processed_at": datetime.now().isoformat(),
        }
        save_progress(str(PROGRESS_PATH), progress_record)

        bulk_record = {
            "id": None,
            "name": name,
            "url": new_url,
            "platform_hint": platform or "",
            "status": "qualified" if priced >= 1 else ("no_pricing" if units >= 1 else "no_units"),
            "units": units,
            "priced": priced,
            "platform_detected": scraper_platform,
            "error": "" if (units or priced) else scraper_note,
            "elapsed_s": 0.0,
            "scraped_at": datetime.utcnow().isoformat(),
        }
        with open(BULK_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(bulk_record, ensure_ascii=False) + "\n")
        results.append(bulk_record)
        time.sleep(1)

    print("\n=== Greystar Brand Remediation Summary ===")
    for r in results:
        print(f"  {r['status']:12s} | {r['name']:32s} | {r['units']:3d}u/{r['priced']:3d}p | {r['url']}")
    total_q = sum(1 for r in results if r["status"] == "qualified")
    total_p = sum(r["priced"] for r in results)
    print(f"\n{total_q}/{len(results)} qualified, {total_p} priced units")


if __name__ == "__main__":
    main()
