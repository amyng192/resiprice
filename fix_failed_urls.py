#!/usr/bin/env python3
"""
fix_failed_urls.py — Remediation for the 10 Avana communities (Greystar) and
1105 Town Brookhaven, whose original URL discovery picked up Bing ad redirects,
Instagram, Airbnb, or aggregator pages.

For each target property, this script:
  1. Replaces the website_url with the known-good URL (Avana's Greystar path
     pattern, or the canonical 1105 site).
  2. Verifies the URL loads with playwright-stealth (bypasses Cloudflare).
  3. Runs the PlaywrightScraper to extract units and detect the platform.
  4. Appends an updated record to pipeline_progress.jsonl. Because
     load_progress keyed by 'name|address' keeps the last record wins, this
     override cleanly replaces the stale entry.
  5. Re-exports qualifying_communities.csv and qualifying_communities_failed.csv.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))
from apartment_scraper import PlaywrightScraper
from audit_properties import check_pricing_on_page
from pipeline_atlanta import load_progress, save_progress, export_csv, print_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fix_failed_urls")

PROGRESS_PATH = Path(__file__).parent / "pipeline_progress.jsonl"
OUTPUT_CSV = Path(__file__).parent / "qualifying_communities.csv"


# (property_name, address) -> corrected website_url
REMEDIATIONS: dict[tuple[str, str], str] = {
    ("1105 Town Brookhaven", "1105 Town Blvd"):
        "https://www.1105townbrookhaven-apts.com/floorplans",
    # --- Avana (Greystar) properties in the failed list ---
    ("Avana Cheshire Bridge", "2124 Cheshire Bridge Road NE"):
        "https://www.avanacheshirebridge.com/atlanta/avana-cheshire-bridge/conventional/",
    ("Avana City North", "3421 Northlake Pkwy NE"):
        "https://www.avanacitynorth.com/atlanta/avana-city-north/conventional/",
    ("Avana Cityview", "1650 Barnes Mill Road"):
        "https://www.avanacityview.com/marietta/avana-cityview/conventional/",
    ("Avana on Main", "508 Main Street NE"):
        "https://www.avanaonmain.com/atlanta/avana-on-main/conventional/",
    ("Avana Portico", "2110 Preston Park Drive"):
        "https://www.avanaportico.com/duluth/avana-portico/conventional/",
    ("Avana Twenty9", "2334 Fuller Way"):
        "https://www.avanatwenty9.com/tucker/avana-twenty9/conventional/",
    # --- Avana properties already marked qualified but with junk URLs ---
    ("Avana Acworth", "4710 Baker Grove Road NW"):
        "https://www.avanaacworth.com/acworth/avana-acworth/conventional/",
    ("Avana Dunwoody", "10 Gentrys Walk"):
        "https://www.avanadunwoody.com/chamblee/avana-dunwoody/conventional/",
    ("Avana Kennesaw", "3840 Jiles Road"):
        "https://www.avanakennesaw.com/kennesaw/avana-kennesaw/conventional/",
    ("Avana TownPark", "3725 George Busbee Pkwy NW"):
        "https://www.avanatownpark.com/kennesaw/avana-townpark/conventional/",
}


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def qualifies(pricing_info: dict, priced_units: int) -> bool:
    if priced_units >= 1:
        return True
    if pricing_info.get("platforms"):
        return True
    if pricing_info.get("has_iframe"):
        return True
    if pricing_info.get("has_unit_pricing"):
        return True
    if pricing_info.get("price_count", 0) >= 5:
        return True
    return False


def run_pricing_check(url: str) -> dict:
    """Open a fresh stealth playwright context, run check_pricing_on_page, close."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
        )
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


def remediate_one(scraper: PlaywrightScraper, record: dict, new_url: str) -> dict:
    name = record["property_name"]
    log.info(f"[{name}] new URL: {new_url}")

    # Step 1: pricing signal check (its own playwright lifetime)
    pricing_info = run_pricing_check(new_url)
    platforms = pricing_info.get("platforms", [])
    log.info(
        f"[{name}] platforms={platforms} prices={pricing_info.get('price_count', 0)} "
        f"iframe={pricing_info.get('has_iframe', False)}"
    )

    # Step 2: run the scraper (has its own playwright lifetime, must NOT be
    # nested inside another sync_playwright block).
    units = 0
    priced = 0
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
        scraper_note = f"scraper: {units} units ({priced} priced)"
        log.info(f"[{name}] {scraper_note}")
    except Exception as e:
        scraper_note = f"scraper error: {str(e)[:200]}"
        log.warning(f"[{name}] {scraper_note}")

    qualified = qualifies(pricing_info, priced)
    platform = (platforms[0].lower() if platforms else None) or scraper_platform

    updated = dict(record)
    updated["website_url"] = new_url
    updated["availability_url"] = new_url
    updated["platform"] = platform
    updated["status"] = "qualified" if qualified else "no_pricing"
    base_note = pricing_info.get("notes", "")
    updated["notes"] = f"Remediated. {base_note}; {scraper_note}".strip("; ")
    updated["processed_at"] = datetime.now().isoformat()
    return updated


def main():
    progress = load_progress(str(PROGRESS_PATH))
    log.info(f"Loaded {len(progress)} progress records")

    scraper = PlaywrightScraper(headless=True, timeout_ms=30000)
    updates: list[dict] = []

    for (name, addr), new_url in REMEDIATIONS.items():
        key = f"{name}|{addr}"
        record = progress.get(key)
        if not record:
            log.warning(f"[{name}] not in progress — skipping")
            continue
        try:
            updated = remediate_one(scraper, record, new_url)
            updates.append(updated)
        except Exception as e:
            log.error(f"[{name}] remediation failed: {e}")
        time.sleep(2)

    for u in updates:
        save_progress(str(PROGRESS_PATH), u)
    log.info(f"Appended {len(updates)} updated records to {PROGRESS_PATH.name}")

    # NOTE: We intentionally do NOT call pipeline_atlanta.export_csv here.
    # The qualifying_communities.csv has been manually curated beyond what the
    # JSONL reflects, and a full re-export would clobber that curation. Run
    # apply_remediations_to_csv.py to surgically patch the CSVs in place.
    print("\n=== Remediation Summary ===")
    for u in updates:
        print(f"  {u['status']:12s} | {u['property_name']:30s} -> {u['website_url']}")
        print(f"  {'':12s}   {u.get('notes', '')}")
    print("\nNext step: run `python apply_remediations_to_csv.py` to update "
          "the CSVs without clobbering manual curation.")


if __name__ == "__main__":
    main()
