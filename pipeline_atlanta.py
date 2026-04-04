"""
Atlanta community discovery pipeline.

Reads apartment communities from an Excel file, finds their websites via
DuckDuckGo search, checks if they have individual unit-level pricing, and
exports qualifying communities as a CSV ready for import into ResiPrice.

Usage:
    python pipeline_atlanta.py                        # Process all, resume from progress
    python pipeline_atlanta.py --start 0 --limit 200  # Process rows 0-199 only
    python pipeline_atlanta.py --export-only           # Generate CSV from progress
    python pipeline_atlanta.py --stats                 # Print summary stats
    python pipeline_atlanta.py --reset                 # Clear progress, start fresh
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from ddgs import DDGS
from playwright.sync_api import sync_playwright

# Add project root so we can import from audit_properties
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audit_properties import (
    SKIP_DOMAINS,
    is_property_website,
    find_availability_page,
    check_pricing_on_page,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pipeline")

DEFAULT_XLSX = str(Path(__file__).parent.parent / "ResiPrice Atlanta.xlsx")
DEFAULT_PROGRESS = str(Path(__file__).parent / "pipeline_progress.jsonl")
DEFAULT_OUTPUT = str(Path(__file__).parent / "qualifying_communities.csv")


# ---------------------------------------------------------------------------
# Progress file (JSONL) — one JSON object per line, append-only
# ---------------------------------------------------------------------------

def load_progress(path: str) -> dict[str, dict]:
    """Load progress from JSONL file. Returns dict keyed by 'name|address'."""
    progress = {}
    if not os.path.exists(path):
        return progress
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                key = f"{record['property_name']}|{record['address']}"
                progress[key] = record
            except (json.JSONDecodeError, KeyError):
                continue
    return progress


def save_progress(path: str, record: dict) -> None:
    """Append one record to the progress file."""
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Qualification logic
# ---------------------------------------------------------------------------

def qualifies(pricing_info: dict) -> bool:
    """Return True if the community has unit-level pricing."""
    if pricing_info.get("platforms"):
        return True
    if pricing_info.get("has_iframe"):
        return True
    if pricing_info.get("has_unit_pricing"):
        return True
    if pricing_info.get("price_count", 0) >= 5:
        return True
    return False


# ---------------------------------------------------------------------------
# DuckDuckGo search via API package
# ---------------------------------------------------------------------------

def ddg_search(ddgs: DDGS, query: str, max_results: int = 8) -> list[dict]:
    """Search DuckDuckGo using the ddgs package. Returns [{title, href}, ...]."""
    try:
        return list(ddgs.text(query, max_results=max_results))
    except Exception as e:
        log.warning(f"DDG search failed: {e}")
        return []


def find_website_from_results(
    results: list[dict], property_name: str
) -> str | None:
    """Pick the most likely official property website from DDG results."""
    name_words = set(property_name.lower().split())

    # First pass: prefer URLs where title/URL contains property name words
    for r in results:
        url = r.get("href", "")
        if not is_property_website(url):
            continue
        url_lower = url.lower()
        title_lower = r.get("title", "").lower()
        matching = sum(
            1 for w in name_words
            if len(w) > 3 and (w in url_lower or w in title_lower)
        )
        if matching >= 1:
            return url

    # Second pass: first non-aggregator result
    for r in results:
        url = r.get("href", "")
        if is_property_website(url):
            return url

    return None


# ---------------------------------------------------------------------------
# Per-community processing
# ---------------------------------------------------------------------------

def process_community(
    ddgs: DDGS,
    page,
    name: str,
    address: str,
    city: str,
    state: str,
    zip_code: str,
) -> dict:
    """Search for a community's website and check for unit-level pricing."""
    record = {
        "property_name": name,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "website_url": "",
        "availability_url": "",
        "platform": None,
        "status": "no_website",
        "notes": "",
        "processed_at": datetime.now().isoformat(),
    }

    # Clean up name for better search results
    # Handle trailing ", The" or "The " prefix patterns
    clean_name = re.sub(r',\s*The$', '', name).strip()
    clean_name = re.sub(r'^The\s+', '', clean_name).strip() if clean_name == name else clean_name

    # Step 1: Search DuckDuckGo
    query = f'{clean_name} {city} {state} apartments'
    search_results = ddg_search(ddgs, query)

    website = find_website_from_results(search_results, name)

    # Fallback search if first attempt failed
    if not website:
        time.sleep(2)
        fallback_query = f'{clean_name} {city} {state} official website'
        search_results = ddg_search(ddgs, fallback_query)
        website = find_website_from_results(search_results, name)

    if not website:
        record["notes"] = "No property website found"
        log.warning(f"  No website: {name}")
        return record

    record["website_url"] = website
    log.info(f"  Website: {website}")

    # Step 2: Find availability / floor plans page
    avail_page = find_availability_page(page, website)
    check_url = website
    if avail_page:
        record["availability_url"] = avail_page
        check_url = avail_page
        log.info(f"  Availability page: {avail_page}")

    # Step 3: Check for pricing
    pricing_info = check_pricing_on_page(page, check_url)

    # Determine platform (first detected)
    platforms = pricing_info.get("platforms", [])
    if platforms:
        record["platform"] = platforms[0].lower()

    record["notes"] = pricing_info.get("notes", "")

    if qualifies(pricing_info):
        record["status"] = "qualified"
        log.info(f"  QUALIFIED — {record['notes']}")
    else:
        record["status"] = "no_pricing"
        log.info(f"  No pricing — {record['notes']}")

    return record


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_csv(progress: dict, output_path: str) -> int:
    """Write qualifying communities to CSV. Returns count."""
    qualified = [
        r for r in progress.values()
        if r.get("status") == "qualified"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "url", "platform"])
        for r in sorted(qualified, key=lambda x: x["property_name"]):
            url = r.get("availability_url") or r.get("website_url", "")
            writer.writerow([
                r["property_name"],
                url,
                r.get("platform") or "",
            ])

    log.info(f"Exported {len(qualified)} qualifying communities to {output_path}")

    # Also export the failed/unresolved list for manual review
    failed_path = output_path.replace(".csv", "_failed.csv")
    failed = [
        r for r in progress.values()
        if r.get("status") != "qualified"
    ]

    with open(failed_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "name", "address", "city", "state", "zip",
            "status", "website_url", "availability_url", "notes",
        ])
        for r in sorted(failed, key=lambda x: x["property_name"]):
            writer.writerow([
                r["property_name"],
                r.get("address", ""),
                r.get("city", ""),
                r.get("state", ""),
                r.get("zip", ""),
                r.get("status", ""),
                r.get("website_url", ""),
                r.get("availability_url", ""),
                r.get("notes", ""),
            ])

    log.info(f"Exported {len(failed)} failed/unresolved communities to {failed_path}")
    return len(qualified)


def print_stats(progress: dict) -> None:
    """Print summary statistics from progress data."""
    total = len(progress)
    if total == 0:
        print("No progress data found.")
        return

    by_status = {}
    by_platform = {}
    for r in progress.values():
        status = r.get("status", "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        if status == "qualified":
            plat = r.get("platform") or "unknown"
            by_platform[plat] = by_platform.get(plat, 0) + 1

    print(f"\n{'='*60}")
    print(f"PIPELINE STATS — {total} communities processed")
    print(f"{'='*60}")
    for status, count in sorted(by_status.items()):
        pct = count / total * 100
        print(f"  {status:15s}  {count:5d}  ({pct:.1f}%)")

    if by_platform:
        print(f"\n  Qualified by platform:")
        for plat, count in sorted(by_platform.items(), key=lambda x: -x[1]):
            print(f"    {plat:15s}  {count:5d}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Atlanta community discovery pipeline")
    parser.add_argument("--xlsx", default=DEFAULT_XLSX, help="Path to Excel file")
    parser.add_argument("--sheet", default="Results", help="Sheet name")
    parser.add_argument("--start", type=int, default=0, help="Starting row index")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0=all)")
    parser.add_argument("--progress", default=DEFAULT_PROGRESS, help="Progress file path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path")
    parser.add_argument("--reset", action="store_true", help="Clear progress and start fresh")
    parser.add_argument("--export-only", action="store_true", help="Generate CSV from progress")
    parser.add_argument("--stats", action="store_true", help="Print stats and exit")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    args = parser.parse_args()

    # Stats only
    if args.stats:
        progress = load_progress(args.progress)
        print_stats(progress)
        return

    # Export only
    if args.export_only:
        progress = load_progress(args.progress)
        export_csv(progress, args.output)
        print_stats(progress)
        return

    # Reset
    if args.reset and os.path.exists(args.progress):
        os.remove(args.progress)
        log.info("Progress file cleared")

    # Load Excel
    log.info(f"Reading {args.xlsx} ...")
    df = pd.read_excel(args.xlsx, sheet_name=args.sheet)

    # Apply start/limit
    if args.limit > 0:
        df = df.iloc[args.start : args.start + args.limit]
    elif args.start > 0:
        df = df.iloc[args.start:]

    # Load existing progress
    progress = load_progress(args.progress)

    # Determine which communities still need processing
    rows_to_process = []
    for idx, row in df.iterrows():
        name = str(row["Property Name"]).strip()
        address = str(row["Address"]).strip()
        key = f"{name}|{address}"
        if key not in progress:
            rows_to_process.append(row)

    total = len(df)
    already_done = total - len(rows_to_process)
    log.info(f"Total: {total} | Already processed: {already_done} | Remaining: {len(rows_to_process)}")

    if not rows_to_process:
        log.info("Nothing to process — all communities already in progress file")
        export_csv(load_progress(args.progress), args.output)
        print_stats(load_progress(args.progress))
        return

    headless = not args.no_headless
    consecutive_no_results = 0

    ddgs = DDGS()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        try:
            for idx, row in enumerate(rows_to_process):
                name = str(row["Property Name"]).strip()
                address = str(row["Address"]).strip()
                city = str(row["City"]).strip()
                state = str(row["State"]).strip()
                zip_code = str(row["ZIP"]).strip()

                log.info(f"[{idx + 1}/{len(rows_to_process)}] {name} — {city}, {state}")

                try:
                    record = process_community(ddgs, page, name, address, city, state, zip_code)
                except Exception as e:
                    log.error(f"  Error: {e}")
                    record = {
                        "property_name": name,
                        "address": address,
                        "city": city,
                        "state": state,
                        "zip": zip_code,
                        "website_url": "",
                        "availability_url": "",
                        "platform": None,
                        "status": "error",
                        "notes": str(e),
                        "processed_at": datetime.now().isoformat(),
                    }

                save_progress(args.progress, record)

                # Rate limiting — pause longer after consecutive failures
                if record["status"] == "no_website":
                    consecutive_no_results += 1
                    if consecutive_no_results >= 3:
                        log.warning("Multiple consecutive no-results — pausing 30s")
                        time.sleep(30)
                        consecutive_no_results = 0
                else:
                    consecutive_no_results = 0

                # Base delay between communities
                time.sleep(3)

                # Progress log every 25 communities
                if (idx + 1) % 25 == 0:
                    done = already_done + idx + 1
                    log.info(f"--- Progress: {done}/{total} total ({done/total*100:.1f}%) ---")

        except KeyboardInterrupt:
            log.info(f"\nInterrupted! Processed {idx} communities this session. Progress saved.")

        browser.close()

    # Final export
    final_progress = load_progress(args.progress)
    export_csv(final_progress, args.output)
    print_stats(final_progress)


if __name__ == "__main__":
    main()
