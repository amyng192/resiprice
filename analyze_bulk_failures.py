#!/usr/bin/env python3
"""
analyze_bulk_failures.py - Categorize the 519 rows in bulk_scrape_failed.csv
so we know where to invest the next remediation pass.

Outputs a breakdown of:
  * URL-quality issues (aggregators, login walls, brand pages) — these are
    remediation-with-url_discovery work, not scraper bugs.
  * Scraper bugs (JS errors, click timeouts, intercepted clicks, navigation
    timeouts) — these are scraper-side fixes.
  * no_units with no error — sites that load fine but don't match the
    scraper's tab-based DOM assumptions.
"""

import csv
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent
JSONL = ROOT / "bulk_scrape_results.jsonl"

# --- URL-quality categories --------------------------------------------------
AGGREGATOR_DOMAINS = {
    "apartments.com", "apartmenthomeliving.com", "apartmentfinder.com",
    "apartmentguide.com", "apartmenthomeliving.com", "apartmentlist.com",
    "forrent.com", "rent.com", "trulia.com", "zillow.com", "zumper.com",
    "hotpads.com", "padmapper.com", "aspensquare.com", "rentable.co",
    "rentcafe.com", "corporatehousing.com",
}
AD_DOMAINS = {"bing.com", "google.com", "doubleclick.net"}
SOCIAL_DOMAINS = {"instagram.com", "facebook.com", "airbnb.com", "airbnb.mx"}
BRAND_PAGE_PATTERNS = [
    r"^(?:www\.)?greystar\.com/.*/p_\d+",  # greystar brand calculator/p_ pages
    r"^(?:www\.)?amli\.com/apartments/",    # amli brand site (JS-heavy)
    r"^(?:www\.)?cortland\.com/",           # cortland brand root
]
LOGIN_WALL_PATTERNS = [
    r"/module/application_authentication",
    r"/residentservices",
    r"/userlogin",
    r"/onlineleasing/.*userlogin",
    r"securecafenet\.com",
]

# --- Scraper error signatures -----------------------------------------------
SCRAPER_ERROR_PATTERNS = {
    "js_error": [
        "Cannot read properties", "TypeError:", "ReferenceError:", "SyntaxError:",
    ],
    "click_timeout": [
        "Locator.click: Timeout", "element is not visible",
    ],
    "click_intercepted": [
        "intercepts pointer events", "subtree intercepts",
    ],
    "navigation_timeout": [
        "page.goto: Timeout", "Navigation failed", "net::ERR_",
    ],
    "target_closed": ["Target closed", "Target page, context or browser has been closed"],
    "cloudflare_block": ["Cloudflare", "Just a moment", "cf-chl"],
}


def classify_url(url: str) -> str | None:
    try:
        netloc = urlparse(url).netloc.lower().replace("www.", "")
        path = urlparse(url).path.lower()
    except Exception:
        return None
    if any(netloc == d or netloc.endswith("." + d) for d in AGGREGATOR_DOMAINS):
        return "aggregator"
    if any(netloc == d or netloc.endswith("." + d) for d in AD_DOMAINS):
        return "ad_redirect"
    if any(netloc == d or netloc.endswith("." + d) for d in SOCIAL_DOMAINS):
        return "social"
    full = f"{netloc}{path}"
    for pat in BRAND_PAGE_PATTERNS:
        if re.search(pat, full):
            return "brand_page"
    for pat in LOGIN_WALL_PATTERNS:
        if re.search(pat, full):
            return "login_wall"
    return None


def classify_error(err: str) -> str | None:
    if not err:
        return None
    for category, patterns in SCRAPER_ERROR_PATTERNS.items():
        for p in patterns:
            if p in err:
                return category
    return "other_error"


def main() -> None:
    records: list[dict] = []
    with open(JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    # Keep last record per url
    by_url: dict[str, dict] = {r["url"]: r for r in records}
    failed = [r for r in by_url.values() if r["status"] != "qualified"]

    url_cats = Counter()
    err_cats = Counter()
    bucket: dict[str, list[dict]] = {}

    for r in failed:
        u_cat = classify_url(r["url"])
        e_cat = classify_error(r.get("error", ""))
        if u_cat:
            url_cats[u_cat] += 1
            bucket.setdefault(u_cat, []).append(r)
        elif e_cat:
            err_cats[e_cat] += 1
            bucket.setdefault(e_cat, []).append(r)
        else:
            # e.g. status=no_units with no error text
            bucket.setdefault(r["status"], []).append(r)

    print(f"\n=== {len(failed)} failures ===\n")
    print("URL-quality issues (remediation work, not scraper bugs):")
    for cat, cnt in url_cats.most_common():
        print(f"  {cat:20s} {cnt:4d}")

    print("\nScraper error categories (scraper-side fixes):")
    for cat, cnt in err_cats.most_common():
        print(f"  {cat:20s} {cnt:4d}")

    # no-error buckets (status=no_units/no_pricing with empty error)
    for key in ("no_units", "no_pricing"):
        n = sum(1 for r in failed
                if r["status"] == key and not classify_url(r["url"])
                and not r.get("error"))
        if n:
            print(f"\n  {key} (clean load, no extraction): {n}")

    # Sample from biggest bucket for inspection
    print("\n=== Sample URLs per bucket (max 5) ===")
    for cat, items in sorted(bucket.items(), key=lambda x: -len(x[1])):
        print(f"\n[{cat}] {len(items)} items")
        for r in items[:5]:
            print(f"  {r['name'][:38]:38s}  {r['url'][:90]}")

    # Write a prioritized remediation list
    remediation_path = ROOT / "bulk_failures_by_category.csv"
    with open(remediation_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["category", "name", "url", "status", "units", "priced", "error"])
        for cat, items in sorted(bucket.items(), key=lambda x: -len(x[1])):
            for r in items:
                w.writerow([
                    cat, r["name"], r["url"], r["status"],
                    r.get("units", 0), r.get("priced", 0),
                    (r.get("error") or "")[:200],
                ])
    print(f"\nWrote categorized remediation list to {remediation_path.name}")


if __name__ == "__main__":
    main()
