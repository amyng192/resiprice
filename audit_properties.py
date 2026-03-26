"""
Audit apartment properties for scrapability.

For each property: search Google for their website, find the availability page,
test scraping, and score the result.
"""

import csv
import json
import re
import time
import logging
import sys
from dataclasses import dataclass, field
from urllib.parse import urlparse

import pandas as pd
from playwright.sync_api import sync_playwright

from apartment_scraper import PlaywrightScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PropertyAudit")


@dataclass
class AuditResult:
    property_name: str
    address: str
    website_url: str = ""
    availability_page_url: str = ""
    score: str = "no data"
    units_found: int = 0
    notes: str = ""


def web_search(page, query: str) -> list[dict]:
    """Search DuckDuckGo and return top organic results as {title, url, snippet}."""
    from urllib.parse import quote_plus
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2000)
    except Exception as e:
        log.warning(f"Search failed: {e}")
        return []

    results = []
    items = page.locator(".result").all()
    for item in items[:10]:
        try:
            link_el = item.locator(".result__a").first
            href = link_el.get_attribute("href") or ""
            title = link_el.inner_text(timeout=2000)
            snippet = ""
            try:
                snippet = item.locator(".result__snippet").first.inner_text(timeout=1000)
            except Exception:
                pass
            if href.startswith("http"):
                results.append({"title": title, "url": href, "snippet": snippet})
            elif href.startswith("//"):
                # DuckDuckGo sometimes uses protocol-relative redirect URLs
                # Extract the actual URL from the redirect
                actual = re.search(r'uddg=(https?[^&]+)', href)
                if actual:
                    from urllib.parse import unquote
                    results.append({"title": title, "url": unquote(actual.group(1)), "snippet": snippet})
        except Exception:
            continue

    return results


# Domains to skip — these are listing aggregators, not property websites
SKIP_DOMAINS = {
    "apartments.com", "zillow.com", "trulia.com", "rent.com",
    "apartmentfinder.com", "forrent.com", "apartmentguide.com",
    "realtor.com", "zumper.com", "hotpads.com", "rentcafe.com",
    "padmapper.com", "apartmentlist.com", "google.com", "yelp.com",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "bbb.org", "yellowpages.com", "mapquest.com",
}


def is_property_website(url: str) -> bool:
    """Check if a URL is likely a direct property website (not an aggregator)."""
    domain = urlparse(url).netloc.lower().replace("www.", "")
    return not any(skip in domain for skip in SKIP_DOMAINS)


def find_property_website(search_results: list[dict], property_name: str) -> str | None:
    """Pick the most likely official property website from search results."""
    name_words = set(property_name.lower().split())

    # First pass: prefer URLs where the domain or path contains property name words
    for r in search_results:
        if not is_property_website(r["url"]):
            continue
        url_lower = r["url"].lower()
        title_lower = r["title"].lower()
        # Check if the property name appears in the title or URL
        matching_words = sum(1 for w in name_words if len(w) > 3 and (w in url_lower or w in title_lower))
        if matching_words >= 1:
            return r["url"]

    # Second pass: just take the first non-aggregator result
    for r in search_results:
        if is_property_website(r["url"]):
            return r["url"]

    return None


def find_availability_page(page, base_url: str) -> str | None:
    """Look for a floor plans or available apartments link on the page."""
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(3000)
    except Exception:
        return None

    # Dismiss common overlays
    for sel in ["#onetrust-accept-btn-handler", "[class*='cookie'] button",
                "button[class*='accept']", "[class*='modal'] [class*='close']"]:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=500):
                el.click(timeout=2000)
                page.wait_for_timeout(500)
        except Exception:
            pass

    # Look for links to floor plans / availability pages
    keywords = [
        "available apartments", "availability", "available units",
        "floor plans", "floorplans", "floor plan", "pricing",
        "apartments", "find your home", "view all",
    ]

    links = page.locator("a").all()
    for link in links:
        try:
            text = link.inner_text(timeout=500).strip().lower()
            href = link.get_attribute("href") or ""
            # Check link text
            for kw in keywords:
                if kw in text:
                    if href.startswith("http"):
                        return href
                    elif href.startswith("/"):
                        parsed = urlparse(base_url)
                        return f"{parsed.scheme}://{parsed.netloc}{href}"
            # Check href path
            href_lower = href.lower()
            for kw in ["floor-plan", "floorplan", "available", "pricing", "apartments"]:
                if kw in href_lower and href != base_url:
                    if href.startswith("http"):
                        return href
                    elif href.startswith("/"):
                        parsed = urlparse(base_url)
                        return f"{parsed.scheme}://{parsed.netloc}{href}"
        except Exception:
            continue

    return None


def check_pricing_on_page(page, url: str) -> dict:
    """Visit a page and check what kind of pricing data is visible."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(4000)
    except Exception as e:
        return {"has_pricing": False, "notes": f"Page load failed: {e}"}

    # Dismiss overlays
    for sel in ["#onetrust-accept-btn-handler", "[class*='cookie'] button",
                "button[class*='accept']", "[class*='modal'] [class*='close']",
                "[class*='popup'] [class*='close']"]:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=500):
                el.click(timeout=2000)
        except Exception:
            pass

    content = page.content()
    text = page.inner_text("body", timeout=5000)

    # Count dollar amounts that look like rent prices ($800-$3500 range)
    price_matches = re.findall(r'\$[\d,]{3,5}', text)
    rent_prices = [p for p in price_matches if 500 <= int(p.replace("$", "").replace(",", "")) <= 5000]

    # Check for "call for pricing" or similar
    call_phrases = re.findall(r'call\s+(?:for|us|to)', text.lower())

    # Check for unit numbers
    unit_matches = re.findall(r'(?:unit|apt|apartment)\s*#?\s*\d+', text.lower())

    # Check for known platforms in page source
    platforms = []
    for platform, marker in [
        ("Entrata", "entrata"),
        ("RENTCafe", "rentcafe"),
        ("RealPage", "realpage"),
        ("Yardi", "yardi"),
        ("SightMap", "sightmap"),
        ("Cortland", "cortland.com"),
        ("MAA", "maac.com"),
        ("AppFolio", "appfolio"),
    ]:
        if marker in content.lower():
            platforms.append(platform)

    # Check for iframes that might embed leasing widgets
    iframes = page.locator("iframe").all()
    iframe_srcs = []
    for iframe in iframes:
        try:
            src = iframe.get_attribute("src") or ""
            if src and any(kw in src.lower() for kw in ["sightmap", "entrata", "rentcafe", "realpage", "yardi", "lease", "apartment"]):
                iframe_srcs.append(src)
        except Exception:
            pass

    notes_parts = []
    if platforms:
        notes_parts.append(f"Platform: {', '.join(platforms)}")
    if iframe_srcs:
        notes_parts.append(f"Leasing iframe detected")
    if call_phrases:
        notes_parts.append(f"'Call for pricing' found")

    return {
        "has_unit_pricing": len(rent_prices) >= 3 and len(unit_matches) >= 2,
        "has_range_pricing": len(rent_prices) >= 1,
        "price_count": len(rent_prices),
        "unit_count": len(unit_matches),
        "platforms": platforms,
        "has_iframe": len(iframe_srcs) > 0,
        "notes": "; ".join(notes_parts) if notes_parts else "Standard HTML",
    }


def audit_property(browser_page, scraper, name: str, address: str, city: str) -> AuditResult:
    """Full audit pipeline for a single property."""
    result = AuditResult(property_name=name, address=f"{address}, {city}, GA")
    log.info(f"{'='*60}")
    log.info(f"Auditing: {name} — {address}, {city}")

    # Step 1: Google search
    query = f"{name} {city} GA apartments official website"
    search_results = web_search(browser_page, query)
    if not search_results:
        result.notes = "No Google results found"
        log.warning(f"  No search results for {name}")
        return result

    # Step 2: Find official website
    website = find_property_website(search_results, name)
    if not website:
        result.notes = "No official website found (only aggregator listings)"
        log.warning(f"  No property website found for {name}")
        return result

    result.website_url = website
    log.info(f"  Website: {website}")

    # Step 3: Find availability / floor plans page
    avail_page = find_availability_page(browser_page, website)
    if avail_page:
        result.availability_page_url = avail_page
        log.info(f"  Availability page: {avail_page}")
        check_url = avail_page
    else:
        log.info(f"  No separate availability page found — checking main site")
        check_url = website

    # Step 4: Check pricing on the page
    pricing_info = check_pricing_on_page(browser_page, check_url)
    notes = pricing_info["notes"]

    # Step 5: Try the scraper
    try:
        log.info(f"  Running scraper on: {check_url}")
        prop = scraper.scrape(check_url, tab_labels=["0", "1", "2", "3", "4", "5"], tab_type="floor")
        result.units_found = len(prop.units)

        if result.units_found >= 5:
            # Check if units have real prices
            priced = sum(1 for u in prop.units if u.rent_min and u.rent_min > 0)
            if priced >= 3:
                result.score = "scrapable"
                notes += f"; Scraper found {result.units_found} units, {priced} with prices"
            else:
                result.score = "limited"
                notes += f"; Scraper found {result.units_found} units but only {priced} have prices"
        elif result.units_found > 0:
            result.score = "limited"
            notes += f"; Scraper found only {result.units_found} units"
        elif pricing_info["has_range_pricing"]:
            result.score = "limited"
            notes += f"; Page has {pricing_info['price_count']} prices visible but scraper couldn't extract units"
        else:
            result.score = "no data"
            notes += "; No unit data found"

    except Exception as e:
        log.error(f"  Scraper error: {e}")
        if pricing_info["has_unit_pricing"]:
            result.score = "limited"
            notes += f"; Scraper failed but page has visible pricing ({e})"
        elif pricing_info["has_range_pricing"]:
            result.score = "limited"
            notes += f"; Scraper failed, page has some prices visible ({e})"
        else:
            result.score = "no data"
            notes += f"; Scraper failed: {e}"

    result.notes = notes
    log.info(f"  Score: {result.score} | Units: {result.units_found} | {notes}")
    return result


def main():
    xlsx_path = r"C:\Users\amyn1\OneDrive\Desktop\AG\ResiPrice\ResiPrice - Gwinnett County.xlsx"
    output_path = r"C:\Users\amyn1\OneDrive\Desktop\AG\ResiPrice\audit_results.csv"

    df = pd.read_excel(xlsx_path)
    # First 10 properties
    batch = df.head(10)

    log.info(f"Auditing {len(batch)} properties from {xlsx_path}")

    scraper = PlaywrightScraper(headless=True, timeout_ms=25000)

    with sync_playwright() as pw:
        # Separate browser for Google searches / page checks
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        results = []
        for _, row in batch.iterrows():
            name = str(row["Property Name"]).strip()
            address = str(row["Property Address"]).strip()
            city = str(row["City"]).strip()

            try:
                result = audit_property(page, scraper, name, address, city)
            except Exception as e:
                log.error(f"Failed to audit {name}: {e}")
                result = AuditResult(
                    property_name=name,
                    address=f"{address}, {city}, GA",
                    notes=f"Audit error: {e}",
                )
            results.append(result)

            # Brief pause between properties
            time.sleep(2)

        browser.close()

    # Write results to CSV
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Property Name", "Address", "Website URL",
            "Availability Page URL", "Score", "Units Found", "Notes",
        ])
        for r in results:
            writer.writerow([
                r.property_name, r.address, r.website_url,
                r.availability_page_url, r.score, r.units_found, r.notes,
            ])

    log.info(f"Results saved to {output_path}")

    # Print summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    for r in results:
        print(f"  {r.score:10s} | {r.units_found:3d} units | {r.property_name}")
    scrapable = sum(1 for r in results if r.score == "scrapable")
    limited = sum(1 for r in results if r.score == "limited")
    no_data = sum(1 for r in results if r.score == "no data")
    print(f"\n  Scrapable: {scrapable}  |  Limited: {limited}  |  No Data: {no_data}")


if __name__ == "__main__":
    main()
