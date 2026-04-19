#!/usr/bin/env python3
"""
url_discovery.py - Discover correct URLs for apartment communities with bad URLs.

Reads qualifying_communities_failed.csv, filters rows with bing.com/aclick or
apartmenthomeliving.com URLs, and attempts to discover the real property website
by trying common apartment URL patterns.

Usage:
    python url_discovery.py
"""

import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_CSV = Path(__file__).parent / "qualifying_communities_failed.csv"
OUTPUT_CSV = Path(__file__).parent / "url_discovery_results.csv"

REQUEST_TIMEOUT_MS = 12000  # Playwright navigation timeout
# Delay between requests to be polite
REQUEST_DELAY = 0.5  # seconds

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# URL suffixes to try for each base domain
URL_SUFFIXES = [
    "/floorplans",
    "/floor-plans",
    "/",
    "",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_bad_url(url: str) -> bool:
    """Return True if the URL is a Bing ad redirect or apartmenthomeliving link."""
    if not url:
        return False
    url_lower = url.lower()
    return "bing.com/aclick" in url_lower or "apartmenthomeliving.com" in url_lower


def clean_name(name: str) -> str:
    """
    Normalize a community name for slug generation.

    Handles:
      - "Carter @ 4250, The" -> "The Carter 4250"
      - Strips special chars like @, commas, parentheses
      - Collapses whitespace
    """
    # Handle ", The" suffix -> move "The" to front
    if name.endswith(', The"') or name.endswith(", The"):
        name = re.sub(r',\s*The"?$', "", name)
        name = "The " + name
    # Strip surrounding quotes
    name = name.strip('"').strip("'")
    # Remove special characters (keep alphanumeric, spaces, hyphens)
    name = re.sub(r"[^a-zA-Z0-9\s-]", " ", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def generate_slugs(name: str) -> list[str]:
    """
    Generate multiple slug variations from a community name.

    For "1105 Town Brookhaven" produces:
      - 1105townbrookhaven
      - 1105-town-brookhaven
      - 1105townbrookhavenapts
      - etc.

    For "The Carter 4250" produces both with and without "the".
    """
    cleaned = clean_name(name)
    words = cleaned.lower().split()

    # Base slug variations: joined and hyphenated
    joined = "".join(words)
    hyphenated = "-".join(words)

    slugs = [joined, hyphenated]

    # Without "the" prefix if present
    if words and words[0] == "the":
        words_no_the = words[1:]
        if words_no_the:
            slugs.append("".join(words_no_the))
            slugs.append("-".join(words_no_the))

    # Without common filler words like "at", "on", "in"
    filler = {"at", "on", "in", "of"}
    words_no_filler = [w for w in words if w not in filler and w != "the"]
    if words_no_filler and words_no_filler != words:
        slugs.append("".join(words_no_filler))
        slugs.append("-".join(words_no_filler))

    # With "the" prefix but without fillers
    if words and words[0] == "the" and words_no_filler:
        slugs.append("the" + "".join(words_no_filler))
        slugs.append("the-" + "-".join(words_no_filler))

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique


def _camel_split(word: str) -> list[str]:
    """Split a CamelCase/PascalCase word into parts (e.g. 'TownPark' -> ['Town','Park'])."""
    parts = re.findall(r"[A-Z][a-z0-9]*|[a-z0-9]+", word)
    return parts if parts else [word]


def generate_greystar_avana_urls(name: str, city: str) -> list[str]:
    """
    Greystar-managed Avana properties follow the pattern:
        https://www.avana<slug>.com/<city>/avana-<slug>/conventional/

    The domain drops spaces; the path hyphenates the property slug. Example:
        'Avana City North' + 'Atlanta' ->
        https://www.avanacitynorth.com/atlanta/avana-city-north/conventional/

    Also generates fallbacks for CamelCase names (e.g. 'TownPark' as 'town-park').
    """
    if not name.lower().strip().startswith("avana"):
        return []
    # Strip leading "Avana"
    rest = re.sub(r"^avana\b", "", name.strip(), flags=re.IGNORECASE).strip()
    if not rest:
        return []

    # Generate word-list variants: plain lowercase words, plus CamelCase-split words.
    words_lists: list[list[str]] = []
    base_words = [w for w in re.split(r"\s+", rest) if w]
    words_lists.append([w.lower() for w in base_words])
    camel_expanded = []
    for w in base_words:
        camel_expanded.extend(p.lower() for p in _camel_split(w))
    if camel_expanded != words_lists[0]:
        words_lists.append(camel_expanded)

    city_slug = (city or "").strip().lower().replace(" ", "-")
    urls: list[str] = []
    for words in words_lists:
        joined = "".join(words)
        hyph = "-".join(words)
        if city_slug:
            urls.append(
                f"https://www.avana{joined}.com/{city_slug}/avana-{hyph}/conventional/"
            )
        # Also try a simple floorplans suffix as a fallback
        urls.append(f"https://www.avana{joined}.com/floorplans")
        urls.append(f"https://www.avana{joined}.com/")

    # Deduplicate, preserve order
    seen, unique = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def generate_candidate_urls(name: str, city: str = "") -> list[str]:
    """
    Generate all candidate URLs to try for a given community name.

    Brand-specific patterns (e.g. Greystar Avana) are tried first, followed by
    generic slug x suffix combinations.
    """
    candidates: list[str] = []

    # Brand-specific patterns take priority
    for url in generate_greystar_avana_urls(name, city):
        if url not in candidates:
            candidates.append(url)

    slugs = generate_slugs(name)

    # Domain patterns for each slug
    domain_templates = [
        "https://www.{slug}.com",
        "https://www.{slug}apartments.com",
        "https://www.{slug}-apartments.com",
        "https://www.{slug}apts.com",
        "https://www.{slug}-apts.com",
        "https://www.liveat{slug}.com",
        "https://www.live{slug}.com",
        "https://{slug}.com",
    ]

    for slug in slugs:
        for tmpl in domain_templates:
            base = tmpl.format(slug=slug)
            for suffix in URL_SUFFIXES:
                url = base + suffix
                if url not in candidates:
                    candidates.append(url)

    return candidates


PARKING_SIGNALS = [
    "domain is for sale",
    "buy this domain",
    "domain parking",
    "sedoparking",
    "hugedomains",
    "this domain may be for sale",
    "register this domain",
]


def check_url(page, url: str) -> bool:
    """
    Check if a URL loads to a real apartment-like page using a playwright-stealth
    browser. This bypasses Cloudflare challenges that block plain `requests` calls.
    """
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
    except Exception:
        return False
    if resp is None or resp.status >= 400:
        return False

    # Allow Cloudflare challenge to resolve if present
    for _ in range(4):
        try:
            title = (page.title() or "").lower()
        except Exception:
            title = ""
        if "just a moment" in title or "attention required" in title:
            page.wait_for_timeout(2500)
        else:
            break

    try:
        content = page.content()[:4000].lower()
    except Exception:
        return False

    for signal in PARKING_SIGNALS:
        if signal in content:
            return False

    # Prefer apartment-like content as positive signal
    apt_signals = [
        "floor plan", "floorplan", "apartment", "bedroom",
        "availability", "leasing", "rent",
    ]
    return any(s in content for s in apt_signals)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not INPUT_CSV.exists():
        print(f"ERROR: Input file not found: {INPUT_CSV}")
        sys.exit(1)

    # Read CSV
    with open(INPUT_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Filter to bad URLs
    bad_rows = [r for r in rows if is_bad_url(r.get("website_url", ""))]
    total = len(bad_rows)
    print(f"Found {total} communities with bad URLs out of {len(rows)} total rows.")
    print(f"Output will be written to: {OUTPUT_CSV}\n")

    results = []
    found_count = 0

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
            print("WARN: playwright-stealth not installed — Cloudflare-protected "
                  "sites may be reported as not found.")

        for i, row in enumerate(bad_rows, 1):
            name = row["name"]
            city = row.get("city", "")
            state = row.get("state", "")
            old_url = row.get("website_url", "")

            print(f"[{i}/{total}] {name} ({city}, {state}) ... ", end="", flush=True)

            candidates = generate_candidate_urls(name, city)
            discovered_url = None
            method = None

            for url in candidates:
                if check_url(page, url):
                    discovered_url = url
                    parsed = urlparse(url)
                    method = f"pattern: {parsed.netloc}{parsed.path}"
                    break
                time.sleep(REQUEST_DELAY)

            if discovered_url:
                found_count += 1
                print(f"FOUND -> {discovered_url}")
            else:
                print("not found")

            results.append({
                "name": name,
                "city": city,
                "state": state,
                "old_url": old_url,
                "discovered_url": discovered_url or "",
                "method": method or "",
            })

        browser.close()

    # Write output CSV
    fieldnames = ["name", "city", "state", "old_url", "discovered_url", "method"]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone! Discovered URLs for {found_count}/{total} communities.")
    print(f"Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
