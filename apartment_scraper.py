"""
Multifamily Apartment Data Extraction Engine — Playwright Edition
==================================================================
A headless-browser scraper that navigates apartment community websites,
clicks through floor tabs, bedroom filters, pagination, and "Load More"
buttons to capture every available unit.

Designed for property-owned websites only (no ILS scraping).

Usage:
    # Single property
    python apartment_scraper.py --url "https://rosemontberkeleylake.com/floor-plans/"

    # With explicit tab strategy
    python apartment_scraper.py --url "https://..." --tabs "0,1,2" --tab-type floor

    # Batch from file
    python apartment_scraper.py --batch urls.csv --output results.json

Requirements:
    pip install playwright beautifulsoup4 lxml pandas
    playwright install chromium
"""

import json
import re
import csv
import time
import logging
import argparse
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("ApartmentScraper")


# ═══════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════

class UnitStatus(str, Enum):
    AVAILABLE = "available"
    WAITLIST = "waitlist"
    NOT_AVAILABLE = "not_available"
    UNKNOWN = "unknown"


@dataclass
class Special:
    description: str
    discount_amount: Optional[float] = None
    discount_percent: Optional[float] = None
    valid_through: Optional[str] = None
    lease_term_months: Optional[int] = None


@dataclass
class Unit:
    unit_number: str
    floor_plan_name: Optional[str] = None
    unit_type: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    sqft: Optional[int] = None
    rent_min: Optional[float] = None
    rent_max: Optional[float] = None
    deposit: Optional[float] = None
    available_date: Optional[str] = None
    status: UnitStatus = UnitStatus.UNKNOWN
    floor: Optional[int] = None
    amenities: list[str] = field(default_factory=list)
    specials: list[Special] = field(default_factory=list)
    lease_terms: list[int] = field(default_factory=list)


@dataclass
class Property:
    name: str
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zipcode: Optional[str] = None
    phone: Optional[str] = None
    website_url: Optional[str] = None
    platform: Optional[str] = None
    total_units: Optional[int] = None
    community_amenities: list[str] = field(default_factory=list)
    units: list[Unit] = field(default_factory=list)
    specials: list[Special] = field(default_factory=list)
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def unit_mix(self) -> dict:
        mix = {}
        for u in self.units:
            key = u.unit_type or "Unknown"
            mix[key] = mix.get(key, 0) + 1
        return mix

    @property
    def available_count(self) -> int:
        return sum(1 for u in self.units if u.status == UnitStatus.AVAILABLE)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["unit_mix"] = self.unit_mix
        d["available_count"] = self.available_count
        return d


# ═══════════════════════════════════════════════════════════════════════════
# PARSING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def parse_rent(text: str) -> tuple[Optional[float], Optional[float]]:
    if not text:
        return (None, None)
    text = text.replace(",", "")
    prices = [float(p) for p in re.findall(r"\$?([\d]+(?:\.\d{2})?)", text) if float(p) > 100]
    if len(prices) >= 2:
        return (min(prices), max(prices))
    if len(prices) == 1:
        return (prices[0], prices[0])
    return (None, None)


def parse_sqft(text: str) -> Optional[int]:
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"(\d+)\s*(?:sq\.?\s*ft|sqft|sf)", text, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        if 200 <= val <= 5000:
            return val
    return None


def parse_beds_baths(text: str) -> tuple[Optional[int], Optional[float]]:
    if not text:
        return (None, None)
    tl = text.lower()
    beds = 0 if "studio" in tl else None
    m = re.search(r"(\d+)\s*(?:bed|br|bedroom)", tl)
    if m:
        beds = int(m.group(1))
    baths = None
    m = re.search(r"(\d+\.?\d*)\s*(?:bath|ba)", tl)
    if m:
        baths = float(m.group(1))
    return (beds, baths)


def parse_availability(text: str) -> tuple[Optional[str], UnitStatus]:
    if not text:
        return (None, UnitStatus.UNKNOWN)
    tl = text.lower().strip()
    if any(w in tl for w in ["now", "immediate", "today", "move in today", "available"]):
        return ("Now", UnitStatus.AVAILABLE)
    if any(w in tl for w in ["waitlist", "wait list"]):
        return (None, UnitStatus.WAITLIST)
    if any(w in tl for w in ["unavailable", "not available", "sold out", "call for"]):
        return (None, UnitStatus.NOT_AVAILABLE)
    dm = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", text)
    if dm:
        mo, dy, yr = dm.groups()
        yr = "20" + yr if len(yr) == 2 else yr
        try:
            return (date(int(yr), int(mo), int(dy)).isoformat(), UnitStatus.AVAILABLE)
        except ValueError:
            pass
    return (text.strip(), UnitStatus.AVAILABLE)


def parse_special(text: str) -> Special:
    """Parse a special/promo description into a structured Special object."""
    text = text.strip()
    sp = Special(description=text)
    tl = text.lower()

    # Months free: "1 month free", "1/2 month free", "½ month free", "2 months free"
    m = re.search(r"(\d+(?:/\d+)?|½)\s*months?\s*free", tl)
    if m:
        raw = m.group(1)
        if raw == "½":
            sp.discount_amount = 0.5
        elif "/" in raw:
            num, den = raw.split("/")
            sp.discount_amount = float(num) / float(den)
        else:
            sp.discount_amount = float(raw)

    # Percentage off: "10% off", "50% off first month"
    m = re.search(r"(\d+)%\s*off", tl)
    if m:
        sp.discount_percent = float(m.group(1))

    # Dollar amount off: "$500 off", "$1,000 off"
    if sp.discount_amount is None:
        m = re.search(r"\$\s*([\d,]+)\s*off", tl)
        if m:
            sp.discount_amount = float(m.group(1).replace(",", ""))

    # Lease term: "12-month lease", "13 month lease"
    m = re.search(r"(\d+)\s*-?\s*months?\s*lease", tl)
    if m:
        sp.lease_term_months = int(m.group(1))

    # Valid through: "through 3/31", "expires 04/15/2026", "valid until 3/31"
    m = re.search(r"(?:through|expires?|until|by)\s+(\d{1,2}/\d{1,2}(?:/\d{2,4})?)", tl)
    if m:
        sp.valid_through = m.group(1)

    return sp


def build_unit_type(beds, baths):
    if beds is None:
        return None
    bed_str = "Studio" if beds == 0 else f"{beds} Bed"
    bath_str = f" / {baths} Bath" if baths else ""
    return f"{bed_str}{bath_str}"


# ═══════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT BROWSER ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class PlaywrightScraper:
    """
    Core headless browser engine that:
      1. Loads the property website
      2. Detects interactive elements (floor tabs, bedroom filters, pagination)
      3. Clicks through each one and captures the rendered HTML at each state
      4. Parses every unit from the accumulated HTML snapshots
    """

    # Common selectors for tabs/filters on apartment sites
    TAB_SELECTORS = [
        # Floor selectors (Rosemont-style numbered buttons)
        "button[data-floor]",
        "[class*='floor'] button",
        "[class*='floor'] a",
        ".floor-selector button",
        ".floor-selector a",
        ".floor-tab",
        # Bedroom filters
        "button[data-beds]",
        "[class*='bedroom'] button",
        ".bed-filter button",
        # Generic tab/filter patterns
        ".tab-button",
        ".filter-btn",
        "[role='tab']",
        ".nav-tabs li a",
        ".nav-pills li a",
        # RentCafe specific
        ".fp-tab",
        ".availabilityTab",
        "#tabFloorplans li a",
        # Entrata specific
        ".entrata-tab",
        ".floor-plan-tab",
    ]

    # Selectors for "Load More" / "View All" buttons
    LOAD_MORE_SELECTORS = [
        "button:has-text('View All')",
        "button:has-text('Show More')",
        "button:has-text('Load More')",
        "button:has-text('See All')",
        "button:has-text('View Available')",
        "a:has-text('View All')",
        "a:has-text('Show More')",
        "a:has-text('Load More')",
        "a:has-text('See All')",
        ".load-more",
        ".view-all",
        ".show-more",
    ]

    # Selectors for unit cards/rows
    UNIT_SELECTORS = [
        ".unit-card",
        ".unit-item",
        ".unit-row",
        ".apartment-card",
        ".apt-card",
        ".fp-row",
        ".floor-plan-card",
        ".floorplan-listing__item",
        ".floorplan-listing",
        ".availableFloorplan",
        "[data-unit]",
        "[data-unit-id]",
        "tr.unit-row",
        ".result-card",
        # RentCafe
        ".fp-unit",
        ".apartment-unit",
        # Entrata
        ".unit-result",
        ".available-unit",
        # MAA
        ".available-apartments__body--apt",
        ".single-apartment",
        ".floorplan-detail",
        ".floor-plan-item",
        "[class*='floorplan-listing'] [class*='item']",
        # Cortland
        ".floorplan-listing__item-details",
        # Generic table rows with unit info
        "table.units tbody tr",
        ".pricing-table tbody tr",
    ]

    def __init__(self, headless: bool = True, timeout_ms: int = 30000):
        self.headless = headless
        self.timeout_ms = timeout_ms

    def scrape(
        self,
        url: str,
        tab_labels: Optional[list[str]] = None,
        tab_type: str = "auto",
    ) -> Property:
        """
        Main entry point.

        Args:
            url: The property website URL (floor plans / availability page).
            tab_labels: Optional explicit list of tab labels to click
                        (e.g., ["0", "1", "2"] for floor tabs).
            tab_type: "floor", "bedroom", "auto". Helps target the right
                      tab selectors if specified.
        """
        from playwright.sync_api import sync_playwright

        log.info(f"Starting scrape: {url}")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            page = context.new_page()

            # ── Capture XHR responses that might contain unit JSON ────────
            api_responses = []

            def capture_response(response):
                ct = response.headers.get("content-type", "")
                if "json" in ct:
                    req_url = response.url.lower()
                    keywords = [
                        "unit", "apartment", "availability", "floorplan",
                        "floor-plan", "floor_plan", "pricing", "inventory",
                        "getapartment", "sightmap",
                    ]
                    if any(kw in req_url for kw in keywords):
                        try:
                            body = response.text()
                            if len(body) > 50:
                                api_responses.append({
                                    "url": response.url,
                                    "body": body,
                                })
                                log.debug(f"Captured API response: {response.url[:100]}")
                        except Exception:
                            pass

            page.on("response", capture_response)

            try:
                # ── Load the page ────────────────────────────────────────
                log.info("Loading page...")
                try:
                    page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
                except Exception:
                    log.warning("networkidle timed out — retrying with domcontentloaded")
                    page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                page.wait_for_timeout(3000)

                # ── Dismiss overlays / cookie banners / popups ─────────
                self._dismiss_overlays(page)

                # ── Extract property-level info ──────────────────────────
                prop = self._extract_property_info(page, url)

                # ── Click "Load More" / "View All" buttons ───────────────
                self._click_load_more(page)

                # ── Discover and click through tabs ──────────────────────
                html_snapshots = []

                if tab_labels:
                    log.info(f"Using explicit tabs: {tab_labels}")
                    html_snapshots = self._click_explicit_tabs(page, tab_labels)

                if not html_snapshots:
                    tabs = self._detect_tabs(page, tab_type)
                    if tabs:
                        log.info(f"Detected {len(tabs)} tabs")
                        html_snapshots = self._click_detected_tabs(page, tabs)
                    else:
                        log.info("No tabs detected — capturing single page state")

                # Always capture current page state too
                html_snapshots.append(page.content())

                # ── Scroll to trigger any lazy-loaded content ────────────
                page.evaluate("""
                    async () => {
                        const delay = ms => new Promise(r => setTimeout(r, ms));
                        for (let i = 0; i < document.body.scrollHeight; i += 500) {
                            window.scrollTo(0, i);
                            await delay(200);
                        }
                    }
                """)
                page.wait_for_timeout(2000)
                html_snapshots.append(page.content())

                # ── Also check for iframe-embedded leasing widgets ───────
                iframe_html = self._extract_iframe_content(page)
                if iframe_html:
                    html_snapshots.append(iframe_html)

            finally:
                browser.close()

        # ── Parse all collected HTML + API responses ─────────────────────
        all_units = self._parse_all_api_responses(api_responses)
        seen_unit_ids = set()
        for u in all_units:
            seen_unit_ids.add(f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}")

        # Parse HTML snapshots
        for snapshot in html_snapshots:
            units = self._parse_html(snapshot)
            for u in units:
                uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                if uid not in seen_unit_ids:
                    seen_unit_ids.add(uid)
                    all_units.append(u)

        # Parse embedded JS data
        for snapshot in html_snapshots:
            units = self._parse_embedded_js(snapshot)
            for u in units:
                uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                if uid not in seen_unit_ids:
                    seen_unit_ids.add(uid)
                    all_units.append(u)

        # Propagate community-level specials to units that have none
        if prop.specials:
            for u in all_units:
                if not u.specials:
                    u.specials = list(prop.specials)

        prop.units = all_units
        log.info(f"RESULT: {len(all_units)} units from \"{prop.name}\"")
        return prop

    # ── Property-level info ──────────────────────────────────────────────

    @staticmethod
    def _best_name_from_title(raw: str) -> str:
        """Pick the most likely community name from a page/og title.

        Titles often look like:
          "Floor Plans | Rosemont Berkeley Lake"
          "Montrose Berkeley Lake - Apartments in ..."
          "Rosemont Grayson"
          "MAA Berkeley Lake luxury apartments bring exquisite amenities..."
        We split on common delimiters and pick the segment that is NOT a
        generic page-section label (Floor Plans, Pricing, etc.).
        """
        generic = {
            "floor plans", "floorplans", "pricing", "availability",
            "apartments", "apartment", "home", "welcome",
            "available apartments",
        }
        # Split on | and – / — / - delimiters
        parts = [s.strip() for s in raw.replace("–", "|").replace("—", "|").replace(" - ", "|").split("|") if s.strip()]
        # Prefer the first non-generic part
        for part in parts:
            if part.lower() not in generic:
                # If the part is very long (likely a description), take first few words
                words = part.split()
                if len(words) > 6:
                    # Try to find a natural break point
                    for i, w in enumerate(words):
                        if w.lower() in ("luxury", "apartments", "apartment", "brings",
                                         "bring", "featuring", "offers", "located", "is"):
                            if i >= 2:
                                return " ".join(words[:i])
                    return " ".join(words[:4])
                return part
        # All parts are generic — return full title cleaned up
        return parts[0] if parts else raw.strip()

    def _extract_property_info(self, page, url) -> Property:
        """Pull property name, address, specials from the page."""
        prop = Property(name="Unknown Property", website_url=url)

        # Try og:site_name first (usually the cleanest community name)
        try:
            og_site = page.locator("meta[property='og:site_name']").get_attribute("content")
            if og_site and len(og_site) < 80:
                prop.name = og_site.strip()
        except Exception:
            pass

        # Try og:title
        if prop.name == "Unknown Property":
            try:
                og_title = page.locator("meta[property='og:title']").get_attribute("content")
                if og_title:
                    prop.name = self._best_name_from_title(og_title)
            except Exception:
                pass

        # Fallback to page title
        if prop.name == "Unknown Property":
            try:
                title = page.title()
                if title:
                    prop.name = self._best_name_from_title(title)
            except Exception:
                pass

        # MAA: data-property-name on the Vue component
        if prop.name == "Unknown Property" or prop.name.lower() in ("available apartments",):
            try:
                maa_el = page.locator("[data-property-name]").first
                maa_name = maa_el.get_attribute("data-property-name", timeout=2000)
                if maa_name and len(maa_name) < 80:
                    prop.name = maa_name.strip()
            except Exception:
                pass

        # Try to find address
        try:
            addr_el = page.locator(
                "[class*='address'], [itemprop='streetAddress'], "
                ".property-address, .community-address"
            ).first
            if addr_el:
                prop.address = addr_el.inner_text().strip()
        except Exception:
            pass

        # Detect platform
        source = page.content()
        url_lower = url.lower()
        for platform, kws in [
            ("cortland", ["cortland.com"]),
            ("maa", ["maac.com"]),
            ("sightmap", ["sightmap"]),
            ("rentcafe", ["rentcafe"]),
            ("entrata", ["entrata"]),
            ("realpage", ["realpage"]),
            ("appfolio", ["appfolio"]),
        ]:
            if any(kw in url_lower or kw in source.lower()[:5000] for kw in kws):
                prop.platform = platform
                break

        # Extract specials from banners, popdowns, alerts, and dedicated sections
        special_keywords = re.compile(
            r"month[s]?\s*free|free\s*rent|%\s*off|\$\d+\s*off|"
            r"free\s*(?:app|admin|application|processing)|"
            r"waive[ds]?\s*(?:fee|admin|application)|"
            r"reduced\s*(?:fee|rent|deposit)|"
            r"look\s*(?:and|&)\s*lease|move.in\s*special|concession",
            re.I,
        )
        seen_specials = set()
        try:
            for sel in [
                # Dedicated specials elements
                ".special", ".promo", ".concession", ".move-in-special",
                "[class*='special']", "[class*='promo']", "[class*='concession']",
                # Banner / popdown / alert patterns
                ".popdown__title-text", ".popdown__content",
                "[class*='popdown'] [class*='title']",
                "[class*='banner'] [class*='text']", "[class*='banner'] [class*='title']",
                ".alert-banner", "[class*='alert-banner']",
                "[class*='hero'] [class*='special']",
                "[class*='ribbon']", "[class*='badge'] [class*='special']",
                # Notification / announcement bars
                "[class*='announcement']", "[class*='notification-bar']",
                "[class*='promo-bar']", "[class*='offer-bar']",
            ]:
                for el in page.locator(sel).all():
                    try:
                        text = el.inner_text(timeout=2000).strip()
                        if len(text) < 5 or len(text) > 500:
                            continue
                        # Normalize whitespace for dedup
                        norm = " ".join(text.split())
                        if not special_keywords.search(norm):
                            continue
                        # Skip if this text is a substring of (or contains)
                        # an already-seen special
                        is_dup = False
                        for existing in list(seen_specials):
                            if norm in existing or existing in norm:
                                is_dup = True
                                # Keep the shorter (more focused) version
                                if len(norm) < len(existing):
                                    seen_specials.discard(existing)
                                    prop.specials[:] = [
                                        s for s in prop.specials
                                        if " ".join(s.description.split()) != existing
                                    ]
                                    break
                                else:
                                    break
                        if not is_dup or len(norm) < min((len(e) for e in seen_specials), default=9999):
                            seen_specials.add(norm)
                            prop.specials.append(parse_special(text))
                    except Exception:
                        continue
        except Exception:
            pass

        # Text-based fallback: scan plain elements for specials keywords
        # (catches Elementor/WordPress sites that don't use special-related CSS classes)
        if not seen_specials:
            try:
                for el in page.locator("p, div, span, li").all()[:300]:
                    try:
                        text = el.inner_text(timeout=1000).strip()
                        if len(text) < 5 or len(text) > 500:
                            continue
                        norm = " ".join(text.split())
                        if not special_keywords.search(norm):
                            continue
                        is_dup = False
                        for existing in list(seen_specials):
                            if norm in existing or existing in norm:
                                is_dup = True
                                if len(norm) < len(existing):
                                    seen_specials.discard(existing)
                                    prop.specials[:] = [
                                        s for s in prop.specials
                                        if " ".join(s.description.split()) != existing
                                    ]
                                    break
                                else:
                                    break
                        if not is_dup or len(norm) < min(
                            (len(e) for e in seen_specials), default=9999
                        ):
                            seen_specials.add(norm)
                            prop.specials.append(parse_special(text))
                    except Exception:
                        continue
            except Exception:
                pass

        return prop

    # ── Tab Detection & Clicking ─────────────────────────────────────────

    # Words that indicate a navigation menu item, not a floor/filter tab
    NAV_KEYWORDS = {
        "view", "details", "apply", "contact", "schedule", "tour",
        "learn more", "see more", "read more",
        "home", "amenities", "amenity", "neighborhood", "gallery",
        "residents", "resident", "about", "photos", "map",
        "virtual tour", "reviews", "faq", "blog", "news",
        "login", "sign in", "portal", "pay rent",
    }

    def _is_nav_text(self, text: str) -> bool:
        """Check if text looks like a navigation menu item rather than a tab."""
        if not text:
            return True
        lower = text.lower().strip()
        return lower in self.NAV_KEYWORDS or any(kw in lower for kw in self.NAV_KEYWORDS)

    def _detect_tabs(self, page, tab_type: str) -> list[dict]:
        """Find clickable tab/filter elements on the page."""
        tabs = []

        for selector in self.TAB_SELECTORS:
            try:
                elements = page.locator(selector).all()
                if len(elements) >= 2:  # Need at least 2 to be a tab set
                    for el in elements:
                        try:
                            if el.is_visible():
                                text = el.inner_text().strip()
                                if self._is_nav_text(text):
                                    continue
                                tabs.append({
                                    "element": el,
                                    "text": text,
                                    "selector": selector,
                                })
                        except Exception:
                            continue

                    if tabs:
                        log.info(f"Found tabs via '{selector}': "
                                 f"{[t['text'] for t in tabs]}")
                        return tabs
            except Exception:
                continue

        # Fallback: look for any set of buttons/links near "floor" or "bedroom"
        try:
            for container_sel in [
                "[class*='floor']", "[class*='Floor']",
                "[class*='filter']", "[class*='tab']",
                "[class*='selector']", "[class*='toggle']",
            ]:
                container = page.locator(container_sel).first
                if container and container.is_visible():
                    buttons = container.locator("button, a, [role='tab']").all()
                    if len(buttons) >= 2:
                        for btn in buttons:
                            try:
                                if btn.is_visible(timeout=500):
                                    text = btn.inner_text(timeout=500).strip()
                                    if self._is_nav_text(text):
                                        continue
                                    tabs.append({
                                        "element": btn,
                                        "text": text,
                                        "selector": container_sel,
                                    })
                            except Exception:
                                continue
                        if tabs:
                            log.info(f"Found tabs via '{container_sel}': "
                                     f"{[t['text'] for t in tabs]}")
                            return tabs
        except Exception:
            pass

        return tabs

    def _click_detected_tabs(self, page, tabs: list[dict]) -> list[str]:
        """Click each detected tab and capture the page HTML after each."""
        snapshots = []
        for i, tab in enumerate(tabs):
            try:
                log.info(f"  Clicking tab {i+1}/{len(tabs)}: \"{tab['text']}\"")
                tab["element"].click(timeout=5000)
                page.wait_for_timeout(2000)
                # Wait for any loading spinners to disappear
                try:
                    page.wait_for_selector(
                        ".loading, .spinner, [class*='loading']",
                        state="hidden", timeout=3000,
                    )
                except Exception:
                    pass
                page.wait_for_timeout(1000)
                snapshots.append(page.content())
            except Exception as e:
                log.warning(f"  Failed to click tab \"{tab['text']}\": {e}")
        return snapshots

    @staticmethod
    def _tab_text_matches(el_text: str, label: str) -> bool:
        """Strict tab text matching — avoid false positives like '1' matching '1344 Sq. Ft.'."""
        if el_text == label:
            return True
        # Allow patterns like "1 (5 units)" or "Floor 3" but NOT "100 Bradford" or "1 Bed from..."
        # The label must appear as a standalone token
        return bool(re.match(
            rf"^{re.escape(label)}(?:\s*\(.*\))?$",
            el_text,
        ))

    def _click_explicit_tabs(self, page, tab_labels: list[str]) -> list[str]:
        """
        Click tabs by matching their visible text to the provided labels.
        Uses targeted selectors to avoid scanning the entire DOM.
        """
        snapshots = []
        # Narrow search to likely tab containers — skip broad fallback
        tab_selectors = [
            "[role='tablist'] button, [role='tablist'] a, [role='tablist'] [role='tab']",
            "[class*='tab'] button, [class*='tab'] a",
            "[class*='floor'] button, [class*='floor'] a",
            "[class*='filter'] button, [class*='filter'] a",
        ]

        for label in tab_labels:
            clicked = False
            label_clean = label.strip()

            for sel in tab_selectors:
                try:
                    candidates = page.locator(sel).all()
                    for el in candidates:
                        try:
                            if not el.is_visible(timeout=500):
                                continue
                            el_text = el.inner_text(timeout=500).strip()
                            if self._tab_text_matches(el_text, label_clean):
                                log.info(f"  Clicking tab: \"{el_text}\"")
                                el.click()
                                page.wait_for_timeout(2000)
                                try:
                                    page.wait_for_selector(
                                        ".loading, .spinner",
                                        state="hidden", timeout=3000,
                                    )
                                except Exception:
                                    pass
                                snapshots.append(page.content())
                                clicked = True
                                break
                        except Exception:
                            continue
                    if clicked:
                        break
                except Exception:
                    continue

            if not clicked:
                log.debug(f"  Could not find tab with label \"{label_clean}\"")

        return snapshots

    def _dismiss_overlays(self, page):
        """Close cookie banners, popups, and overlays that block clicks."""
        dismiss_selectors = [
            # Cookie / consent banners
            "#onetrust-accept-btn-handler",
            "[id*='cookie'] button",
            "[class*='cookie'] button",
            "button[class*='accept']",
            # Generic close / dismiss buttons on overlays
            "[class*='overlay'] [class*='close']",
            "[class*='modal'] [class*='close']",
            "[class*='popup'] [class*='close']",
            "[class*='hours'] [class*='close']",
            # Property hours overlay (Cortland)
            ".property-hours__overlay",
        ]
        for sel in dismiss_selectors:
            try:
                el = page.locator(sel).first
                if el and el.is_visible(timeout=500):
                    el.click(timeout=2000)
                    log.info(f"  Dismissed overlay: {sel}")
                    page.wait_for_timeout(500)
            except Exception:
                continue
        # Also try to remove blocking overlays via JS if they can't be clicked
        page.evaluate("""
            () => {
                for (const sel of ['.property-hours', '#onetrust-consent-sdk', '[class*="overlay"]']) {
                    const el = document.querySelector(sel);
                    if (el) el.style.display = 'none';
                }
            }
        """)

    def _click_load_more(self, page):
        """Repeatedly click Load More / View All until no more appear."""
        max_clicks = 10
        for _ in range(max_clicks):
            clicked = False
            for selector in self.LOAD_MORE_SELECTORS:
                try:
                    btn = page.locator(selector).first
                    if btn and btn.is_visible():
                        log.info(f"  Clicking load-more: {selector}")
                        btn.click()
                        page.wait_for_timeout(2000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                break

    def _extract_iframe_content(self, page) -> Optional[str]:
        """Check for iframes containing leasing widgets."""
        try:
            iframes = page.frames
            for frame in iframes:
                frame_url = frame.url.lower()
                # Skip chat/communication widgets (not leasing data)
                if "comms.entrata" in frame_url or "chat" in frame_url:
                    continue
                if any(kw in frame_url for kw in [
                    "sightmap", "rentcafe", "realpage", "leasing",
                    "availability", "floorplan", "apartment",
                    "maac.com", "maa.com",
                ]):
                    log.info(f"  Found leasing iframe: {frame.url[:80]}")
                    return frame.content()
        except Exception:
            pass
        return None

    # ── HTML Parsing ─────────────────────────────────────────────────────

    def _parse_html(self, html: str) -> list[Unit]:
        """Parse rendered HTML for unit data using BeautifulSoup."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            log.warning("beautifulsoup4 not installed")
            return []

        soup = BeautifulSoup(html, "lxml")

        # Try platform-specific parsers first
        cortland_units = self._parse_cortland(soup)
        if cortland_units:
            return cortland_units

        maa_units = self._parse_maa_html(soup)
        if maa_units:
            return maa_units

        units = []

        # Find unit cards/rows
        cards = []
        for sel in self.UNIT_SELECTORS:
            # BeautifulSoup doesn't support :has-text, use CSS selectors
            css_sel = sel.split(":has-text")[0] if ":has-text" in sel else sel
            try:
                found = soup.select(css_sel)
                if found:
                    cards = found
                    break
            except Exception:
                continue

        for card in cards:
            unit = self._parse_card(card)
            if unit:
                units.append(unit)

        # Also look for floor-plan-level info with unit lists nested inside
        if not units:
            units = self._parse_floorplan_sections(soup)

        return units

    def _parse_card(self, card) -> Optional[Unit]:
        """Parse a single HTML element into a Unit."""
        text = card.get_text(" ", strip=True)
        if len(text) < 5:
            return None

        # Unit number
        unit_num = "N/A"
        for attr in ["data-unit", "data-unit-id", "data-apartment", "data-unitid"]:
            if card.get(attr):
                unit_num = card[attr]
                break
        if unit_num == "N/A":
            for sel in [".unit-number", ".apt-number", ".unitNumber",
                        ".unit-name", ".apt-name", ".unit-id", ".unit"]:
                el = card.select_one(sel)
                if el:
                    unit_num = el.get_text(strip=True).lstrip("#")
                    break

        # Floor plan
        fp_el = card.select_one(
            ".floor-plan-name, .floorplan-name, .fp-name, "
            ".plan-name, .planName, .floorplan, h3, h4"
        )
        fp_name = fp_el.get_text(strip=True) if fp_el else None

        beds, baths = parse_beds_baths(text)
        sqft = parse_sqft(text)

        # Rent
        rent_el = card.select_one(
            ".rent, .price, .rent-amount, .pricing, "
            ".unit-price, .monthlyRent, [class*='rent'], [class*='price']"
        )
        rent_text = rent_el.get_text(strip=True) if rent_el else text
        rent_min, rent_max = parse_rent(rent_text)

        # Availability
        avail_el = card.select_one(
            ".availability, .available-date, .move-in-date, "
            ".availableDate, [class*='avail']"
        )
        avail_text = avail_el.get_text(strip=True) if avail_el else ""
        avail_date, status = parse_availability(avail_text)

        # Deposit
        deposit = None
        dep_el = card.select_one(".deposit, .deposit-amount, [class*='deposit']")
        if dep_el:
            dep_match = re.findall(r"\$?([\d,]+)", dep_el.get_text())
            if dep_match:
                deposit = float(dep_match[0].replace(",", ""))

        # Specials
        specials = []
        for sp_el in card.select(".special, .concession, .promo, [class*='special']"):
            sp_text = sp_el.get_text(strip=True)
            if sp_text and len(sp_text) > 3:
                specials.append(parse_special(sp_text))

        # Floor
        floor = None
        floor_el = card.select_one("[data-floor], .floor, .floor-number")
        if floor_el:
            fl_text = floor_el.get_text(strip=True) if not floor_el.get("data-floor") else floor_el["data-floor"]
            fl_match = re.search(r"(\d+)", fl_text)
            if fl_match:
                floor = int(fl_match.group(1))

        unit_type = build_unit_type(beds, baths)

        # Skip if we have almost no data
        if unit_num == "N/A" and not fp_name and not rent_min:
            return None

        return Unit(
            unit_number=unit_num,
            floor_plan_name=fp_name,
            unit_type=unit_type,
            bedrooms=beds,
            bathrooms=baths,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            deposit=deposit,
            available_date=avail_date,
            status=status,
            floor=floor,
            specials=specials,
        )

    # ── Cortland-Specific Parsing ───────────────────────────────────────

    def _parse_cortland(self, soup) -> list[Unit]:
        """Parse Cortland apartment cards (.apartments__card elements)."""
        units = []
        cards = soup.select(".apartments__card")
        if not cards:
            return units

        for card in cards:
            # Unit number from the bold text inside .apartments__card-number
            unit_num = "N/A"
            num_el = card.select_one(".apartments__card-number strong")
            if num_el:
                unit_num = num_el.get_text(strip=True).lstrip("#")

            # Floor plan name
            fp_el = card.select_one(".apartments__card-floorplan")
            fp_name = fp_el.get_text(strip=True) if fp_el else None

            # Price — "Starting at $1,557"
            rent_min = rent_max = None
            price_el = card.select_one(".apartments__card-price")
            if price_el:
                rent_min, rent_max = parse_rent(price_el.get_text(strip=True))

            # Floor — "Floor 3"
            floor = None
            floor_el = card.select_one(".apartments__card-info--location")
            if floor_el:
                fl_match = re.search(r"(\d+)", floor_el.get_text(strip=True))
                if fl_match:
                    floor = int(fl_match.group(1))

            # Bed / Bath from .apartments__card-info--main
            beds = baths = None
            meta_el = card.select_one(".apartments__card-info--main")
            if meta_el:
                meta_text = meta_el.get_text(" ", strip=True)
                beds, baths = parse_beds_baths(meta_text)

            # Sqft from .apartments__card-sqft
            sqft = None
            sqft_el = card.select_one(".apartments__card-sqft")
            if sqft_el:
                sqft = parse_sqft(sqft_el.get_text(strip=True))

            # Availability — "Available Now" or "Available starting 3/27"
            avail_date, status = None, UnitStatus.UNKNOWN
            avail_el = card.select_one(".apartments__card-info--avail")
            if avail_el:
                avail_date, status = parse_availability(avail_el.get_text(strip=True))

            units.append(Unit(
                unit_number=unit_num,
                floor_plan_name=fp_name,
                unit_type=build_unit_type(beds, baths),
                bedrooms=beds,
                bathrooms=baths,
                sqft=sqft,
                rent_min=rent_min,
                rent_max=rent_max,
                available_date=avail_date,
                status=status,
                floor=floor,
            ))

        if units:
            log.info(f"  Parsed {len(units)} units from Cortland HTML")
        return units

    # ── MAA-Specific Parsing (HTML) ──────────────────────────────────

    def _parse_maa_html(self, soup) -> list[Unit]:
        """Parse MAA apartment cards (.available-apartments__body--apt)."""
        units = []
        cards = soup.select(".available-apartments__body--apt")
        if not cards:
            return units

        for card in cards:
            # Unit number — <span class="unit">Unit #01318</span>
            unit_num = "N/A"
            unit_el = card.select_one(".unit")
            if unit_el:
                raw = unit_el.get_text(strip=True)
                # Strip "Unit #" prefix
                unit_num = re.sub(r"^Unit\s*#?\s*", "", raw, flags=re.I).lstrip("0") or "N/A"

            # Price — <span class="price">$1863</span>
            rent_min = rent_max = None
            price_el = card.select_one(".price")
            if price_el:
                rent_min, rent_max = parse_rent(price_el.get_text(strip=True))

            # Details from .apt-details li elements
            beds = baths = sqft = floor = None
            avail_date, status = None, UnitStatus.UNKNOWN
            for li in card.select(".apt-details li"):
                li_text = li.get_text(strip=True)
                if "bed" in li_text.lower() or "bath" in li_text.lower():
                    beds, baths = parse_beds_baths(li_text)
                elif "sq" in li_text.lower():
                    sqft = parse_sqft(li_text)
                elif "floor" in li_text.lower() or "ground" in li_text.lower():
                    fl_match = re.search(r"(\d+)", li_text)
                    if fl_match:
                        floor = int(fl_match.group(1))
                    elif "ground" in li_text.lower():
                        floor = 0
                elif "move" in li_text.lower():
                    # "Move-in: 03/23 - 03/26" → extract first date
                    clean = re.sub(r"^move[- ]?in:?\s*", "", li_text, flags=re.I).strip()
                    # Take first date from a range like "03/23 - 03/26"
                    date_match = re.search(r"(\d{1,2}/\d{1,2})", clean)
                    if date_match:
                        avail_date = date_match.group(1)
                        status = UnitStatus.AVAILABLE
                    else:
                        avail_date, status = parse_availability(clean)

            # Floor plan name from amenities text (e.g., "22B-FP")
            fp_name = None
            amen_el = card.select_one(".apt-amenities")
            if amen_el:
                fp_match = re.search(r"(\w+)-FP", amen_el.get_text())
                if fp_match:
                    fp_name = fp_match.group(1)

            # Specials — .move-in-special
            specials = []
            sp_el = card.select_one(".move-in-special")
            if sp_el:
                sp_text = sp_el.get_text(" ", strip=True)
                if sp_text and len(sp_text) > 3:
                    specials.append(parse_special(sp_text))

            units.append(Unit(
                unit_number=unit_num,
                floor_plan_name=fp_name,
                unit_type=build_unit_type(beds, baths),
                bedrooms=beds,
                bathrooms=baths,
                sqft=sqft,
                rent_min=rent_min,
                rent_max=rent_max,
                available_date=avail_date,
                status=status,
                floor=floor,
                specials=specials,
            ))

        if units:
            log.info(f"  Parsed {len(units)} units from MAA HTML")
        return units

    # ── MAA-Specific Parsing (JSON API) ────────────────────────────────

    def _parse_maa_response(self, body: str) -> list[Unit]:
        """Parse MAA /api/apartments/search JSON response."""
        units = []
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return units

        if not isinstance(data, dict):
            return units

        apartments = data.get("apartments", [])
        if not isinstance(apartments, list):
            return units

        for apt in apartments:
            if not isinstance(apt, dict):
                continue

            unit_num = str(apt.get("UnitNumber", "N/A")).lstrip("0") or "N/A"
            fp_name = apt.get("FloorPlanName")
            beds = apt.get("Beds")
            baths = apt.get("Baths")
            if baths is not None:
                baths = float(baths)
            sqft = apt.get("SqFt")

            rent_min = apt.get("MinPrice")
            if rent_min is not None:
                rent_min = float(rent_min)
            rent_max = apt.get("MaxPrice")
            if rent_max is not None:
                rent_max = float(rent_max)

            # Availability — "01/22/2026" or MS JSON date
            avail_text = apt.get("FormattedMoveIn", "")
            avail_date, status = parse_availability(avail_text)

            # Floor — "1st Floor", "2nd Floor", "3rd Floor", "Ground Floor"
            floor = None
            floor_str = apt.get("FloorBuilding", "")
            if floor_str:
                fl_match = re.search(r"(\d+)", floor_str)
                if fl_match:
                    floor = int(fl_match.group(1))
                elif "ground" in floor_str.lower():
                    floor = 0

            # Specials
            specials = []
            sp_text = apt.get("Specials", "")
            if sp_text and len(sp_text) > 3:
                specials.append(parse_special(sp_text))

            # Amenities
            amenities = apt.get("Amenities", [])
            if not isinstance(amenities, list):
                amenities = []

            units.append(Unit(
                unit_number=unit_num,
                floor_plan_name=fp_name,
                unit_type=build_unit_type(beds, baths),
                bedrooms=beds,
                bathrooms=baths,
                sqft=sqft,
                rent_min=rent_min,
                rent_max=rent_max,
                available_date=avail_date,
                status=status,
                floor=floor,
                specials=specials,
                amenities=amenities,
            ))

        if units:
            log.info(f"  Parsed {len(units)} units from MAA API")
        return units

    def _parse_floorplan_sections(self, soup) -> list[Unit]:
        """
        Handle sites that show floor plan headers with units nested below.
        Common in RentCafe: a floor plan card contains multiple unit rows.
        """
        units = []
        fp_containers = soup.select(
            ".floorplan, .floor-plan, [class*='floorplan'], "
            "[class*='floor-plan'], .fp-container"
        )

        for fp in fp_containers:
            # Get floor plan level info
            fp_header = fp.select_one("h2, h3, h4, .fp-name, .plan-name")
            fp_name = fp_header.get_text(strip=True) if fp_header else None
            fp_text = fp.get_text(" ", strip=True)

            beds, baths = parse_beds_baths(fp_text)
            sqft = parse_sqft(fp_text)

            # Look for individual unit rows inside
            unit_rows = fp.select(
                ".unit-row, .unit-item, tr, .apt-unit, "
                "[class*='unit'], li[class*='unit']"
            )

            if unit_rows:
                for row in unit_rows:
                    row_text = row.get_text(" ", strip=True)
                    if len(row_text) < 5:
                        continue

                    # Try to get unit-specific data
                    row_rent_min, row_rent_max = parse_rent(row_text)
                    row_sqft = parse_sqft(row_text) or sqft
                    row_beds = parse_beds_baths(row_text)[0] or beds
                    row_baths = parse_beds_baths(row_text)[1] or baths

                    # Unit number from row
                    unit_num = "N/A"
                    for sel in [".unit-number", ".apt-number", ".unitNumber",
                                "td:first-child", ".unit-name"]:
                        el = row.select_one(sel)
                        if el:
                            t = el.get_text(strip=True)
                            if t and len(t) < 30:
                                unit_num = t
                                break

                    if unit_num == "N/A":
                        num_match = re.search(r"(?:unit|apt|#)\s*([\w\-]+)", row_text, re.I)
                        if num_match:
                            unit_num = num_match.group(1)

                    avail_date, status = parse_availability(row_text)

                    units.append(Unit(
                        unit_number=unit_num,
                        floor_plan_name=fp_name,
                        unit_type=build_unit_type(row_beds, row_baths),
                        bedrooms=row_beds,
                        bathrooms=row_baths,
                        sqft=row_sqft,
                        rent_min=row_rent_min or None,
                        rent_max=row_rent_max or None,
                        available_date=avail_date,
                        status=status,
                    ))
            else:
                # No unit rows — create one entry for the floor plan
                fp_rent_min, fp_rent_max = parse_rent(fp_text)
                if fp_name or fp_rent_min:
                    units.append(Unit(
                        unit_number=fp_name or "N/A",
                        floor_plan_name=fp_name,
                        unit_type=build_unit_type(beds, baths),
                        bedrooms=beds,
                        bathrooms=baths,
                        sqft=sqft,
                        rent_min=fp_rent_min,
                        rent_max=fp_rent_max,
                        status=UnitStatus.AVAILABLE if fp_rent_min else UnitStatus.UNKNOWN,
                    ))

        return units

    # ── Embedded JavaScript Parsing ──────────────────────────────────────

    def _parse_embedded_js(self, html: str) -> list[Unit]:
        """Extract unit data from JavaScript variables embedded in the page."""
        units = []
        patterns = [
            r'var\s+defined_FPUnits\s*=\s*(\[.*?\]);',
            r'var\s+defined_FloorPlans\s*=\s*(\[.*?\]);',
            r'"units"\s*:\s*(\[.*?\])',
            r'"apartments"\s*:\s*(\[.*?\])',
            r'"availableUnits"\s*:\s*(\[.*?\])',
            r'"floorPlanUnits"\s*:\s*(\[.*?\])',
            r'window\.__DATA__\s*=\s*(\{.*?\});',
            r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
        ]
        for pat in patterns:
            match = re.search(pat, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, list):
                        for item in data:
                            u = self._parse_js_unit(item)
                            if u:
                                units.append(u)
                    elif isinstance(data, dict):
                        # Look for unit arrays inside the object
                        for key in ["units", "apartments", "availableUnits",
                                     "floorPlanUnits", "results"]:
                            if key in data and isinstance(data[key], list):
                                for item in data[key]:
                                    u = self._parse_js_unit(item)
                                    if u:
                                        units.append(u)
                    if units:
                        log.info(f"  Extracted {len(units)} units from embedded JS")
                        return units
                except (json.JSONDecodeError, TypeError):
                    continue
        return units

    def _parse_js_unit(self, item: dict) -> Optional[Unit]:
        if not isinstance(item, dict):
            return None

        def get(*keys, default=None):
            for k in keys:
                v = item.get(k)
                if v is not None:
                    return v
            return default

        unit_num = str(get("UnitId", "ApartmentId", "unitId", "unit_number",
                           "apartmentName", "UnitNumber", "unitNumber",
                           "unit_id", "name", "unit", "id",
                           default="N/A"))
        fp_name = get("FloorplanName", "floorPlanName", "planName",
                       "floor_plan_name", "FloorPlan", "floorplan_name",
                       "floorPlan", "layout", "layoutName", "floor_plan",
                       "floorplanName")

        beds_raw = get("Beds", "beds", "bedrooms", "NumberOfBedrooms", "Bedrooms",
                        "bedroom_count", "bedroomCount", "bed_count", "numBeds")
        beds = int(beds_raw) if beds_raw is not None else None

        baths_raw = get("Baths", "baths", "bathrooms", "NumberOfBathrooms", "Bathrooms",
                         "bathroom_count", "bathroomCount", "bath_count", "numBaths")
        baths = float(baths_raw) if baths_raw is not None else None

        sqft_raw = get("SquareFeet", "sqft", "squareFeet", "MaximumSquareFeet",
                        "MinimumSquareFeet", "area", "SqFt", "SQFT",
                        "square_feet", "squareFt", "size", "maxSqft", "minSqft")
        sqft = int(float(str(sqft_raw).replace(",", ""))) if sqft_raw else None

        rent_raw = get("MinimumRent", "Rent", "rent", "price", "MonthlyRent",
                        "minimumRent", "Price", "BaseRent",
                        "effectiveRent", "marketRent", "min_price", "startingPrice",
                        "monthly_rent", "rentAmount")
        rent_min = float(str(rent_raw).replace(",", "").replace("$", "")) if rent_raw else None

        rent_max_raw = get("MaximumRent", "maximumRent", "maxRent", "MaxRent",
                            "max_price", "maxPrice")
        rent_max = float(str(rent_max_raw).replace(",", "").replace("$", "")) if rent_max_raw else rent_min

        avail_raw = str(get("AvailableDate", "availableDate", "MoveInDate",
                             "available_date", "DateAvailable", "moveInDate",
                             "move_in_date", "availableOn", "available_on",
                             default=""))
        avail_date, status = parse_availability(avail_raw)

        floor_raw = get("Floor", "floor", "FloorNumber", "floorNumber")
        floor = int(floor_raw) if floor_raw is not None else None

        return Unit(
            unit_number=unit_num,
            floor_plan_name=fp_name,
            unit_type=build_unit_type(beds, baths),
            bedrooms=beds,
            bathrooms=baths,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            available_date=avail_date,
            status=status,
            floor=floor,
        )

    # ── API/XHR Response Parsing ─────────────────────────────────────────

    def _parse_all_api_responses(self, api_responses: list[dict]) -> list["Unit"]:
        """Parse all captured API responses, deduplicating units."""
        all_units = []
        seen = set()

        # MAA first (dedicated parser for /api/apartments/search)
        for resp in api_responses:
            if "maac.com" in resp["url"].lower() or "/api/apartments/" in resp["url"].lower():
                for u in self._parse_maa_response(resp["body"]):
                    uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                    if uid not in seen:
                        seen.add(uid)
                        all_units.append(u)

        # SightMap (highest fidelity)
        for resp in api_responses:
            if "sightmap" in resp["url"].lower():
                for u in self._parse_sightmap_response(resp["body"]):
                    uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                    if uid not in seen:
                        seen.add(uid)
                        all_units.append(u)

        # Then other API responses
        for resp in api_responses:
            url_lower = resp["url"].lower()
            if ("sightmap" not in url_lower
                    and "maac.com" not in url_lower
                    and "/api/apartments/" not in url_lower):
                for u in self._parse_api_response(resp["body"]):
                    uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                    if uid not in seen:
                        seen.add(uid)
                        all_units.append(u)

        return all_units

    def _parse_api_response(self, body: str) -> list[Unit]:
        """Parse intercepted XHR/API responses for unit data."""
        units = []
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return units

        # Handle various API response shapes
        if isinstance(data, list):
            for item in data:
                u = self._parse_js_unit(item)
                if u:
                    units.append(u)
        elif isinstance(data, dict):
            # Check common top-level keys
            for key in ["units", "apartments", "availableUnits", "results",
                         "floorPlanUnits", "data", "items", "floorplans",
                         "floor_plans", "floorPlans", "listings", "availability",
                         "availabilities", "propertyUnits"]:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        u = self._parse_js_unit(item)
                        if u:
                            units.append(u)
                    if units:
                        break
            # Check nested structures (2 levels deep)
            if not units:
                for key, val in data.items():
                    if isinstance(val, dict):
                        for inner_key in ["units", "apartments", "floorplans",
                                          "floor_plans", "availableUnits", "results"]:
                            if inner_key in val and isinstance(val[inner_key], list):
                                for item in val[inner_key]:
                                    u = self._parse_js_unit(item)
                                    if u:
                                        units.append(u)
                                if units:
                                    break
                    if units:
                        break

        if units:
            log.info(f"  Parsed {len(units)} units from API response")
        return units

    # ── SightMap API Parsing ───────────────────────────────────────────────

    def _parse_sightmap_response(self, body: str) -> list[Unit]:
        """Parse SightMap API response which contains units and floor_plans."""
        units = []
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return units

        # SightMap wraps everything under a "data" key
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        if not isinstance(data, dict):
            return units

        # Build floor_plan lookup: id -> {bedroom_count, bathroom_count, name, filter_label}
        fp_lookup = {}
        for fp in data.get("floor_plans", []):
            fp_lookup[str(fp.get("id", ""))] = {
                "name": fp.get("filter_label") or fp.get("name"),
                "bedrooms": fp.get("bedroom_count"),
                "bathrooms": fp.get("bathroom_count"),
            }

        for item in data.get("units", []):
            fp_id = str(item.get("floor_plan_id", ""))
            fp_info = fp_lookup.get(fp_id, {})

            unit_num = item.get("unit_number") or item.get("label") or "N/A"
            beds = fp_info.get("bedrooms")
            baths = fp_info.get("bathrooms")
            fp_name = fp_info.get("name")

            sqft = item.get("area")
            if sqft is not None:
                sqft = int(sqft)

            price = item.get("price")
            rent_min = float(price) if price is not None else None

            # total_price may be [min, max]
            total_price = item.get("total_price")
            if isinstance(total_price, list) and len(total_price) >= 2:
                rent_min = float(total_price[0])
                rent_max = float(total_price[1])
            else:
                rent_max = rent_min

            avail_text = item.get("display_available_on") or item.get("available_on") or ""
            avail_date, status = parse_availability(avail_text)

            units.append(Unit(
                unit_number=str(unit_num),
                floor_plan_name=fp_name,
                unit_type=build_unit_type(beds, baths),
                bedrooms=beds,
                bathrooms=float(baths) if baths is not None else None,
                sqft=sqft,
                rent_min=rent_min,
                rent_max=rent_max,
                available_date=avail_date,
                status=status,
            ))

        if units:
            log.info(f"  Parsed {len(units)} units from SightMap API")
        return units


# ═══════════════════════════════════════════════════════════════════════════
# BATCH ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

class ApartmentScraper:
    def __init__(self, headless: bool = True):
        self.engine = PlaywrightScraper(headless=headless)

    def scrape(self, url: str, **kwargs) -> Property:
        return self.engine.scrape(url, **kwargs)

    def scrape_batch(self, urls: list[str], delay: float = 2.0) -> list[Property]:
        results = []
        for i, url in enumerate(urls):
            try:
                prop = self.scrape(url)
                results.append(prop)
            except Exception as e:
                log.error(f"Failed: {url} — {e}")
                results.append(Property(name=f"ERROR: {url}", website_url=url))
            if i < len(urls) - 1:
                time.sleep(delay)
        return results


# ═══════════════════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════════════════

def to_json(properties: list[Property], path: str):
    data = [p.to_dict() for p in properties]
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    log.info(f"Saved {len(properties)} properties to {path}")


def to_csv(properties: list[Property], path: str):
    rows = []
    for prop in properties:
        for unit in prop.units:
            rows.append({
                "property_name": prop.name,
                "address": prop.address,
                "city": prop.city,
                "state": prop.state,
                "zip": prop.zipcode,
                "platform": prop.platform,
                "unit_number": unit.unit_number,
                "floor_plan": unit.floor_plan_name,
                "unit_type": unit.unit_type,
                "bedrooms": unit.bedrooms,
                "bathrooms": unit.bathrooms,
                "sqft": unit.sqft,
                "rent_min": unit.rent_min,
                "rent_max": unit.rent_max,
                "deposit": unit.deposit,
                "available_date": unit.available_date,
                "status": unit.status.value if isinstance(unit.status, UnitStatus) else unit.status,
                "floor": unit.floor,
                "specials": "; ".join(s.description for s in unit.specials),
                "community_specials": "; ".join(s.description for s in prop.specials),
                "scraped_at": prop.scraped_at,
            })
    if not rows:
        log.warning("No units to export")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Saved {len(rows)} unit rows to {path}")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Scrape apartment websites for unit data using Playwright."
    )
    parser.add_argument("--url", help="Property URL to scrape")
    parser.add_argument("--batch", help="File with one URL per line")
    parser.add_argument("--output", default="output.json", help="Output path")
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    parser.add_argument(
        "--tabs",
        help="Comma-separated tab labels to click (e.g., '0,1,2')",
    )
    parser.add_argument(
        "--tab-type",
        choices=["floor", "bedroom", "auto"],
        default="auto",
        help="Type of tabs on the page",
    )
    parser.add_argument("--headed", action="store_true",
                        help="Run with visible browser (for debugging)")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.url and not args.batch:
        parser.error("Provide --url or --batch")

    scraper = ApartmentScraper(headless=not args.headed)

    tab_labels = args.tabs.split(",") if args.tabs else None

    urls = []
    if args.url:
        urls.append(args.url)
    if args.batch:
        with open(args.batch) as f:
            urls.extend(line.strip() for line in f if line.strip())

    if len(urls) == 1:
        props = [scraper.scrape(urls[0], tab_labels=tab_labels, tab_type=args.tab_type)]
    else:
        props = scraper.scrape_batch(urls)

    if args.format == "csv" or args.output.endswith(".csv"):
        to_csv(props, args.output)
    else:
        to_json(props, args.output)


if __name__ == "__main__":
    main()
