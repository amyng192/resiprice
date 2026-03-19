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
        # Generic table rows with unit info
        "table.units tbody tr",
        ".pricing-table tbody tr",
    ]

    def __init__(self, headless: bool = True, timeout_ms: int = 45000):
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
                if "json" in ct or "javascript" in ct:
                    req_url = response.url.lower()
                    keywords = [
                        "unit", "apartment", "availability", "floorplan",
                        "floor-plan", "pricing", "inventory", "getapartment",
                        "sightmap",
                    ]
                    if any(kw in req_url for kw in keywords):
                        try:
                            body = response.text()
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
                page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
                page.wait_for_timeout(3000)

                # ── Extract property-level info ──────────────────────────
                prop = self._extract_property_info(page, url)

                # ── Click "Load More" / "View All" buttons ───────────────
                self._click_load_more(page)

                # ── Discover and click through tabs ──────────────────────
                html_snapshots = []

                if tab_labels:
                    # User provided explicit tab labels
                    log.info(f"Using explicit tabs: {tab_labels}")
                    html_snapshots = self._click_explicit_tabs(page, tab_labels)
                else:
                    # Auto-detect tabs
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
        all_units = []
        seen_unit_ids = set()

        # Parse SightMap API responses first (highest fidelity)
        for resp in api_responses:
            if "sightmap" in resp["url"].lower():
                units = self._parse_sightmap_response(resp["body"])
                for u in units:
                    uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                    if uid not in seen_unit_ids:
                        seen_unit_ids.add(uid)
                        all_units.append(u)

        # Parse other API/XHR JSON responses (most structured)
        for resp in api_responses:
            if "sightmap" not in resp["url"].lower():
                units = self._parse_api_response(resp["body"])
                for u in units:
                    uid = f"{u.unit_number}_{u.floor_plan_name}_{u.sqft}"
                    if uid not in seen_unit_ids:
                        seen_unit_ids.add(uid)
                        all_units.append(u)

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

        prop.units = all_units
        log.info(f"RESULT: {len(all_units)} units from \"{prop.name}\"")
        return prop

    # ── Property-level info ──────────────────────────────────────────────

    def _extract_property_info(self, page, url) -> Property:
        """Pull property name, address, specials from the page."""
        prop = Property(name="Unknown Property", website_url=url)

        # Try meta tags first
        try:
            og_title = page.locator("meta[property='og:title']").get_attribute("content")
            if og_title:
                prop.name = og_title.split("|")[0].split("-")[0].strip()
        except Exception:
            pass

        # Fallback to page title
        if prop.name == "Unknown Property":
            try:
                title = page.title()
                if title:
                    prop.name = title.split("|")[0].split("-")[0].strip()
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
            ("sightmap", ["sightmap"]),
            ("rentcafe", ["rentcafe"]),
            ("entrata", ["entrata"]),
            ("realpage", ["realpage"]),
            ("appfolio", ["appfolio"]),
        ]:
            if any(kw in url_lower or kw in source.lower()[:5000] for kw in kws):
                prop.platform = platform
                break

        # Extract specials
        try:
            for sel in [".special", ".promo", ".concession", ".move-in-special",
                        "[class*='special']", "[class*='promo']"]:
                for el in page.locator(sel).all():
                    text = el.inner_text().strip()
                    if len(text) > 5 and len(text) < 500:
                        prop.specials.append(Special(description=text))
        except Exception:
            pass

        return prop

    # ── Tab Detection & Clicking ─────────────────────────────────────────

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
                                if btn.is_visible():
                                    tabs.append({
                                        "element": btn,
                                        "text": btn.inner_text().strip(),
                                        "selector": container_sel,
                                    })
                            except Exception:
                                continue
                        if tabs:
                            log.info(f"Found tabs in container '{container_sel}': "
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
                tab["element"].click()
                page.wait_for_timeout(2000)
                # Wait for any loading spinners to disappear
                try:
                    page.wait_for_selector(
                        ".loading, .spinner, [class*='loading']",
                        state="hidden", timeout=5000,
                    )
                except Exception:
                    pass
                page.wait_for_timeout(1000)
                snapshots.append(page.content())
            except Exception as e:
                log.warning(f"  Failed to click tab \"{tab['text']}\": {e}")
        return snapshots

    def _click_explicit_tabs(self, page, tab_labels: list[str]) -> list[str]:
        """
        Click tabs by matching their visible text to the provided labels.
        Falls back to clicking by index if text matching fails.
        """
        snapshots = []
        for label in tab_labels:
            clicked = False
            label_clean = label.strip()

            # Strategy 1: find by exact or partial text match
            for tag in ["button", "a", "[role='tab']", "li", "span", "div"]:
                try:
                    candidates = page.locator(tag).all()
                    for el in candidates:
                        try:
                            if not el.is_visible():
                                continue
                            el_text = el.inner_text().strip()
                            # Match: exact text, starts with label, or contains
                            # "(X units)" style text with the floor number
                            if (el_text == label_clean or
                                el_text.startswith(label_clean) or
                                re.match(rf"^{re.escape(label_clean)}\b", el_text)):
                                log.info(f"  Clicking tab: \"{el_text}\"")
                                el.click()
                                page.wait_for_timeout(2500)
                                try:
                                    page.wait_for_selector(
                                        ".loading, .spinner",
                                        state="hidden", timeout=5000,
                                    )
                                except Exception:
                                    pass
                                page.wait_for_timeout(1000)
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
                log.warning(f"  Could not find tab with label \"{label_clean}\"")

        return snapshots

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
                        ".unit-name", ".apt-name", ".unit-id"]:
                el = card.select_one(sel)
                if el:
                    unit_num = el.get_text(strip=True)
                    break

        # Floor plan
        fp_el = card.select_one(
            ".floor-plan-name, .floorplan-name, .fp-name, "
            ".plan-name, .planName, h3, h4"
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
                specials.append(Special(description=sp_text))

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
                           default="N/A"))
        fp_name = get("FloorplanName", "floorPlanName", "planName",
                       "floor_plan_name", "FloorPlan")

        beds_raw = get("Beds", "beds", "bedrooms", "NumberOfBedrooms", "Bedrooms")
        beds = int(beds_raw) if beds_raw is not None else None

        baths_raw = get("Baths", "baths", "bathrooms", "NumberOfBathrooms", "Bathrooms")
        baths = float(baths_raw) if baths_raw is not None else None

        sqft_raw = get("SquareFeet", "sqft", "squareFeet", "MaximumSquareFeet",
                        "MinimumSquareFeet", "area", "SqFt", "SQFT")
        sqft = int(float(str(sqft_raw).replace(",", ""))) if sqft_raw else None

        rent_raw = get("MinimumRent", "Rent", "rent", "price", "MonthlyRent",
                        "minimumRent", "Price", "BaseRent")
        rent_min = float(str(rent_raw).replace(",", "").replace("$", "")) if rent_raw else None

        rent_max_raw = get("MaximumRent", "maximumRent", "maxRent", "MaxRent")
        rent_max = float(str(rent_max_raw).replace(",", "").replace("$", "")) if rent_max_raw else rent_min

        avail_raw = str(get("AvailableDate", "availableDate", "MoveInDate",
                             "available_date", "DateAvailable", default=""))
        avail_date, status = parse_availability(avail_raw)

        floor_raw = get("Floor", "floor", "FloorNumber")
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
            for key in ["units", "apartments", "availableUnits", "results",
                         "floorPlanUnits", "data", "items"]:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        u = self._parse_js_unit(item)
                        if u:
                            units.append(u)
                    if units:
                        break
            # Also check nested structures
            if not units:
                for key, val in data.items():
                    if isinstance(val, dict) and "units" in val:
                        for item in val["units"]:
                            u = self._parse_js_unit(item)
                            if u:
                                units.append(u)

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
