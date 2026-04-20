#!/usr/bin/env python3
"""
probe_amli.py - Load an AMLI floorplans?tab=map page and log all XHR/fetch
responses so we can see where unit availability actually comes from.
"""

import json
from playwright.sync_api import sync_playwright

URL = "https://www.amli.com/apartments/atlanta/midtown-apartments/amli-arts-center/floorplans?tab=map"


def main() -> None:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
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
            from playwright_stealth import Stealth
            Stealth().apply_stealth_sync(page)
        except ImportError:
            pass

        hits: list[dict] = []

        def on_response(resp):
            ct = resp.headers.get("content-type", "")
            url = resp.url
            if "json" in ct or "api" in url.lower() or "unit" in url.lower() or "floorplan" in url.lower() or "availab" in url.lower():
                try:
                    body = resp.text()[:1500]
                except Exception:
                    body = "<binary>"
                hits.append({
                    "url": url,
                    "status": resp.status,
                    "ct": ct,
                    "body_preview": body,
                })

        page.on("response", on_response)
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(8000)

        # Try scrolling and waiting for the map view to load
        try:
            page.evaluate("window.scrollBy(0, 600)")
            page.wait_for_timeout(4000)
        except Exception:
            pass

        # Look for iframes
        iframes = page.locator("iframe").all()
        iframe_info = []
        for f in iframes:
            try:
                src = f.get_attribute("src") or ""
                if src:
                    iframe_info.append(src)
            except Exception:
                pass

        # Look for SightMap/widget divs
        widget_selectors = [
            "[class*='sightmap']", "[data-sightmap]",
            "[class*='interactive-map']", "[id*='map']",
            "[class*='availability']",
        ]
        widget_hits = {}
        for sel in widget_selectors:
            try:
                count = page.locator(sel).count()
                if count > 0:
                    widget_hits[sel] = count
            except Exception:
                pass

        title = page.title()
        html_len = len(page.content())

        print(f"TITLE: {title}")
        print(f"HTML length: {html_len}")
        print(f"IFRAMES ({len(iframe_info)}):")
        for s in iframe_info:
            print(f"  {s}")
        print(f"WIDGETS: {widget_hits}")
        print(f"\nNETWORK HITS ({len(hits)}):")
        for h in hits[:30]:
            print(f"  [{h['status']}] {h['url'][:120]}")
            if h['body_preview'] and '<' not in h['body_preview'][:5]:
                preview = h['body_preview'].replace('\n', ' ')[:180]
                print(f"    body: {preview}")

        browser.close()


if __name__ == "__main__":
    main()
