#!/usr/bin/env python3
"""Quick utility to load a list of URLs with playwright-stealth and log
iframe sources + recent XHR response URLs containing hints of the
leasing platform. Useful when we don't know which platform a site uses.
"""

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def probe(url: str) -> None:
    print(f"\n=== {url} ===")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()
        try:
            from playwright_stealth import Stealth
            Stealth().apply_stealth_sync(page)
        except ImportError:
            pass

        platforms: set[str] = set()
        interesting_urls: list[str] = []

        def on_response(resp):
            u = resp.url.lower()
            for marker in [
                "sightmap.com", "rentcafe.com", "entrata.com", "realpage",
                "yardi", "engrain", "meetelise.com", "appfolio",
                "securecafe", "onlineleasing", "prospectportal",
            ]:
                if marker in u:
                    platforms.add(marker)
            if any(k in u for k in ["unit", "availab", "floorplan", "inventor", "pricing", "api/"]):
                ct = resp.headers.get("content-type", "")
                if "json" in ct:
                    interesting_urls.append(f"[{resp.status}] {resp.url}")

        page.on("response", on_response)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
            page.wait_for_timeout(6000)
        except Exception as e:
            print(f"  nav error: {e}")
            browser.close()
            return

        iframe_srcs = []
        for f in page.locator("iframe").all():
            try:
                s = f.get_attribute("src") or ""
                if s:
                    iframe_srcs.append(s)
            except Exception:
                pass

        try:
            title = page.title()
        except Exception:
            title = ""

        print(f"  title: {title[:80]}")
        print(f"  platform hints: {sorted(platforms)}")
        if iframe_srcs:
            print(f"  iframes:")
            for s in iframe_srcs[:5]:
                print(f"    {s[:150]}")
        if interesting_urls:
            print(f"  json hits (max 8):")
            for u in interesting_urls[:8]:
                print(f"    {u[:150]}")

        browser.close()


def main() -> None:
    urls = sys.argv[1:] or [
        # default smoke targets
        "https://www.windsorkennesaw.com/floorplans",
    ]
    for u in urls:
        try:
            probe(u)
        except Exception as e:
            print(f"  probe crash: {e}")


if __name__ == "__main__":
    main()
