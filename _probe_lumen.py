import re
from playwright.sync_api import sync_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

URLS = [
    "https://www.lumenbriarcliff.com/apartments/ga/atlanta/floor-plans",
    "https://www.lumenbriarcliff.com/apartments/ga/atlanta/floor-plans#/",
]

for url in URLS:
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

        platform_hints = set()
        units_hits = []

        def on_resp(r):
            u = r.url.lower()
            for m in ["sightmap", "entrata", "rentcafe", "realpage", "yardi", "engrain", "meetelise", "g5search"]:
                if m in u:
                    platform_hints.add(m)
            if "json" in r.headers.get("content-type", "") and any(
                k in u for k in ["unit", "availab", "floorplan", "pricing", "inventor"]
            ):
                try:
                    body = r.text()[:500]
                except Exception:
                    body = ""
                units_hits.append((r.status, r.url[:100], body[:200]))

        page.on("response", on_resp)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(9000)

        iframes = [f.get_attribute("src") or "" for f in page.locator("iframe").all()]

        print(f"  platforms: {sorted(platform_hints)}")
        print(f"  iframes:")
        for s in iframes:
            if s and "recaptcha" not in s:
                print(f"    {s[:140]}")
        print(f"  unit/avail JSON hits ({len(units_hits)}):")
        for s, u, b in units_hits[:6]:
            print(f"    [{s}] {u}")
            if b and "<" not in b[:5]:
                print(f"      body: {b.replace(chr(10), ' ')[:180]}")

        text = page.inner_text("body")
        prices = re.findall(r"\$[\d,]{3,5}", text)
        rent_prices = [p for p in prices if 500 <= int(p.replace("$", "").replace(",", "")) <= 6000]
        print(f"  rent prices visible: {len(rent_prices)}, sample: {rent_prices[:10]}")
        browser.close()
