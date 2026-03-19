"""
Quick Test Script — Rosemont Berkeley Lake
==========================================
Run this to verify your Playwright setup works.

Usage:
    python test_rosemont.py

This will:
  1. Open the Rosemont Berkeley Lake floor plans page
  2. Click through floor tabs (0, 1, 2)
  3. Extract all available units
  4. Save results to rosemont_results.json
  5. Print a summary
"""

import json
import sys
import os

def check_dependencies():
    """Check if required packages are installed."""
    missing = []

    try:
        import playwright
    except ImportError:
        missing.append("playwright")

    try:
        import bs4
    except ImportError:
        missing.append("beautifulsoup4")

    try:
        import lxml
    except ImportError:
        missing.append("lxml")

    if missing:
        print("\n❌ Missing packages:", ", ".join(missing))
        print("\nRun these commands to install them:\n")
        print(f"    pip install {' '.join(missing)}")
        if "playwright" in missing:
            print("    playwright install chromium")
        print()
        sys.exit(1)

    # Check if Chromium browser is installed for Playwright
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(headless=True)
                browser.close()
            except Exception:
                print("\n❌ Chromium browser not installed for Playwright.")
                print("\nRun this command:\n")
                print("    playwright install chromium")
                print()
                sys.exit(1)
    except Exception as e:
        print(f"\n❌ Playwright error: {e}")
        sys.exit(1)

    print("✅ All dependencies are installed!\n")


def main():
    print("=" * 60)
    print("  Rosemont Berkeley Lake — Test Extraction")
    print("=" * 60)
    print()

    # Step 1: Check dependencies
    print("Step 1: Checking dependencies...")
    check_dependencies()

    # Step 2: Import the scraper
    print("Step 2: Loading scraper...")
    try:
        from apartment_scraper import PlaywrightScraper
    except ImportError:
        # Try adding current directory to path
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from apartment_scraper import PlaywrightScraper

    # Step 3: Run the scrape
    url = "https://rosemontberkeleylake.com/floor-plans/"
    tab_labels = ["0", "1", "2"]  # The floor tabs on the site

    print(f"Step 3: Scraping {url}")
    print(f"         Clicking floor tabs: {tab_labels}")
    print(f"         (this may take 30-60 seconds...)\n")

    scraper = PlaywrightScraper(headless=True)
    result = scraper.scrape(url, tab_labels=tab_labels, tab_type="floor")

    # Step 4: Save results
    output_file = "rosemont_results.json"
    output_data = result.to_dict()
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    # Step 5: Print summary
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"\n  Property:    {result.name}")
    print(f"  Address:     {result.address or 'N/A'}")
    print(f"  Platform:    {result.platform or 'N/A'}")
    print(f"  Total Units: {len(result.units)}")
    print(f"  Available:   {result.available_count}")

    if result.specials:
        print(f"\n  Specials:")
        for s in result.specials:
            print(f"    • {s.description[:80]}")

    if result.units:
        print(f"\n  Unit Mix:")
        for unit_type, count in result.unit_mix.items():
            print(f"    {unit_type}: {count}")

        print(f"\n  Sample Units (first 10):")
        print(f"  {'Unit':<15} {'Plan':<20} {'Type':<18} {'SqFt':<8} {'Rent':<10} {'Available':<12}")
        print(f"  {'-'*83}")
        for u in result.units[:10]:
            rent = f"${u.rent_min:,.0f}" if u.rent_min else "N/A"
            print(f"  {u.unit_number:<15} {(u.floor_plan_name or 'N/A'):<20} "
                  f"{(u.unit_type or 'N/A'):<18} {(str(u.sqft) if u.sqft else 'N/A'):<8} "
                  f"{rent:<10} {(u.available_date or 'N/A'):<12}")

        if len(result.units) > 10:
            print(f"  ... and {len(result.units) - 10} more units")
    else:
        print("\n  ⚠️  No units were extracted.")
        print("     This could mean:")
        print("     - The leasing widget is in an iframe (scraper checks for this)")
        print("     - The site uses a platform not yet supported")
        print("     - The tab labels '0, 1, 2' don't match what's on the page")
        print("\n     Try running with --headed to see what the browser sees:")
        print("     python apartment_scraper.py --url \"https://rosemontberkeleylake.com/floor-plans/\" --tabs \"0,1,2\" --headed")

    print(f"\n  Full results saved to: {output_file}")
    print()


if __name__ == "__main__":
    main()
