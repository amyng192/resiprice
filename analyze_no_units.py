#!/usr/bin/env python3
"""
analyze_no_units.py - Deep-dive on bulk_scrape 'no_units' entries.

Groups by domain suffix + URL path pattern + explicit aggregator detection
so we can see which buckets have >5 properties (worth a dedicated fix
script) vs the long tail of one-offs.

Also splits the "path ends in /floorplans(/)" cases out — per the user's
observation, some of those might be hiding units behind a /map or ?tab=map
variant (like AMLI). We'll print the top path suffixes so we know which
are common.
"""

import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent
JSONL = ROOT / "bulk_scrape_results.jsonl"

AGGREGATOR_DOMAINS = {
    "apartments.com", "apartmenthomeliving.com", "apartmentfinder.com",
    "apartmentguide.com", "apartmentlist.com", "apartmentratings.com",
    "forrent.com", "rent.com", "trulia.com", "zillow.com", "zumper.com",
    "hotpads.com", "padmapper.com", "aspensquare.com", "rentable.co",
    "rentcafe.com", "corporatehousing.com", "hellolanding.com",
    "veryapt.com", "rentals.com", "apartmentsearch.com", "furnishedhousing.com",
    "homes.com", "redfin.com", "realtor.com", "livebh.com",
}
AD_DOMAINS = {"bing.com", "google.com", "doubleclick.net"}
SOCIAL_DOMAINS = {"instagram.com", "facebook.com", "airbnb.com", "airbnb.mx", "tiktok.com"}
LOGIN_PATTERNS = [
    r"/module/application_authentication",
    r"/residentservices/",
    r"/userlogin",
    r"/schedule-a-tour",
    r"/apply$",
]


def classify(url: str) -> tuple[str, str]:
    """Return (bucket, detail)."""
    try:
        p = urlparse(url)
        netloc = p.netloc.lower().replace("www.", "")
        path = p.path.lower()
    except Exception:
        return "unparseable", url

    if any(netloc == d or netloc.endswith("." + d) for d in AGGREGATOR_DOMAINS):
        return "aggregator", netloc
    if any(netloc == d or netloc.endswith("." + d) for d in AD_DOMAINS):
        return "ad_redirect", netloc
    if any(netloc == d or netloc.endswith("." + d) for d in SOCIAL_DOMAINS):
        return "social", netloc
    for pat in LOGIN_PATTERNS:
        if re.search(pat, path):
            return "login_or_apply", f"{netloc}{path}"

    # Examine path pattern
    if re.search(r"/floor-?plans/?$", path):
        return "property_site_floorplans", netloc
    if "/availab" in path:
        return "property_site_availability", netloc
    if re.search(r"/apartments(?:/[^/]+)*/floorplans", path):
        return "property_site_floorplans", netloc
    if path in ("", "/"):
        return "property_site_root", netloc
    return "property_site_other", f"{netloc}{path[:40]}"


def main() -> None:
    by_url: dict[str, dict] = {}
    with open(JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            by_url[rec["url"]] = rec

    no_units = [r for r in by_url.values() if r["status"] == "no_units"]
    print(f"Total no_units records: {len(no_units)}\n")

    buckets = Counter()
    samples: dict[str, list[dict]] = {}
    detail_counts: dict[str, Counter] = {}

    for r in no_units:
        bucket, detail = classify(r["url"])
        buckets[bucket] += 1
        samples.setdefault(bucket, []).append(r)
        detail_counts.setdefault(bucket, Counter())
        detail_counts[bucket][detail] += 1

    print("=" * 70)
    print("Buckets (count):")
    for b, c in buckets.most_common():
        print(f"  {b:30s} {c:4d}")

    print("\n" + "=" * 70)
    print("Top domains per bucket (property_site_*):")
    for b in ["property_site_floorplans", "property_site_root",
              "property_site_availability", "property_site_other"]:
        if b not in detail_counts:
            continue
        print(f"\n[{b}]")
        for detail, cnt in detail_counts[b].most_common(15):
            print(f"  {cnt:3d}  {detail}")

    print("\n" + "=" * 70)
    print("Samples per bucket (max 10):")
    for b in buckets:
        print(f"\n[{b}] {len(samples[b])} rows")
        for r in samples[b][:10]:
            print(f"  {r['name'][:38]:38s}  {r['url'][:95]}")


if __name__ == "__main__":
    main()
