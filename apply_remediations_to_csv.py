#!/usr/bin/env python3
"""
apply_remediations_to_csv.py — Surgically update qualifying_communities.csv
and qualifying_communities_failed.csv with the 11 remediated rows, without
clobbering the manual curation that already exists in the CSVs.

Source of truth for the remediated URLs: pipeline_progress.jsonl (last record
per key wins, so the entries appended by fix_failed_urls.py are authoritative
for these 11 properties).

Why this exists: pipeline_atlanta.export_csv() rebuilds the qualifying CSV
from the JSONL, but the JSONL has drifted from the curated CSV (many rows are
'no_pricing' in JSONL yet manually present in qualifying.csv). Overwriting the
CSV would wipe hundreds of manual entries.
"""

import csv
from pathlib import Path

from pipeline_atlanta import load_progress

ROOT = Path(__file__).parent
PROGRESS = ROOT / "pipeline_progress.jsonl"
QUALIFYING = ROOT / "qualifying_communities.csv"
FAILED = ROOT / "qualifying_communities_failed.csv"

REMEDIATED_KEYS = [
    ("1105 Town Brookhaven", "1105 Town Blvd"),
    ("Avana Cheshire Bridge", "2124 Cheshire Bridge Road NE"),
    ("Avana City North", "3421 Northlake Pkwy NE"),
    ("Avana Cityview", "1650 Barnes Mill Road"),
    ("Avana on Main", "508 Main Street NE"),
    ("Avana Portico", "2110 Preston Park Drive"),
    ("Avana Twenty9", "2334 Fuller Way"),
    ("Avana Acworth", "4710 Baker Grove Road NW"),
    ("Avana Dunwoody", "10 Gentrys Walk"),
    ("Avana Kennesaw", "3840 Jiles Road"),
    ("Avana TownPark", "3725 George Busbee Pkwy NW"),
]


def main() -> None:
    progress = load_progress(str(PROGRESS))

    # Build {name: (url, platform)} from the latest JSONL records
    remediated: dict[str, tuple[str, str]] = {}
    for name, addr in REMEDIATED_KEYS:
        key = f"{name}|{addr}"
        rec = progress.get(key)
        if not rec:
            print(f"WARN: {key} not in progress")
            continue
        url = rec.get("availability_url") or rec.get("website_url", "")
        platform = rec.get("platform") or ""
        remediated[name] = (url, platform)

    # --- Update qualifying_communities.csv -----------------------------------
    with open(QUALIFYING, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0]
    data = rows[1:]

    existing_names = {r[0]: i for i, r in enumerate(data) if r}
    added, updated = [], []
    for name, (url, platform) in remediated.items():
        new_row = [name, url, platform]
        if name in existing_names:
            idx = existing_names[name]
            if data[idx] != new_row:
                data[idx] = new_row
                updated.append(name)
        else:
            data.append(new_row)
            added.append(name)

    # Keep alphabetical order (same as pipeline_atlanta.export_csv)
    data.sort(key=lambda r: r[0] if r else "")

    with open(QUALIFYING, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

    print(f"qualifying_communities.csv: +{len(added)} added, {len(updated)} updated")
    if added:
        for n in added:
            print(f"  + {n}")
    if updated:
        for n in updated:
            print(f"  ~ {n}")

    # --- Remove remediated rows from qualifying_communities_failed.csv -------
    with open(FAILED, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        frows = list(reader)
    fheader = frows[0]
    fdata = frows[1:]

    remediated_names = set(remediated.keys())
    before = len(fdata)
    fdata = [r for r in fdata if not (r and r[0] in remediated_names)]
    removed = before - len(fdata)

    with open(FAILED, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fheader)
        writer.writerows(fdata)

    print(f"qualifying_communities_failed.csv: -{removed} removed")


if __name__ == "__main__":
    main()
