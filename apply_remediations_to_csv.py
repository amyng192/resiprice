#!/usr/bin/env python3
"""
apply_remediations_to_csv.py — Surgically patch qualifying_communities.csv
and qualifying_communities_failed.csv using the *latest* records in
pipeline_progress.jsonl.

By default this pulls all JSONL records whose notes start with the remediation
marker (currently "Remediated" or "AMLI remediation") and applies them. This
avoids clobbering hundreds of manually-curated rows — we never re-export the
whole CSV from the JSONL, only patch the rows we know we touched.

Invoke without args to auto-patch from the JSONL. Pass --names NAME1,NAME2
to limit to specific community names.
"""

import argparse
import csv
from pathlib import Path

from pipeline_atlanta import load_progress

ROOT = Path(__file__).parent
PROGRESS = ROOT / "pipeline_progress.jsonl"
QUALIFYING = ROOT / "qualifying_communities.csv"
FAILED = ROOT / "qualifying_communities_failed.csv"

REMEDIATION_NOTE_PREFIXES = ("Remediated", "AMLI remediation")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--names", default="", help="Comma-separated subset of names to patch")
    args = ap.parse_args()

    progress = load_progress(str(PROGRESS))

    name_filter = {n.strip() for n in args.names.split(",") if n.strip()} if args.names else None

    # Build {name: (url, platform)} from the latest remediation JSONL records
    remediated: dict[str, tuple[str, str]] = {}
    for rec in progress.values():
        notes = rec.get("notes", "") or ""
        if not any(notes.startswith(p) for p in REMEDIATION_NOTE_PREFIXES):
            continue
        name = rec.get("property_name")
        if name_filter and name not in name_filter:
            continue
        url = rec.get("availability_url") or rec.get("website_url", "")
        platform = rec.get("platform") or ""
        if name and url:
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
