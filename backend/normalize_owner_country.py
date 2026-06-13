"""Normalise messy HMLR free-text country values in ultimate_owner_type.

The ownership walker copied HMLR's "Country Incorporated" field verbatim,
which is free text: typos ("United Kingdon"), abbreviations ("Eng",
"Gbr"), counties ("Cheshire"), and variants ("Republic Of Ireland").

This pass:
  * canonicalises real offshore jurisdictions (Jersey, Luxembourg, …)
  * reclassifies UK-equivalent strings back to domestic types
    (FUND -> PE / FUND / INSTITUTIONAL, ENTITY -> CORPORATE GROUP),
    matching the original classify_top logic.

Idempotent. Usage: python normalize_owner_country.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

# free-text -> canonical jurisdiction
CANON = {
    "united states": "United States", "united states of america": "United States",
    "usa": "United States", "delaware": "United States", "missouri": "United States",
    "virgin islands, british": "British Virgin Islands", "bvi": "British Virgin Islands",
    "republic of ireland": "Ireland", "dublin": "Ireland", "eire": "Ireland",
    "isle of man": "Isle of Man", "iom": "Isle of Man",
    "not specified/other": "Other / unspecified",
    "germany/pullach": "Germany",
}
# strings that are really UK -> reclassify to domestic
UK_EQUIV = {
    "e&w", "eng", "england", "wales", "scotland", "northern ireland",
    "gbr", "great britain", "united kingdom", "united kingdon", "uk",
    "cheshire", "gb",
}

_LABEL = re.compile(r"^OFFSHORE (ENTITY|FUND) \((.+)\)$")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    changes = Counter()
    with engine.begin() as c:
        rows = c.execute(text("""
            SELECT id, ultimate_owner_type FROM companies
            WHERE ultimate_owner_type LIKE 'OFFSHORE %'
        """)).fetchall()
        for cid, label in rows:
            m = _LABEL.match(label or "")
            if not m:
                continue
            kind, raw = m.group(1), m.group(2).strip()
            key = raw.lower()
            if key in UK_EQUIV:
                new = ("PE / FUND / INSTITUTIONAL" if kind == "FUND"
                       else "CORPORATE GROUP")
            else:
                canon = CANON.get(key, raw.title() if raw.islower() or raw.isupper() else raw)
                new = f"OFFSHORE {kind} ({canon})"
            if new != label:
                changes[f"{label}  ->  {new}"] += 1
                if not args.dry_run:
                    c.execute(text(
                        "UPDATE companies SET ultimate_owner_type=:n WHERE id=:i"
                    ), {"n": new[:60], "i": cid})

    print(f"=== {'DRY RUN' if args.dry_run else 'APPLIED'}: "
          f"{sum(changes.values())} rows across {len(changes)} mappings ===")
    for k, v in changes.most_common():
        print(f"  {v:>4}  {k}")


if __name__ == "__main__":
    main()
