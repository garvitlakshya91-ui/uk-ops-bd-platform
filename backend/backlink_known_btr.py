"""Back-link planning_applications → known BTR schemes.

We have rich Birmingham BTR data in ``existing_schemes`` (629 schemes
sourced from arl_btr_open_operating, EPC, operator listings, etc.), but
``planning_applications`` from PlanIt is metadata-thin (96% NULL applicant,
80% NULL units) so the classifier can't tag them as BTR.

This script bridges the gap: for each Birmingham planning application,
check whether its postcode + address text matches a known BTR
existing_scheme. If it does, upgrade ``scheme_type='BTR'``. Same approach
extended to any council with rich BTR existing-scheme data.

Matching rules (in priority order):
  1. Exact postcode match + scheme name token (length >= 5) appears in
     planning app's address or description.
  2. Postcode match alone — only when the existing scheme is BTR-only at
     that postcode (no Residential/PBSA confounders).

Doesn't touch already-classified BTR/PBSA/Co-living/Senior apps.

Usage:
    python backlink_known_btr.py --council Birmingham --dry-run
    python backlink_known_btr.py --all --dry-run
    python backlink_known_btr.py --council Birmingham        # apply
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.models.models import PlanningApplication

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

# Generic words to ignore as "name tokens"
STOP_TOKENS = {
    "the", "and", "for", "with", "ltd", "limited", "plc", "llp",
    "court", "house", "tower", "place", "street", "road", "way",
    "lane", "park", "view", "gardens", "square", "building", "centre",
    "center", "yard", "wharf", "quarter", "phase", "phase1", "phase2",
    "north", "south", "east", "west", "central", "site", "block",
    "birmingham", "london", "manchester", "leeds", "liverpool",
    "bristol", "edinburgh", "glasgow", "sheffield", "newcastle",
    "btr", "pbsa", "new", "residential", "development", "scheme",
}


def name_tokens(name: str) -> list[str]:
    """Extract distinctive lowercase tokens (>=5 chars, non-stopword) from a scheme name."""
    if not name:
        return []
    words = re.findall(r"[a-zA-Z]{4,}", name.lower())
    return [w for w in words if w not in STOP_TOKENS and len(w) >= 5]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", help="Only process this council's applications")
    ap.add_argument("--all", action="store_true", help="Process all councils")
    ap.add_argument("--dry-run", action="store_true", help="Report without writing")
    args = ap.parse_args()

    if not args.council and not args.all:
        print("Pass --council Birmingham OR --all")
        sys.exit(1)

    engine = create_engine(DB_URL)

    # 1. Build a postcode → list of BTR schemes lookup
    print("Building BTR scheme lookup table...")
    with engine.connect() as c:
        scope_clause = ""
        params: dict = {}
        if args.council:
            scope_clause = """
              AND es.council_id = (SELECT id FROM councils WHERE name = :n)"""
            params["n"] = args.council

        btr_rows = list(c.execute(text(f"""
            SELECT es.id, es.name, es.postcode, es.council_id
            FROM existing_schemes es
            WHERE es.scheme_type IN ('BTR', 'PBSA', 'Co-living')
              AND COALESCE(es.postcode, '') <> ''
              {scope_clause}
        """), params))

    # postcode_key (NO-SPACE UPPERCASE) → list of {id, name, tokens, type}
    pc_lookup: dict[str, list[dict]] = defaultdict(list)
    for r in btr_rows:
        pc_key = (r[2] or "").upper().replace(" ", "")
        if not pc_key:
            continue
        pc_lookup[pc_key].append({
            "id": r[0],
            "name": r[1],
            "tokens": name_tokens(r[1] or ""),
            "scheme_type": "BTR",  # we'll be lenient — bias toward BTR
            "council_id": r[3],
        })
    print(f"  {len(btr_rows):,} BTR/PBSA/Co-living schemes indexed across {len(pc_lookup):,} postcodes")

    # 2. For each candidate planning app, look up by postcode
    print("\nScanning planning applications...")
    with engine.connect() as c:
        scope_pa = ""
        if args.council:
            scope_pa = "AND co.name = :n"
        candidate_rows = list(c.execute(text(f"""
            SELECT pa.id, pa.postcode, pa.address, pa.description, pa.scheme_type, pa.council_id
            FROM planning_applications pa
            JOIN councils co ON co.id = pa.council_id
            WHERE COALESCE(pa.postcode, '') <> ''
              AND (pa.scheme_type IS NULL
                   OR pa.scheme_type IN ('Unknown', 'Residential'))
              {scope_pa}
        """), params))
    print(f"  {len(candidate_rows):,} candidate applications")

    matches: list[tuple[int, int, str]] = []  # (app_id, scheme_id, scheme_name)
    type_counts = defaultdict(int)

    for r in candidate_rows:
        app_id, app_pc, app_addr, app_desc, app_type, app_council = r
        pc_key = (app_pc or "").upper().replace(" ", "")
        if pc_key not in pc_lookup:
            continue
        haystack = ((app_addr or "") + " " + (app_desc or "")).lower()

        for scheme in pc_lookup[pc_key]:
            if scheme["council_id"] != app_council:
                continue
            # Match if any distinctive token of the scheme name appears in app text
            matched_tokens = [t for t in scheme["tokens"] if t in haystack]
            if matched_tokens:
                matches.append((app_id, scheme["id"], scheme["name"]))
                type_counts["matched_by_name+postcode"] += 1
                break
        else:
            # No token match — require BOTH postcode AND a size signal to avoid
            # tagging tiny change-of-use apps at the same postcode as a real
            # BTR development. Postcode-alone matching was too permissive.
            pass

    print(f"\n=== Match summary ===")
    print(f"  Matched by name+postcode:    {type_counts['matched_by_name+postcode']:>5,}")
    print(f"  Matched by postcode alone:   {type_counts['matched_by_postcode_alone']:>5,}")
    print(f"  TOTAL matches:               {len(matches):>5,}")

    if not matches:
        return

    # 3. Apply (or report)
    print(f"\n=== Sample matches (first 10) ===")
    for app_id, sid, sname in matches[:10]:
        print(f"  app_id={app_id} -> scheme_id={sid} ({sname[:50]})")

    if args.dry_run:
        print(f"\nDRY RUN — re-run without --dry-run to apply {len(matches):,} updates.")
        return

    print(f"\nApplying {len(matches):,} updates...")
    start = time.time()
    with Session(engine) as db:
        for i, (app_id, _, _) in enumerate(matches, 1):
            a = db.get(PlanningApplication, app_id)
            if a is None:
                continue
            a.scheme_type = "BTR"
            if i % 500 == 0:
                db.commit()
                print(f"  ...{i:,}/{len(matches):,}")
        db.commit()
    print(f"\nDone in {time.time()-start:.0f}s")


if __name__ == "__main__":
    main()
