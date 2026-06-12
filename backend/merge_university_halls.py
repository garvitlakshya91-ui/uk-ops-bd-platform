"""Merge university-halls scrape into existing_schemes.

University-owned halls are operating PBSA stock (operator = the
university). Partner halls carry their private operator. Both are
high-value in secondary cities where directories list nothing.

Skips room-grade category rows ("Classic Halls") and nav junk.

Usage:
    python merge_university_halls.py --dry-run
    python merge_university_halls.py
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from merge_afs_schemes import (
    CITY_COUNCIL, DB_URL, find_council_id, find_or_create_company,
    find_existing,
)

HALLS_DIR = Path(__file__).parent / "data" / "university_halls"

# extra mapping — agent also scraped Middlesbrough
CITY_COUNCIL = dict(CITY_COUNCIL)

# Room-grade categories and nav junk masquerading as hall names —
# e.g. "Ensuite Plus Halls", "Catered Accommodation", "Postgraduate
# Halls - City Campus". Real halls have proper names.
_GRADE_WORDS = (
    r"(?:traditional|classic|standard|premium|deluxe|superior|economy|"
    r"en.?suite|ensuite|catered|self.?catered|postgraduate|undergraduate|"
    r"shared|studio|plus)"
)
_SKIP_NAME_PAT = re.compile(
    rf"^(index|accommodation|home|halls?)$|"
    rf"^(?:{_GRADE_WORDS}\s*)+(halls?|accommodation|rooms?)\b", re.I,
)

_PC_SUFFIX_PAT = re.compile(r",\s*[A-Z]{1,2}\d[0-9A-Z]?\s*\d[A-Z]{2}\s*$")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    inserted = skipped = filtered = op_filled = 0

    with engine.begin() as c:
        for path in sorted(HALLS_DIR.glob("*.jsonl")):
            city = path.stem
            council = CITY_COUNCIL.get(city)
            if not council:
                print(f"[warn] no council mapping for {city}")
                continue
            council_id = find_council_id(c, council)
            if not council_id:
                print(f"[warn] council {council!r} not in DB")
                continue
            rows = [json.loads(l) for l in open(path, encoding="utf-8")]
            print(f"\n--- {city} ({council}): {len(rows)} halls ---")
            for r in rows:
                name = (r.get("hall_name") or "").strip()
                name = _PC_SUFFIX_PAT.sub("", name)  # "Eldon Street, YO31 7NE" -> "Eldon Street"
                if not name or _SKIP_NAME_PAT.match(name):
                    filtered += 1
                    continue
                postcode = (r.get("postcode") or "").strip()
                operator = (r.get("operator") or "").strip() or r.get("university", "")
                existing = find_existing(c, council_id, name, postcode)
                if existing:
                    ex_id, ex_op, locked = existing
                    locked = locked if isinstance(locked, dict) else {}
                    if operator and not ex_op and "operator_company_id" not in locked:
                        if not args.dry_run:
                            op_id, _ = find_or_create_company(c, operator, False)
                            c.execute(text("""
                                UPDATE existing_schemes
                                SET operator_company_id = :op, updated_at = NOW()
                                WHERE id = :i
                            """), {"op": op_id, "i": ex_id})
                        print(f"  [op-fill] #{ex_id} {name!r} -> {operator!r}")
                        op_filled += 1
                    else:
                        skipped += 1
                    continue

                if args.dry_run:
                    print(f"  [WOULD INSERT] {name!r} ({postcode or 'no pc'}, "
                          f"op={operator}, {r.get('ownership')})")
                    inserted += 1
                    continue

                op_id, _ = find_or_create_company(c, operator, False)
                row = c.execute(text("""
                    INSERT INTO existing_schemes
                        (name, address, postcode, council_id, scheme_type, status,
                         operator_company_id, source, source_reference, notes,
                         created_at, updated_at, locked_fields)
                    VALUES
                        (:n, :a, :pc, :cid, 'PBSA', 'Operational',
                         :op, 'university_halls', :ref, :notes,
                         NOW(), NOW(), '{}'::jsonb)
                    RETURNING id
                """), {
                    "n": name[:255],
                    "a": (r.get("address") or "")[:500],
                    "pc": postcode[:10],
                    "cid": council_id,
                    "op": op_id,
                    "ref": (r.get("source_url") or "")[:500],
                    "notes": (f"{r.get('ownership','unknown')} accommodation of "
                              f"{r.get('university','?')}. {r.get('notes','')}")[:1000],
                }).first()
                print(f"  [+] #{row[0]} {name!r} (op={operator})")
                inserted += 1

    print(f"\n=== Summary ===")
    print(f"  {'WOULD insert' if args.dry_run else 'Inserted':<14}: {inserted}")
    print(f"  Operator fills: {op_filled}")
    print(f"  Skipped (dup) : {skipped}")
    print(f"  Filtered junk : {filtered}")


if __name__ == "__main__":
    main()
