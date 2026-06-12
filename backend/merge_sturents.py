"""Merge StuRents PBSA candidates into existing_schemes.

Only rows flagged is_pbsa_candidate (block-scale stock) are merged —
the bulk of StuRents is HMO-scale landlord listings, kept on disk for
the later rents/landlord work.

Usage:
    python merge_sturents.py --dry-run
    python merge_sturents.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from merge_afs_schemes import (
    CITY_COUNCIL, DB_URL, find_council_id, find_or_create_company,
    find_existing,
)

DIR = Path(__file__).parent / "data" / "sturents"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    stats = Counter()

    with engine.begin() as c:
        for path in sorted(DIR.glob("*.jsonl")):
            city = path.stem
            council = CITY_COUNCIL.get(city)
            if not council:
                print(f"[warn] no council mapping for {city}")
                continue
            council_id = find_council_id(c, council)
            if not council_id:
                print(f"[warn] council {council!r} not in DB")
                continue
            cands = [json.loads(l) for l in open(path, encoding="utf-8")]
            cands = [r for r in cands if r.get("is_pbsa_candidate")]
            if not cands:
                continue
            print(f"--- {city}: {len(cands)} PBSA candidates ---")
            for r in cands:
                name = (r.get("name") or "").strip()
                if not name:
                    stats["no_name"] += 1
                    continue
                postcode = (r.get("postcode") or "").strip()
                operator = (r.get("agent") or "").strip()
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
                        stats["op_filled"] += 1
                    else:
                        stats["dup"] += 1
                    continue

                stats["inserted"] += 1
                if args.dry_run:
                    print(f"  [WOULD INSERT] {name!r} ({postcode or 'no pc'}, "
                          f"{r.get('beds','?')} beds, op={operator or '-'})")
                    continue
                op_id, _ = find_or_create_company(c, operator, False) if operator else (None, False)
                c.execute(text("""
                    INSERT INTO existing_schemes
                        (name, address, postcode, council_id, scheme_type, status,
                         num_units, total_units, operator_company_id,
                         source, source_reference, notes,
                         created_at, updated_at, locked_fields)
                    VALUES
                        (:n, :a, :pc, :cid, 'PBSA', 'Operational',
                         :u, :u, :op,
                         'sturents', :ref, :notes,
                         NOW(), NOW(), '{}'::jsonb)
                """), {
                    "n": name[:255],
                    "a": (r.get("street_address") or "")[:500],
                    "pc": postcode[:10],
                    "cid": council_id,
                    "u": r.get("beds"),
                    "op": op_id,
                    "ref": (r.get("url") or "")[:500],
                    "notes": f"StuRents block listing, agent: {operator or '?'}.",
                })

    print(f"\n=== Summary ({'DRY RUN' if args.dry_run else 'APPLIED'}) ===")
    for k in ("inserted", "op_filled", "dup", "no_name"):
        print(f"  {k:<12}: {stats[k]}")


if __name__ == "__main__":
    main()
