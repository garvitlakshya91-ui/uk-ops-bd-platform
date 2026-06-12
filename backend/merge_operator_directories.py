"""Merge operator-website property directories into existing_schemes.

Input: data/operator_directories/<brand>.jsonl from
run_operator_directory_scrape.py — every property is operator-branded
PBSA by definition, national coverage (67 cities).

Council resolution: postcode → admin_district via postcodes.io bulk
API (free), falling back to city-name match. Unresolvable records are
reported, not guessed.

Usage:
    python merge_operator_directories.py --dry-run
    python merge_operator_directories.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

from merge_afs_schemes import DB_URL, find_council_id, find_or_create_company, find_existing

DIR = Path(__file__).parent / "data" / "operator_directories"

# admin_district / city aliases → council names as stored in our councils table
DISTRICT_ALIASES = {
    "Bristol, City of": "Bristol",
    "Kingston upon Hull, City of": "Kingston upon Hull",
    "Herefordshire, County of": "Herefordshire",
    "City of Edinburgh": "City of Edinburgh",
    "Newcastle upon Tyne": "Newcastle",
    "Glasgow": "Glasgow City",
    "Edinburgh": "City of Edinburgh",
    "Kingston": "Kingston upon Thames",
    "Kingston Upon Thames": "Kingston upon Thames",
}


def bulk_postcode_lookup(postcodes: list[str]) -> dict[str, str]:
    """postcode (normalised, no space, upper) -> admin_district."""
    out: dict[str, str] = {}
    client = httpx.Client(timeout=30)
    uniq = sorted({p.upper().replace(" ", "") for p in postcodes if p})
    for i in range(0, len(uniq), 100):
        batch = uniq[i:i + 100]
        try:
            r = client.post("https://api.postcodes.io/postcodes",
                            json={"postcodes": batch})
            r.raise_for_status()
        except Exception as e:
            print(f"  [warn] postcodes.io batch failed: {e}")
            continue
        for item in r.json().get("result", []):
            res = item.get("result")
            if res and res.get("admin_district"):
                out[item["query"].upper().replace(" ", "")] = res["admin_district"]
        time.sleep(0.3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    records = []
    for path in sorted(DIR.glob("*.jsonl")):
        for line in open(path, encoding="utf-8"):
            r = json.loads(line)
            r["_brand"] = path.stem
            records.append(r)
    print(f"Loaded {len(records)} properties from "
          f"{len(set(r['_brand'] for r in records))} brands")

    print("Resolving councils via postcodes.io ...")
    pc_map = bulk_postcode_lookup([r.get("postcode", "") for r in records])
    print(f"  {len(pc_map)} postcodes resolved")

    engine = create_engine(DB_URL)
    stats = Counter()
    unresolved: list[str] = []
    council_cache: dict[str, int | None] = {}

    with engine.begin() as c:
        def council_for(district: str) -> int | None:
            district = DISTRICT_ALIASES.get(district, district)
            if district not in council_cache:
                council_cache[district] = find_council_id(c, district)
            return council_cache[district]

        for r in records:
            name = (r.get("name") or "").strip()
            if not name:
                stats["no_name"] += 1
                continue
            pc = (r.get("postcode") or "").upper().replace(" ", "")
            district = pc_map.get(pc, "")
            council_id = council_for(district) if district else None
            if not council_id and r.get("city"):
                council_id = council_for(r["city"].strip())
            if not council_id:
                stats["unresolved_council"] += 1
                unresolved.append(f"{r['_brand']}: {name} "
                                  f"({r.get('city','?')}, {r.get('postcode','no pc')})")
                continue

            operator = r.get("operator") or r["_brand"].replace("_", " ").title()
            postcode = (r.get("postcode") or "").strip()
            existing = find_existing(c, council_id, name, postcode)
            if not existing and not name.lower().startswith(operator.lower().split()[0]):
                # operator sites use bare names ("Bruce Street") while
                # directories prefix the brand — try the branded form too
                existing = find_existing(c, council_id, f"{operator} {name}", postcode)
            if existing:
                ex_id, ex_op, locked = existing
                locked = locked if isinstance(locked, dict) else {}
                if not ex_op and "operator_company_id" not in locked:
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
                continue
            op_id, _ = find_or_create_company(c, operator, False)
            c.execute(text("""
                INSERT INTO existing_schemes
                    (name, address, postcode, council_id, scheme_type, status,
                     operator_company_id, source, source_reference, notes,
                     created_at, updated_at, locked_fields)
                VALUES
                    (:n, :a, :pc, :cid, 'PBSA', 'Operational',
                     :op, 'operator_directory', :ref, :notes,
                     NOW(), NOW(), '{}'::jsonb)
            """), {
                "n": name[:255],
                "a": (r.get("address") or "")[:500],
                "pc": postcode[:10],
                "cid": council_id,
                "op": op_id,
                "ref": (r.get("url") or "")[:500],
                "notes": f"From {operator} property directory.",
            })

    print(f"\n=== Summary ({'DRY RUN' if args.dry_run else 'APPLIED'}) ===")
    for k in ("inserted", "op_filled", "dup", "unresolved_council", "no_name"):
        print(f"  {k:<20}: {stats[k]}")
    if unresolved:
        print(f"\nUnresolved ({len(unresolved)}), first 15:")
        for u in unresolved[:15]:
            print(f"  {u}")


if __name__ == "__main__":
    main()
