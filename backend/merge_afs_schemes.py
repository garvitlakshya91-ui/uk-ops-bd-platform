"""Merge AFS-discovered PBSA properties into existing_schemes.

Reads data/afs/<city>.jsonl produced by run_afs_scrape.py, groups
listings into scheme candidates (one per name+postcode), then:

  1. INSERTs schemes not already in the DB (matched by name OR
     postcode+name-token, same rules as seed_city_btr.py)
  2. For schemes that DO already exist with no operator recorded,
     fills operator_company_id from the AFS accommodationProvider
     (unless that field is locked).

Sets scheme_type='PBSA', status='Operational', source='afs_directory'.
Rent ppw is converted to avg_rent_pcm (x52/12). Coordinates stored.

Usage:
    python merge_afs_schemes.py --dry-run
    python merge_afs_schemes.py                 # apply
    python merge_afs_schemes.py --city canterbury --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

AFS_DIR = Path(__file__).parent / "data" / "afs"

CITY_COUNCIL = {
    "leeds": "Leeds",
    "cardiff": "Cardiff",
    "exeter": "Exeter",
    "southampton": "Southampton",
    "middlesbrough": "Middlesbrough",
    "colchester": "Colchester",
    "canterbury": "Canterbury",
    "lincoln": "Lincoln",
    "chester": "Cheshire West and Chester",
    "worcester": "Worcester",
    "winchester": "Winchester",
    "lancaster": "Lancaster",
    "durham": "Durham",
    "bangor": "Gwynedd",
    "aberystwyth": "Ceredigion",
    "york": "York",
}

# Only these become scheme rows. Standard adverts in the flats category
# are individual lets, not blocks.
def is_scheme_candidate(rec: dict) -> bool:
    if (rec.get("advert_type") or "").lower() == "pbsa":
        return True
    if (rec.get("property_type") or "").lower() == "halls":
        return True
    # studio blocks listed by a brand operator
    if rec.get("category") == "studios" and rec.get("operator"):
        return True
    return False


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


# Studio-category listings often carry ROOM-TYPE names ("Classic Studio",
# "Elite Studio", "Premium Studio Accessible") — these are units inside a
# block, not separate schemes. Detect and collapse them onto the real
# block at the same postcode.
_ROOMTYPE_PAT = re.compile(
    r"\b(studio|studios|en.?suite|room|bedroom|apartment|flat|penthouse|"
    r"twin|dual occupancy|accessible)\b", re.I,
)


def is_roomtype_name(name: str) -> bool:
    return bool(_ROOMTYPE_PAT.search(name or ""))


def find_council_id(c, name: str):
    row = c.execute(
        text("SELECT id FROM councils WHERE LOWER(name) = LOWER(:n) LIMIT 1"),
        {"n": name},
    ).first()
    return row[0] if row else None


def find_or_create_company(c, name: str, dry: bool):
    if not name:
        return None, False
    norm = _norm(name)
    row = c.execute(text("""
        SELECT id FROM companies
        WHERE LOWER(name) = LOWER(:n) OR normalized_name = :nn
        LIMIT 1
    """), {"n": name.strip(), "nn": norm}).first()
    if row:
        return row[0], False
    if dry:
        return None, True
    row = c.execute(text("""
        INSERT INTO companies (name, normalized_name, created_at, updated_at)
        VALUES (:n, :nn, NOW(), NOW())
        RETURNING id
    """), {"n": name.strip()[:200], "nn": norm[:200]}).first()
    return row[0], True


def find_existing(c, council_id: int, name: str, postcode: str):
    primary_token = name.lower().split(",")[0].split()[0] if name else ""
    if len(primary_token) < 4:
        primary_token = name.lower()
    row = c.execute(text("""
        SELECT id, operator_company_id, COALESCE(locked_fields, '{}'::jsonb)
        FROM existing_schemes
        WHERE council_id = :cid
          AND (
            LOWER(TRIM(name)) = LOWER(TRIM(:n))
            OR (
                REPLACE(UPPER(COALESCE(postcode,'')),' ','') =
                REPLACE(UPPER(:pc),' ','')
                AND LOWER(name) ILIKE :pat
            )
          )
        LIMIT 1
    """), {
        "cid": council_id, "n": name, "pc": postcode or "",
        "pat": "%" + primary_token + "%",
    }).first()
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="Single city slug")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cities = [args.city] if args.city else list(CITY_COUNCIL)

    # ---- load + group listings into scheme candidates ----
    candidates: dict[tuple, dict] = {}
    for city in cities:
        path = AFS_DIR / f"{city}.jsonl"
        if not path.exists():
            continue
        for line in open(path, encoding="utf-8"):
            rec = json.loads(line)
            if not is_scheme_candidate(rec):
                continue
            key = (city, _norm(rec.get("name", "")),
                   (rec.get("postcode") or "").upper().replace(" ", ""))
            cur = candidates.get(key)
            if cur is None:
                candidates[key] = rec
            else:
                # keep the record with more data (prefer halls > studios)
                if rec.get("property_type") == "halls" and cur.get("property_type") != "halls":
                    candidates[key] = rec

    # ---- collapse room-type listings onto real blocks at same postcode ----
    by_pc = defaultdict(list)
    for (city, _, pc), rec in candidates.items():
        by_pc[(city, pc)].append(rec)

    final: list[dict] = []
    dropped_roomtypes = 0
    for (city, pc), recs in by_pc.items():
        real = [r for r in recs if not is_roomtype_name(r.get("name", ""))]
        roomtypes = [r for r in recs if is_roomtype_name(r.get("name", ""))]
        if real:
            final.extend(real)
            dropped_roomtypes += len(roomtypes)
        elif roomtypes:
            # No real-named sibling — keep ONE, renamed from its street
            # address if available so we don't insert "Premium Studio".
            keep = roomtypes[0]
            addr_name = (keep.get("address") or "").split(",")[0].strip()
            if addr_name and not is_roomtype_name(addr_name):
                keep = dict(keep)
                keep["name"] = addr_name
            final.append(keep)
            dropped_roomtypes += len(roomtypes) - 1

    by_city = defaultdict(list)
    for rec in final:
        by_city[rec["city_slug"]].append(rec)
    print(f"Collapsed {dropped_roomtypes} room-type listings into their blocks")

    engine = create_engine(DB_URL)
    inserted = skipped = op_filled = new_companies = 0

    with engine.begin() as c:
        for city, recs in sorted(by_city.items()):
            council = CITY_COUNCIL[city]
            council_id = find_council_id(c, council)
            if not council_id:
                print(f"[warn] council {council!r} not found, skipping {city}")
                continue
            print(f"\n--- {city} ({council}, council_id={council_id}): "
                  f"{len(recs)} scheme candidates ---")
            for rec in sorted(recs, key=lambda r: r.get("name", "")):
                name = (rec.get("name") or "").strip()
                if not name:
                    continue
                postcode = (rec.get("postcode") or "").strip()
                operator = (rec.get("operator") or "").strip()
                existing = find_existing(c, council_id, name, postcode)

                if existing:
                    ex_id, ex_op, locked = existing
                    locked = locked if isinstance(locked, dict) else {}
                    if operator and not ex_op and "operator_company_id" not in locked:
                        if args.dry_run:
                            print(f"  [WOULD FILL OP] #{ex_id} {name!r} -> {operator!r}")
                        else:
                            op_id, created = find_or_create_company(c, operator, args.dry_run)
                            new_companies += int(created)
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

                rent_pcm = None
                if rec.get("rent_ppw"):
                    rent_pcm = round(float(rec["rent_ppw"]) * 52 / 12, 2)

                if args.dry_run:
                    print(f"  [WOULD INSERT] {name!r} ({postcode}, op={operator or '-'}"
                          f", £{rec.get('rent_ppw') or '?'}/wk)")
                    inserted += 1
                    continue

                op_id, created = find_or_create_company(c, operator, args.dry_run)
                new_companies += int(created)
                row = c.execute(text("""
                    INSERT INTO existing_schemes
                        (name, address, postcode, council_id, scheme_type, status,
                         operator_company_id, latitude, longitude, avg_rent_pcm,
                         source, source_reference, notes,
                         created_at, updated_at, locked_fields)
                    VALUES
                        (:n, :a, :pc, :cid, 'PBSA', 'Operational',
                         :op, :lat, :lng, :rent,
                         'afs_directory', :ref, :notes,
                         NOW(), NOW(), '{}'::jsonb)
                    RETURNING id
                """), {
                    "n": name[:255],
                    "a": (rec.get("address") or "")[:500],
                    "pc": postcode[:10],
                    "cid": council_id,
                    "op": op_id,
                    "lat": rec.get("latitude"),
                    "lng": rec.get("longitude"),
                    "rent": rent_pcm,
                    "ref": (rec.get("url") or "")[:500],
                    "notes": f"Discovered via AFS directory ({rec.get('category')}); "
                             f"advert_type={rec.get('advert_type')}; "
                             f"academic_year={rec.get('raw', {}).get('academicYearLabel')}"[:1000],
                }).first()
                print(f"  [+] #{row[0]} {name!r} ({postcode}, op={operator or '-'})")
                inserted += 1

    print(f"\n=== Summary ===")
    print(f"  {'WOULD insert' if args.dry_run else 'Inserted':<14}: {inserted}")
    print(f"  Operator fills: {op_filled}")
    print(f"  Skipped (dup) : {skipped}")
    print(f"  New companies : {new_companies}")


if __name__ == "__main__":
    main()
