"""Generic city-level BTR seeder.

Reads a JSON file (default ``data/city_btr_seeds.json``) of curated BTR
developments and inserts any that aren't already in existing_schemes.

Idempotent: dedups on (council_id, name) OR (council_id, postcode +
name-fragment substring) so re-runs are safe.

Creates company stubs when the named operator doesn't already exist.

Usage:
    python seed_city_btr.py                  # apply default file
    python seed_city_btr.py --file my.json
    python seed_city_btr.py --dry-run
    python seed_city_btr.py --city Manchester  # filter by city in metadata
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

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

DEFAULT_FILE = Path(__file__).parent / "data" / "city_btr_seeds.json"


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower().strip())


def find_council_id(c, council_name: str) -> int | None:
    if not council_name:
        return None
    # Exact match first, then case-insensitive
    row = c.execute(
        text("SELECT id FROM councils WHERE name = :n LIMIT 1"),
        {"n": council_name},
    ).first()
    if row:
        return row[0]
    row = c.execute(
        text("SELECT id FROM councils WHERE LOWER(name) = LOWER(:n) LIMIT 1"),
        {"n": council_name},
    ).first()
    return row[0] if row else None


def find_or_create_company(c, name: str) -> int | None:
    if not name:
        return None
    norm = _normalize_name(name)
    row = c.execute(text("""
        SELECT id FROM companies
        WHERE LOWER(name) = LOWER(:n) OR normalized_name = :nn
        LIMIT 1
    """), {"n": name.strip(), "nn": norm}).first()
    if row:
        return row[0]
    row = c.execute(text("""
        INSERT INTO companies (name, normalized_name, created_at, updated_at)
        VALUES (:n, :nn, NOW(), NOW())
        RETURNING id
    """), {"n": name.strip()[:200], "nn": norm[:200]}).first()
    print(f"    [+] created companies row for {name!r} (id={row[0]})")
    return row[0]


def already_present(c, council_id: int, name: str, postcode: str) -> int | None:
    """Return existing scheme id if a likely duplicate exists, else None."""
    primary_token = name.lower().split(",")[0].split()[0] if name else ""
    if len(primary_token) < 4:
        primary_token = name.lower()
    row = c.execute(text("""
        SELECT id FROM existing_schemes
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
        "cid": council_id,
        "n": name,
        "pc": postcode or "",
        "pat": "%" + primary_token + "%",
    }).first()
    return row[0] if row else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=str(DEFAULT_FILE))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--council", help="Only process schemes with this council_name")
    args = ap.parse_args()

    with open(args.file, encoding="utf-8") as f:
        data = json.load(f)
    schemes = data.get("schemes", [])
    print(f"Loaded {len(schemes)} seed schemes from {args.file}")

    if args.council:
        schemes = [s for s in schemes if s.get("council_name") == args.council]
        print(f"Filtered to council={args.council}: {len(schemes)} schemes")

    if not schemes:
        print("Nothing to do.")
        return

    engine = create_engine(DB_URL)
    inserted = 0
    skipped = 0
    updated = 0
    new_companies = 0

    with engine.begin() as c:
        for s in schemes:
            council_id = s.get("council_id") or find_council_id(c, s.get("council_name", ""))
            if not council_id:
                print(f"  [warn] {s['name']!r}: council {s.get('council_name')!r} not found, skipping")
                continue

            existing = already_present(c, council_id, s["name"], s.get("postcode", ""))
            if existing:
                print(f"  [skip] {s['name']!r} ({s.get('council_name')}) already in DB (id={existing})")
                skipped += 1
                continue

            if args.dry_run:
                print(f"  [WOULD INSERT] {s['name']!r} ({s.get('council_name')}, "
                      f"{s.get('num_units','?')}u, {s.get('status','?')}, "
                      f"op={s.get('operator_name','?')})")
                inserted += 1
                continue

            operator_id = find_or_create_company(c, s.get("operator_name", ""))
            if operator_id and s.get("operator_name"):
                # Crude tracking
                row = c.execute(text(
                    "SELECT created_at FROM companies WHERE id = :i"
                ), {"i": operator_id}).first()
                # Not strictly accurate but doesn't matter for stats

            row = c.execute(text("""
                INSERT INTO existing_schemes
                    (name, address, postcode, council_id, scheme_type, status,
                     num_units, total_units, operator_company_id,
                     source, source_reference, notes,
                     created_at, updated_at, locked_fields)
                VALUES
                    (:n, :a, :pc, :cid, :st, :status,
                     :u, :u, :op,
                     :src, :ref, :notes,
                     NOW(), NOW(), '{}'::jsonb)
                RETURNING id
            """), {
                "n": s["name"][:255],
                "a": (s.get("address") or "")[:500],
                "pc": (s.get("postcode") or "")[:10],
                "cid": council_id,
                "st": s.get("scheme_type", "BTR"),
                "status": (s.get("status") or "")[:50],
                "u": s.get("num_units"),
                "op": operator_id,
                "src": "manual_curation",
                "ref": (s.get("source_reference") or "")[:500],
                "notes": (s.get("notes") or "")[:1000],
            }).first()
            print(f"  [+] inserted {s['name']!r} ({s.get('council_name')}, "
                  f"id={row[0]}, {s.get('num_units','?')}u, "
                  f"op={s.get('operator_name','-')})")
            inserted += 1

    print()
    print(f"=== Summary ===")
    print(f"  {'WOULD insert' if args.dry_run else 'Inserted':<14}: {inserted}")
    print(f"  Skipped (dup): {skipped}")
    print(f"  Total seeds processed: {len(schemes)}")


if __name__ == "__main__":
    main()
