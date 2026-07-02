"""Credit-free operator backfill (rules-based).

Fills ``existing_schemes.operator_company_id`` for BD schemes using
deterministic rules — no AI:

  Rule 1  SELF-MANAGED: owner is a housing association / council /
          registered society  ->  operator := owner.
          (HAs and councils manage their own stock.)
  Rule 2  SELF-OPERATED BRAND: owner name matches a known operator
          brand (Unite, Grainger, ...)  ->  operator := owner.
  Rule 3  BRAND IN SCHEME NAME: scheme name starts with / contains a
          known brand -> link to that brand's existing company row.
  Rule 4  POSTCODE NEIGHBOUR: another scheme at the same postcode has
          an operator (same building/phase) -> propagate it.

Provenance appended to notes per rule. Idempotent. Never overwrites.

Usage:
    python backfill_operator_rules.py --dry-run
    python backfill_operator_rules.py
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

BD = "('BTR','PBSA','Co-living','Senior')"

# ---- Rule 1: self-managed social/council landlords -----------------------
HA_PATTERNS = [
    "housing", "council", "borough", "mayor and burgesses", "city of",
    "peabody", "notting hill genesis", "notting hill home",
    "sovereign network", "home group", "anchor hanover", "abri group",
    "stonewater", "settle group", "torus", "guinness partnership",
    "places for people", "riverside group", "a2dominion", "livewest",
    "moat homes", "paragon asra", "orbit ", "midland heart", "vivid",
    "hyde ", "swan ", "bromford", "wandle", "octavia", "one manchester",
    "salix homes", "onward homes", "yorkshire housing", "karbon homes",
    "believe housing", "thirteen group", "flagship", "futures housing",
]
# Never self-manage: investors/banks/housebuilders that may own stock
EXCLUDE_PATTERNS = [
    "bank", "capital", "invest", "fund", "reit", "pension",
    "barratt", "persimmon", "bellway", "taylor wimpey", "redrow",
    "berkeley", "crest nicholson", "vistry", "countryside", "cala ",
    "miller homes", "david wilson", "charles church", "keepmoat",
    "avant homes", "story homes", "bloor", "gleeson", "jelson",
    "developments", "construction", "contractors",
]

# ---- Rules 2 & 3: operator brands ----------------------------------------
# brand token -> canonical operator company name (must exist or be created)
BRANDS = {
    "unite students": "UNITE STUDENTS LIMITED",
    "unite group": "UNITE STUDENTS LIMITED",
    "iq student": "iQ Student Accommodation",
    "vita student": "Vita Student",
    "chapter ": "Chapter Living",
    "moda living": "Moda Living",
    "moda,": "Moda Living",
    "grainger": "GRAINGER REAL ESTATE LIMITED",
    "get living": "Get Living",
    "quintain": "Quintain Living",
    "essential living": "Essential Living",
    "urbanbubble": "urbanbubble",
    "fresh property": "Fresh Property Group",
    "fresh student": "Fresh Property Group",
    "homes for students": "Homes for Students",
    "student roost": "Student Roost",
    "yugo": "Yugo",
    "collegiate": "Collegiate AC",
    "mezzino": "Mezzino",
    "prestige student": "Prestige Student Living",
    "hello student": "Hello Student",
    "downing students": "Downing Students",
    "study inn": "Study Inn",
    "abodus": "Abodus Students",
    "host student": "Host",
    "crm students": "CRM Students",
    "scape": "Scape Living",
    "urbanest": "Urbanest",
    "nido": "Nido Living",
    "true student": "True Student",
    "dandara living": "Dandara Living",
    "simple life": "Simple Life",
    "l&g ": "Legal & General Affordable Homes",
    "legal & general": "Legal & General Affordable Homes",
    "way of life": "Way of Life",
    "greystar": "Greystar",
    "apo ": "Apo",
    "uncle ": "UNCLE",
    "folio london": "Folio London",
    "fizzy living": "Fizzy Living",
}


def matches_any(name: str, patterns) -> bool:
    low = f" {name.lower()} "
    return any(p in low for p in patterns)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    engine = create_engine(DB_URL)
    stats = Counter()

    with engine.begin() as c:
        # ---------------- Rule 1 + 2: operator := owner --------------------
        rows = c.execute(text(f"""
            SELECT es.id, co.id, co.name, co.ultimate_owner_type
            FROM existing_schemes es
            JOIN companies co ON co.id = es.owner_company_id
            WHERE es.scheme_type IN {BD} AND es.operator_company_id IS NULL
        """)).fetchall()
        print(f"{len(rows):,} operator-less BD schemes with an owner")

        for sid, oid, oname, uot in rows:
            if matches_any(oname, EXCLUDE_PATTERNS):
                stats["excluded_owner"] += 1
                continue
            rule = None
            if (matches_any(oname, HA_PATTERNS)
                    or "SOCIETY" in (uot or "")):
                rule = "self-managed (HA/council)"
                stats["rule1_self_managed"] += 1
            elif matches_any(oname, BRANDS.keys()):
                rule = "self-operated (brand owner)"
                stats["rule2_brand_owner"] += 1
            if not rule:
                continue
            if not args.dry_run:
                c.execute(text("""
                    UPDATE existing_schemes
                    SET operator_company_id = :o,
                        notes = LEFT(COALESCE(notes,'') ||
                                ' Operator=owner: ' || :r || '.', 1000),
                        updated_at = NOW()
                    WHERE id = :s AND operator_company_id IS NULL
                """), {"o": oid, "s": sid, "r": rule})

        # ---------------- Rule 3: brand in scheme name ---------------------
        # resolve brand -> company id (reuse existing rows; match loosely)
        def company_for(brand_name: str):
            row = c.execute(text("""
                SELECT id FROM companies
                WHERE LOWER(name) = LOWER(:n)
                   OR LOWER(name) LIKE LOWER(:like)
                ORDER BY (LOWER(name) = LOWER(:n)) DESC LIMIT 1
            """), {"n": brand_name, "like": f"%{brand_name.split()[0]}%"}).first()
            return row[0] if row else None

        schemes = c.execute(text(f"""
            SELECT id, name FROM existing_schemes
            WHERE scheme_type IN {BD} AND operator_company_id IS NULL
        """)).fetchall()
        brand_cache: dict[str, int | None] = {}
        for sid, sname in schemes:
            low = f" {(sname or '').lower()} "
            for tok, canon in BRANDS.items():
                if tok in low:
                    if canon not in brand_cache:
                        brand_cache[canon] = company_for(canon)
                    cid = brand_cache[canon]
                    if not cid:
                        stats["rule3_no_company_row"] += 1
                        break
                    stats["rule3_brand_in_name"] += 1
                    if not args.dry_run:
                        c.execute(text("""
                            UPDATE existing_schemes
                            SET operator_company_id = :o,
                                notes = LEFT(COALESCE(notes,'') ||
                                        ' Operator from brand in name.', 1000),
                                updated_at = NOW()
                            WHERE id = :s AND operator_company_id IS NULL
                        """), {"o": cid, "s": sid})
                    break

        # ---------------- Rule 4: postcode neighbour ------------------------
        nb = c.execute(text(f"""
            SELECT a.id, MIN(b.operator_company_id)
            FROM existing_schemes a
            JOIN existing_schemes b
              ON REPLACE(UPPER(a.postcode),' ','') = REPLACE(UPPER(b.postcode),' ','')
             AND b.operator_company_id IS NOT NULL AND a.id <> b.id
            WHERE a.scheme_type IN {BD} AND a.operator_company_id IS NULL
              AND COALESCE(a.postcode,'') <> ''
            GROUP BY a.id
            HAVING COUNT(DISTINCT b.operator_company_id) = 1
        """)).fetchall()
        for sid, oid in nb:
            stats["rule4_postcode_neighbour"] += 1
            if not args.dry_run:
                c.execute(text("""
                    UPDATE existing_schemes
                    SET operator_company_id = :o,
                        notes = LEFT(COALESCE(notes,'') ||
                                ' Operator from same-postcode scheme.', 1000),
                        updated_at = NOW()
                    WHERE id = :s AND operator_company_id IS NULL
                """), {"o": oid, "s": sid})

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"   {k:28} {v:,}")


if __name__ == "__main__":
    main()
