"""Curated seed of Birmingham BTR developments missing from existing_schemes.

Verified via web research on 2026-06-01 from the following sources:
  - BD Online, Building Design (Curzon Wharf)
  - Way of Life website (The Lansdowne)
  - Centrick communities directory (St Martin's Place, The Kettleworks)
  - Savills, Martin & Co, Rightmove (Broadway Residences)
  - Bisnow Birmingham (Cortland Ryland Street)
  - Birmingham Biz, Tilbury Douglas (The Lansdowne)

Cross-referenced against our 95 existing Birmingham BTR records — only
schemes NOT already present (under any name/postcode variant) are seeded.

Idempotent: re-running checks (name, postcode) duplicates first.
"""
from __future__ import annotations

import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

BIRMINGHAM_COUNCIL_ID = 18

# Verified curated seeds — name, address, postcode, units, scheme_type,
# status, operator_company (string, will be looked up), source_reference
SEEDS: list[dict] = [
    {
        "name": "The Lansdowne",
        "address": "25 Hagley Road, Birmingham",
        "postcode": "B16 8FY",
        "num_units": 206,
        "scheme_type": "BTR",
        "status": "Operational",
        "operator_name": "Way of Life",
        "source_reference": "wayoflife.com/locations/birmingham/the-lansdowne; "
                            "tilburydouglas.co.uk/projects/the-lansdowne; "
                            "birminghambiz.co.uk/news/edgbaston-rent-apartments",
        "notes": "Way of Life-managed BTR. £37m scheme. 18 duplexes incl. "
                 "10,000+ sqft shared social spaces.",
    },
    {
        "name": "The Kettleworks",
        "address": "Pope Street, Jewellery Quarter, Birmingham",
        "postcode": "B1 3DQ",
        "num_units": 292,
        "scheme_type": "BTR",
        "status": "Operational",
        "operator_name": "Centrick",
        "source_reference": "centrick.co.uk/our-communities/the-kettleworks-jewellery-quarter; "
                            "bdg.uk.com/projects/showcase/the-kettleworks-jewellery-quarter-birmingham",
        "notes": "Developed by Seven Capital, operated by Centrick. "
                 "Completed 2018. Part of St George's Urban Village.",
    },
    {
        "name": "Curzon Wharf",
        "address": "Curzon Street, Birmingham",
        "postcode": "B4 7XG",   # approximate — near Aston University
        "num_units": 620,  # 498 BTR + 122 separate residential block (53-storey tower scheme)
        "scheme_type": "BTR",
        "status": "Approved",
        "operator_name": "Woodbourne Group",
        "source_reference": "bdonline.co.uk/news/associated-architects-wins-approval-for-53-storey-build-to-rent-tower; "
                            "constructionwave.co.uk/2023/04/28/360m-curzon-wharf-tallest-tower-in-birmingham-green-lit; "
                            "woodbournegroup.com/curzon-wharf",
        "notes": "£360m scheme; 53-storey BTR tower (Birmingham's tallest). "
                 "Also includes a 41-storey 732-bed PBSA tower and a "
                 "14-storey block of 122 homes. Net zero carbon ready.",
    },
    {
        "name": "Broadway Residences",
        "address": "105 Broad Street, Birmingham",
        "postcode": "B15 1BF",
        "num_units": 214,
        "scheme_type": "BTR",
        "status": "Operational",
        "operator_name": "Seven Living",
        "source_reference": "rightmove.co.uk/properties/172006223; "
                            "search.savills.com/property-detail/gbbmrebrm190041l; "
                            "visitbirmingham.com/listing/broadway-residences-broad-street",
        "notes": "Operated by Seven Living. Broad Street. Concierge + gym.",
    },
    {
        "name": "St Martin's Place",
        "address": "169 Broad Street, Birmingham",
        "postcode": "B15 1DT",
        "num_units": 228,
        "scheme_type": "BTR",
        "status": "Operational",
        "operator_name": "Seven Living",
        "source_reference": "centrick.co.uk/our-communities/st-martins-place-birmingham; "
                            "onthemarket.com/details/10665048; "
                            "bdg.uk.com/projects/showcase/st-martins-place-birmingham",
        "notes": "Operated by Seven Living. Adjacent to Park Regis Hotel with "
                 "shared gym/cafe/cinema facilities. 228 city-centre apartments.",
    },
    {
        "name": "Cortland Ryland Street",
        "address": "Broad Street / Ryland Street, Birmingham",
        "postcode": "B15 1AS",  # same outcode as Cortland Broad Street
        "num_units": 800,  # estimated; 35-storey building announced
        "scheme_type": "BTR",
        "status": "Planning",
        "operator_name": "Cortland",
        "source_reference": "cortland.com/in-the-news/cortland-and-harrison-street-acquire-birmingham-btr-project; "
                            "bisnow.com/birmingham (Cortland Ryland Street acquisition)",
        "notes": "Cortland + Harrison Street acquired site for second "
                 "Birmingham BTR (after Cortland Broad Street). 35-storey "
                 "tower. Unit count provisional pending planning detail.",
    },
]


def find_or_create_company(c, name: str) -> int | None:
    """Find a company by name; create stub if not present. Returns company id."""
    if not name:
        return None
    row = c.execute(
        text("SELECT id FROM companies WHERE LOWER(name) = LOWER(:n) LIMIT 1"),
        {"n": name.strip()},
    ).first()
    if row:
        return row[0]
    import re
    norm = re.sub(r"[^a-z0-9]+", "", name.lower().strip())
    row = c.execute(text("""
        INSERT INTO companies (name, normalized_name, created_at, updated_at)
        VALUES (:n, :nn, NOW(), NOW())
        RETURNING id
    """), {"n": name.strip()[:200], "nn": norm[:200]}).first()
    print(f"    [+] created companies row for {name!r} (id={row[0]})")
    return row[0]


def main():
    engine = create_engine(DB_URL)

    inserted = 0
    skipped = 0
    updated = 0

    with engine.begin() as c:
        for seed in SEEDS:
            # Idempotency: skip if name+postcode already exists
            existing = c.execute(text("""
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
                "cid": BIRMINGHAM_COUNCIL_ID,
                "n": seed["name"],
                "pc": seed["postcode"],
                "pat": "%" + seed["name"].lower().split()[0] + "%",
            }).first()
            if existing:
                print(f"  [skip] {seed['name']!r} already in DB (id={existing[0]})")
                skipped += 1
                continue

            operator_id = find_or_create_company(c, seed["operator_name"])

            row = c.execute(text("""
                INSERT INTO existing_schemes
                    (name, address, postcode, council_id, scheme_type, status,
                     num_units, total_units, operator_company_id,
                     source, source_reference, notes,
                     created_at, updated_at)
                VALUES
                    (:n, :a, :pc, :cid, :st, :status,
                     :u, :u, :op,
                     :src, :ref, :notes,
                     NOW(), NOW())
                RETURNING id
            """), {
                "n": seed["name"][:255],
                "a": seed["address"][:500],
                "pc": seed["postcode"][:10],
                "cid": BIRMINGHAM_COUNCIL_ID,
                "st": seed["scheme_type"],
                "status": seed["status"][:50],
                "u": seed["num_units"],
                "op": operator_id,
                "src": "manual_curation",
                "ref": seed["source_reference"][:500],
                "notes": seed["notes"][:1000],
            }).first()
            print(f"  [+] inserted {seed['name']!r} (id={row[0]}, "
                  f"{seed['num_units']}u, {seed['status']}, "
                  f"operator={seed['operator_name']})")
            inserted += 1

    print()
    print(f"=== Summary ===")
    print(f"  Inserted: {inserted}")
    print(f"  Skipped (already present): {skipped}")
    print(f"  Total Birmingham BTR seeds: {len(SEEDS)}")


if __name__ == "__main__":
    main()
