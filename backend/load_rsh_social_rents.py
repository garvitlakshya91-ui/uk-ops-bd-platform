"""Load RSH LA-level social rents for HA/council-owned BD schemes.

Source: Regulator of Social Housing 'Registered providers look-up tool'
(free, annual) — combined SDR+LADR average weekly rents by local
authority x rent type x bedsize. For schemes owned by housing
associations / councils the appropriate area benchmark is the GN
(general needs) social rent, not the ONS private-market average, so
these rows are ADDED alongside the ONS area rows:

    source    = 'rsh_sdr_la'
    room_type = 'Social rent GN 1-bed (LA)' / '2-bed' / '3-bed'

Idempotent. Usage:
    python load_rsh_social_rents.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

import openpyxl
from sqlalchemy import create_engine, text

from backfill_operator_rules import HA_PATTERNS, EXCLUDE_PATTERNS, matches_any
from load_ons_area_rents import norm

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
SOURCE = "rsh_sdr_la"
FILE = "data/rsh/rp_lookup_2025.xlsx"
BEDS = {"1Bd": "Social rent GN 1-bed (LA)",
        "2Bd": "Social rent GN 2-bed (LA)",
        "3Bd": "Social rent GN 3-bed (LA)"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"Parsing {FILE} ...")
    wb = openpyxl.load_workbook(FILE, read_only=True)
    ws = wb["Flat_File"]
    la_rents: dict[str, list] = {}   # norm(la_name) -> [(label, weekly)]
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r[3] != "GN" or r[4] not in BEDS:
            continue
        try:
            wk = float(r[14])
        except (TypeError, ValueError):
            continue
        if wk > 0:
            la_rents.setdefault(norm(r[1]), []).append((BEDS[r[4]], wk, r[0]))
    print(f"  {len(la_rents):,} LAs with GN social rents")

    engine = create_engine(DB_URL)
    stats = Counter()
    with engine.begin() as c:
        councils = dict(c.execute(text("SELECT id, name FROM councils")).fetchall())
        cid2rents = {cid: la_rents[norm(nm)] for cid, nm in councils.items()
                     if norm(nm) in la_rents}
        print(f"  matched to {len(cid2rents):,} councils")

        schemes = c.execute(text("""
            SELECT es.id, es.council_id, co.name
            FROM existing_schemes es
            JOIN companies co ON co.id = es.owner_company_id
            WHERE es.scheme_type IN ('BTR','Senior','Co-living')
              AND es.council_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM scheme_rents sr
                              WHERE sr.scheme_id = es.id
                                AND sr.source NOT IN ('ons_pipr_area'))
        """)).fetchall()

        if not args.dry_run:
            c.execute(text("DELETE FROM scheme_rents WHERE source = :s"),
                      {"s": SOURCE})

        for sid, cid, owner in schemes:
            if matches_any(owner, EXCLUDE_PATTERNS) or not matches_any(owner, HA_PATTERNS):
                stats["owner_not_social"] += 1
                continue
            vals = cid2rents.get(cid)
            if not vals:
                stats["no_la_data"] += 1
                continue
            stats["schemes_covered"] += 1
            for label, wk, code in vals:
                stats["rows"] += 1
                if args.dry_run:
                    continue
                c.execute(text("""
                    INSERT INTO scheme_rents
                        (scheme_id, room_type, rent_per_week, rent_per_month,
                         currency, source, source_reference,
                         scraped_at, created_at)
                    VALUES (:sid, :rt, :wk, :pcm, 'GBP', :src, :ref,
                            NOW(), NOW())
                """), {"sid": sid, "rt": label, "wk": wk,
                       "pcm": round(wk * 52 / 12, 2), "src": SOURCE,
                       "ref": f"RSH SDR/LADR 2024-25 {code}"})

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"   {k:18} {v:,}")


if __name__ == "__main__":
    main()
