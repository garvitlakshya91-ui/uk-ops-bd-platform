"""Load RSH LA-level social rents as council-level benchmarks.

Source: Regulator of Social Housing 'Registered providers look-up tool'
(free, annual) — combined SDR+LADR average weekly rents by local
authority x rent type x bedsize. For schemes owned by housing
associations / councils the appropriate area benchmark is the GN
(general needs) social rent, not the ONS private-market average, so
these rows sit in ``area_rent_benchmarks`` alongside the ONS rows — one
per council x segment x period, never attached to a scheme:

    source    = 'rsh_sdr_la'
    segment   = 'Social rent GN 1-bed (LA)' / '2-bed' / '3-bed'
    period    = '2024/25' (the settlement year)

Idempotent: a newer settlement year supersedes the previous one;
reloading the same year only writes where a value changed. Usage:
    python load_rsh_social_rents.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

import openpyxl
from sqlalchemy import func, text

from app.database import SessionLocal
from app.models.models import AreaRentBenchmark
from app.models.rents import record_area_benchmark
from backfill_operator_rules import HA_PATTERNS, EXCLUDE_PATTERNS, matches_any
from load_ons_area_rents import norm

SOURCE = "rsh_sdr_la"
FILE = "data/rsh/rp_lookup_2025.xlsx"
PERIOD = "2024/25"
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
    la_rents: dict[str, list] = {}   # norm(la_name) -> [(label, weekly, code, name)]
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r[3] != "GN" or r[4] not in BEDS:
            continue
        try:
            wk = float(r[14])
        except (TypeError, ValueError):
            continue
        if wk > 0:
            la_rents.setdefault(norm(r[1]), []).append((BEDS[r[4]], wk, r[0], r[1]))
    print(f"  {len(la_rents):,} LAs with GN social rents")

    db = SessionLocal()
    stats = Counter()
    try:
        councils = dict(db.execute(text("SELECT id, name FROM councils")).fetchall())
        cid2rents = {cid: la_rents[norm(nm)] for cid, nm in councils.items()
                     if norm(nm) in la_rents}
        print(f"  matched to {len(cid2rents):,} councils")

        if not args.dry_run:
            # Append-only: supersede, never delete — keeps the yearly
            # RSH settlement series. Rows for the year being loaded stay
            # current so a reload is a no-op.
            stats["superseded"] += (
                db.query(AreaRentBenchmark)
                .filter(AreaRentBenchmark.source == SOURCE,
                        AreaRentBenchmark.is_current.is_(True),
                        AreaRentBenchmark.period != PERIOD)
                .update({"is_current": False, "superseded_at": func.now()},
                        synchronize_session=False))

        # one benchmark row per council x segment x period
        for cid, vals in cid2rents.items():
            for label, wk, code, name in vals:
                stats["benchmark_rows"] += 1
                if args.dry_run:
                    continue
                outcome = record_area_benchmark(
                    db, council_id=cid, source=SOURCE, segment=label,
                    rent_ppw=wk, rent_pcm=round(wk * 52 / 12, 2),
                    period=PERIOD, area_code=str(code) if code else None,
                    area_name=name,
                    source_reference=f"RSH SDR/LADR 2024-25 {code}")
                stats[f"benchmark_{outcome}"] += 1

        # Coverage summary only: how many rent-less HA/council-owned BD
        # schemes now sit in a council with a social-rent benchmark.
        schemes = db.execute(text("""
            SELECT es.id, es.council_id, co.name
            FROM existing_schemes es
            JOIN companies co ON co.id = es.owner_company_id
            WHERE es.scheme_type IN ('BTR','Senior','Co-living')
              AND es.council_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM scheme_room_rents rr
                              WHERE rr.scheme_id = es.id)
              AND NOT EXISTS (SELECT 1 FROM scheme_advertised_rents ar
                              WHERE ar.scheme_id = es.id)
        """)).fetchall()
        for sid, cid, owner in schemes:
            if matches_any(owner, EXCLUDE_PATTERNS) or not matches_any(owner, HA_PATTERNS):
                stats["owner_not_social"] += 1
                continue
            if not cid2rents.get(cid):
                stats["no_la_data"] += 1
                continue
            stats["schemes_covered"] += 1

        if args.dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"   {k:18} {v:,}")


if __name__ == "__main__":
    main()
