"""Load ONS PIPR area rent levels as council-level benchmarks.

Source: ONS Price Index of Private Rents (monthly, free) — average private
rent levels per local authority by bedroom count. These are AREA averages,
not scheme rents, and are stored in ``area_rent_benchmarks`` — one row
per council x segment x period, never attached to a scheme:

    source        = 'ons_pipr_area'
    segment       = 'Area avg 1-bed (LA)' / '2-bed' / '3-bed'
    period        = '2026-06' (the workbook's latest month)

Also fills existing_schemes.avg_rent_pcm (LA 2-bed value) on rent-less
BD schemes (BTR / Senior / Co-living) where empty. Idempotent: a newer
month supersedes the previous month's rows; reloading the same month
only writes where a value changed.

Usage:
    python load_ons_area_rents.py [--dry-run] [--file data/ons/pipr_2026_06.xlsx]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

import openpyxl
from sqlalchemy import func, text

from app.database import SessionLocal
from app.models.models import AreaRentBenchmark
from app.models.rents import record_area_benchmark

SOURCE = "ons_pipr_area"
# LA-level ONS codes (England unitary/district/met/London, Wales, Scotland, NI)
# NB: the PIPR workbook uses S33* codes for Scottish council areas, not S12.
LA_PREFIXES = ("E06", "E07", "E08", "E09", "W06", "S12", "S33", "N09")
# (column index, label) in Table 1 — header row 3
PRICE_COLS = [(11, "Area avg 1-bed (LA)"), (15, "Area avg 2-bed (LA)"),
              (19, "Area avg 3-bed (LA)")]

ALIASES = {
    "bristol, city of": "bristol",
    "kingston upon hull, city of": "kingston upon hull",
    "herefordshire, county of": "herefordshire",
    "st. helens": "st helens",
    "st. albans": "st albans",
}

# Scotland: the PIPR workbook publishes S33* Broad Rental Market Areas,
# not council areas. Map each BRMA to the council(s) it covers.
SCOT_BRMA_TO_COUNCILS = {
    "aberdeen and shire": ["Aberdeen City", "Aberdeenshire"],
    "argyll and bute": ["Argyll and Bute"],
    "ayrshires": ["North Ayrshire", "South Ayrshire", "East Ayrshire"],
    "dumfries and galloway": ["Dumfries and Galloway"],
    "dundee and angus": ["Dundee City", "Angus"],
    "east dunbartonshire": ["East Dunbartonshire"],
    "fife": ["Fife"],
    "forth valley": ["Falkirk", "Stirling", "Clackmannanshire"],
    "greater glasgow": ["Glasgow City", "Renfrewshire", "East Renfrewshire",
                        "Inverclyde", "West Dunbartonshire"],
    "highland and islands": ["Highland", "Moray", "Na h-Eileanan Siar",
                             "Orkney Islands", "Shetland Islands"],
    "lothian": ["City of Edinburgh", "East Lothian", "Midlothian",
                "West Lothian"],
    "north lanarkshire": ["North Lanarkshire"],
    "perth and kinross": ["Perth and Kinross"],
    "scottish borders": ["Scottish Borders"],
    "south lanarkshire": ["South Lanarkshire"],
    "west lothian": ["West Lothian"],
}


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = ALIASES.get(s, s)
    s = re.sub(r"\b(city of|county of|the)\b", " ", s)
    s = re.sub(r"[^a-z ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--file", default="data/ons/pipr_2026_06.xlsx")
    args = ap.parse_args()

    print(f"Parsing {args.file} ...")
    wb = openpyxl.load_workbook(args.file, read_only=True)
    ws = wb["Table 1"]

    # keep the latest period's row per LA
    latest: dict[str, tuple] = {}
    for r in ws.iter_rows(min_row=4, values_only=True):
        code = r[1]
        if not code or not str(code).startswith(LA_PREFIXES):
            continue
        prev = latest.get(code)
        if prev is None or (r[0] and r[0] > prev[0]):
            latest[code] = r
    period = max(r[0] for r in latest.values())
    period_label = f"{period:%Y-%m}"
    print(f"  {len(latest):,} local authorities, latest period {period_label}")

    db = SessionLocal()
    stats = Counter()
    try:
        councils = db.execute(text("SELECT id, name FROM councils")).fetchall()
        by_norm = {norm(n): i for i, n in councils}

        la_rents: dict[int, list] = {}   # council_id -> [(label, pcm, code, name), ...]
        unmatched = []
        for code, r in latest.items():
            # Scottish BRMAs fan out to their member councils; everything
            # else matches a single council by normalised name.
            if str(code).startswith("S33"):
                targets = [by_norm.get(norm(n)) for n in
                           SCOT_BRMA_TO_COUNCILS.get((r[2] or "").lower().strip(), [])]
                cids = [t for t in targets if t]
            else:
                cid = by_norm.get(norm(r[2]))
                cids = [cid] if cid else []
            if not cids:
                unmatched.append(r[2])
                continue
            vals = []
            for col, label in PRICE_COLS:
                try:
                    pcm = float(r[col])
                except (TypeError, ValueError):
                    continue
                if pcm > 0:
                    vals.append((label, pcm, str(code), r[2]))
            if vals:
                for cid in cids:
                    la_rents.setdefault(cid, vals)
        print(f"  matched {len(la_rents):,} councils "
              f"({len(unmatched)} ONS areas unmatched)")

        if not args.dry_run:
            # Append-only: supersede, never delete — each monthly ONS
            # load becomes a point in the area-rent series. Rows for the
            # month being loaded stay current so a reload is a no-op.
            stats["superseded"] += (
                db.query(AreaRentBenchmark)
                .filter(AreaRentBenchmark.source == SOURCE,
                        AreaRentBenchmark.is_current.is_(True),
                        AreaRentBenchmark.period != period_label)
                .update({"is_current": False, "superseded_at": func.now()},
                        synchronize_session=False))

        # one benchmark row per council x segment x period
        for cid, vals in la_rents.items():
            for label, pcm, code, name in vals:
                stats["benchmark_rows"] += 1
                if args.dry_run:
                    continue
                outcome = record_area_benchmark(
                    db, council_id=cid, source=SOURCE, segment=label,
                    rent_pcm=pcm, rent_ppw=round(pcm * 12 / 52, 2),
                    period=period_label, area_code=code, area_name=name,
                    source_reference=f"ONS PIPR {period_label} {code}")
                stats[f"benchmark_{outcome}"] += 1

        # rent-less BD schemes: fill avg_rent_pcm from the LA 2-bed average
        schemes = db.execute(text("""
            SELECT es.id, es.council_id, es.avg_rent_pcm IS NULL
            FROM existing_schemes es
            WHERE es.scheme_type IN ('BTR','Senior','Co-living')
              AND es.council_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM scheme_room_rents rr
                              WHERE rr.scheme_id = es.id)
              AND NOT EXISTS (SELECT 1 FROM scheme_advertised_rents ar
                              WHERE ar.scheme_id = es.id)
        """)).fetchall()
        print(f"  {len(schemes):,} rent-less BTR/Senior/Co-living schemes")

        for sid, cid, no_avg in schemes:
            vals = la_rents.get(cid)
            if not vals:
                stats["no_la_data"] += 1
                continue
            stats["schemes_covered"] += 1
            if no_avg:
                two_bed = next((p for l, p, _, _ in vals if "2-bed" in l), None)
                if two_bed:
                    stats["avg_rent_pcm_filled"] += 1
                    if args.dry_run:
                        continue
                    db.execute(text("""
                        UPDATE existing_schemes
                        SET avg_rent_pcm = :p,
                            notes = LEFT(COALESCE(notes,'') ||
                                    ' avg_rent_pcm = ONS LA 2-bed average.', 1000)
                        WHERE id = :sid AND avg_rent_pcm IS NULL
                    """), {"p": two_bed, "sid": sid})

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
