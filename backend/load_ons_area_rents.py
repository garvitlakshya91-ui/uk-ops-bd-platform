"""Load ONS PIPR area rent levels as estimates for rent-less BD schemes.

Source: ONS Price Index of Private Rents (monthly, free) — average private
rent levels per local authority by bedroom count. These are AREA averages,
not scheme rents, and are stored clearly labelled:

    source        = 'ons_pipr_area'
    room_type     = 'Area avg 1-bed (LA)' / '2-bed' / '3-bed'

Applied to BD schemes (BTR / Senior / Co-living) that have no observed
rent rows. Also fills existing_schemes.avg_rent_pcm (2-bed value) where
empty. Idempotent: wipes + reloads its own source label.

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
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

SOURCE = "ons_pipr_area"
# LA-level ONS codes (England unitary/district/met/London, Wales, Scotland, NI)
LA_PREFIXES = ("E06", "E07", "E08", "E09", "W06", "S12", "N09")
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
    print(f"  {len(latest):,} local authorities, latest period {period:%Y-%m}")

    engine = create_engine(DB_URL)
    stats = Counter()
    with engine.begin() as c:
        councils = c.execute(text("SELECT id, name FROM councils")).fetchall()
        by_norm = {norm(n): i for i, n in councils}

        la_rents: dict[int, list] = {}   # council_id -> [(label, pcm), ...]
        unmatched = []
        for code, r in latest.items():
            cid = by_norm.get(norm(r[2]))
            if not cid:
                unmatched.append(r[2])
                continue
            vals = []
            for col, label in PRICE_COLS:
                try:
                    pcm = float(r[col])
                except (TypeError, ValueError):
                    continue
                if pcm > 0:
                    vals.append((label, pcm, str(code)))
            if vals:
                la_rents[cid] = vals
        print(f"  matched {len(la_rents):,} councils "
              f"({len(unmatched)} ONS areas unmatched)")

        schemes = c.execute(text("""
            SELECT es.id, es.council_id, es.avg_rent_pcm IS NULL
            FROM existing_schemes es
            WHERE es.scheme_type IN ('BTR','Senior','Co-living')
              AND es.council_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM scheme_rents sr
                              WHERE sr.scheme_id = es.id
                                AND sr.source <> :src)
        """), {"src": SOURCE}).fetchall()
        print(f"  {len(schemes):,} rent-less BTR/Senior/Co-living schemes")

        if not args.dry_run:
            c.execute(text("DELETE FROM scheme_rents WHERE source = :s"),
                      {"s": SOURCE})

        for sid, cid, no_avg in schemes:
            vals = la_rents.get(cid)
            if not vals:
                stats["no_la_data"] += 1
                continue
            stats["schemes_covered"] += 1
            for label, pcm, code in vals:
                stats["rows"] += 1
                if args.dry_run:
                    continue
                c.execute(text("""
                    INSERT INTO scheme_rents
                        (scheme_id, room_type, rent_per_month, rent_per_week,
                         currency, source, source_reference,
                         scraped_at, created_at)
                    VALUES (:sid, :rt, :pcm, :ppw, 'GBP', :src, :ref,
                            NOW(), NOW())
                """), {"sid": sid, "rt": label, "pcm": pcm,
                       "ppw": round(pcm * 12 / 52, 2), "src": SOURCE,
                       "ref": f"ONS PIPR {period:%Y-%m} {code}"})
            if no_avg and not args.dry_run:
                two_bed = next((p for l, p, _ in vals if "2-bed" in l), None)
                if two_bed:
                    c.execute(text("""
                        UPDATE existing_schemes
                        SET avg_rent_pcm = :p,
                            notes = LEFT(COALESCE(notes,'') ||
                                    ' avg_rent_pcm = ONS LA 2-bed average.', 1000)
                        WHERE id = :sid AND avg_rent_pcm IS NULL
                    """), {"p": two_bed, "sid": sid})

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"   {k:18} {v:,}")


if __name__ == "__main__":
    main()
