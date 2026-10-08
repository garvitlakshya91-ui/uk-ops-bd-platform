"""Attach scraped AFS/StuRents rents to schemes by name+postcode match.

load_scheme_rents.py joins rents to schemes on the exact listing URL,
which only works for schemes that were created from those listings.
Schemes that entered the census another way (benchmark packs, tenders,
EPC) have no listing URL — this loader matches on postcode + name
instead, so a city's rent coverage follows its census, not its origin.

Append-only: previous rows for the same source and scheme are
superseded, never deleted.

Usage:
    python load_rents_by_match.py --city Birmingham \
        --afs data/afs/birmingham.jsonl \
        --sturents data/sturents/birmingham.jsonl [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme, SchemeRent
from app.models.rent_history import supersede_rents
from app.scrapers.scheme_matching import best_match, build_index

AFS = "afs_directory"
ST = "sturents"
OP = "operator_directory"


def read_jsonl(path: str) -> list[dict]:
    rows = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def afs_rents(rec: dict) -> list[dict]:
    wk = rec.get("rent_ppw")
    if not wk:
        return []
    return [{
        "room_type": "From (advertised)",
        "rent_per_week": float(wk),
        "source": AFS,
        "source_reference": rec.get("url"),
    }]


def opdir_rents(rec: dict) -> list[dict]:
    out = []
    for label, wk in [("From (advertised)", rec.get("rent_ppw_min")),
                      ("To (advertised)", rec.get("rent_ppw_max"))]:
        if wk:
            out.append({
                "room_type": label,
                "rent_per_week": float(wk),
                "source": OP,
                "source_reference": rec.get("url"),
            })
    if len(out) == 2 and out[0]["rent_per_week"] == out[1]["rent_per_week"]:
        out = out[:1]
    return out


MIN_RENT_PPW, MAX_RENT_PPW = 60.0, 700.0   # outside this band it is not a PBSA room rent


def sturents_rents(rec: dict) -> list[dict]:
    # StuRents "house" listings are HMOs on the same street, not scheme
    # rooms: they must never attach to a PBSA scheme (George Road, 800
    # Bristol Road and Metchley Lane were mis-attached this way).
    if not rec.get("is_pbsa_candidate") or "/house/" in (rec.get("url") or ""):
        return []
    out = []
    for label, wk in [("From (advertised)", rec.get("rent_pppw_min")),
                      ("To (advertised)", rec.get("rent_pppw_max"))]:
        if wk:
            out.append({
                "room_type": label,
                "rent_per_week": float(wk),
                "contract_length_weeks": rec.get("lease_weeks"),
                "source": ST,
                "source_reference": rec.get("url"),
            })
    # Drop a To row equal to the From row (single-price listing)
    if len(out) == 2 and out[0]["rent_per_week"] == out[1]["rent_per_week"]:
        out = out[:1]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True, help="Council name")
    ap.add_argument("--afs", nargs="*", default=[])
    ap.add_argument("--sturents", nargs="*", default=[])
    ap.add_argument("--opdir", nargs="*", default=[],
                    help="Operator-directory brand JSONLs (all cities; "
                         "filtered to records whose city matches --city)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    index = build_index(
        db.query(ExistingScheme)
        .filter(ExistingScheme.council_id == council.id)
        .all()
    )
    print(f"{args.city}: matching against {len(index)} schemes")

    pending: dict[str, list[tuple[int, dict]]] = {AFS: [], ST: [], OP: []}
    unmatched = 0
    city_lower = args.city.lower()
    sources = [(AFS, args.afs, afs_rents), (ST, args.sturents, sturents_rents),
               (OP, args.opdir, opdir_rents)]
    for source, files, to_rents in sources:
        for path in files:
            for rec in read_jsonl(path):
                # Brand files span every city the operator trades in; only
                # the requested city's records may match its schemes.
                if source == OP and city_lower not in (rec.get("city") or "").lower():
                    continue
                rents = [r for r in to_rents(rec)
                         if MIN_RENT_PPW <= r["rent_per_week"] <= MAX_RENT_PPW]
                if not rents:
                    continue
                scheme, score = best_match(index, rec.get("name") or "",
                                           rec.get("postcode"))
                if scheme is None:
                    unmatched += 1
                    continue
                for r in rents:
                    pending[source].append((scheme.id, r))

    for source, rows in pending.items():
        sids = sorted({sid for sid, _ in rows})
        print(f"{source}: {len(rows)} rent rows across {len(sids)} schemes"
              + (f"; {unmatched} records unmatched" if source == ST else ""))
        if args.dry_run or not rows:
            continue
        superseded = supersede_rents(
            db.connection(), [source], scheme_ids=sids)
        for sid, r in rows:
            db.add(SchemeRent(scheme_id=sid, currency="GBP", **r))
        print(f"  superseded {superseded} prior rows, inserted {len(rows)}")

    if not args.dry_run:
        db.commit()
        total = db.execute(text(
            "SELECT COUNT(DISTINCT sr.scheme_id) FROM scheme_rents sr "
            "JOIN existing_schemes es ON es.id = sr.scheme_id "
            "WHERE es.council_id = :cid AND sr.is_current"
        ), {"cid": council.id}).scalar()
        print(f"{args.city}: {total} schemes now have current rent rows")
    db.close()


if __name__ == "__main__":
    main()
