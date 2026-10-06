"""Backfill pbsa_beds and expected_delivery_year on planning applications.

Runs the regex extractor over PBSA-flagged applications whose bed count
is not yet set. Re-runnable; only NULL fields are written.

Usage:
    python parse_pbsa_beds.py --dry-run
    python parse_pbsa_beds.py [--council Birmingham] [--limit 50000]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import or_

from app.database import SessionLocal
from app.models.models import Council, PlanningApplication
from app.scrapers.pbsa_beds import expected_delivery_year, parse_beds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", default=None)
    ap.add_argument("--limit", type=int, default=200000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    q = db.query(PlanningApplication).filter(
        or_(PlanningApplication.is_pbsa.is_(True),
            PlanningApplication.scheme_type == "PBSA"),
        PlanningApplication.pbsa_beds.is_(None),
    )
    if args.council:
        council = db.query(Council).filter(Council.name.ilike(args.council)).first()
        if council is None:
            raise SystemExit(f"Council {args.council!r} not found")
        q = q.filter(PlanningApplication.council_id == council.id)

    stats = Counter()
    batch = 0
    for app_row in q.limit(args.limit).yield_per(1000):
        stats["scanned"] += 1
        beds = parse_beds(app_row.description)
        year = expected_delivery_year(
            app_row.status or app_row.decision, app_row.decision_date
        )
        if beds is None and year is None:
            stats["no_extract"] += 1
            continue
        if beds is not None:
            stats["beds_set"] += 1
            if not args.dry_run:
                app_row.pbsa_beds = beds
        if year is not None and app_row.expected_delivery_year is None:
            stats["delivery_year_set"] += 1
            if not args.dry_run:
                app_row.expected_delivery_year = year
        batch += 1
        if not args.dry_run and batch % 1000 == 0:
            db.commit()
    if not args.dry_run:
        db.commit()
    db.close()

    print(f"=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"  {k:18} {v:,}")


if __name__ == "__main__":
    main()
