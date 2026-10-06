"""Load a city's university-owned student stock into existing_schemes.

Reads the university_stock section of a benchmark JSON (interim source —
re-verify against the universities' own accommodation pages before a
report is sold externally) and inserts one scheme per property, sector
flagged via the operator company = the university.

Idempotent: matches on (council, normalised name) before inserting.

Usage:
    python load_university_stock.py --city Birmingham \
        --benchmark data/benchmarks/birmingham_student_source_jan26.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme
from app.scrapers.scheme_matching import best_match, build_index
from reconcile_census import find_or_create_operator

SOURCE = "benchmark_report"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--benchmark", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    bench = json.loads(Path(args.benchmark).read_text())
    rows = bench.get("university_stock", [])
    if not rows:
        raise SystemExit("benchmark has no university_stock")

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    index = build_index(
        db.query(ExistingScheme)
        .filter(ExistingScheme.council_id == council.id).all()
    )

    created = skipped = 0
    for r in rows:
        name = r["property"]
        existing, _ = best_match(index, name, r.get("postcode"), threshold=0.75)
        if existing is not None:
            skipped += 1
            continue
        created += 1
        if args.dry_run:
            continue
        uni = find_or_create_operator(db, r["university"])
        if uni is not None and uni.company_type != "University":
            uni.company_type = "University"
        db.add(ExistingScheme(
            name=name,
            address=f"{name}, {r['university']}, {bench['city']}",
            postcode=r.get("postcode"),
            council_id=council.id,
            scheme_type="PBSA",
            status="operational",
            operating_status="live",
            beds_total=r["beds"],
            operator_company_id=uni.id if uni else None,
            owner_company_id=uni.id if uni else None,
            source=SOURCE,
            source_reference=f"{bench.get('producer')} {bench['city']} "
                             f"{bench.get('report_date')} (university stock)",
            data_confidence_score=0.85,
        ))
    if not args.dry_run:
        db.commit()
    total_beds = sum(r["beds"] for r in rows)
    print(f"{bench['city']}: {created} university properties created, "
          f"{skipped} already present ({len(rows)} rows, {total_beds:,} beds)")
    db.close()


if __name__ == "__main__":
    main()
