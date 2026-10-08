"""Admit schemes our own sources observed operating but the census lacks.

The census was seeded from a licensed pack and the reconciliation only
accepted pack-matched schemes, so new openings our scrapers saw (Crown
Place, Gough Street, Haus, St Chads) never became census rows. This
script creates them from a dated admissions file, records every value as
an observation with its source, and keeps the pack's bed count (if any)
as a QA-only observation that never reaches the published value.

Usage: .venv/bin/python admit_schemes.py --city Birmingham [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
from collections import Counter
from pathlib import Path

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme
from app.models.observations import record_observation
from app.scrapers.scheme_matching import best_match, build_index
from reconcile_census import find_or_create_operator


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    slug = args.city.lower().replace(" ", "_")
    data = json.loads((Path(__file__).parent / "data" / f"admissions_{slug}.json").read_text())
    observed_at = data.get("observed_at") or datetime.date.today().isoformat()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    existing = (db.query(ExistingScheme)
                .filter(ExistingScheme.council_id == council.id).all())
    index = build_index(existing)
    status_value = Counter(s.status for s in existing if s.scheme_type == "PBSA").most_common(1)
    status_value = status_value[0][0] if status_value else None

    created = 0
    for e in data["entries"]:
        hit, score = best_match(index, e["name"], e.get("postcode"), threshold=0.9)
        if hit is not None:
            print(f"skip {e['name']}: already in census as {hit.name} (score {score})")
            continue
        operator = find_or_create_operator(db, e["operator"])
        scheme = ExistingScheme(
            name=e["name"], postcode=e.get("postcode"), address=e.get("address"),
            council_id=council.id, scheme_type="PBSA", status=status_value,
            operating_status="live",
            beds_total=e.get("beds") or e.get("qa_beds"),
            build_year=e.get("build_year"),
            operator_company_id=operator.id if operator else None,
            source=e["source"], source_reference=e.get("source_url"),
            last_verified_at=datetime.datetime.fromisoformat(observed_at),
            data_confidence_score=0.7,
        )
        db.add(scheme)
        db.flush()
        src0 = e["observed_sources"][0]
        record_observation(db, scheme, "observed", e["observed_sources"], src0,
                           reference=e.get("source_url"))
        record_observation(db, scheme, "operator", e["operator"], src0,
                           reference=e.get("source_url"))
        if e.get("beds") and e.get("beds_source"):
            record_observation(db, scheme, "beds_total", e["beds"], e["beds_source"],
                               reference=e.get("beds_reference"))
        if e.get("build_year") and e.get("build_year_source"):
            record_observation(db, scheme, "build_year", e["build_year"], e["build_year_source"],
                               reference=e.get("source_url"))
        if e.get("qa_beds"):
            record_observation(db, scheme, "beds_total", e["qa_beds"], "benchmark_report",
                               reference=e.get("qa_reference"), refresh_cache=False)
        if e.get("consent_reference"):
            record_observation(db, scheme, "planning_reference", e["consent_reference"],
                               "planning_consent", reference=e["consent_reference"])
        created += 1
        print(f"admit {e['name']} ({e['operator']}, {e.get('postcode')}): beds "
              f"{e.get('beds') or '—'} own / {e.get('qa_beds') or '—'} QA, built {e.get('build_year') or '—'}")
    if args.dry_run:
        db.rollback()
        print(f"(dry run) would admit {created}")
    else:
        db.commit()
        print(f"admitted {created} schemes")
    db.close()


if __name__ == "__main__":
    main()
