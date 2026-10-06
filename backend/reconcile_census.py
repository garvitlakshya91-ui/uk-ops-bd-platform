"""Reconcile our scheme census against a benchmark city pack.

Diffs existing_schemes for one city against a benchmark JSON produced by
``extract_benchmark_xlsx.py`` and reports: matched schemes (with bed
deltas and fillable fields), benchmark schemes we are missing, and our
schemes the benchmark lacks. ``--apply`` then closes the gap through the
field-protection layer: missing schemes are inserted and NULL fields
filled, under source ``benchmark_report`` (precedence 70) — existing
higher-precedence values are never overwritten.

Usage:
    python reconcile_census.py --benchmark data/benchmarks/birmingham_student_source_jan26.json
    python reconcile_census.py --benchmark ... --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import or_

from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme
from app.scrapers.field_protection import set_field
from app.scrapers.scheme_matching import best_match, build_index

SOURCE = "benchmark_report"


def match(bench_rows, ours):
    """Greedy best-match: postcode agreement + name token overlap."""
    index = build_index(ours)
    matched, missing = [], []
    used: set[int] = set()
    for b in bench_rows:
        scheme, score = best_match(index, b["property"], b.get("postcode"), used=used)
        if scheme is not None:
            used.add(scheme.id)
            matched.append((b, scheme, score))
        else:
            missing.append(b)
    ours_only = [o["scheme"] for o in index if o["scheme"].id not in used]
    return matched, missing, ours_only


def find_or_create_operator(db, name: str) -> Company | None:
    if not name or name.lower() in {"n/a", "unknown", "tbc"}:
        return None
    normalized = re.sub(r"[^a-z0-9]+", "", name.lower())
    company = (
        db.query(Company)
        .filter(or_(Company.normalized_name == normalized,
                    Company.name.ilike(name.strip())))
        .first()
    )
    if company is None:
        company = Company(
            name=name.strip(), normalized_name=normalized,
            company_type="Operator", is_active=True,
        )
        db.add(company)
        db.flush()
    return company


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", required=True)
    ap.add_argument("--council", default=None,
                    help="Council name (defaults to the benchmark's city)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--out", default=None, help="Write the diff as JSON")
    args = ap.parse_args()

    bench = json.loads(Path(args.benchmark).read_text())
    city = bench["city"]
    council_name = args.council or city

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(council_name)).first()
    if council is None:
        raise SystemExit(f"Council {council_name!r} not found")

    ours = (
        db.query(ExistingScheme)
        .filter(ExistingScheme.council_id == council.id)
        .all()
    )
    pbsa_ours = [s for s in ours if (s.scheme_type or "").upper() == "PBSA"]
    print(f"{city}: benchmark {len(bench['private_stock'])} private PBSA schemes; "
          f"we hold {len(ours)} schemes in {council.name} "
          f"({len(pbsa_ours)} PBSA)")

    matched, missing, ours_only = match(bench["private_stock"], ours)

    bed_deltas = []
    for b, s, score in matched:
        current_beds = s.beds_total or s.num_units or s.total_units
        if current_beds and abs(current_beds - b["beds"]) > max(5, b["beds"] * 0.05):
            bed_deltas.append((b, s, current_beds))

    print(f"\nMatched: {len(matched)}   Missing from ours: {len(missing)}   "
          f"Ours not in benchmark: {len(ours_only)}   Bed mismatches: {len(bed_deltas)}")
    for b, s, score in matched[:10]:
        print(f"  = [{score}] {b['property']!r} -> #{s.id} {s.name!r}")
    for b in missing[:10]:
        print(f"  + {b['property']!r} ({b.get('operator')}, {b.get('postcode')}, {b['beds']} beds)")
    for b, s, beds in bed_deltas:
        print(f"  ! beds {b['property']!r}: benchmark {b['beds']} vs ours {beds} (#{s.id})")

    created = filled = 0
    if args.apply:
        ref = f"{bench.get('producer')} {city} {bench.get('report_date')}"
        for b in missing:
            operator = find_or_create_operator(db, b.get("operator") or "")
            status = "operational"
            op_status = "live"
            if b.get("no_letting_presence"):
                op_status = "no_letting_presence"
            scheme = ExistingScheme(
                name=b["property"],
                postcode=b.get("postcode"),
                address=f"{b['property']}, {city}",
                council_id=council.id,
                scheme_type="PBSA",
                status=status,
                operating_status=op_status,
                beds_total=b["beds"],
                build_year=b.get("build_year"),
                nominations=b.get("nominations") or None,
                operator_company_id=operator.id if operator else None,
                source=SOURCE,
                source_reference=ref,
                data_confidence_score=0.9,
            )
            db.add(scheme)
            created += 1
        for b, s, score in matched:
            wrote = False
            if s.beds_total is None and set_field(s, "beds_total", b["beds"], SOURCE, db):
                wrote = True
            if s.build_year is None and b.get("build_year") and \
                    set_field(s, "build_year", b["build_year"], SOURCE, db):
                wrote = True
            if s.operating_status is None:
                op_status = "no_letting_presence" if b.get("no_letting_presence") else "live"
                if set_field(s, "operating_status", op_status, SOURCE, db):
                    wrote = True
            if b.get("nominations") and s.nominations is None and \
                    set_field(s, "nominations", True, SOURCE, db):
                wrote = True
            if s.operator_company_id is None and b.get("operator"):
                operator = find_or_create_operator(db, b["operator"])
                if operator and set_field(s, "operator_company_id", operator.id, SOURCE, db):
                    wrote = True
            if wrote:
                filled += 1
        db.commit()
        print(f"\nApplied: {created} schemes created, {filled} schemes had fields filled.")

    if args.out:
        Path(args.out).write_text(json.dumps({
            "city": city,
            "benchmark": os.path.basename(args.benchmark),
            "matched": [
                {"benchmark": b["property"], "scheme_id": s.id, "score": score}
                for b, s, score in matched
            ],
            "missing_from_ours": missing,
            "ours_not_in_benchmark": [
                {"id": s.id, "name": s.name, "type": s.scheme_type} for s in ours_only
            ],
            "bed_mismatches": [
                {"benchmark": b["property"], "scheme_id": s.id,
                 "benchmark_beds": b["beds"], "our_beds": beds}
                for b, s, beds in bed_deltas
            ],
        }, indent=1))
        print(f"Diff written to {args.out}")

    db.close()


if __name__ == "__main__":
    main()
