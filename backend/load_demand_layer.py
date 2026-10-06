"""Load the demand layer: institutions, HESA enrolments, maintenance loans.

Enrolment sources, in order of preference:
  --hesa-csv FILE       HESA Student Record table-1 bulk CSV (official;
                        download from hesa.ac.uk — Cloudflare blocks
                        programmatic fetch from this environment)
  --from-benchmark FILE benchmark pack's HESA table (interim; same
                        underlying HESA figures, re-load from the bulk
                        CSV before external publication)

Maintenance loans are a built-in table of SLC maximum rates (England).

Usage:
    python load_demand_layer.py --institutions data/institutions.json \
        --from-benchmark data/benchmarks/birmingham_student_source_jan26.json
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import Council, HesaEnrolment, Institution, MaintenanceLoan

# SLC maximum maintenance loan, England, by academic year (GBP/year).
# Approximate figures compiled from gov.uk student finance rates; verify
# against the published rates before any external publication.
MAINTENANCE_LOANS = {
    "2016-17": {"outside_london": 8200, "london": 10702, "parental_home": 6904},
    "2017-18": {"outside_london": 8430, "london": 11002, "parental_home": 7097},
    "2018-19": {"outside_london": 8700, "london": 11354, "parental_home": 7324},
    "2019-20": {"outside_london": 8944, "london": 11672, "parental_home": 7529},
    "2020-21": {"outside_london": 9203, "london": 12010, "parental_home": 7747},
    "2021-22": {"outside_london": 9488, "london": 12382, "parental_home": 7987},
    "2022-23": {"outside_london": 9706, "london": 12667, "parental_home": 8171},
    "2023-24": {"outside_london": 9978, "london": 13022, "parental_home": 8400},
    "2024-25": {"outside_london": 10227, "london": 13348, "parental_home": 8610},
    "2025-26": {"outside_london": 10544, "london": 13762, "parental_home": 8877},
}
LOAN_SOURCE = "gov.uk student finance rates (approximate)"


def norm_year(y: str) -> str:
    return y.strip().replace("/", "-")


def norm_inst(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower().replace("the ", "", 1))


def seed_institutions(db, path: str) -> dict[str, Institution]:
    """Upsert institutions; return alias-normalised lookup."""
    lookup: dict[str, Institution] = {}
    for row in json.loads(Path(path).read_text()):
        inst = db.query(Institution).filter(
            Institution.name == row["name"]).first()
        if inst is None:
            council = db.query(Council).filter(
                Council.name.ilike(row["council"])).first()
            inst = Institution(
                name=row["name"],
                council_id=council.id if council else None,
                city=row.get("city"),
                demand_adjustment=row.get("demand_adjustment", 1.0),
                campus_notes=row.get("campus_notes"),
            )
            db.add(inst)
            db.flush()
        for alias in [row["name"], *row.get("aliases", [])]:
            lookup[norm_inst(alias)] = inst
    return lookup


def upsert_enrolment(db, inst: Institution, year: str, n: int, source: str) -> bool:
    year = norm_year(year)
    row = (
        db.query(HesaEnrolment)
        .filter(HesaEnrolment.institution_id == inst.id,
                HesaEnrolment.academic_year == year,
                HesaEnrolment.level == "all")
        .first()
    )
    if row is not None:
        # The official CSV outranks a benchmark-sourced figure.
        if source.startswith("hesa") and row.source != source and row.full_time_students != n:
            row.full_time_students, row.source = n, source
            return True
        return False
    db.add(HesaEnrolment(institution_id=inst.id, academic_year=year,
                         full_time_students=n, source=source))
    return True


def load_benchmark(db, lookup, path: str) -> int:
    bench = json.loads(Path(path).read_text())
    n = 0
    for row in bench.get("hesa", []):
        inst = lookup.get(norm_inst(row["institution"]))
        if inst is None:
            print(f"  unmapped institution: {row['institution']!r} — "
                  f"add it to data/institutions.json")
            continue
        for year, students in row["full_time_students"].items():
            if upsert_enrolment(db, inst, year, students,
                                "hesa_via_benchmark_pack"):
                n += 1
    return n


def load_hesa_csv(db, lookup, path: str) -> int:
    """HESA table-1 bulk CSV: one row per provider/year/level/mode."""
    n = 0
    with open(path, newline="", encoding="utf-8-sig") as fh:
        # HESA CSVs carry preamble lines before the header row.
        lines = [ln for ln in fh if ln.strip()]
    header_i = next(i for i, ln in enumerate(lines) if "HE provider" in ln)
    reader = csv.DictReader(lines[header_i:])
    totals: dict[tuple[int, str], int] = {}
    for rec in reader:
        if (rec.get("Mode of study") or "").strip() != "Full-time":
            continue
        level = (rec.get("Level of study") or "").strip()
        if level not in ("All", "Total", ""):
            continue
        inst = lookup.get(norm_inst(rec.get("HE provider", "")))
        if inst is None:
            continue
        year = norm_year(rec.get("Academic Year", ""))
        try:
            num = int(str(rec.get("Number", "")).replace(",", ""))
        except ValueError:
            continue
        totals[(inst.id, year)] = num
    by_id = {i.id: i for i in lookup.values()}
    for (inst_id, year), num in totals.items():
        if upsert_enrolment(db, by_id[inst_id], year, num, "hesa_table1_csv"):
            n += 1
    return n


def seed_loans(db) -> int:
    n = 0
    for year, regions in MAINTENANCE_LOANS.items():
        for region, amount in regions.items():
            exists = (
                db.query(MaintenanceLoan)
                .filter(MaintenanceLoan.academic_year == year,
                        MaintenanceLoan.region == region)
                .first()
            )
            if exists is None:
                db.add(MaintenanceLoan(academic_year=year, region=region,
                                       max_loan_gbp=amount, source=LOAN_SOURCE))
                n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--institutions", default="data/institutions.json")
    ap.add_argument("--from-benchmark", nargs="*", default=[])
    ap.add_argument("--hesa-csv", default=None)
    args = ap.parse_args()

    db = SessionLocal()
    lookup = seed_institutions(db, args.institutions)
    print(f"institutions: {len({i.id for i in lookup.values()})}")
    rows = 0
    for path in args.from_benchmark:
        rows += load_benchmark(db, lookup, path)
    if args.hesa_csv:
        rows += load_hesa_csv(db, lookup, args.hesa_csv)
    loans = seed_loans(db)
    db.commit()
    print(f"enrolment rows written: {rows}; maintenance-loan rows: {loans}")
    db.close()


if __name__ == "__main__":
    main()
