"""Load UCAS end-of-cycle placed-applicant numbers per provider.

UCAS publishes end-of-cycle data as CSVs (ucas.com -> data and analysis).
The exact file layout shifts year to year, so this loader takes any CSV
with a provider-name column and per-cycle acceptance/placed columns and
maps providers through data/institutions.json aliases.

Rows land in hesa_enrolments with level='ucas_placed' so the demand
chapter can show the forward-cycle signal next to enrolments without a
schema change.

Usage:
    python load_ucas_acceptances.py --csv <end_of_cycle.csv> \
        [--provider-col "Provider name"] [--year-col "Cycle year"] \
        [--value-col "Placed applicants"]
"""
from __future__ import annotations

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from load_demand_layer import norm_inst, norm_year, seed_institutions


def sniff_column(fieldnames: list[str], hints: list[str]) -> str | None:
    low = {f.lower(): f for f in fieldnames}
    for hint in hints:
        for name_low, name in low.items():
            if hint in name_low:
                return name
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--institutions", default="data/institutions.json")
    ap.add_argument("--provider-col", default=None)
    ap.add_argument("--year-col", default=None)
    ap.add_argument("--value-col", default=None)
    args = ap.parse_args()

    db = SessionLocal()
    lookup = seed_institutions(db, args.institutions)

    with open(args.csv, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        fields = reader.fieldnames or []
        provider_col = args.provider_col or sniff_column(fields, ["provider"])
        year_col = args.year_col or sniff_column(fields, ["cycle", "year"])
        value_col = args.value_col or sniff_column(
            fields, ["placed", "accept"])
        if not (provider_col and year_col and value_col):
            raise SystemExit(f"could not identify columns in {fields}; "
                             f"pass --provider-col/--year-col/--value-col")
        print(f"columns: provider={provider_col!r} year={year_col!r} "
              f"value={value_col!r}")

        n = unmapped = 0
        seen_unmapped = set()
        for rec in reader:
            inst = lookup.get(norm_inst(rec.get(provider_col, "")))
            if inst is None:
                name = rec.get(provider_col, "")
                if name and name not in seen_unmapped:
                    seen_unmapped.add(name)
                    unmapped += 1
                continue
            try:
                value = int(str(rec.get(value_col, "")).replace(",", ""))
            except ValueError:
                continue
            # UCAS cycles are single years ("2025"); store as-is.
            year = norm_year(str(rec.get(year_col, "")))
            if upsert_with_level(db, inst, year, value):
                n += 1
    db.commit()
    db.close()
    print(f"ucas rows written: {n} (providers not in institutions.json: {unmapped})")


def upsert_with_level(db, inst, year, value) -> bool:
    from app.models.models import HesaEnrolment
    row = (
        db.query(HesaEnrolment)
        .filter(HesaEnrolment.institution_id == inst.id,
                HesaEnrolment.academic_year == year,
                HesaEnrolment.level == "ucas_placed")
        .first()
    )
    if row is not None:
        if row.full_time_students != value:
            row.full_time_students = value
            return True
        return False
    db.add(HesaEnrolment(institution_id=inst.id, academic_year=year,
                         full_time_students=value, level="ucas_placed",
                         source="ucas_end_of_cycle_csv"))
    return True


if __name__ == "__main__":
    main()
