"""Load HESA open-data tables used by the demand model.

  --chart4 FILE    Chart 4: FT students by term-time accommodation x
                   entrant marker x year (UK). -> demand-pool coefficients
  --table64 FILE   Table 64: placement marker per provider. Thick-sandwich
                   FT students are on a year-long placement, not in the city.
  --table65 FILE   Table 65: study-abroad marker per provider. FT students
                   abroad for the full session are not in the city.
  --table1 FILE    Table 1: enrolments by provider. Used only to set each
                   institution's UKPRN (the download's mode filter decides
                   whether its totals are FT-comparable, so totals are not
                   loaded unless --table1-fulltime is passed).

Per-provider absence rows land in hesa_enrolments under levels
'ft_placement_full_year' and 'ft_abroad_full_year'.

Usage:
    python load_hesa_open_tables.py --chart4 chart-4.csv \
        --table64 table-64.csv --table65 table-65.csv --table1 table-1.csv
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import HesaEnrolment, HesaTermTimeAccommodation, Institution
from load_demand_layer import norm_inst, norm_year, seed_institutions


def data_rows(path: str, header_first_cell: str) -> list[dict]:
    """HESA CSVs carry a metadata preamble; start at the real header."""
    with open(path, encoding="utf-8-sig", newline="") as fh:
        lines = fh.read().splitlines()
    start = next(i for i, ln in enumerate(lines)
                 if ln.strip('"').startswith(header_first_cell))
    return list(csv.DictReader(io.StringIO("\n".join(lines[start:]))))


def to_int(v) -> int | None:
    try:
        return int(str(v).replace(",", "").strip())
    except ValueError:
        return None


def resolve(lookup, by_ukprn, ukprn: str, name: str) -> Institution | None:
    return by_ukprn.get((ukprn or "").strip()) or lookup.get(norm_inst(name or ""))


def upsert_level(db, inst, year, level, value, source) -> bool:
    row = (db.query(HesaEnrolment)
           .filter(HesaEnrolment.institution_id == inst.id,
                   HesaEnrolment.academic_year == year,
                   HesaEnrolment.level == level).first())
    if row is None:
        db.add(HesaEnrolment(institution_id=inst.id, academic_year=year,
                             full_time_students=value, level=level, source=source))
        return True
    if row.full_time_students != value:
        row.full_time_students, row.source = value, source
        return True
    return False


def load_chart4(db, path) -> int:
    n = 0
    for rec in data_rows(path, "Entrant marker"):
        value = to_int(rec.get("Number"))
        if value is None:
            continue
        year = norm_year(rec["Academic Year"])
        marker = rec["Entrant marker"].strip()
        accom = rec["Term-time accomodation"].strip()  # HESA's spelling
        row = (db.query(HesaTermTimeAccommodation)
               .filter_by(academic_year=year, entrant_marker=marker,
                          accommodation=accom).first())
        if row is None:
            db.add(HesaTermTimeAccommodation(
                academic_year=year, entrant_marker=marker, accommodation=accom,
                students=value, source="hesa_chart4"))
            n += 1
        elif row.students != value:
            row.students = value
            n += 1
    return n


def load_marker_table(db, lookup, by_ukprn, path, marker_col, marker_value,
                      level, source) -> int:
    n = 0
    for rec in data_rows(path, "UKPRN"):
        if (rec.get("Country of HE provider") != "All"
                or rec.get("Region of HE provider") != "All"
                or rec.get("Level of study") != "All"
                or rec.get("Mode of study") != "Full-time"
                or rec.get(marker_col) != marker_value):
            continue
        inst = resolve(lookup, by_ukprn, rec.get("UKPRN"), rec.get("HE Provider"))
        value = to_int(rec.get("Number"))
        if inst is None or value is None:
            continue
        if upsert_level(db, inst, norm_year(rec["Academic year"]), level, value, source):
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--institutions", default="data/institutions.json")
    ap.add_argument("--chart4")
    ap.add_argument("--table64")
    ap.add_argument("--table65")
    ap.add_argument("--table1")
    args = ap.parse_args()

    db = SessionLocal()
    lookup = seed_institutions(db, args.institutions)

    if args.table1:
        set_n = 0
        for rec in data_rows(args.table1, "UKPRN"):
            inst = lookup.get(norm_inst(rec.get("HE provider", "")))
            ukprn = (rec.get("UKPRN") or "").strip()
            if inst is not None and ukprn and inst.ukprn != ukprn:
                inst.ukprn = ukprn
                set_n += 1
        db.flush()
        print(f"table 1: UKPRN set on {set_n} institutions")

    by_ukprn = {i.ukprn: i for i in db.query(Institution).all() if i.ukprn}

    if args.chart4:
        print(f"chart 4: {load_chart4(db, args.chart4)} accommodation rows")
    if args.table64:
        print("table 64: {} placement rows".format(load_marker_table(
            db, lookup, by_ukprn, args.table64, "Placement marker",
            "Thick sandwich", "ft_placement_full_year", "hesa_table64")))
    if args.table65:
        print("table 65: {} study-abroad rows".format(load_marker_table(
            db, lookup, by_ukprn, args.table65, "Study abroad marker",
            "Studying abroad for the full StudentCourseSession",
            "ft_abroad_full_year", "hesa_table65")))
    db.commit()
    db.close()


if __name__ == "__main__":
    main()
