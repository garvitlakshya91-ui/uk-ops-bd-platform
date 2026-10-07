"""Load Home Office sponsored-study visa grants (quarterly, national).

Takes the entry-clearance visas CSV from a gov.uk quarterly release
(column layouts shift between releases, so columns are sniffed; pass
them explicitly when sniffing misses). Rows upsert on (quarter,
nationality); pass --totals-only to keep just the all-nationalities
series.

Usage:
    python load_visa_stats.py --csv <visas.csv> [--totals-only]
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import VisaIssuance
from load_ucas_acceptances import sniff_column


def norm_quarter(value: str) -> str | None:
    v = value.strip().upper().replace(" ", "")
    m = re.match(r"^(20\d{2})Q([1-4])$", v) or re.match(r"^Q([1-4])(20\d{2})$", v)
    if m:
        g = m.groups()
        return f"{g[0]}Q{g[1]}" if len(g[0]) == 4 else f"{g[1]}Q{g[0]}"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--quarter-col", default=None)
    ap.add_argument("--nationality-col", default=None)
    ap.add_argument("--value-col", default=None)
    ap.add_argument("--study-filter-col", default=None,
                    help="Column holding the visa-type; rows must contain "
                         "'study' or 'sponsored study' in it")
    ap.add_argument("--totals-only", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    with open(args.csv, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        fields = reader.fieldnames or []
        q_col = args.quarter_col or sniff_column(fields, ["quarter"])
        n_col = args.nationality_col or sniff_column(fields, ["nationality"])
        v_col = args.value_col or sniff_column(
            fields, ["grant", "decision", "visas"])
        s_col = args.study_filter_col or sniff_column(
            fields, ["visa type", "application type", "reason"])
        if not (q_col and v_col):
            raise SystemExit(f"could not identify columns in {fields}")
        print(f"columns: quarter={q_col!r} nationality={n_col!r} "
              f"value={v_col!r} filter={s_col!r}")

        totals: dict[tuple[str, str | None], int] = {}
        for rec in reader:
            if s_col and "stud" not in (rec.get(s_col) or "").lower():
                continue
            quarter = norm_quarter(str(rec.get(q_col, "")))
            if quarter is None:
                continue
            try:
                value = int(str(rec.get(v_col, "")).replace(",", ""))
            except ValueError:
                continue
            nationality = (rec.get(n_col) or "").strip() or None if n_col else None
            if args.totals_only:
                nationality = None
            key = (quarter, nationality)
            totals[key] = totals.get(key, 0) + value

    n = 0
    for (quarter, nationality), value in totals.items():
        row = (
            db.query(VisaIssuance)
            .filter(VisaIssuance.quarter == quarter,
                    VisaIssuance.nationality == nationality)
            .first()
        )
        if row is None:
            db.add(VisaIssuance(quarter=quarter, nationality=nationality,
                                visas_granted=value,
                                source=os.path.basename(args.csv)))
            n += 1
        elif row.visas_granted != value:
            row.visas_granted = value
            n += 1
    db.commit()
    db.close()
    print(f"visa rows written: {n} (from {len(totals)} aggregated keys)")


if __name__ == "__main__":
    main()
