"""How complete the named schemes and their companies are, and what a step gained.

Counts are over named schemes only (record_class = 'scheme', not retired) and
over the companies that own or operate them. Read-only.

    python fill_report.py                      # print the table
    python fill_report.py --save before_step1  # also store it under data/enrichment/
    python fill_report.py --compare before_step1 after_step1
    python fill_report.py --compare before_step1 after_step1 --markdown
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import engine  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enrichment")
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"

GROUPS = [
    ("Student, operating", "es.scheme_type = 'PBSA' AND coalesce(es.status, 'operational') = 'operational'"),
    ("Build-to-rent, operating", "es.scheme_type = 'BTR' AND es.status = 'operational'"),
    ("Build-to-rent, pipeline", "es.scheme_type = 'BTR' AND es.status IN ('planned', 'under_construction')"),
]

SCHEME_FIELDS = [
    ("Who runs it", "es.operator_company_id IS NOT NULL"),
    ("Who owns it", "es.owner_company_id IS NOT NULL"),
    ("How many beds or homes", f"{SIZE} IS NOT NULL"),
    ("What it charges", """(EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)
        OR EXISTS (SELECT 1 FROM scheme_advertised_rents a WHERE a.scheme_id = es.id AND a.is_current))"""),
    ("Prices by room type", "EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)"),
    ("When it opened", "(es.completion_date IS NOT NULL OR es.build_year IS NOT NULL)"),
    ("When the contract ends", "es.contract_end_date IS NOT NULL"),
    ("How full it is", """(es.occupancy_rate IS NOT NULL OR es.occupancy_pct IS NOT NULL
        OR EXISTS (SELECT 1 FROM scheme_availability v WHERE v.scheme_id = es.id))"""),
    ("Postcode", "nullif(trim(es.postcode), '') IS NOT NULL"),
    ("Map position", "es.lat IS NOT NULL AND es.lng IS NOT NULL"),
]

COMPANY_SET = f"""
    SELECT DISTINCT x.cid FROM existing_schemes es
    CROSS JOIN LATERAL (VALUES (es.operator_company_id), (es.owner_company_id)) x(cid)
    WHERE {NAMED} AND x.cid IS NOT NULL"""
OPERATOR_SET = f"""
    SELECT DISTINCT es.operator_company_id cid FROM existing_schemes es
    WHERE {NAMED} AND es.operator_company_id IS NOT NULL"""

COMPANY_FIELDS = [
    ("Companies House number", "c.companies_house_number IS NOT NULL"),
    ("Registered or head office address", "nullif(trim(coalesce(c.registered_address, c.headquarters_address)), '') IS NOT NULL"),
    ("Website", "nullif(trim(c.website), '') IS NOT NULL"),
    ("Phone number", """(nullif(trim(c.phone), '') IS NOT NULL
        OR EXISTS (SELECT 1 FROM contacts k WHERE k.company_id = c.id AND nullif(trim(k.phone), '') IS NOT NULL))"""),
    ("Email address", """(nullif(trim(c.key_contact_email), '') IS NOT NULL
        OR EXISTS (SELECT 1 FROM contacts k WHERE k.company_id = c.id AND nullif(trim(k.email), '') IS NOT NULL))"""),
    ("A named person", """(nullif(trim(c.key_contact_name), '') IS NOT NULL
        OR EXISTS (SELECT 1 FROM contacts k WHERE k.company_id = c.id AND k.full_name NOT ILIKE 'General enquiries%'))"""),
]


def measure() -> dict:
    out = {"taken_at": dt.datetime.now().isoformat(timespec="seconds"), "schemes": {}, "companies": {}}
    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        for label, where in GROUPS:
            total = c.execute(text(f"SELECT count(*) FROM existing_schemes es WHERE {NAMED} AND {where}")).scalar()
            cells = {"_total": total}
            for fname, cond in SCHEME_FIELDS:
                cells[fname] = c.execute(text(
                    f"SELECT count(*) FROM existing_schemes es WHERE {NAMED} AND {where} AND {cond}")).scalar()
            out["schemes"][label] = cells
        for label, subset in (("All owners and operators", COMPANY_SET), ("Operators", OPERATOR_SET)):
            total = c.execute(text(f"SELECT count(*) FROM ({subset}) s")).scalar()
            cells = {"_total": total}
            for fname, cond in COMPANY_FIELDS:
                cells[fname] = c.execute(text(
                    f"SELECT count(*) FROM ({subset}) s JOIN companies c ON c.id = s.cid WHERE {cond}")).scalar()
            out["companies"][label] = cells
        out["contacts_rows"] = c.execute(text("SELECT count(*) FROM contacts")).scalar()
    return out


def _cell(n: int, total: int) -> str:
    return f"{n:,} ({n / total:.0%})" if total else "0"


def print_table(m: dict) -> None:
    for section, fields in (("schemes", SCHEME_FIELDS), ("companies", COMPANY_FIELDS)):
        groups = list(m[section])
        print("\n" + f"{'':34s}" + "".join(f"{g + ': ' + format(m[section][g]['_total'], ','):>34s}" for g in groups))
        for fname, _ in fields:
            print(f"{fname:34s}" + "".join(f"{_cell(m[section][g][fname], m[section][g]['_total']):>34s}" for g in groups))


def load(label: str) -> dict:
    with open(os.path.join(OUT_DIR, f"fill_{label}.json"), encoding="utf-8") as fh:
        return json.load(fh)


def compare(a: dict, b: dict, markdown: bool) -> None:
    """Only the rows that moved, as before -> after with the gain."""
    for section, fields in (("schemes", SCHEME_FIELDS), ("companies", COMPANY_FIELDS)):
        rows = []
        for g in b[section]:
            tot = b[section][g]["_total"]
            for fname, _ in fields:
                x, y = a[section].get(g, {}).get(fname, 0), b[section][g][fname]
                if x != y:
                    rows.append((g, fname, x, y, tot))
        if not rows:
            continue
        if markdown:
            print(f"\n| {section.title()} | Fact | Before | After | Gain |\n|---|---|---|---|---|")
            for g, fname, x, y, tot in rows:
                print(f"| {g}: {tot:,} | {fname} | {_cell(x, tot)} | {_cell(y, tot)} | {y - x:+,} |")
        else:
            print(f"\n{section}")
            for g, fname, x, y, tot in rows:
                print(f"  {g + ' (' + format(tot, ',') + ')':34s} {fname:34s} {_cell(x, tot):>14s} -> {_cell(y, tot):<14s} {y - x:+,}")
    print(f"\ncontacts rows: {a.get('contacts_rows')} -> {b.get('contacts_rows')}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", metavar="LABEL")
    ap.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"))
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if args.compare:
        compare(load(args.compare[0]), load(args.compare[1]), args.markdown)
        return
    m = measure()
    print_table(m)
    if args.save:
        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"fill_{args.save}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(m, fh, indent=1)
        print(f"\nsaved {path}")


if __name__ == "__main__":
    main()
