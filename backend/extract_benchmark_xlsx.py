"""Extract a Student Source-format city PBSA workbook into benchmark JSON.

The output feeds ``reconcile_census.py``: our scheme census is diffed
against it before a city report ships. The source workbooks are licensed
client files — keep them out of git; the extracted JSON is for internal
QA only and must not be republished in a report.

Usage:
    python extract_benchmark_xlsx.py --file "<workbook.xlsx>" \
        --city Birmingham --producer "Student Source" --report-date 2026-01 \
        --out data/benchmarks/birmingham_student_source_jan26.json
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import openpyxl


def _s(v) -> str:
    return str(v).strip() if v is not None else ""


def _int(v):
    try:
        n = int(float(str(v).strip()))
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _year(v):
    m = re.search(r"(19|20)\d\d", _s(v))
    return int(m.group(0)) if m else None


def _header_map(ws, needles: dict[str, list[str]], scan_rows: int = 3):
    """Find the header row and map field -> column index by substring.

    ``needles``: field name -> lowercase substrings tried in order.
    Returns (first data row index, {field: column index}).
    """
    for r, row in enumerate(ws.iter_rows(min_row=1, max_row=scan_rows, values_only=True), start=1):
        cells = [_s(c).lower() for c in row]
        cols = {}
        for field, subs in needles.items():
            for i, cell in enumerate(cells):
                # "^needle" anchors to the start of the header cell, for
                # fields whose needle is a substring of another header
                # ("Beds (Total)" vs "Refused / Proposed Beds (Total)").
                if cell and any(
                    cell.startswith(sub[1:]) if sub.startswith("^") else sub in cell
                    for sub in subs
                ):
                    cols[field] = i
                    break
        required = [f for f, subs in needles.items() if subs and subs[-1] != "?"]
        if sum(1 for f in cols) >= max(2, len(required) - 2):
            return r + 1, cols
    raise SystemExit(f"{ws.title}: header row not found")


def _get(row, cols, field):
    i = cols.get(field)
    return row[i] if i is not None and i < len(row) else None


def extract_private_stock(ws) -> list[dict]:
    start, cols = _header_map(ws, {
        "operator": ["operator"],
        "property": ["property name"],
        "postcode": ["postcode"],
        "beds": ["total beds"],
        "build": ["build date"],
        "captured": ["capture"],
        "notes": ["notes", "pricing / general"],
    })
    rows = []
    for row in ws.iter_rows(min_row=start, values_only=True):
        prop, beds = _s(_get(row, cols, "property")), _int(_get(row, cols, "beds"))
        if not prop or beds is None or "total" in prop.lower():
            continue
        notes = _s(_get(row, cols, "notes"))
        rows.append({
            "property": prop,
            "operator": _s(_get(row, cols, "operator")) or None,
            "postcode": _s(_get(row, cols, "postcode")) or None,
            "beds": beds,
            "build_year": _year(_get(row, cols, "build")),
            "captured": _s(_get(row, cols, "captured")) or None,
            "nominations": "nomination" in notes.lower(),
            "no_letting_presence": "no letting presence" in notes.lower()
            or "letting" in notes.lower() and "no " in notes.lower(),
            "notes": notes or None,
        })
    return rows


def extract_university_stock(ws) -> list[dict]:
    start, cols = _header_map(ws, {
        "university": ["university name"],
        "campus": ["campus", "village name"],
        "property": ["block / property", "property / village", "property name"],
        "postcode": ["postcode"],
        "beds": ["total beds", "beds uuk"],
    })
    rows = []
    for row in ws.iter_rows(min_row=start, values_only=True):
        uni = _s(_get(row, cols, "university"))
        prop = _s(_get(row, cols, "property")) or _s(_get(row, cols, "campus"))
        beds = _int(_get(row, cols, "beds"))
        if not uni or not prop or beds is None or "total" in prop.lower():
            continue
        rows.append({
            "university": uni,
            "property": prop,
            "campus": _s(_get(row, cols, "campus")) or None,
            "postcode": _s(_get(row, cols, "postcode")) or None,
            "beds": beds,
        })
    return rows


def extract_pipeline(ws) -> list[dict]:
    start, cols = _header_map(ws, {
        "site": ["site/property", "site / property"],
        "address": ["street"],
        "postcode": ["postcode"],
        "planning_ref": ["planning ref"],
        "status": ["proposal / application"],
        "checked": ["status review"],
        "build_status": ["glenigan"],
        "operator_clue": ["operator notes"],
        "developer": ["developer"],
        "architect": ["architect"],
        "sector": ["private sector / university"],
        "beds": ["^beds (total"],
        "beds_approved": ["beds approved", "beds confirmed"],
        "studios": ["studios"],
        "delivery": ["delivery year"],
    })
    rows = []
    for row in ws.iter_rows(min_row=start, values_only=True):
        site, status = _s(_get(row, cols, "site")), _s(_get(row, cols, "status"))
        if not site or not status or site.lower().startswith("total"):
            continue
        rows.append({
            "site": site,
            "address": _s(_get(row, cols, "address")) or None,
            "postcode": _s(_get(row, cols, "postcode")) or None,
            "planning_ref": _s(_get(row, cols, "planning_ref")) or None,
            "status": status,
            "status_checked": _s(_get(row, cols, "checked")) or None,
            "build_status": _s(_get(row, cols, "build_status")) or None,
            "operator_clue": _s(_get(row, cols, "operator_clue")) or None,
            "developer": _s(_get(row, cols, "developer")) or None,
            "architect": _s(_get(row, cols, "architect")) or None,
            "sector": _s(_get(row, cols, "sector")) or None,
            "beds": _int(_get(row, cols, "beds")),
            "beds_approved": _int(_get(row, cols, "beds_approved")),
            "studios": _int(_get(row, cols, "studios")),
            "delivery_year": _year(_get(row, cols, "delivery")),
        })
    return rows


def extract_hesa(ws) -> list[dict]:
    header = None
    rows = []
    for row in ws.iter_rows(min_row=1, values_only=True):
        cells = [_s(c) for c in row]
        if header is None:
            if any("Higher Education" in c for c in cells):
                idx = next(i for i, c in enumerate(cells) if "Higher Education" in c)
                header = (idx, [c for c in cells[idx + 1:] if re.match(r"^\d{4}", c)])
            continue
        idx, years = header
        name = cells[idx]
        if not name:
            continue
        values = {}
        for j, year in enumerate(years):
            n = _int(row[idx + 1 + j])
            if n is not None:
                values[year] = n
        if values:
            rows.append({"institution": name, "full_time_students": values})
    return rows


SHEET_HINTS = {
    "private_stock": ["Private Sector Stock"],
    "university_stock": ["University Stock"],
    "pipeline": ["PBSA Pipeline"],
    "hesa": ["HESA Numbers", "HESA Student Numbers"],
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--city", required=True)
    ap.add_argument("--producer", default="Student Source")
    ap.add_argument("--report-date", default=None)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    wb = openpyxl.load_workbook(args.file, read_only=True, data_only=True)

    def sheet(kind):
        for name in SHEET_HINTS[kind]:
            if name in wb.sheetnames:
                return wb[name]
        return None

    out = {
        "city": args.city,
        "producer": args.producer,
        "report_date": args.report_date,
        "license_note": "Licensed client report data. Internal benchmark/QA "
        "use only — never republish these figures.",
        "private_stock": extract_private_stock(sheet("private_stock")),
        "university_stock": extract_university_stock(sheet("university_stock")),
        "pipeline": extract_pipeline(sheet("pipeline")),
        "hesa": extract_hesa(sheet("hesa")) if sheet("hesa") else [],
    }
    out["totals"] = {
        "private_beds": sum(r["beds"] for r in out["private_stock"]),
        "private_schemes": len(out["private_stock"]),
        "university_beds": sum(r["beds"] for r in out["university_stock"]),
        "pipeline_beds": sum(r["beds"] or 0 for r in out["pipeline"]),
        "pipeline_sites": len(out["pipeline"]),
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=1))
    print(f"{args.city}: {out['totals']['private_schemes']} private schemes / "
          f"{out['totals']['private_beds']:,} beds; "
          f"{len(out['university_stock'])} uni properties / "
          f"{out['totals']['university_beds']:,} beds; "
          f"{out['totals']['pipeline_sites']} pipeline sites / "
          f"{out['totals']['pipeline_beds']:,} beds -> {args.out}")


if __name__ == "__main__":
    main()
