"""Demand-side context for a city report: students, balance, affordability.

Conventions follow the sector's published reports: the headline is beds
per full-time student (Student Source's "supply/demand ratio"); forward
scenarios cross demand growth (0/1/2% a year) with pipeline delivery
(none / approved / all identified); affordability compares advertised
rents to the SLC maximum maintenance loan outside London.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.models import HesaEnrolment, Institution, MaintenanceLoan

FORWARD_YEARS = 5
GROWTH_SCENARIOS = [0.0, 0.01, 0.02]
ASSUMED_TENANCY_WEEKS = 51


def gather_demand_context(db: Session, council_id: int) -> dict | None:
    institutions = (
        db.query(Institution)
        .filter(Institution.council_id == council_id)
        .all()
    )
    if not institutions:
        return None

    rows = (
        db.query(HesaEnrolment)
        .filter(HesaEnrolment.institution_id.in_([i.id for i in institutions]),
                HesaEnrolment.level == "all")
        .all()
    )
    if not rows:
        return None

    by_inst: dict[int, dict[str, int]] = {}
    for r in rows:
        by_inst.setdefault(r.institution_id, {})[r.academic_year] = r.full_time_students

    years = sorted({r.academic_year for r in rows}, reverse=True)[:5]
    latest = years[0]

    table = []
    for inst in sorted(institutions, key=lambda i: -(by_inst.get(i.id, {}).get(latest) or 0)):
        table.append({
            "institution": inst.name,
            "adjustment": inst.demand_adjustment,
            "notes": inst.campus_notes,
            "years": {y: by_inst.get(inst.id, {}).get(y) for y in years},
        })
    totals = {
        y: sum(v["years"][y] or 0 for v in table) for y in years
    }
    adjusted_students = sum(
        (by_inst.get(i.id, {}).get(latest) or 0) * i.demand_adjustment
        for i in institutions
    )

    loan = (
        db.query(MaintenanceLoan)
        .filter(MaintenanceLoan.region == "outside_london")
        .order_by(MaintenanceLoan.academic_year.desc())
        .first()
    )

    sources = sorted({r.source for r in rows if r.source})
    return {
        "years": years,
        "latest_year": latest,
        "table": table,
        "totals": totals,
        "adjusted_students": round(adjusted_students),
        "loan_year": loan.academic_year if loan else None,
        "max_loan": loan.max_loan_gbp if loan else None,
        "sources": sources,
    }


def balance_scenarios(students: float, beds_now: int,
                      approved_beds: int, identified_beds: int) -> dict:
    """Beds-per-student grid: growth scenarios x pipeline delivery."""
    cases = [("Current stock only", beds_now),
             ("Plus approved pipeline", beds_now + approved_beds),
             ("Plus all identified pipeline", beds_now + identified_beds)]
    grid = []
    for g in GROWTH_SCENARIOS:
        future_students = students * (1 + g) ** FORWARD_YEARS
        grid.append({
            "growth": f"{g:.0%}",
            "cells": [
                {"case": case, "ratio": round(beds / future_students, 3)}
                for case, beds in cases
            ] if future_students else [],
        })
    return {
        "ratio_now": round(beds_now / students, 3) if students else None,
        "ratio_approved": round((beds_now + approved_beds) / students, 3)
        if students else None,
        "cases": [c for c, _ in cases],
        "grid": grid,
        "horizon": FORWARD_YEARS,
    }


def affordability(db: Session, council_id: int, max_loan: int | None) -> dict | None:
    if not max_loan:
        return None
    rows = db.execute(text("""
        SELECT sr.rent_per_week, COALESCE(sr.contract_length_weeks, :wk) AS weeks
        FROM scheme_rents sr
        JOIN existing_schemes es ON es.id = sr.scheme_id
        WHERE es.council_id = :cid AND sr.is_current
          AND sr.rent_per_week IS NOT NULL
    """), {"cid": council_id, "wk": ASSUMED_TENANCY_WEEKS}).fetchall()
    if not rows:
        return None
    annual = [float(r[0]) * float(r[1]) for r in rows]
    above = sum(1 for a in annual if a > max_loan)
    return {
        "rows": len(annual),
        "above_pct": round(100 * above / len(annual), 1),
        "weekly_loan": round(max_loan / ASSUMED_TENANCY_WEEKS),
        "max_loan": max_loan,
    }
