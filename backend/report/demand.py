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

from app.models.models import (
    HesaEnrolment, HesaTermTimeAccommodation, Institution, MaintenanceLoan,
)

ABSENCE_LEVELS = ("ft_placement_full_year", "ft_abroad_full_year")
# Term-time situations outside the PBSA market (CBRE's deductions).
NON_MARKET_ACCOMMODATION = ("Parental/guardian home",
                            "Own residence (including rented)")


def accommodation_shares(db: Session, year: str) -> dict | None:
    """National FT term-time accommodation shares for the nearest year.

    Shares exclude 'Not available' from the denominator.
    """
    years = sorted({r[0] for r in db.query(
        HesaTermTimeAccommodation.academic_year).distinct()})
    if not years:
        return None
    use = year if year in years else max((y for y in years if y <= year),
                                         default=years[-1])
    rows = (db.query(HesaTermTimeAccommodation)
            .filter_by(academic_year=use, entrant_marker="All").all())
    known = {r.accommodation: r.students for r in rows
             if r.accommodation != "Not available"}
    total = sum(known.values())
    if not total:
        return None
    share = {k: v / total for k, v in known.items()}
    return {
        "year": use,
        "parental": share.get("Parental/guardian home", 0.0),
        "own_residence": share.get("Own residence (including rented)", 0.0),
        "pbsa_now": share.get("Provider maintained property", 0.0)
        + share.get("Private-sector halls", 0.0),
    }

FORWARD_YEARS = 5
# Bear/bull bands around the modelled (historic-CAGR) growth rate.
SCENARIO_BAND = 0.015
CAGR_FLOOR, CAGR_CAP = -0.03, 0.05
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

    # Demand mix for the latest year: first-years and domicile per
    # institution (HESA table 1 segments), the inputs for propensity.
    seg_rows = (
        db.query(HesaEnrolment)
        .filter(HesaEnrolment.institution_id.in_([i.id for i in institutions]),
                HesaEnrolment.academic_year == latest,
                HesaEnrolment.level.in_(("entrant", "dom_uk", "dom_eu", "dom_non_eu")))
        .all()
    )
    segs: dict[int, dict[str, int]] = {}
    for r in seg_rows:
        segs.setdefault(r.institution_id, {})[r.level] = r.full_time_students
    mix = []
    for inst in institutions:
        total = by_inst.get(inst.id, {}).get(latest)
        s = segs.get(inst.id)
        if not total or not s:
            continue
        intl = (s.get("dom_eu") or 0) + (s.get("dom_non_eu") or 0)
        mix.append({
            "institution": inst.name, "total": total,
            "entrant_pct": round(100 * s["entrant"] / total, 1) if s.get("entrant") else None,
            "intl_pct": round(100 * intl / total, 1) if intl else None,
            "non_eu_pct": round(100 * (s.get("dom_non_eu") or 0) / total, 1)
            if s.get("dom_non_eu") else None,
        })
    mix.sort(key=lambda m: -m["total"])
    city_total = sum(m["total"] for m in mix) or None
    mix_city = None
    if city_total:
        ent = sum(segs[i.id].get("entrant", 0) for i in institutions if i.id in segs)
        intl = sum((segs[i.id].get("dom_eu", 0) + segs[i.id].get("dom_non_eu", 0))
                   for i in institutions if i.id in segs)
        mix_city = {"entrant_pct": round(100 * ent / city_total, 1),
                    "intl_pct": round(100 * intl / city_total, 1)}

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

    # Demand pool: FT students physically in the city (minus year-long
    # placements and full-year study abroad, HESA tables 64/65) and in the
    # rental market (minus parental home and own residence, HESA chart 4
    # national shares).
    absent_rows = (
        db.query(HesaEnrolment)
        .filter(HesaEnrolment.institution_id.in_([i.id for i in institutions]),
                HesaEnrolment.level.in_(ABSENCE_LEVELS))
        .all()
    )
    absent_years = sorted({r.academic_year for r in absent_rows})
    absent_year = (latest if latest in absent_years
                   else (absent_years[0] if absent_years and absent_years[0] > latest
                         else (absent_years[-1] if absent_years else None)))
    adj = {i.id: i.demand_adjustment for i in institutions}
    absent = sum(r.full_time_students * adj[r.institution_id]
                 for r in absent_rows if r.academic_year == absent_year)
    shares = accommodation_shares(db, latest)
    pool = None
    if shares:
        in_city = adjusted_students - absent
        home = shares["parental"] + shares["own_residence"]
        pool = {
            "in_city": round(in_city),
            "absent": round(absent),
            "absent_year": absent_year,
            "shares": shares,
            "pool": round(in_city * (1 - home)),
            # The living-at-home share is the one modelled input. Until
            # provider-level term-time accommodation data is loaded, show
            # the pool as a band around the national share, not a point.
            "cases": [
                {"case": label, "home_share_pct": round(100 * min(max(home + d, 0.0), 0.95), 1),
                 "pool": round(in_city * (1 - min(max(home + d, 0.0), 0.95)))}
                for label, d in (("Low-commuter case (10pp fewer at home)", -0.10),
                                 ("Central (UK full-time shares)", 0.0),
                                 ("Commuter case (10pp more at home)", 0.10))
            ],
        }

    loan = (
        db.query(MaintenanceLoan)
        .filter(MaintenanceLoan.region == "outside_london")
        .order_by(MaintenanceLoan.academic_year.desc())
        .first()
    )

    # City growth model: CAGR over the covered span, clamped to a sane
    # band — replaces guessed flat growth rates in the scenario grid.
    span_years = [y for y in reversed(years) if totals.get(y)]
    cagr = None
    if len(span_years) >= 3:
        first, last = totals[span_years[0]], totals[span_years[-1]]
        if first > 0:
            cagr = (last / first) ** (1 / (len(span_years) - 1)) - 1
            cagr = max(CAGR_FLOOR, min(CAGR_CAP, cagr))

    sources = sorted({r.source for r in rows if r.source})
    return {
        "years": years,
        "latest_year": latest,
        "cagr": cagr,
        "table": table,
        "totals": totals,
        "adjusted_students": round(adjusted_students),
        "pool": pool,
        "mix": mix,
        "mix_city": mix_city,
        "loan_year": loan.academic_year if loan else None,
        "max_loan": loan.max_loan_gbp if loan else None,
        "sources": sources,
    }


def balance_scenarios(students: float, beds_now: int,
                      approved_beds: int, identified_beds: int,
                      model_growth: float | None = None) -> dict:
    """Beds-per-student grid: growth scenarios x pipeline delivery.

    Growth rows are modelled from the city's own enrolment CAGR with a
    bear/bull band, falling back to 0/1/2% when history is too short.
    """
    cases = [("Current stock only", beds_now),
             ("Plus risk-weighted pipeline", beds_now + approved_beds),
             ("Plus all live consents and applications (max)", beds_now + identified_beds)]
    if model_growth is not None:
        scenarios = [
            (f"Bear ({model_growth - SCENARIO_BAND:+.1%})",
             model_growth - SCENARIO_BAND),
            (f"Model — historic CAGR ({model_growth:+.1%})", model_growth),
            (f"Bull ({model_growth + SCENARIO_BAND:+.1%})",
             model_growth + SCENARIO_BAND),
        ]
    else:
        scenarios = [("0%", 0.0), ("1%", 0.01), ("2%", 0.02)]
    grid = []
    for label, g in scenarios:
        future_students = students * (1 + g) ** FORWARD_YEARS
        grid.append({
            "growth": label,
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


def affordability(db: Session, scheme_ids: list[int], max_loan: int | None,
                  excluded=("From (advertised)", "To (advertised)", "Range"),
                  nonstudent=()) -> dict | None:
    if not max_loan:
        return None
    # Room-tier observations on census schemes only: a scheme's "from"
    # price is its cheapest tier and would understate the share of rooms
    # above the loan; area and social rents are not student rooms.
    rows = db.execute(text("""
        SELECT sr.rent_per_week, COALESCE(sr.contract_length_weeks, :wk) AS weeks
        FROM scheme_rents sr
        WHERE sr.scheme_id = ANY(:sids) AND sr.is_current
          AND sr.rent_per_week BETWEEN 60 AND 700
          AND sr.room_type IS NOT NULL
          AND sr.room_type <> ALL(:excluded) AND sr.source <> ALL(:nonstudent)
    """), {"sids": list(scheme_ids) or [0], "wk": ASSUMED_TENANCY_WEEKS,
           "excluded": list(excluded), "nonstudent": list(nonstudent)}).fetchall()
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
