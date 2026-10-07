"""Underwriting data product: scheme-level export and competitor sets.

Two endpoints over the master database:

  GET /api/v2/data/{council}/schemes?format=json|csv
      One row per PBSA scheme with the latest value AND provenance of
      every field, current rents by room type, latest booking state and
      nearby approved pipeline. publication=true keeps only publishable
      (own-sourced) values.

  GET /api/v2/data/competitors?lat=&lng=&radius_km=&beds=
      Everything a site or asset competes with inside a radius: live
      schemes with rents and booking state, plus consented and pending
      pipeline, each with distance.
"""
from __future__ import annotations

import csv
import io
import math
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.auth import get_current_user
from app.census_sources import publishable
from app.database import get_db
from app.models.models import Company, Council, ExistingScheme, PlanningApplication
from app.models.user import User

router = APIRouter(prefix="/api/v2/data", tags=["Data product"])

EXPORT_FIELDS = ["beds_total", "operator", "build_year", "amenities", "observed"]


def _haversine_km(lat1, lng1, lat2, lng2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _scheme_rows(db: Session, schemes: list[ExistingScheme], publication: bool) -> list[dict]:
    ids = [s.id for s in schemes] or [0]
    companies = {c.id: c for c in db.query(Company).filter(Company.id.in_(
        [x for s in schemes for x in (s.operator_company_id, s.owner_company_id) if x] or [0]))}
    rents = db.execute(text("""
        SELECT scheme_id, room_type, sub_classification, rent_per_week,
               contract_length_weeks, academic_year, source, scraped_at
        FROM scheme_rents WHERE is_current AND scheme_id = ANY(:ids)
        ORDER BY scheme_id, rent_per_week
    """), {"ids": ids}).fetchall()
    rent_by: dict[int, list] = {}
    for r in rents:
        rent_by.setdefault(r[0], []).append({
            "room_type": r[1], "tier": r[2], "rent_ppw": float(r[3]) if r[3] else None,
            "weeks": r[4], "academic_year": r[5], "source": r[6],
            "observed_at": r[7].isoformat() if r[7] else None})
    avail = {r[0]: {"state": r[1], "academic_year": r[2], "observed_at": r[3].isoformat()}
             for r in db.execute(text("""
                 SELECT DISTINCT ON (scheme_id) scheme_id, state, academic_year, captured_at
                 FROM scheme_availability WHERE scheme_id = ANY(:ids)
                 ORDER BY scheme_id, captured_at DESC
             """), {"ids": ids}).fetchall()}
    sold_out = {r[0]: r[1].isoformat() for r in db.execute(text("""
        SELECT scheme_id, MIN(captured_at) FROM scheme_availability
        WHERE state = 'sold_out' AND scheme_id = ANY(:ids) GROUP BY scheme_id
    """), {"ids": ids}).fetchall()}

    rows = []
    for s in schemes:
        prov = s.field_provenance or {}
        fields = {}
        for f in EXPORT_FIELDS:
            e = prov.get(f)
            if e is None or (publication and not publishable(e)):
                fields[f] = None
                continue
            fields[f] = {"value": e.get("value"), "source": e.get("source"),
                         "basis": e.get("basis", "observed"), "at": e.get("at"),
                         "ref": e.get("ref")}
        op = companies.get(s.operator_company_id)
        owner = companies.get(s.owner_company_id)
        rows.append({
            "scheme_id": s.id, "name": s.name, "postcode": s.postcode,
            "lat": s.lat or s.latitude, "lng": s.lng or s.longitude,
            "scheme_type": s.scheme_type, "status": s.status,
            "operating_status": s.operating_status,
            "operator": None if publication else (op.name if op else None),
            "operator_company_number": op.companies_house_number if op else None,
            "owner": owner.name if owner else None,
            "fields": fields,
            "rents": rent_by.get(s.id, []),
            "booking": avail.get(s.id),
            "first_sold_out_seen": sold_out.get(s.id),
        })
    return rows


@router.get("/{council_name}/schemes")
def export_schemes(
    council_name: str,
    format: str = Query("json", pattern="^(json|csv)$"),
    publication: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    council = db.query(Council).filter(Council.name.ilike(council_name)).first()
    if council is None:
        raise HTTPException(404, f"Council {council_name!r} not found")
    schemes = (db.query(ExistingScheme)
               .filter(ExistingScheme.council_id == council.id,
                       ExistingScheme.scheme_type == "PBSA").all())
    rows = _scheme_rows(db, schemes, publication)
    if format == "json":
        return {"council": council.name, "count": len(rows), "publication": publication,
                "schemes": rows}

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["scheme_id", "name", "postcode", "lat", "lng", "operator", "owner",
                "beds", "beds_source", "build_year", "build_year_source",
                "min_rent_ppw", "rent_rows", "booking_state", "booking_year",
                "first_sold_out_seen"])
    for r in rows:
        beds, by = r["fields"].get("beds_total"), r["fields"].get("build_year")
        rents = [x["rent_ppw"] for x in r["rents"] if x["rent_ppw"]]
        w.writerow([r["scheme_id"], r["name"], r["postcode"], r["lat"], r["lng"],
                    r["operator"] or (r["fields"]["operator"] or {}).get("value"),
                    r["owner"], beds and beds["value"], beds and beds["source"],
                    by and by["value"], by and by["source"],
                    min(rents) if rents else None, len(r["rents"]),
                    (r["booking"] or {}).get("state"), (r["booking"] or {}).get("academic_year"),
                    r["first_sold_out_seen"]])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="{council.name.lower()}_pbsa_schemes.csv"'})


@router.get("/competitors")
def competitor_set(
    lat: float, lng: float,
    radius_km: float = Query(1.5, gt=0, le=20),
    publication: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Live schemes and pipeline within a radius of a point, with distance."""
    box = radius_km / 111.0
    live = (db.query(ExistingScheme)
            .filter(ExistingScheme.scheme_type == "PBSA",
                    ExistingScheme.lat.between(lat - box, lat + box),
                    ExistingScheme.lng.between(lng - box * 1.6, lng + box * 1.6)).all())
    live = [s for s in live if _haversine_km(lat, lng, s.lat, s.lng) <= radius_km]
    rows = _scheme_rows(db, live, publication)
    for s, r in zip(live, rows):
        r["distance_km"] = round(_haversine_km(lat, lng, s.lat, s.lng), 2)
    rows.sort(key=lambda r: r["distance_km"])

    apps = (db.query(PlanningApplication)
            .filter(PlanningApplication.is_pbsa.is_(True),
                    PlanningApplication.latitude.between(lat - box, lat + box),
                    PlanningApplication.longitude.between(lng - box * 1.6, lng + box * 1.6)).all())
    pipeline = []
    for a in apps:
        d = _haversine_km(lat, lng, a.latitude, a.longitude)
        if d > radius_km:
            continue
        pipeline.append({
            "reference": a.reference, "address": a.address, "status": a.status,
            "decision": a.decision, "beds": a.pbsa_beds,
            "expected_delivery_year": a.expected_delivery_year,
            "construction_status": a.construction_status,
            "decision_date": a.decision_date.isoformat() if a.decision_date else None,
            "distance_km": round(d, 2),
        })
    pipeline.sort(key=lambda p: p["distance_km"])
    return {"centre": {"lat": lat, "lng": lng}, "radius_km": radius_km,
            "live_schemes": rows, "live_beds": sum(
                (r["fields"]["beds_total"] or {}).get("value") or 0 for r in rows),
            "pipeline": pipeline, "pipeline_beds": sum(p["beds"] or 0 for p in pipeline)}
