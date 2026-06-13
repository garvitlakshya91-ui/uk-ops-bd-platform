"""Ownership intelligence endpoints.

Surfaces the walked Companies House PSC chains: scheme → owner SPV →
asset-management platform → ultimate owner / PE fund.

  GET /api/v2/ownership/stats             — headline KPIs
  GET /api/v2/ownership/scheme/{id}       — chain for one scheme's owner
  GET /api/v2/ownership/targets           — PE / platform target list
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app.api.auth import get_current_user
from app.models.user import User


router = APIRouter(prefix="/api/v2/ownership", tags=["Ownership"])


class ChainNode(BaseModel):
    level: int
    name: str
    kind: str
    ch_number: Optional[str] = None
    country: Optional[str] = None


class SchemeOwnership(BaseModel):
    scheme_id: int
    scheme_name: str
    owner_company_id: Optional[int]
    owner_name: Optional[str]
    owner_ch_number: Optional[str]
    is_spv_candidate: Optional[bool]
    ultimate_owner_name: Optional[str]
    ultimate_owner_type: Optional[str]
    registered_office: Optional[str]
    chain: list[ChainNode]


@router.get("/stats")
def ownership_stats(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict[str, Any]:
    counts = db.execute(text("""
        SELECT ultimate_owner_type, COUNT(DISTINCT co.id) AS companies,
               COUNT(DISTINCT es.id) AS schemes
        FROM companies co
        LEFT JOIN existing_schemes es ON es.owner_company_id = co.id
        WHERE co.ultimate_owner_type IS NOT NULL
        GROUP BY ultimate_owner_type
        ORDER BY companies DESC
    """)).fetchall()
    walked = db.execute(text(
        "SELECT COUNT(*) FROM companies WHERE ownership_checked_at IS NOT NULL"
    )).scalar()
    spvs = db.execute(text(
        "SELECT COUNT(*) FROM companies WHERE is_spv_candidate = TRUE"
    )).scalar()
    platforms = db.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT office_cluster_key FROM companies
            WHERE COALESCE(office_cluster_key,'') <> ''
            GROUP BY office_cluster_key HAVING COUNT(*) >= 3
        ) q
    """)).scalar()
    return {
        "companies_walked": walked,
        "spv_candidates": spvs,
        "platform_clusters": platforms,
        "by_type": [
            {"type": r[0], "companies": r[1], "schemes": r[2]} for r in counts
        ],
    }


@router.get("/company/{company_id}/holdings")
def company_holdings(
    company_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    limit: int = Query(200, le=500),
) -> dict[str, Any]:
    """HMLR title holdings for a company — its real property portfolio.

    Matches the company's CH number against title proprietors. Flags each
    title as ``in_system`` (we already have a scheme at that postcode) or a
    candidate we're missing. This is the "what does this owner / SPV / PE
    vehicle actually control" view.
    """
    co = db.execute(text(
        "SELECT name, companies_house_number FROM companies WHERE id = :i"
    ), {"i": company_id}).first()
    if not co:
        raise HTTPException(404, "Company not found")
    if not (co[1] or "").strip():
        return {"company": co[0], "ch_number": None, "holdings": [],
                "count": 0, "note": "No Companies House number — cannot match titles."}

    rows = db.execute(text("""
        SELECT t.source, t.title_number, t.address, t.postcode, t.tenure,
               t.price_paid, t.date_added, t.country_1,
               EXISTS (
                   SELECT 1 FROM existing_schemes es
                   WHERE REPLACE(UPPER(COALESCE(es.postcode,'')),' ','') = t.pc_key
               ) AS in_system
        FROM title_ownership t
        WHERE :ch IN (t.ch_number_1, t.ch_number_2, t.ch_number_3, t.ch_number_4)
        ORDER BY in_system, t.price_paid DESC NULLS LAST
        LIMIT :lim
    """), {"ch": co[1].strip(), "lim": limit}).fetchall()

    holdings = [
        {
            "source": r[0], "title_number": r[1], "address": r[2],
            "postcode": r[3], "tenure": r[4], "price_paid": r[5],
            "date_added": str(r[6]) if r[6] else None,
            "country": r[7], "in_system": r[8],
        }
        for r in rows
    ]
    return {
        "company": co[0],
        "ch_number": co[1].strip(),
        "holdings": holdings,
        "count": len(holdings),
        "new_to_system": sum(1 for h in holdings if not h["in_system"]),
    }


@router.get("/scheme/{scheme_id}", response_model=SchemeOwnership)
def scheme_ownership(
    scheme_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SchemeOwnership:
    row = db.execute(text("""
        SELECT es.id, es.name, co.id, co.name, co.companies_house_number,
               co.is_spv_candidate, co.ultimate_owner_name,
               co.ultimate_owner_type, co.registered_address
        FROM existing_schemes es
        LEFT JOIN companies co ON co.id = es.owner_company_id
        WHERE es.id = :i
    """), {"i": scheme_id}).first()
    if not row:
        raise HTTPException(404, "Scheme not found")
    chain: list[ChainNode] = []
    if row[2]:
        nodes = db.execute(text("""
            SELECT level, node_name, node_kind, node_ch_number, node_country
            FROM ownership_chain_nodes
            WHERE company_id = :c
            ORDER BY level, id
        """), {"c": row[2]}).fetchall()
        chain = [ChainNode(level=n[0], name=n[1], kind=n[2],
                           ch_number=n[3], country=n[4]) for n in nodes]
    return SchemeOwnership(
        scheme_id=row[0], scheme_name=row[1],
        owner_company_id=row[2], owner_name=row[3], owner_ch_number=row[4],
        is_spv_candidate=row[5], ultimate_owner_name=row[6],
        ultimate_owner_type=row[7], registered_office=row[8],
        chain=chain,
    )


@router.get("/targets/schemes")
def target_schemes(
    target: str = Query(..., description="Ultimate owner / target name from /targets"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict[str, Any]:
    """All schemes behind one pitch target (same grouping key as /targets)."""
    rows = db.execute(text("""
        SELECT es.id, es.name, cc.name AS council, es.scheme_type,
               COALESCE(es.num_units, es.total_units) AS units,
               es.arrears_risk_score, co.name AS vehicle, es.postcode
        FROM existing_schemes es
        JOIN companies co ON co.id = es.owner_company_id
        LEFT JOIN councils cc ON cc.id = es.council_id
        WHERE co.ownership_checked_at IS NOT NULL
          AND COALESCE(NULLIF(co.ultimate_owner_name,''), co.name) = :t
        ORDER BY COALESCE(es.num_units, es.total_units, 0) DESC NULLS LAST, es.name
        LIMIT 200
    """), {"t": target}).fetchall()
    return {
        "target": target,
        "schemes": [
            {
                "id": r[0], "name": r[1], "council": r[2],
                "scheme_type": r[3], "units": r[4],
                "arrears": r[5], "vehicle": r[6], "postcode": r[7],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/targets")
def ownership_targets(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    owner_type: Optional[str] = Query(None, description="Filter ultimate_owner_type"),
    min_schemes: int = Query(1, ge=1),
    limit: int = Query(100, le=500),
) -> dict[str, Any]:
    """PE / asset-manager pitch list.

    Groups owner companies by ultimate owner (falling back to the company
    itself), with portfolio size, SPV count, councils and distress signal.
    """
    type_clause = ""
    params: dict[str, Any] = {"min": min_schemes, "lim": limit}
    if owner_type:
        type_clause = "AND co.ultimate_owner_type = :ot"
        params["ot"] = owner_type
    rows = db.execute(text(f"""
        SELECT
            COALESCE(NULLIF(co.ultimate_owner_name,''), co.name) AS target,
            MAX(co.ultimate_owner_type) AS owner_type,
            COUNT(DISTINCT co.id) AS vehicles,
            COUNT(DISTINCT co.id) FILTER (WHERE co.is_spv_candidate) AS spvs,
            COUNT(DISTINCT es.id) AS schemes,
            SUM(COALESCE(es.num_units, es.total_units, 0)) AS units,
            ARRAY_AGG(DISTINCT cc.name) AS councils,
            MAX(es.arrears_risk_score) AS max_arrears,
            ARRAY_AGG(DISTINCT co.name) AS vehicle_names
        FROM companies co
        JOIN existing_schemes es ON es.owner_company_id = co.id
        JOIN councils cc ON cc.id = es.council_id
        WHERE co.ownership_checked_at IS NOT NULL
          {type_clause}
        GROUP BY COALESCE(NULLIF(co.ultimate_owner_name,''), co.name)
        HAVING COUNT(DISTINCT es.id) >= :min
        ORDER BY COUNT(DISTINCT es.id) DESC, units DESC NULLS LAST
        LIMIT :lim
    """), params).fetchall()
    return {
        "targets": [
            {
                "target": r[0],
                "owner_type": r[1],
                "vehicles": r[2],
                "spv_count": r[3],
                "schemes": r[4],
                "units": int(r[5] or 0),
                "councils": r[6][:8],
                "max_arrears": r[7],
                "vehicle_names": r[8][:6],
            }
            for r in rows
        ],
        "count": len(rows),
    }
