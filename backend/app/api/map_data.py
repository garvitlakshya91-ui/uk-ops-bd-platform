"""Map data endpoint — compact coordinates payload for the map page.

Returns every scheme with coordinates in a minimal array form so the
frontend can cluster-render tens of thousands of points:

    GET /api/v2/map/schemes?scope=bd|all

Keys are single letters to keep the payload small:
    i=id, n=name, la=lat, ln=lng, t=scheme_type, u=units,
    b=bd_score, h=arrears_risk_score (operator health), c=council
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db
from app.api.auth import get_current_user
from app.models.user import User

router = APIRouter(prefix="/api/v2/map", tags=["Map"])

BD_TYPES = ("BTR", "PBSA", "Co-living", "Senior")


@router.get("/schemes")
def map_schemes(
    scope: Literal["bd", "all"] = Query("bd"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict[str, Any]:
    type_clause = (
        "AND es.scheme_type IN ('BTR','PBSA','Co-living','Senior')"
        if scope == "bd" else ""
    )
    rows = db.execute(text(f"""
        SELECT es.id, es.name,
               COALESCE(es.lat, es.latitude)  AS la,
               COALESCE(es.lng, es.longitude) AS ln,
               es.scheme_type,
               COALESCE(es.num_units, es.total_units),
               es.bd_score, es.arrears_risk_score, cc.name
        FROM existing_schemes es
        LEFT JOIN councils cc ON cc.id = es.council_id
        WHERE COALESCE(es.lat, es.latitude) IS NOT NULL
          {type_clause}
    """)).fetchall()
    return {
        "count": len(rows),
        "schemes": [
            {
                "i": r[0], "n": r[1], "la": round(r[2], 5),
                "ln": round(r[3], 5), "t": r[4], "u": r[5],
                "b": r[6], "h": r[7], "c": r[8],
            }
            for r in rows
        ],
    }


@router.get("/applications")
def map_applications(
    scope: Literal["bd", "all"] = Query("bd"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    limit: int = Query(50000, le=80000),
) -> dict[str, Any]:
    """Planning applications with coordinates, for the applications map.

    scope=bd (default) returns only BD-scored applications (~14k);
    scope=all returns every geocoded application (~40k). Only ~3% of the
    full 1.36M planning table is geocoded, so ``all`` is still bounded.

    Compact keys: i=id, n=address, la=lat, ln=lng, t=scheme_type,
    u=units, b=bd_score, s=status, r=reference, c=council.
    """
    bd_clause = "AND pa.bd_score IS NOT NULL" if scope == "bd" else ""
    rows = db.execute(text(f"""
        SELECT pa.id, pa.address, pa.latitude, pa.longitude,
               pa.scheme_type, pa.num_units, pa.bd_score, pa.status,
               pa.reference, cc.name
        FROM planning_applications pa
        LEFT JOIN councils cc ON cc.id = pa.council_id
        WHERE pa.latitude IS NOT NULL AND pa.longitude IS NOT NULL
          {bd_clause}
        ORDER BY pa.bd_score DESC NULLS LAST
        LIMIT :lim
    """), {"lim": limit}).fetchall()
    return {
        "count": len(rows),
        "applications": [
            {
                "i": str(r[0]), "n": (r[1] or "")[:80],
                "la": round(r[2], 5), "ln": round(r[3], 5),
                "t": r[4], "u": r[5], "b": r[6], "s": r[7],
                "r": r[8], "c": r[9],
            }
            for r in rows
        ],
    }
