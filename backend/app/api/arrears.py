"""Arrears & Operator Distress Hub endpoints.

A single overview endpoint powering the /arrears intelligence page, plus
a per-company drill-down for the Distress Signals card on operator pages.
Pure DB aggregations on top of ``existing_schemes.arrears_risk_score``.
"""

from __future__ import annotations

import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import case, func, or_, desc as sa_desc
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models.models import Company, ExistingScheme
from app.api.auth import get_current_user
from app.models.user import User


router = APIRouter(prefix="/api/v2/arrears", tags=["Arrears"])

BD_TYPES = ("BTR", "PBSA", "Co-living", "Senior")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ArrearsKPIs(BaseModel):
    critical_count: int            # arrears >= 80
    distressed_count: int          # 60-79
    caution_count: int             # 35-59
    healthy_count: int             # 0-34
    distressed_operators: int      # avg(arrears) >= 50 across >= 2 schemes
    newly_flagged_7d: int          # refreshed in last 7d with score >= 60


class ArrearsPlayCard(BaseModel):
    key: str
    title: str
    trigger: str
    count: int
    top: list[dict]


class ArrearsOperatorRow(BaseModel):
    company_id: int
    company_name: str
    ch_number: Optional[str] = None
    scheme_count: int
    avg_arrears: float
    max_arrears: float
    critical_count: int
    latest_signal: Optional[str] = None
    sample_schemes: list[dict]
    last_checked: Optional[str] = None


class ArrearsSchemeRow(BaseModel):
    scheme_id: int
    name: str
    postcode: Optional[str] = None
    council: Optional[str] = None
    operator: Optional[str] = None
    operator_company_id: Optional[int] = None
    scheme_type: Optional[str] = None
    units: Optional[int] = None
    arrears_score: float
    bucket: str
    top_signal: Optional[str] = None
    contract_end_date: Optional[str] = None
    bd_score: Optional[float] = None
    last_checked: Optional[str] = None


class ArrearsSignal(BaseModel):
    scheme_id: int
    scheme_name: str
    operator: Optional[str] = None
    arrears_score: float
    last_checked: str
    summary: str


class ArrearsDistribution(BaseModel):
    healthy: int
    caution: int
    distressed: int
    critical: int


class ArrearsOverviewResponse(BaseModel):
    kpis: ArrearsKPIs
    plays: list[ArrearsPlayCard]
    top_operators: list[ArrearsOperatorRow]
    hot_schemes: list[ArrearsSchemeRow]
    recent_signals: list[ArrearsSignal]
    distribution: ArrearsDistribution
    generated_at: str
    total_scored: int
    total_bd_cohort: int


class CompanyArrearsBreakdown(BaseModel):
    company_id: int
    company_name: str
    ch_number: Optional[str] = None
    scheme_count: int
    scored_count: int
    avg_arrears: Optional[float] = None
    max_arrears: Optional[float] = None
    bucket_counts: ArrearsDistribution
    schemes: list[ArrearsSchemeRow]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bucket(score: Optional[float]) -> str:
    if score is None:
        return "unknown"
    if score >= 80:
        return "critical"
    if score >= 60:
        return "distressed"
    if score >= 35:
        return "caution"
    return "healthy"


def _summarize_signal(score: Optional[float]) -> str:
    """Reconstruct the dominant CH signal label from a live arrears_risk_score.

    Note: the previous implementation read ``scheme.bd_score_breakdown['financial_distress']``,
    but that field is a snapshot captured when BD scores were last backfilled —
    so it lags whenever the arrears refresh runs after a BD-score backfill.
    We now feed the live ``arrears_risk_score`` column directly to keep the
    label in sync with the actual score we display.
    """
    if score is None:
        return ""
    if score >= 90:
        return "Dissolved / Liquidation"
    if score >= 70:
        return "Multiple distress flags"
    if score >= 50:
        return "Overdue filings or recent charge"
    if score >= 35:
        return "Mild distress signal"
    return "Healthy"


def _scheme_to_row(s: ExistingScheme) -> ArrearsSchemeRow:
    return ArrearsSchemeRow(
        scheme_id=s.id,
        name=s.name,
        postcode=s.postcode,
        council=s.council.name if s.council else None,
        operator=s.operator_company.name if s.operator_company else None,
        operator_company_id=s.operator_company_id,
        scheme_type=s.scheme_type,
        units=s.num_units,
        arrears_score=round(s.arrears_risk_score, 1) if s.arrears_risk_score is not None else 0.0,
        bucket=_bucket(s.arrears_risk_score),
        top_signal=_summarize_signal(s.arrears_risk_score),
        contract_end_date=s.contract_end_date.isoformat() if s.contract_end_date else None,
        bd_score=round(float(s.bd_score), 1) if s.bd_score is not None else None,
        last_checked=s.arrears_checked_at.isoformat() if s.arrears_checked_at else None,
    )


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get("/overview", response_model=ArrearsOverviewResponse)
def arrears_overview(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Powers the /arrears intelligence hub page in one round trip."""
    base_q = db.query(ExistingScheme).filter(
        ExistingScheme.scheme_type.in_(BD_TYPES),
    )

    total_bd_cohort = base_q.count()
    total_scored = base_q.filter(ExistingScheme.arrears_risk_score.isnot(None)).count()

    # ---- KPIs ----
    def _bucket_count(lo: float, hi: Optional[float]) -> int:
        q = base_q.filter(ExistingScheme.arrears_risk_score >= lo)
        if hi is not None:
            q = q.filter(ExistingScheme.arrears_risk_score < hi)
        return q.count()

    critical_count = _bucket_count(80, None)
    distressed_count = _bucket_count(60, 80)
    caution_count = _bucket_count(35, 60)
    healthy_count = _bucket_count(0, 35)

    seven_days_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    newly_flagged_7d = base_q.filter(
        ExistingScheme.arrears_risk_score >= 60,
        ExistingScheme.arrears_checked_at >= seven_days_ago,
    ).count()

    operator_agg = (
        db.query(
            Company.id.label("company_id"),
            Company.name.label("company_name"),
            Company.companies_house_number.label("ch_number"),
            func.count(ExistingScheme.id).label("scheme_count"),
            func.avg(ExistingScheme.arrears_risk_score).label("avg_arrears"),
            func.max(ExistingScheme.arrears_risk_score).label("max_arrears"),
            func.sum(
                case((ExistingScheme.arrears_risk_score >= 80, 1), else_=0)
            ).label("critical_count"),
            func.max(ExistingScheme.arrears_checked_at).label("last_checked"),
        )
        .join(ExistingScheme, ExistingScheme.operator_company_id == Company.id)
        .filter(
            ExistingScheme.scheme_type.in_(BD_TYPES),
            ExistingScheme.arrears_risk_score.isnot(None),
        )
        .group_by(Company.id, Company.name, Company.companies_house_number)
        .having(func.count(ExistingScheme.id) >= 2)
        .having(func.avg(ExistingScheme.arrears_risk_score) >= 50)
        .order_by(sa_desc("avg_arrears"))
        .all()
    )
    distressed_operators = len(operator_agg)

    kpis = ArrearsKPIs(
        critical_count=critical_count,
        distressed_count=distressed_count,
        caution_count=caution_count,
        healthy_count=healthy_count,
        distressed_operators=distressed_operators,
        newly_flagged_7d=newly_flagged_7d,
    )

    # ---- BD play cards ----
    plays: list[ArrearsPlayCard] = []

    # Play 1
    plays.append(ArrearsPlayCard(
        key="distressed_operator_pitch",
        title="Pitch distressed operators",
        trigger="Avg arrears >= 50 across >= 2 schemes",
        count=distressed_operators,
        top=[
            {
                "company_id": r.company_id,
                "name": r.company_name,
                "schemes": r.scheme_count,
                "avg_arrears": round(float(r.avg_arrears or 0), 1),
            }
            for r in operator_agg[:3]
        ],
    ))

    # Play 2
    today = datetime.date.today()
    twelve_months = today + datetime.timedelta(days=365)
    play2_q = (
        base_q
        .filter(ExistingScheme.arrears_risk_score >= 70)
        .filter(ExistingScheme.contract_end_date.isnot(None))
        .filter(ExistingScheme.contract_end_date <= twelve_months)
        .filter(ExistingScheme.contract_end_date >= today)
        .order_by(sa_desc(ExistingScheme.arrears_risk_score))
    )
    plays.append(ArrearsPlayCard(
        key="single_scheme_rescue",
        title="Rescue single schemes",
        trigger="Arrears >= 70 AND contract ends within 12 months",
        count=play2_q.count(),
        top=[
            {
                "scheme_id": s.id,
                "name": s.name,
                "score": round(s.arrears_risk_score, 1),
                "contract_end": s.contract_end_date.isoformat() if s.contract_end_date else None,
            }
            for s in play2_q.limit(3).all()
        ],
    ))

    # Play 3
    play3_q = (
        base_q
        .filter(ExistingScheme.arrears_risk_score >= 60)
        .filter(ExistingScheme.arrears_checked_at >= seven_days_ago)
        .order_by(sa_desc(ExistingScheme.arrears_risk_score))
    )
    plays.append(ArrearsPlayCard(
        key="pre_distress_watchlist",
        title="Pre-distress watchlist",
        trigger="Score >= 60 flagged in last 7 days",
        count=play3_q.count(),
        top=[
            {
                "scheme_id": s.id,
                "name": s.name,
                "score": round(s.arrears_risk_score, 1),
            }
            for s in play3_q.limit(3).all()
        ],
    ))

    # Play 4
    plays.append(ArrearsPlayCard(
        key="competitive_intel",
        title="Competitive intel report",
        trigger="Full distress snapshot across BD cohort",
        count=total_scored,
        top=[
            {"label": "Healthy", "count": healthy_count},
            {"label": "Caution", "count": caution_count},
            {"label": "Distressed", "count": distressed_count},
        ],
    ))

    # Play 5
    play5_q = (
        base_q
        .filter(ExistingScheme.arrears_risk_score >= 90)
        .order_by(sa_desc(ExistingScheme.arrears_risk_score))
    )
    plays.append(ArrearsPlayCard(
        key="acquisitions",
        title="Distressed acquisition targets",
        trigger="Arrears >= 90 (dissolved / liquidation signals)",
        count=play5_q.count(),
        top=[
            {
                "scheme_id": s.id,
                "name": s.name,
                "score": round(s.arrears_risk_score, 1),
                "operator": s.operator_company.name if s.operator_company else None,
            }
            for s in play5_q.options(joinedload(ExistingScheme.operator_company)).limit(3).all()
        ],
    ))

    # ---- Operator leaderboard (top 25) ----
    top_operators: list[ArrearsOperatorRow] = []
    for r in operator_agg[:25]:
        sample_q = (
            db.query(ExistingScheme)
            .filter(
                ExistingScheme.operator_company_id == r.company_id,
                ExistingScheme.arrears_risk_score.isnot(None),
            )
            .order_by(sa_desc(ExistingScheme.arrears_risk_score))
            .limit(3)
            .all()
        )
        sample_schemes = [
            {
                "scheme_id": s.id,
                "name": s.name,
                "arrears_score": round(s.arrears_risk_score, 1),
                "units": s.num_units,
            }
            for s in sample_q
        ]
        latest_signal = _summarize_signal(
            sample_q[0].arrears_risk_score if sample_q else None,
        )
        top_operators.append(ArrearsOperatorRow(
            company_id=r.company_id,
            company_name=r.company_name,
            ch_number=r.ch_number,
            scheme_count=r.scheme_count,
            avg_arrears=round(float(r.avg_arrears or 0), 1),
            max_arrears=round(float(r.max_arrears or 0), 1),
            critical_count=int(r.critical_count or 0),
            latest_signal=latest_signal,
            sample_schemes=sample_schemes,
            last_checked=r.last_checked.isoformat() if r.last_checked else None,
        ))

    # ---- Scheme hot list (top 100) ----
    hot_q = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.council),
            joinedload(ExistingScheme.operator_company),
        )
        .filter(
            ExistingScheme.scheme_type.in_(BD_TYPES),
            ExistingScheme.arrears_risk_score.isnot(None),
            ExistingScheme.arrears_risk_score >= 35,
        )
        .order_by(sa_desc(ExistingScheme.arrears_risk_score))
        .limit(100)
        .all()
    )
    hot_schemes = [_scheme_to_row(s) for s in hot_q]

    # ---- Recent signals (last 7 days) ----
    recent_q = (
        db.query(ExistingScheme)
        .options(joinedload(ExistingScheme.operator_company))
        .filter(
            ExistingScheme.scheme_type.in_(BD_TYPES),
            ExistingScheme.arrears_risk_score >= 60,
            ExistingScheme.arrears_checked_at >= seven_days_ago,
        )
        .order_by(sa_desc(ExistingScheme.arrears_checked_at))
        .limit(30)
        .all()
    )
    recent_signals = [
        ArrearsSignal(
            scheme_id=s.id,
            scheme_name=s.name,
            operator=s.operator_company.name if s.operator_company else None,
            arrears_score=round(s.arrears_risk_score, 1),
            last_checked=s.arrears_checked_at.isoformat(),
            summary=_summarize_signal(s.arrears_risk_score),
        )
        for s in recent_q
    ]

    return ArrearsOverviewResponse(
        kpis=kpis,
        plays=plays,
        top_operators=top_operators,
        hot_schemes=hot_schemes,
        recent_signals=recent_signals,
        distribution=ArrearsDistribution(
            healthy=healthy_count, caution=caution_count,
            distressed=distressed_count, critical=critical_count,
        ),
        generated_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        total_scored=total_scored,
        total_bd_cohort=total_bd_cohort,
    )


# ---------------------------------------------------------------------------
# Per-company drill-down
# ---------------------------------------------------------------------------

@router.get("/company/{company_id}", response_model=CompanyArrearsBreakdown)
def company_arrears_breakdown(
    company_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Per-company arrears breakdown for the Distress Signals card on
    /companies/{id} and the operator leaderboard row drill-down.
    """
    co = db.query(Company).filter(Company.id == company_id).first()
    if not co:
        raise HTTPException(status_code=404, detail="Company not found")

    schemes_q = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.council),
            joinedload(ExistingScheme.operator_company),
        )
        .filter(
            or_(
                ExistingScheme.operator_company_id == company_id,
                ExistingScheme.owner_company_id == company_id,
            )
        )
        .order_by(sa_desc(ExistingScheme.arrears_risk_score.nullslast()))
        .all()
    )

    scored = [s for s in schemes_q if s.arrears_risk_score is not None]
    avg_arrears = round(sum(s.arrears_risk_score for s in scored) / len(scored), 1) if scored else None
    max_arrears = round(max((s.arrears_risk_score for s in scored), default=0.0), 1) if scored else None

    buckets = ArrearsDistribution(
        healthy=sum(1 for s in scored if s.arrears_risk_score < 35),
        caution=sum(1 for s in scored if 35 <= s.arrears_risk_score < 60),
        distressed=sum(1 for s in scored if 60 <= s.arrears_risk_score < 80),
        critical=sum(1 for s in scored if s.arrears_risk_score >= 80),
    )

    return CompanyArrearsBreakdown(
        company_id=co.id,
        company_name=co.name,
        ch_number=co.companies_house_number,
        scheme_count=len(schemes_q),
        scored_count=len(scored),
        avg_arrears=avg_arrears,
        max_arrears=max_arrears,
        bucket_counts=buckets,
        schemes=[_scheme_to_row(s) for s in schemes_q],
    )
