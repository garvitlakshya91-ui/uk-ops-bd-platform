"""Adapter endpoints that reshape backend data to match the Next.js frontend's
expected TypeScript types.

The existing API returns nested ORM objects; the frontend expects flat JSON with
renamed fields and computed values.  These v2 / dashboard adapter routes sit
alongside the original API and delegate to the same DB models.
"""

import datetime
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import func, and_, cast, Date as SADate
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models.models import (
    PlanningApplication,
    PipelineOpportunity,
    ExistingScheme,
    SchemeContract,
    SchemeRent,
    Alert,
    Council,
    Company,
    Contact,
    ScraperRun,
)
from app.api.auth import get_current_user, require_role
from app.models.user import User
from app.api.permissions import get_allowed_alert_types

router = APIRouter(prefix="/api", tags=["Frontend Adapters"])

# ---------------------------------------------------------------------------
# Priority mapping: backend -> frontend
# ---------------------------------------------------------------------------
_PRIORITY_TO_FRONTEND = {"hot": "high", "warm": "medium", "cold": "low"}
_PRIORITY_TO_BACKEND = {"high": "hot", "medium": "warm", "low": "cold"}


# =========================================================================
# 1. GET /api/dashboard/stats
# =========================================================================

class DashboardStatsAdapted(BaseModel):
    total_applications: int
    new_this_week: int
    pipeline_opportunities: int
    contracts_expiring_6m: int
    total_applications_trend: float
    new_this_week_trend: float
    pipeline_trend: float
    contracts_trend: float


@router.get("/dashboard/stats", response_model=DashboardStatsAdapted)
def dashboard_stats_adapted(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    now = datetime.datetime.now(datetime.timezone.utc)
    week_ago = now - datetime.timedelta(days=7)
    today = datetime.date.today()
    six_months = today + datetime.timedelta(days=182)

    total_applications = (
        db.query(func.count(PlanningApplication.id)).scalar() or 0
    )
    new_this_week = (
        db.query(func.count(PlanningApplication.id))
        .filter(
            (PlanningApplication.submission_date >= week_ago)
            | (
                PlanningApplication.submission_date.is_(None)
                & (PlanningApplication.created_at >= week_ago)
            )
        )
        .scalar()
        or 0
    )
    pipeline_opportunities = (
        db.query(func.count(PipelineOpportunity.id)).scalar() or 0
    )
    contracts_expiring_6m = (
        db.query(func.count(ExistingScheme.id))
        .filter(
            and_(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date >= today,
                ExistingScheme.contract_end_date <= six_months,
            )
        )
        .scalar()
        or 0
    )

    return DashboardStatsAdapted(
        total_applications=total_applications,
        new_this_week=new_this_week,
        pipeline_opportunities=pipeline_opportunities,
        contracts_expiring_6m=contracts_expiring_6m,
        total_applications_trend=0,
        new_this_week_trend=0,
        pipeline_trend=0,
        contracts_trend=0,
    )


# =========================================================================
# 2. GET /api/dashboard/trends?days=30
# =========================================================================

class TrendPoint(BaseModel):
    date: str
    applications: int
    opportunities: int


@router.get("/dashboard/trends", response_model=list[TrendPoint])
def dashboard_trends(
    days: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days - 1)

    # Applications grouped by date
    app_rows = (
        db.query(
            cast(PlanningApplication.created_at, SADate).label("day"),
            func.count(PlanningApplication.id),
        )
        .filter(cast(PlanningApplication.created_at, SADate) >= start_date)
        .group_by(cast(PlanningApplication.created_at, SADate))
        .all()
    )
    app_map: dict[datetime.date, int] = {row[0]: row[1] for row in app_rows}

    # Opportunities grouped by date
    opp_rows = (
        db.query(
            cast(PipelineOpportunity.created_at, SADate).label("day"),
            func.count(PipelineOpportunity.id),
        )
        .filter(cast(PipelineOpportunity.created_at, SADate) >= start_date)
        .group_by(cast(PipelineOpportunity.created_at, SADate))
        .all()
    )
    opp_map: dict[datetime.date, int] = {row[0]: row[1] for row in opp_rows}

    result: list[TrendPoint] = []
    for offset in range(days):
        d = start_date + datetime.timedelta(days=offset)
        result.append(
            TrendPoint(
                date=d.isoformat(),
                applications=app_map.get(d, 0),
                opportunities=opp_map.get(d, 0),
            )
        )

    return result


# =========================================================================
# 3. GET /api/dashboard/top-opportunities
# =========================================================================

class TopOpportunity(BaseModel):
    id: str
    company_name: str
    company_id: str
    scheme_type: Optional[str] = None
    stage: str
    bd_score: Optional[float] = None
    priority: str
    assigned_to: Optional[str] = None
    council: Optional[str] = None
    units: Optional[int] = None
    estimated_value: Optional[float] = None
    last_activity: Optional[str] = None
    created_at: str


@router.get("/dashboard/top-opportunities", response_model=list[TopOpportunity])
def dashboard_top_opportunities(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    opps = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application)
            .joinedload(PlanningApplication.council),
            joinedload(PipelineOpportunity.scheme)
            .joinedload(ExistingScheme.council),
        )
        .order_by(PipelineOpportunity.bd_score.desc().nullslast())
        .limit(10)
        .all()
    )

    result: list[TopOpportunity] = []
    for opp in opps:
        # Derive council name + units + scheme_type from linked application or scheme
        council_name: Optional[str] = None
        units: Optional[int] = None
        scheme_type: Optional[str] = None

        if opp.planning_application:
            pa = opp.planning_application
            council_name = pa.council.name if pa.council else None
            units = pa.num_units
            scheme_type = pa.scheme_type
        elif opp.scheme:
            s = opp.scheme
            council_name = s.council.name if s.council else None
            units = s.num_units
            scheme_type = s.scheme_type

        result.append(
            TopOpportunity(
                id=str(opp.id),
                company_name=opp.company.name if opp.company else "",
                company_id=str(opp.company_id),
                scheme_type=scheme_type,
                stage=opp.stage,
                bd_score=opp.bd_score,
                priority=_PRIORITY_TO_FRONTEND.get(opp.priority, opp.priority),
                assigned_to=opp.assigned_to,
                council=council_name,
                units=units,
                estimated_value=None,
                last_activity=(
                    opp.last_contact_date.isoformat()
                    if opp.last_contact_date
                    else None
                ),
                created_at=opp.created_at.isoformat(),
            )
        )

    return result


# =========================================================================
# 4. GET /api/v2/applications  (flattened)
# =========================================================================

class ApplicationFlat(BaseModel):
    id: str
    reference: str
    address: Optional[str] = None
    postcode: Optional[str] = None
    council: Optional[str] = None
    scheme_type: str
    units: Optional[int] = None
    status: Optional[str] = None
    applicant: Optional[str] = None
    date: Optional[str] = None
    bd_score: Optional[float] = None
    description: Optional[str] = None
    case_officer: Optional[str] = None
    decision_date: Optional[str] = None


class ApplicationFlatListResponse(BaseModel):
    items: list[ApplicationFlat]
    total: int
    skip: int
    limit: int


@router.get("/v2/application-councils")
def list_application_councils(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Return list of councils that have planning applications (for filter dropdowns)."""
    results = (
        db.query(Council.name)
        .join(PlanningApplication, PlanningApplication.council_id == Council.id)
        .distinct()
        .order_by(Council.name)
        .all()
    )
    return [r[0] for r in results]


@router.get("/v2/scheme-councils")
def list_scheme_councils(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Return councils that have at least one existing scheme (for filter dropdowns).

    Returns ``{id, name}`` because the /v2/schemes endpoint filters by ``council_id``.
    """
    results = (
        db.query(Council.id, Council.name)
        .join(ExistingScheme, ExistingScheme.council_id == Council.id)
        .distinct()
        .order_by(Council.name)
        .all()
    )
    return [{"id": r[0], "name": r[1]} for r in results]


@router.get("/v2/applications", response_model=ApplicationFlatListResponse)
def list_applications_flat(
    skip: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=2000),
    council_id: Optional[int] = None,
    council: Optional[str] = None,
    scheme_type: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = "desc",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from sqlalchemy import or_, desc as sa_desc, asc as sa_asc

    query = (
        db.query(PlanningApplication)
        .options(
            joinedload(PlanningApplication.council),
            joinedload(PlanningApplication.pipeline_opportunity),
        )
    )

    # Default to newest-first when no explicit sort is given.
    if sort_by is None:
        sort_by = "date"

    if council_id is not None:
        query = query.filter(PlanningApplication.council_id == council_id)
    if council is not None:
        query = query.filter(PlanningApplication.council.has(Council.name == council))
    if scheme_type is not None:
        query = query.filter(PlanningApplication.scheme_type == scheme_type)
    if status is not None:
        query = query.filter(PlanningApplication.status == status)
    if search is not None:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                PlanningApplication.reference.ilike(pattern),
                PlanningApplication.address.ilike(pattern),
                PlanningApplication.postcode.ilike(pattern),
                PlanningApplication.description.ilike(pattern),
                PlanningApplication.applicant_name.ilike(pattern),
                PlanningApplication.council.has(Council.name.ilike(pattern)),
            )
        )

    total = query.count()

    # Sorting
    dir_fn = sa_asc if sort_dir == "asc" else sa_desc
    sort_map = {
        "units": PlanningApplication.num_units,
        "date": PlanningApplication.submission_date,
        "bd_score": PlanningApplication.num_units,  # proxy: larger = higher BD value
    }
    order_col = sort_map.get(sort_by, PlanningApplication.submission_date)
    items = (
        query.order_by(dir_fn(order_col).nullslast())
        .offset(skip)
        .limit(limit)
        .all()
    )

    # BD score weights for inline calculation
    SCHEME_SCORES = {"BTR": 100, "PBSA": 85, "Co-living": 80, "Senior": 70, "Affordable": 60, "Mixed": 65, "Residential": 50}
    STATUS_SCORES = {
        "Approved": 100, "Permissioned": 90, "Pending Decision": 65,
        "Allocated": 55, "Pre-Application": 70, "Pending": 60,
        "Refused": 20, "Withdrawn": 10,
    }

    def _compute_bd_score(a: PlanningApplication) -> float:
        """Quick inline BD score: 40% scheme type + 30% units + 30% status."""
        s = SCHEME_SCORES.get(a.scheme_type or "", 30)
        u = min(100, ((a.num_units or 0) / 500) * 100)
        st = STATUS_SCORES.get(a.status or "", 40)
        return round(s * 0.4 + u * 0.3 + st * 0.3, 1)

    def _extract_applicant(a: PlanningApplication) -> Optional[str]:
        """Extract applicant from applicant_name or description."""
        if a.applicant_name:
            # Skip if it looks like a planning reference
            # (e.g. "2024/12345/PA", "PA/2024/0123", "DC/24/00123")
            if not re.search(r"\d+/|/\d+", a.applicant_name):
                return a.applicant_name
        # Try to extract from description
        desc = a.description or ""
        for pattern in [
            r"(?:applicant|developer|client|proposed by|submitted by)[:\s]+([A-Z][A-Za-z\s&']+?)(?:\.|,|$)",
            r"on behalf of\s+([A-Z][A-Za-z\s&']+?)(?:\.|,|\s+for|\s+at|$)",
        ]:
            m = re.search(pattern, desc)
            if m:
                name = m.group(1).strip()
                # Must look like a proper noun (capitalized) and not generic text
                if len(name) > 3 and len(name) < 100 and name[0].isupper():
                    return name
        return None

    flat_items: list[ApplicationFlat] = []
    for app in items:
        bd_score = _compute_bd_score(app)
        if app.pipeline_opportunity and app.pipeline_opportunity.bd_score:
            bd_score = app.pipeline_opportunity.bd_score

        flat_items.append(
            ApplicationFlat(
                id=str(app.id),
                reference=app.reference,
                address=app.address,
                postcode=app.postcode,
                council=app.council.name if app.council else None,
                scheme_type=app.scheme_type,
                units=app.num_units,
                status=app.status,
                applicant=_extract_applicant(app),
                date=(
                    app.submission_date.isoformat()
                    if app.submission_date
                    else None
                ),
                bd_score=bd_score,
                description=app.description,
                case_officer=getattr(app, 'case_officer', None),
                decision_date=(
                    app.decision_date.isoformat()
                    if app.decision_date
                    else None
                ),
            )
        )

    return ApplicationFlatListResponse(
        items=flat_items, total=total, skip=skip, limit=limit
    )


# =========================================================================
# 5. GET /api/v2/pipeline  (flattened)
# =========================================================================

class PipelineFlat(BaseModel):
    id: str
    company_name: str
    company_id: str
    scheme_type: Optional[str] = None
    stage: str
    bd_score: Optional[float] = None
    priority: str
    assigned_to: Optional[str] = None
    council: Optional[str] = None
    units: Optional[int] = None
    estimated_value: Optional[float] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    notes: Optional[str] = None
    last_activity: Optional[str] = None
    created_at: str


class PipelineFlatListResponse(BaseModel):
    items: list[PipelineFlat]
    total: int
    skip: int
    limit: int


@router.get("/v2/pipeline", response_model=PipelineFlatListResponse)
def list_pipeline_flat(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    stage: Optional[str] = None,
    priority: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company).joinedload(Company.contacts),
            joinedload(PipelineOpportunity.planning_application)
            .joinedload(PlanningApplication.council),
            joinedload(PipelineOpportunity.scheme)
            .joinedload(ExistingScheme.council),
        )
    )

    # Auto-filter for analysts: only see their own records
    if current_user.role == "bd_analyst":
        query = query.filter(PipelineOpportunity.assigned_to_user_id == current_user.id)

    if stage is not None:
        query = query.filter(PipelineOpportunity.stage == stage)
    if priority is not None:
        # Accept frontend priority values and map to backend
        backend_priority = _PRIORITY_TO_BACKEND.get(priority, priority)
        query = query.filter(PipelineOpportunity.priority == backend_priority)

    total = query.count()
    items = (
        query.order_by(PipelineOpportunity.bd_score.desc().nullslast())
        .offset(skip)
        .limit(limit)
        .all()
    )

    flat_items: list[PipelineFlat] = []
    for opp in items:
        # Derive council, units, scheme_type from linked entity
        council_name: Optional[str] = None
        units: Optional[int] = None
        scheme_type: Optional[str] = None

        if opp.planning_application:
            pa = opp.planning_application
            council_name = pa.council.name if pa.council else None
            units = pa.num_units
            scheme_type = pa.scheme_type
        elif opp.scheme:
            s = opp.scheme
            council_name = s.council.name if s.council else None
            units = s.num_units
            scheme_type = s.scheme_type

        # Primary contact from the company
        contact_name: Optional[str] = None
        contact_email: Optional[str] = None
        if opp.company and opp.company.contacts:
            primary = opp.company.contacts[0]
            contact_name = primary.full_name
            contact_email = primary.email

        flat_items.append(
            PipelineFlat(
                id=str(opp.id),
                company_name=opp.company.name if opp.company else "",
                company_id=str(opp.company_id),
                scheme_type=scheme_type,
                stage=opp.stage,
                bd_score=opp.bd_score,
                priority=_PRIORITY_TO_FRONTEND.get(opp.priority, opp.priority),
                assigned_to=opp.assigned_to,
                council=council_name,
                units=units,
                estimated_value=None,
                contact_name=contact_name,
                contact_email=contact_email,
                notes=opp.notes,
                last_activity=(
                    opp.last_contact_date.isoformat()
                    if opp.last_contact_date
                    else None
                ),
                created_at=opp.created_at.isoformat(),
            )
        )

    return PipelineFlatListResponse(
        items=flat_items, total=total, skip=skip, limit=limit
    )


# =========================================================================
# 6. GET /api/v2/schemes  (flattened)
# =========================================================================

class ScoreBreakdownFlat(BaseModel):
    contract_proximity: float = 0
    performance_gap: float = 0
    market_opportunity: float = 0
    relationship_strength: float = 0
    scheme_size: float = 0


class SchemeFlat(BaseModel):
    id: str
    name: str
    operator: Optional[str] = None
    council: Optional[str] = None
    region: Optional[str] = None
    units: Optional[int] = None
    contract_end: Optional[str] = None
    performance: Optional[float] = None
    satisfaction: Optional[float] = None
    bd_score: Optional[float] = None
    priority: Optional[str] = None
    scheme_type: Optional[str] = None
    address: Optional[str] = None
    postcode: Optional[str] = None
    owner: Optional[str] = None
    asset_manager: Optional[str] = None
    landlord: Optional[str] = None
    contract_start: Optional[str] = None
    regulatory_rating: Optional[str] = None
    financial_health: Optional[float] = None
    status: Optional[str] = None
    data_confidence: Optional[float] = None
    last_verified: Optional[str] = None
    occupancy_rate: Optional[float] = None
    revenue_per_unit: Optional[float] = None
    score_breakdown: Optional[ScoreBreakdownFlat] = None
    operator_company_id: Optional[str] = None
    pipeline_opportunity_id: Optional[str] = None
    locked_fields: dict[str, str] = {}
    min_rent_per_week: Optional[float] = None
    rent_tier_count: int = 0


class SchemeFlatListResponse(BaseModel):
    items: list[SchemeFlat]
    total: int
    skip: int
    limit: int


def _resolve_contract_end(scheme: ExistingScheme, db: Session) -> Optional[str]:
    """Return contract_end_date from scheme or its current SchemeContract."""
    if scheme.contract_end_date:
        return scheme.contract_end_date.isoformat()
    # Fallback: check SchemeContract table for current contract
    current_contract = (
        db.query(SchemeContract)
        .filter(
            SchemeContract.scheme_id == scheme.id,
            SchemeContract.is_current.is_(True),
        )
        .first()
    )
    if current_contract and current_contract.contract_end_date:
        return current_contract.contract_end_date.isoformat()
    return None


def _resolve_contract_start(scheme: ExistingScheme, db: Session) -> Optional[str]:
    """Return contract_start_date from scheme or its current SchemeContract."""
    if scheme.contract_start_date:
        return str(scheme.contract_start_date)
    current_contract = (
        db.query(SchemeContract)
        .filter(
            SchemeContract.scheme_id == scheme.id,
            SchemeContract.is_current.is_(True),
        )
        .first()
    )
    if current_contract and current_contract.contract_start_date:
        return str(current_contract.contract_start_date)
    return None


def _compute_score_breakdown(scheme: ExistingScheme, db: Session) -> ScoreBreakdownFlat:
    """Compute BD score breakdown components for a scheme."""
    # Contract proximity
    contract_end = _resolve_contract_end(scheme, db)
    if contract_end:
        try:
            end_date = datetime.date.fromisoformat(contract_end)
            months_remaining = (end_date - datetime.date.today()).days / 30.0
            if months_remaining <= 6:
                contract_proximity = 100.0
            elif months_remaining <= 12:
                contract_proximity = 80.0
            elif months_remaining <= 24:
                contract_proximity = 50.0
            else:
                contract_proximity = 20.0
        except (ValueError, TypeError):
            contract_proximity = 30.0
    else:
        contract_proximity = 30.0

    # Performance gap (inverted — lower performance = higher opportunity)
    perf = scheme.performance_rating
    performance_gap = (100.0 - perf) if perf is not None else 50.0

    # Market opportunity (based on satisfaction — lower = higher opportunity)
    sat = scheme.satisfaction_score
    market_opportunity = (100.0 - sat) if sat is not None else 50.0

    # Relationship strength (inverse of financial health risk)
    fin = scheme.financial_health_score
    relationship_strength = fin if fin is not None else 50.0

    # Scheme size
    units = scheme.num_units or 0
    if units > 500:
        scheme_size = 100.0
    elif units > 200:
        scheme_size = 70.0
    elif units > 100:
        scheme_size = 50.0
    else:
        scheme_size = 30.0

    return ScoreBreakdownFlat(
        contract_proximity=round(contract_proximity, 1),
        performance_gap=round(performance_gap, 1),
        market_opportunity=round(market_opportunity, 1),
        relationship_strength=round(relationship_strength, 1),
        scheme_size=round(scheme_size, 1),
    )


def _compute_bd_score(breakdown: ScoreBreakdownFlat) -> float:
    """Weighted BD score from breakdown components."""
    return round(
        breakdown.contract_proximity * 0.35
        + breakdown.performance_gap * 0.25
        + breakdown.market_opportunity * 0.15
        + breakdown.relationship_strength * 0.15
        + breakdown.scheme_size * 0.10,
        1,
    )


def _derive_priority(bd_score: float) -> str:
    """Derive priority label from BD score."""
    if bd_score >= 70:
        return "high"
    if bd_score >= 45:
        return "medium"
    return "low"


@router.get("/v2/schemes", response_model=SchemeFlatListResponse)
def list_schemes_flat(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=5000),
    search: Optional[str] = None,
    scheme_type: Optional[str] = None,
    source: Optional[str] = None,
    council_id: Optional[int] = None,
    region: Optional[str] = Query(None, description="Filter by council region"),
    sort_by: Optional[str] = Query(None, description="Sort field: name, units, scheme_type, postcode, council, owner, operator, min_rent"),
    sort_dir: Optional[str] = Query("asc", description="Sort direction: asc or desc"),
    has_owner: Optional[bool] = Query(None, description="Filter by owner presence"),
    has_operator: Optional[bool] = Query(None, description="Filter by operator presence"),
    has_rent: Optional[bool] = Query(None, description="Filter schemes that have any rent tier"),
    min_units: Optional[int] = Query(None, description="Minimum units filter"),
    max_units: Optional[int] = Query(None, description="Maximum units filter"),
    min_rent_per_week: Optional[float] = Query(None, description="Minimum weekly rent filter"),
    max_rent_per_week: Optional[float] = Query(None, description="Maximum weekly rent filter"),
    contract_end_within_days: Optional[int] = Query(None, description="Contract ends within N days from now"),
    operator_company_id: Optional[list[int]] = Query(None, description="Filter by operator company id(s) (repeatable)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from sqlalchemy import or_, func as sa_func
    import datetime as _dt

    query = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.council),
            joinedload(ExistingScheme.pipeline_opportunity),
            joinedload(ExistingScheme.asset_manager_company),
            joinedload(ExistingScheme.landlord_company),
        )
    )

    if scheme_type is not None:
        query = query.filter(ExistingScheme.scheme_type == scheme_type)
    if source is not None:
        query = query.filter(ExistingScheme.source == source)
    if council_id is not None:
        query = query.filter(ExistingScheme.council_id == council_id)
    if region is not None:
        query = query.filter(ExistingScheme.council.has(Council.region == region))
    if operator_company_id:
        query = query.filter(ExistingScheme.operator_company_id.in_(operator_company_id))
    if contract_end_within_days is not None and contract_end_within_days > 0:
        today = _dt.date.today()
        horizon = today + _dt.timedelta(days=contract_end_within_days)
        query = query.filter(
            ExistingScheme.contract_end_date.isnot(None),
            ExistingScheme.contract_end_date >= today,
            ExistingScheme.contract_end_date <= horizon,
        )
    if search:
        like_term = f"%{search}%"
        # Check if the search term looks like a numeric scheme ID
        search_conditions = [
            ExistingScheme.name.ilike(like_term),
            ExistingScheme.address.ilike(like_term),
            ExistingScheme.postcode.ilike(like_term),
            ExistingScheme.operator_company.has(Company.name.ilike(like_term)),
            ExistingScheme.owner_company.has(Company.name.ilike(like_term)),
            ExistingScheme.council.has(Council.name.ilike(like_term)),
        ]
        # Support exact scheme ID search for numeric terms
        if search.strip().isdigit():
            search_conditions.append(ExistingScheme.id == int(search.strip()))
        query = query.filter(or_(*search_conditions))

    if has_owner is True:
        query = query.filter(ExistingScheme.owner_company_id.isnot(None))
    elif has_owner is False:
        query = query.filter(ExistingScheme.owner_company_id.is_(None))

    if has_operator is True:
        query = query.filter(ExistingScheme.operator_company_id.isnot(None))
    elif has_operator is False:
        query = query.filter(ExistingScheme.operator_company_id.is_(None))

    if min_units is not None:
        query = query.filter(ExistingScheme.num_units >= min_units)
    if max_units is not None:
        query = query.filter(ExistingScheme.num_units <= max_units)

    # Per-scheme rent summary (min + count) as a subquery we can reuse
    # for filtering, sorting, and response population.
    rent_summary = (
        db.query(
            SchemeRent.scheme_id.label("sid"),
            sa_func.min(SchemeRent.rent_per_week).label("min_rent_wk"),
            sa_func.count(SchemeRent.id).label("rent_count"),
        )
        .group_by(SchemeRent.scheme_id)
        .subquery()
    )

    # has_rent / rent-range filters all require the subquery join
    rent_join_applied = False
    needs_rent_join = (
        has_rent is True
        or min_rent_per_week is not None
        or max_rent_per_week is not None
    )
    if needs_rent_join:
        query = query.join(rent_summary, rent_summary.c.sid == ExistingScheme.id)
        rent_join_applied = True
    elif has_rent is False:
        query = query.outerjoin(
            rent_summary, rent_summary.c.sid == ExistingScheme.id
        ).filter(rent_summary.c.sid.is_(None))
        rent_join_applied = True

    if min_rent_per_week is not None:
        query = query.filter(rent_summary.c.min_rent_wk >= min_rent_per_week)
    if max_rent_per_week is not None:
        query = query.filter(rent_summary.c.min_rent_wk <= max_rent_per_week)

    total = query.count()

    # Server-side sorting
    from sqlalchemy import desc as sa_desc, asc as sa_asc, nulls_last

    sort_column = ExistingScheme.name  # default
    if sort_by == "units":
        sort_column = ExistingScheme.num_units
    elif sort_by == "scheme_type":
        sort_column = ExistingScheme.scheme_type
    elif sort_by == "postcode":
        sort_column = ExistingScheme.postcode
    elif sort_by == "name":
        sort_column = ExistingScheme.name
    elif sort_by == "contract_end":
        sort_column = ExistingScheme.contract_end_date
    elif sort_by == "bd_score":
        sort_column = ExistingScheme.num_units  # fallback; bd_score is computed
    elif sort_by == "min_rent":
        # Join the subquery if not already joined (outer to preserve schemes without rent)
        if not rent_join_applied:
            query = query.outerjoin(
                rent_summary, rent_summary.c.sid == ExistingScheme.id
            )
            rent_join_applied = True
        sort_column = rent_summary.c.min_rent_wk

    if sort_dir == "desc":
        query = query.order_by(nulls_last(sa_desc(sort_column)))
    else:
        query = query.order_by(nulls_last(sa_asc(sort_column)))

    items = (
        query.offset(skip)
        .limit(limit)
        .all()
    )

    # Fetch rent summaries for the page of results
    rent_by_scheme: dict[int, tuple[Optional[float], int]] = {}
    if items:
        scheme_ids = [s.id for s in items]
        rent_rows = (
            db.query(
                SchemeRent.scheme_id,
                sa_func.min(SchemeRent.rent_per_week),
                sa_func.count(SchemeRent.id),
            )
            .filter(SchemeRent.scheme_id.in_(scheme_ids))
            .group_by(SchemeRent.scheme_id)
            .all()
        )
        rent_by_scheme = {sid: (mn, cnt) for sid, mn, cnt in rent_rows}

    flat_items: list[SchemeFlat] = []
    for scheme in items:
        # BD score: prefer stored pipeline_opportunity, else compute on-the-fly
        bd_score: Optional[float] = None
        priority: Optional[str] = None
        breakdown = _compute_score_breakdown(scheme, db)

        if scheme.pipeline_opportunity and scheme.pipeline_opportunity.bd_score is not None:
            bd_score = scheme.pipeline_opportunity.bd_score
            priority = _PRIORITY_TO_FRONTEND.get(
                scheme.pipeline_opportunity.priority,
                scheme.pipeline_opportunity.priority,
            )
        else:
            bd_score = _compute_bd_score(breakdown)
            priority = _derive_priority(bd_score)

        contract_end = _resolve_contract_end(scheme, db)
        contract_start = _resolve_contract_start(scheme, db)

        flat_items.append(
            SchemeFlat(
                id=str(scheme.id),
                name=scheme.name,
                operator=(
                    scheme.operator_company.name
                    if scheme.operator_company
                    else None
                ),
                council=scheme.council.name if scheme.council else None,
                region=scheme.council.region if scheme.council else None,
                units=scheme.num_units,
                contract_end=contract_end,
                performance=scheme.performance_rating,
                satisfaction=scheme.satisfaction_score,
                bd_score=bd_score,
                priority=priority,
                scheme_type=scheme.scheme_type,
                address=scheme.address,
                postcode=scheme.postcode,
                owner=scheme.owner_company.name if scheme.owner_company else None,
                asset_manager=scheme.asset_manager_company.name if hasattr(scheme, 'asset_manager_company') and scheme.asset_manager_company else None,
                landlord=scheme.landlord_company.name if hasattr(scheme, 'landlord_company') and scheme.landlord_company else None,
                contract_start=contract_start,
                regulatory_rating=scheme.regulatory_rating,
                financial_health=scheme.financial_health_score,
                status=scheme.status if hasattr(scheme, 'status') else None,
                data_confidence=scheme.data_confidence_score if hasattr(scheme, 'data_confidence_score') else None,
                last_verified=str(scheme.last_verified_at) if hasattr(scheme, 'last_verified_at') and scheme.last_verified_at else None,
                occupancy_rate=None,
                revenue_per_unit=None,
                score_breakdown=breakdown,
                operator_company_id=str(scheme.operator_company_id) if scheme.operator_company_id else None,
                pipeline_opportunity_id=str(scheme.pipeline_opportunity.id) if scheme.pipeline_opportunity else None,
                locked_fields=scheme.locked_fields or {},
                min_rent_per_week=rent_by_scheme.get(scheme.id, (None, 0))[0],
                rent_tier_count=rent_by_scheme.get(scheme.id, (None, 0))[1],
            )
        )

    return SchemeFlatListResponse(
        items=flat_items, total=total, skip=skip, limit=limit
    )


# =========================================================================
# 6a. PATCH /api/v2/schemes/{scheme_id}/field  (manual override + lock)
# =========================================================================

class SchemeFieldPatchRequest(BaseModel):
    field: str
    value: Any = None


class SchemeFieldPatchResponse(BaseModel):
    scheme_id: int
    field: str
    applied: bool
    new_value: Any = None
    locked_by: Optional[str] = None
    message: str


@router.patch("/v2/schemes/{scheme_id}/field", response_model=SchemeFieldPatchResponse)
def patch_scheme_field(
    scheme_id: int,
    body: SchemeFieldPatchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Manually set a single field on a scheme and lock it to source='manual'.

    Manual is the highest-precedence source — this write will always succeed
    (provided validation passes) and prevents any scraper / AI enrichment
    from overwriting until the user explicitly clears the lock.
    """
    from app.scrapers.field_protection import (
        set_field,
        FieldValidationError,
        WRITABLE_FIELDS,
        PROTECTED_FIELDS,
    )

    # Frontend-friendly aliases for company-name fields
    ALIAS_MAP = {
        "owner": "owner_company_id",
        "operator": "operator_company_id",
        "asset_manager": "asset_manager_company_id",
        "landlord": "landlord_company_id",
    }
    field_to_write = ALIAS_MAP.get(body.field, body.field)

    if field_to_write not in WRITABLE_FIELDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Field not writable: {body.field!r}. Allowed aliases: {list(ALIAS_MAP)}, fields: {sorted(WRITABLE_FIELDS)}",
        )

    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheme not found",
        )

    # Company-name patches: resolve / create Company, then set the _company_id FK
    value_to_write = body.value
    if body.field in ALIAS_MAP:
        # field_to_write already resolved via ALIAS_MAP above
        if body.value:
            # Resolve or create a Company
            name = str(body.value).strip()
            norm = name.lower()
            existing_co = (
                db.query(Company)
                .filter(Company.normalized_name == norm)
                .first()
            )
            if existing_co:
                value_to_write = existing_co.id
            else:
                new_co = Company(
                    name=name[:255],
                    normalized_name=norm[:255],
                    company_type="Operator" if body.field == "operator" else "Developer",
                    is_active=True,
                )
                db.add(new_co)
                db.flush()
                value_to_write = new_co.id
        else:
            value_to_write = None

    try:
        applied = set_field(
            scheme, field_to_write, value_to_write,
            source="manual", db=db,
            changed_by=f"user:{current_user.email}",
        )
    except FieldValidationError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    db.commit()
    db.refresh(scheme)

    new_val = getattr(scheme, field_to_write, None)
    lock_source = (scheme.locked_fields or {}).get(field_to_write)
    return SchemeFieldPatchResponse(
        scheme_id=scheme_id,
        field=field_to_write,
        applied=applied,
        new_value=str(new_val) if new_val is not None else None,
        locked_by=lock_source,
        message=f"{'Applied' if applied else 'No-op (unchanged)'}"
                + (f" and locked (manual)" if applied and field_to_write in PROTECTED_FIELDS else ""),
    )


# =========================================================================
# 6b. GET /api/v2/schemes/{scheme_id}/contracts
# =========================================================================

class SchemeContractFlat(BaseModel):
    id: str
    contract_reference: Optional[str] = None
    contract_type: Optional[str] = None
    operator: Optional[str] = None
    client: Optional[str] = None
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    contract_value: Optional[float] = None
    currency: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = None
    is_current: Optional[bool] = None
    created_at: Optional[str] = None


@router.get("/v2/schemes/{scheme_id}/contracts", response_model=list[SchemeContractFlat])
def get_scheme_contracts(
    scheme_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return all contracts for a given scheme, ordered by date descending."""
    contracts = (
        db.query(SchemeContract)
        .filter(SchemeContract.scheme_id == scheme_id)
        .order_by(SchemeContract.contract_end_date.desc().nullslast())
        .all()
    )

    results: list[SchemeContractFlat] = []
    for c in contracts:
        operator_name = None
        client_name = None
        if c.operator_company_id:
            op = db.query(Company).get(c.operator_company_id)
            if op:
                operator_name = op.name
        if c.client_company_id:
            cl = db.query(Company).get(c.client_company_id)
            if cl:
                client_name = cl.name

        results.append(SchemeContractFlat(
            id=str(c.id),
            contract_reference=c.contract_reference,
            contract_type=c.contract_type,
            operator=operator_name,
            client=client_name,
            contract_start=c.contract_start_date.isoformat() if c.contract_start_date else None,
            contract_end=c.contract_end_date.isoformat() if c.contract_end_date else None,
            contract_value=float(c.contract_value) if c.contract_value else None,
            currency=c.currency,
            source=c.source,
            source_reference=c.source_reference,
            is_current=c.is_current,
            created_at=str(c.created_at) if c.created_at else None,
        ))

    return results


# =========================================================================
# 6c. GET /api/v2/schemes/{scheme_id}/rents
# =========================================================================

class SchemeRentFlat(BaseModel):
    id: str
    room_type: Optional[str] = None
    rent_per_week: Optional[float] = None
    rent_per_month: Optional[float] = None
    currency: str = "GBP"
    academic_year: Optional[str] = None
    contract_length_weeks: Optional[int] = None
    source: Optional[str] = None
    scraped_at: Optional[str] = None


@router.get("/v2/schemes/{scheme_id}/rents", response_model=list[SchemeRentFlat])
def get_scheme_rents(
    scheme_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return all rent tiers for a given scheme, cheapest first."""
    rents = (
        db.query(SchemeRent)
        .filter(SchemeRent.scheme_id == scheme_id)
        .order_by(
            SchemeRent.academic_year.desc().nullslast(),
            SchemeRent.rent_per_week.asc().nullslast(),
        )
        .all()
    )
    return [
        SchemeRentFlat(
            id=str(r.id),
            room_type=r.room_type,
            rent_per_week=r.rent_per_week,
            rent_per_month=r.rent_per_month,
            currency=r.currency or "GBP",
            academic_year=r.academic_year,
            contract_length_weeks=r.contract_length_weeks,
            source=r.source,
            scraped_at=r.scraped_at.isoformat() if r.scraped_at else None,
        )
        for r in rents
    ]


# =========================================================================
# 6d1. GET /api/v2/schemes/{id}/competitors  (real competitors, not mocks)
# =========================================================================

class CompetitorFlat(BaseModel):
    operator_id: int
    operator_name: str
    scheme_count: int
    avg_units: Optional[float] = None
    has_rent_data: bool = False
    sample_scheme_name: Optional[str] = None
    sample_scheme_id: Optional[str] = None


@router.get("/v2/schemes/{scheme_id}/competitors", response_model=list[CompetitorFlat])
def get_scheme_competitors(
    scheme_id: int,
    limit: int = Query(6, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return competing operators for a scheme — i.e. OTHER operators who run
    schemes of the same scheme_type in the same region.

    Not a full market-analysis feature; just surfaces who else you'd be
    competing against for this kind of work in this area.
    """
    from sqlalchemy import func as sa_func, desc as sa_desc, and_

    scheme = (
        db.query(ExistingScheme)
        .options(joinedload(ExistingScheme.council))
        .filter(ExistingScheme.id == scheme_id)
        .first()
    )
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")

    scheme_type = scheme.scheme_type
    region = scheme.council.region if scheme.council else None
    own_operator_id = scheme.operator_company_id

    if not scheme_type:
        return []

    conds = [
        ExistingScheme.scheme_type == scheme_type,
        ExistingScheme.operator_company_id.isnot(None),
    ]
    if own_operator_id:
        conds.append(ExistingScheme.operator_company_id != own_operator_id)
    if region:
        # Prefer same-region competitors; fall back below if zero.
        conds.append(ExistingScheme.council.has(Council.region == region))

    def _aggregate(conditions):
        return (
            db.query(
                Company.id,
                Company.name,
                sa_func.count(ExistingScheme.id).label("cnt"),
                sa_func.avg(ExistingScheme.num_units).label("avg_u"),
                sa_func.min(ExistingScheme.id).label("sample_id"),
                sa_func.min(ExistingScheme.name).label("sample_name"),
            )
            .join(ExistingScheme, ExistingScheme.operator_company_id == Company.id)
            .filter(and_(*conditions))
            .group_by(Company.id, Company.name)
            .order_by(sa_desc("cnt"))
            .limit(limit)
            .all()
        )

    rows = _aggregate(conds)
    # If no region matches, broaden to the whole UK for that scheme type.
    if not rows and region:
        rows = _aggregate([c for c in conds if c is not conds[-1]] if len(conds) > 2 else conds[:2])

    if not rows:
        return []

    # Figure out which operators have rent data (any scheme of theirs has a rent tier).
    op_ids = [r[0] for r in rows]
    rent_op_ids = set(
        r[0] for r in db.query(ExistingScheme.operator_company_id)
        .join(SchemeRent, SchemeRent.scheme_id == ExistingScheme.id)
        .filter(ExistingScheme.operator_company_id.in_(op_ids))
        .distinct()
        .all()
    )

    results: list[CompetitorFlat] = []
    for r in rows:
        results.append(CompetitorFlat(
            operator_id=r[0],
            operator_name=r[1],
            scheme_count=int(r[2] or 0),
            avg_units=float(r[3]) if r[3] is not None else None,
            has_rent_data=r[0] in rent_op_ids,
            sample_scheme_id=str(r[4]) if r[4] else None,
            sample_scheme_name=r[5],
        ))
    return results


# =========================================================================
# 6d. GET /api/v2/operators/autocomplete
# =========================================================================

class OperatorAutocompleteItem(BaseModel):
    id: int
    name: str
    scheme_count: int


@router.get("/v2/operators/autocomplete", response_model=list[OperatorAutocompleteItem])
def autocomplete_operators(
    q: str = Query("", description="Substring to match (case-insensitive)"),
    limit: int = Query(10, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return top operator companies matching ``q``, ranked by how many
    schemes they operate. Empty q returns the top-N operators overall."""
    from sqlalchemy import func as sa_func, desc as sa_desc

    qn = db.query(
        Company.id,
        Company.name,
        sa_func.count(ExistingScheme.id).label("scheme_count"),
    ).join(
        ExistingScheme, ExistingScheme.operator_company_id == Company.id
    ).group_by(Company.id, Company.name)

    if q:
        qn = qn.filter(Company.name.ilike(f"%{q}%"))

    rows = qn.order_by(sa_desc("scheme_count")).limit(limit).all()
    return [
        OperatorAutocompleteItem(id=r[0], name=r[1], scheme_count=int(r[2]))
        for r in rows
    ]


# =========================================================================
# 6e. GET /api/v2/schemes/filter-options
# =========================================================================

class FilterOptionCount(BaseModel):
    value: str
    count: int
    label: Optional[str] = None


class SchemesFilterOptions(BaseModel):
    sources: list[FilterOptionCount]
    scheme_types: list[FilterOptionCount]
    regions: list[str]


_SOURCE_LABELS = {
    "epc_new_dwelling": "EPC New Dwelling",
    "arl_btr_open_operating": "ARL BTR",
    "pbsa_operator": "PBSA Operator",
    "find_a_tender": "Find a Tender",
    "contracts_finder": "Contracts Finder",
    "rsh": "RSH",
    "manual": "Manual",
}


@router.get("/v2/schemes/filter-options", response_model=SchemesFilterOptions)
def get_schemes_filter_options(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return distinct values available for scheme filters (for dropdown
    population). One row per (value, count)."""
    from sqlalchemy import func as sa_func, desc as sa_desc

    source_rows = (
        db.query(ExistingScheme.source, sa_func.count(ExistingScheme.id))
        .filter(ExistingScheme.source.isnot(None))
        .group_by(ExistingScheme.source)
        .order_by(sa_desc(sa_func.count(ExistingScheme.id)))
        .all()
    )
    scheme_type_rows = (
        db.query(ExistingScheme.scheme_type, sa_func.count(ExistingScheme.id))
        .filter(ExistingScheme.scheme_type.isnot(None))
        .group_by(ExistingScheme.scheme_type)
        .order_by(sa_desc(sa_func.count(ExistingScheme.id)))
        .all()
    )
    region_rows = (
        db.query(Council.region)
        .filter(Council.region.isnot(None))
        .distinct()
        .order_by(Council.region)
        .all()
    )
    return SchemesFilterOptions(
        sources=[
            FilterOptionCount(
                value=src, count=int(cnt), label=_SOURCE_LABELS.get(src, src),
            )
            for src, cnt in source_rows
        ],
        scheme_types=[
            FilterOptionCount(value=t, count=int(cnt), label=t)
            for t, cnt in scheme_type_rows
        ],
        regions=[r[0] for r in region_rows if r[0]],
    )


# =========================================================================
# 7. GET /api/v2/alerts  (renamed fields)
# =========================================================================

class AlertAdapted(BaseModel):
    id: str
    type: str
    title: str
    message: Optional[str] = None
    timestamp: str
    read: bool


class AlertAdaptedListResponse(BaseModel):
    items: list[AlertAdapted]
    total: int
    skip: int
    limit: int
    unread_count: int


@router.get("/v2/alerts", response_model=AlertAdaptedListResponse)
def list_alerts_adapted(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    alert_type: Optional[str] = None,
    is_read: Optional[bool] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(Alert)

    # Filter alert types by role
    allowed_types = get_allowed_alert_types(current_user)
    if allowed_types is not None:
        query = query.filter(Alert.type.in_(allowed_types))

    if alert_type is not None:
        query = query.filter(Alert.type == alert_type)
    if is_read is not None:
        query = query.filter(Alert.is_read == is_read)

    total = query.count()
    unread_count = (
        db.query(func.count(Alert.id))
        .filter(Alert.is_read == False)  # noqa: E712
        .scalar()
        or 0
    )
    items = query.order_by(Alert.created_at.desc()).offset(skip).limit(limit).all()

    adapted: list[AlertAdapted] = [
        AlertAdapted(
            id=str(a.id),
            type=a.type,
            title=a.title,
            message=a.message,
            timestamp=a.created_at.isoformat(),
            read=a.is_read,
        )
        for a in items
    ]

    return AlertAdaptedListResponse(
        items=adapted,
        total=total,
        skip=skip,
        limit=limit,
        unread_count=unread_count,
    )


# =========================================================================
# 8. GET /api/v2/scrapers/health  (computed status)
# =========================================================================

class ScraperHealthAdapted(BaseModel):
    council_id: str
    council_name: str
    portal_type: str
    last_run: Optional[str] = None
    success_rate: Optional[float] = None
    applications_found: int
    status: str
    error_message: Optional[str] = None


@router.get("/v2/scrapers/health", response_model=list[ScraperHealthAdapted])
def scraper_health_adapted(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    now = datetime.datetime.now(datetime.timezone.utc)
    seven_days_ago = now - datetime.timedelta(days=7)

    councils = (
        db.query(Council)
        .filter(Council.active == True)  # noqa: E712
        .order_by(Council.name)
        .all()
    )

    result: list[ScraperHealthAdapted] = []
    for council in councils:
        # Latest run
        latest_run = (
            db.query(ScraperRun)
            .filter(ScraperRun.council_id == council.id)
            .order_by(ScraperRun.started_at.desc())
            .first()
        )

        # Recent runs for success rate
        recent_runs = (
            db.query(ScraperRun)
            .filter(
                ScraperRun.council_id == council.id,
                ScraperRun.started_at >= seven_days_ago,
            )
            .all()
        )

        total_recent = len(recent_runs)
        success_count = sum(1 for r in recent_runs if r.status == "success")
        success_rate: Optional[float] = None
        if total_recent > 0:
            success_rate = round(success_count / total_recent * 100, 1)

        # Total applications found across recent runs
        applications_found = sum(r.applications_found for r in recent_runs)

        # Compute status
        is_overdue = False
        if council.last_scraped_at:
            delta = now - council.last_scraped_at
            hours_since = delta.total_seconds() / 3600
            is_overdue = hours_since > council.scrape_frequency_hours

        last_run_status = latest_run.status if latest_run else None

        if is_overdue:
            status_label = "overdue"
        elif last_run_status == "failed":
            status_label = "error"
        elif last_run_status == "running":
            status_label = "running"
        elif last_run_status in ("success", "partial"):
            status_label = "healthy"
        else:
            status_label = "unknown"

        # Error message from latest failed run
        error_message: Optional[str] = None
        if latest_run and latest_run.status == "failed" and latest_run.error_details:
            details = latest_run.error_details
            if isinstance(details, dict):
                error_message = details.get("message") or str(details)
            else:
                error_message = str(details)

        result.append(
            ScraperHealthAdapted(
                council_id=str(council.id),
                council_name=council.name,
                portal_type=council.portal_type,
                last_run=(
                    council.last_scraped_at.isoformat()
                    if council.last_scraped_at
                    else None
                ),
                success_rate=success_rate,
                applications_found=applications_found,
                status=status_label,
                error_message=error_message,
            )
        )

    return result


# =========================================================================
# 9. POST /api/scrapers/{council_id}/trigger  (adapter)
# =========================================================================

class TriggerAdaptedResponse(BaseModel):
    message: str
    council_id: int
    council_name: str
    scraper_run_id: int


@router.post(
    "/scrapers/{council_id}/trigger",
    response_model=TriggerAdaptedResponse,
)
def trigger_scraper_adapted(council_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Adapter: frontend sends council_id as a path param; backend trigger
    endpoint expects it in the request body.  We bridge the two."""
    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")
    if not council.active:
        raise HTTPException(status_code=400, detail="Council scraper is not active")

    running = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.council_id == council.id,
            ScraperRun.status == "running",
        )
        .first()
    )
    if running:
        raise HTTPException(
            status_code=409,
            detail="A scrape is already running for this council",
        )

    run = ScraperRun(council_id=council.id, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    from app.tasks.scraping_tasks import scrape_council

    scrape_council.delay(council.id)

    return TriggerAdaptedResponse(
        message="Scraper run initiated",
        council_id=council.id,
        council_name=council.name,
        scraper_run_id=run.id,
    )


# =========================================================================
# 10. GET /api/scrapers/{council_id}/history  (adapter)
# =========================================================================

class ScraperRunAdapted(BaseModel):
    id: int
    council_id: int
    started_at: str
    completed_at: Optional[str] = None
    status: str
    applications_found: int
    applications_new: int
    applications_updated: int
    errors_count: int
    error_details: Optional[dict] = None
    duration_seconds: Optional[float] = None


@router.get(
    "/scrapers/{council_id}/history",
    response_model=list[ScraperRunAdapted],
)
def scraper_history_adapted(
    council_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Adapter: frontend calls GET /api/scrapers/{council_id}/history;
    backend has GET /api/scrapers/runs?council_id=X.  We bridge the two."""
    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")

    runs = (
        db.query(ScraperRun)
        .filter(ScraperRun.council_id == council_id)
        .order_by(ScraperRun.started_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return [
        ScraperRunAdapted(
            id=r.id,
            council_id=r.council_id,
            started_at=r.started_at.isoformat(),
            completed_at=r.completed_at.isoformat() if r.completed_at else None,
            status=r.status,
            applications_found=r.applications_found,
            applications_new=r.applications_new,
            applications_updated=r.applications_updated,
            errors_count=r.errors_count,
            error_details=r.error_details,
            duration_seconds=r.duration_seconds,
        )
        for r in runs
    ]


# =========================================================================
# 11. PATCH /api/pipeline/{id}/stage  (adapter)
# =========================================================================

class StageUpdateRequest(BaseModel):
    stage: str


class StageUpdateResponse(BaseModel):
    id: str
    stage: str
    updated_at: str


VALID_STAGES = [
    "identified",
    "researched",
    "contacted",
    "meeting",
    "proposal",
    "won",
    "lost",
]


@router.patch("/pipeline/{opportunity_id}/stage", response_model=StageUpdateResponse)
def update_pipeline_stage_adapted(
    opportunity_id: int,
    body: StageUpdateRequest,
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    """Adapter: frontend sends PATCH /api/pipeline/{id}/stage with JSON body;
    backend has PUT /api/pipeline/{id}/stage with a query param.  We bridge."""
    if body.stage not in VALID_STAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid stage. Must be one of: {VALID_STAGES}",
        )

    opp = (
        db.query(PipelineOpportunity)
        .filter(PipelineOpportunity.id == opportunity_id)
        .first()
    )
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    opp.stage = body.stage
    db.commit()
    db.refresh(opp)

    return StageUpdateResponse(
        id=str(opp.id),
        stage=opp.stage,
        updated_at=opp.updated_at.isoformat(),
    )


# =========================================================================
# 12. PATCH /api/alerts/{id}/read  (adapter)
# =========================================================================

class AlertReadResponse(BaseModel):
    id: str
    read: bool


@router.patch("/alerts/{alert_id}/read", response_model=AlertReadResponse)
def mark_alert_read_adapted(alert_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Adapter: frontend sends PATCH /api/alerts/{id}/read;
    backend has PUT /api/alerts/{id}/read.  We bridge."""
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    alert.is_read = True
    db.commit()
    db.refresh(alert)

    return AlertReadResponse(id=str(alert.id), read=alert.is_read)


# =========================================================================
# 13. GET /api/v2/companies  (with counts)
# =========================================================================

class ContactFlat(BaseModel):
    id: str
    name: str
    role: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class LinkedApplicationFlat(BaseModel):
    id: str
    reference: Optional[str] = None
    address: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    date: Optional[str] = None


class LinkedSchemeFlat(BaseModel):
    id: str
    name: str
    units: Optional[int] = None
    scheme_type: Optional[str] = None


class CompanyAdapted(BaseModel):
    id: str
    name: str
    type: Optional[str] = None
    companies_house_number: Optional[str] = None
    applications_count: int
    schemes_count: int
    contacts_count: int
    contacts: list[ContactFlat] = []
    linked_applications: list[LinkedApplicationFlat] = []
    linked_schemes: list[LinkedSchemeFlat] = []


class CompanyAdaptedListResponse(BaseModel):
    items: list[CompanyAdapted]
    total: int
    skip: int
    limit: int


@router.get("/v2/companies", response_model=CompanyAdaptedListResponse)
def list_companies_adapted(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    search: Optional[str] = None,
    company_type: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from sqlalchemy import or_

    query = (
        db.query(Company)
        .options(
            joinedload(Company.contacts),
        )
    )

    if company_type is not None:
        query = query.filter(Company.company_type == company_type)
    if search is not None:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                Company.name.ilike(pattern),
                Company.normalized_name.ilike(pattern),
                Company.companies_house_number.ilike(pattern),
                Company.registered_address.ilike(pattern),
                Company.website.ilike(pattern),
                Company.company_type.ilike(pattern),
            )
        )

    total = query.count()
    rows = query.order_by(Company.name).offset(skip).limit(limit).all()

    adapted: list[CompanyAdapted] = []
    for company in rows:
        # Contacts
        contacts = [
            ContactFlat(
                id=str(c.id),
                name=c.full_name or "",
                role=c.job_title,
                email=None if current_user.role == "viewer" else c.email,
                phone=None if current_user.role == "viewer" else c.phone,
            )
            for c in (company.contacts or [])
        ]

        # Linked applications (as applicant or agent)
        apps = (
            db.query(PlanningApplication)
            .filter(
                (PlanningApplication.applicant_company_id == company.id)
                | (PlanningApplication.agent_company_id == company.id)
            )
            .limit(20)
            .all()
        )
        linked_apps = [
            LinkedApplicationFlat(
                id=str(app.id),
                reference=app.reference,
                address=app.address,
                type=app.application_type,
                status=app.status,
                date=app.submission_date.isoformat() if app.submission_date else None,
            )
            for app in apps
        ]

        # Linked schemes (as operator or owner, deduplicated)
        schemes = (
            db.query(ExistingScheme)
            .filter(
                (ExistingScheme.operator_company_id == company.id)
                | (ExistingScheme.owner_company_id == company.id)
            )
            .limit(20)
            .all()
        )
        seen_scheme_ids: set[int] = set()
        linked_schemes: list[LinkedSchemeFlat] = []
        for scheme in schemes:
            if scheme.id not in seen_scheme_ids:
                seen_scheme_ids.add(scheme.id)
                linked_schemes.append(
                    LinkedSchemeFlat(
                        id=str(scheme.id),
                        name=scheme.name,
                        units=scheme.num_units,
                        scheme_type=scheme.scheme_type,
                    )
                )

        adapted.append(
            CompanyAdapted(
                id=str(company.id),
                name=company.name,
                type=company.company_type,
                companies_house_number=company.companies_house_number,
                applications_count=len(linked_apps),
                schemes_count=len(linked_schemes),
                contacts_count=len(contacts),
                contacts=contacts,
                linked_applications=linked_apps,
                linked_schemes=linked_schemes,
            )
        )

    return CompanyAdaptedListResponse(
        items=adapted, total=total, skip=skip, limit=limit
    )


# =========================================================================
# 14. GET /api/v2/contracts  (flattened)
# =========================================================================

class ContractFlat(BaseModel):
    id: str
    contract_reference: Optional[str] = None
    contract_type: Optional[str] = None
    scheme_name: Optional[str] = None
    scheme_id: Optional[str] = None
    operator: Optional[str] = None
    client: Optional[str] = None
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    contract_value: Optional[float] = None
    currency: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = None
    is_current: Optional[bool] = None
    scheme_type: Optional[str] = None
    scheme_postcode: Optional[str] = None
    scheme_council: Optional[str] = None
    created_at: Optional[str] = None


class ContractFlatListResponse(BaseModel):
    items: list[ContractFlat]
    total: int
    skip: int
    limit: int


class ContractStats(BaseModel):
    total: int
    current: int
    expired: int
    upcoming: int
    expiring_6m: int
    total_value: float
    avg_value: float
    type_distribution: dict[str, int]
    source_distribution: dict[str, int]


@router.get("/v2/contracts/stats", response_model=ContractStats)
def get_contract_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return summary statistics for the contracts page header."""
    import datetime
    from sqlalchemy import func, case, and_

    now = datetime.date.today()
    six_months = now + datetime.timedelta(days=182)

    total = db.query(func.count(SchemeContract.id)).scalar() or 0
    current = db.query(func.count(SchemeContract.id)).filter(
        SchemeContract.is_current.is_(True)
    ).scalar() or 0
    expired = db.query(func.count(SchemeContract.id)).filter(
        SchemeContract.contract_end_date < now
    ).scalar() or 0
    upcoming = db.query(func.count(SchemeContract.id)).filter(
        SchemeContract.contract_start_date > now
    ).scalar() or 0
    expiring_6m = db.query(func.count(SchemeContract.id)).filter(
        and_(
            SchemeContract.contract_end_date >= now,
            SchemeContract.contract_end_date <= six_months,
        )
    ).scalar() or 0

    total_value = db.query(func.sum(SchemeContract.contract_value)).filter(
        SchemeContract.contract_value.isnot(None)
    ).scalar() or 0
    avg_value = db.query(func.avg(SchemeContract.contract_value)).filter(
        SchemeContract.contract_value.isnot(None),
        SchemeContract.contract_value > 0,
    ).scalar() or 0

    type_rows = (
        db.query(SchemeContract.contract_type, func.count())
        .filter(SchemeContract.contract_type.isnot(None))
        .group_by(SchemeContract.contract_type)
        .all()
    )
    source_rows = (
        db.query(SchemeContract.source, func.count())
        .filter(SchemeContract.source.isnot(None))
        .group_by(SchemeContract.source)
        .all()
    )

    return ContractStats(
        total=total,
        current=current,
        expired=expired,
        upcoming=upcoming,
        expiring_6m=expiring_6m,
        total_value=float(total_value),
        avg_value=float(avg_value),
        type_distribution={r[0]: r[1] for r in type_rows},
        source_distribution={r[0]: r[1] for r in source_rows},
    )


@router.get("/v2/contracts", response_model=ContractFlatListResponse)
def list_contracts_flat(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    search: Optional[str] = None,
    scheme_type: Optional[str] = None,
    source: Optional[str] = None,
    contract_type: Optional[str] = None,
    status: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = "desc",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    import datetime
    from sqlalchemy import or_, desc as sa_desc, asc as sa_asc, and_

    query = (
        db.query(SchemeContract)
        .options(
            joinedload(SchemeContract.scheme).joinedload(ExistingScheme.council),
            joinedload(SchemeContract.operator_company),
            joinedload(SchemeContract.client_company),
        )
    )

    if source is not None:
        query = query.filter(SchemeContract.source == source)
    if scheme_type is not None:
        query = query.filter(
            SchemeContract.scheme.has(ExistingScheme.scheme_type == scheme_type)
        )
    if contract_type is not None:
        query = query.filter(SchemeContract.contract_type == contract_type)
    if status is not None:
        now = datetime.date.today()
        six_months = now + datetime.timedelta(days=182)
        if status == "current":
            query = query.filter(SchemeContract.is_current.is_(True))
        elif status == "expired":
            query = query.filter(SchemeContract.contract_end_date < now)
        elif status == "upcoming":
            query = query.filter(SchemeContract.contract_start_date > now)
        elif status == "expiring":
            query = query.filter(
                and_(
                    SchemeContract.contract_end_date >= now,
                    SchemeContract.contract_end_date <= six_months,
                )
            )
    if search is not None:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                SchemeContract.contract_reference.ilike(pattern),
                SchemeContract.contract_type.ilike(pattern),
                SchemeContract.scheme.has(ExistingScheme.name.ilike(pattern)),
                SchemeContract.scheme.has(ExistingScheme.postcode.ilike(pattern)),
                SchemeContract.scheme.has(ExistingScheme.council.has(Council.name.ilike(pattern))),
                SchemeContract.operator_company.has(Company.name.ilike(pattern)),
                SchemeContract.client_company.has(Company.name.ilike(pattern)),
            )
        )

    total = query.count()

    # Sorting
    dir_fn = sa_asc if sort_dir == "asc" else sa_desc
    sort_map = {
        "value": SchemeContract.contract_value,
        "start_date": SchemeContract.contract_start_date,
        "end_date": SchemeContract.contract_end_date,
    }
    order_col = sort_map.get(sort_by, SchemeContract.created_at)
    items = (
        query.order_by(dir_fn(order_col).nullslast())
        .offset(skip)
        .limit(limit)
        .all()
    )

    flat_items: list[ContractFlat] = []
    for contract in items:
        scheme = contract.scheme
        flat_items.append(
            ContractFlat(
                id=str(contract.id),
                contract_reference=contract.contract_reference,
                contract_type=contract.contract_type,
                scheme_name=scheme.name if scheme else None,
                scheme_id=str(contract.scheme_id),
                operator=(
                    contract.operator_company.name
                    if contract.operator_company
                    else None
                ),
                client=(
                    contract.client_company.name
                    if contract.client_company
                    else None
                ),
                contract_start=(
                    contract.contract_start_date.isoformat()
                    if contract.contract_start_date
                    else None
                ),
                contract_end=(
                    contract.contract_end_date.isoformat()
                    if contract.contract_end_date
                    else None
                ),
                contract_value=contract.contract_value,
                currency=contract.currency,
                source=contract.source,
                source_reference=contract.source_reference,
                is_current=contract.is_current,
                scheme_type=scheme.scheme_type if scheme else None,
                scheme_postcode=scheme.postcode if scheme else None,
                scheme_council=(
                    scheme.council.name
                    if scheme and scheme.council
                    else None
                ),
                created_at=(
                    contract.created_at.isoformat()
                    if contract.created_at
                    else None
                ),
            )
        )

    return ContractFlatListResponse(
        items=flat_items, total=total, skip=skip, limit=limit
    )
