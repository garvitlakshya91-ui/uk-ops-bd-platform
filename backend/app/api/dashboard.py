import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, and_, case
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.models import (
    PlanningApplication,
    Council,
    PipelineOpportunity,
    ExistingScheme,
    ScraperRun,
    Alert,
)

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class SchemeTypeBreakdown(BaseModel):
    scheme_type: str
    count: int


class TopCouncil(BaseModel):
    council_id: int
    council_name: str
    application_count: int


class PipelineStageSummary(BaseModel):
    stage: str
    count: int


class ExpiringContract(BaseModel):
    scheme_id: int
    scheme_name: str
    operator: Optional[str] = None
    contract_end_date: Optional[datetime.date] = None
    num_units: Optional[int] = None
    days_until_expiry: int


class ScraperHealth(BaseModel):
    council_id: int
    council_name: str
    last_run_status: Optional[str] = None
    last_scraped_at: Optional[datetime.datetime] = None
    hours_since_last_scrape: Optional[float] = None
    is_overdue: bool


class DashboardStats(BaseModel):
    total_applications: int
    new_this_week: int
    total_companies: int
    total_schemes: int
    total_pipeline_opportunities: int
    unread_alerts: int
    by_scheme_type: list[SchemeTypeBreakdown]
    top_councils: list[TopCouncil]
    pipeline_summary: list[PipelineStageSummary]
    upcoming_contract_expiries: list[ExpiringContract]
    scraper_health: list[ScraperHealth]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("", response_model=DashboardStats)
def dashboard_stats(db: Session = Depends(get_db)):
    from app.models.models import Company

    now = datetime.datetime.now(datetime.timezone.utc)
    week_ago = now - datetime.timedelta(days=7)
    today = datetime.date.today()

    # Totals
    total_applications = db.query(func.count(PlanningApplication.id)).scalar() or 0
    new_this_week = (
        db.query(func.count(PlanningApplication.id))
        .filter(PlanningApplication.created_at >= week_ago)
        .scalar()
        or 0
    )
    total_companies = db.query(func.count(Company.id)).scalar() or 0
    total_schemes = db.query(func.count(ExistingScheme.id)).scalar() or 0
    total_pipeline = db.query(func.count(PipelineOpportunity.id)).scalar() or 0
    unread_alerts = (
        db.query(func.count(Alert.id)).filter(Alert.is_read == False).scalar() or 0  # noqa: E712
    )

    # By scheme type
    by_scheme_type = [
        SchemeTypeBreakdown(scheme_type=row[0], count=row[1])
        for row in db.query(
            PlanningApplication.scheme_type,
            func.count(PlanningApplication.id),
        )
        .group_by(PlanningApplication.scheme_type)
        .order_by(func.count(PlanningApplication.id).desc())
        .all()
    ]

    # Top councils
    top_councils = [
        TopCouncil(council_id=row[0], council_name=row[1], application_count=row[2])
        for row in db.query(
            Council.id,
            Council.name,
            func.count(PlanningApplication.id),
        )
        .join(PlanningApplication, PlanningApplication.council_id == Council.id)
        .group_by(Council.id, Council.name)
        .order_by(func.count(PlanningApplication.id).desc())
        .limit(10)
        .all()
    ]

    # Pipeline summary
    pipeline_summary = [
        PipelineStageSummary(stage=row[0], count=row[1])
        for row in db.query(
            PipelineOpportunity.stage,
            func.count(PipelineOpportunity.id),
        )
        .group_by(PipelineOpportunity.stage)
        .all()
    ]

    # Upcoming contract expiries (next 12 months)
    expiry_cutoff = today + datetime.timedelta(days=365)
    expiring_rows = (
        db.query(ExistingScheme, Company.name)
        .outerjoin(Company, ExistingScheme.operator_company_id == Company.id)
        .filter(
            and_(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date >= today,
                ExistingScheme.contract_end_date <= expiry_cutoff,
            )
        )
        .order_by(ExistingScheme.contract_end_date.asc())
        .limit(20)
        .all()
    )

    upcoming_expiries = [
        ExpiringContract(
            scheme_id=scheme.id,
            scheme_name=scheme.name,
            operator=operator_name,
            contract_end_date=scheme.contract_end_date,
            num_units=scheme.num_units,
            days_until_expiry=(scheme.contract_end_date - today).days,
        )
        for scheme, operator_name in expiring_rows
    ]

    # Scraper health
    # Get the most recent scraper run for each active council
    from sqlalchemy import distinct
    from sqlalchemy.orm import aliased

    latest_run_subq = (
        db.query(
            ScraperRun.council_id,
            func.max(ScraperRun.started_at).label("latest_started"),
        )
        .group_by(ScraperRun.council_id)
        .subquery()
    )

    councils_with_runs = (
        db.query(Council, ScraperRun)
        .outerjoin(latest_run_subq, Council.id == latest_run_subq.c.council_id)
        .outerjoin(
            ScraperRun,
            and_(
                ScraperRun.council_id == Council.id,
                ScraperRun.started_at == latest_run_subq.c.latest_started,
            ),
        )
        .filter(Council.active == True)  # noqa: E712
        .order_by(Council.name)
        .all()
    )

    scraper_health_list: list[ScraperHealth] = []
    for council, latest_run in councils_with_runs:
        hours_since: Optional[float] = None
        is_overdue = False
        if council.last_scraped_at:
            delta = now - council.last_scraped_at
            hours_since = round(delta.total_seconds() / 3600, 1)
            is_overdue = hours_since > council.scrape_frequency_hours

        scraper_health_list.append(
            ScraperHealth(
                council_id=council.id,
                council_name=council.name,
                last_run_status=latest_run.status if latest_run else None,
                last_scraped_at=council.last_scraped_at,
                hours_since_last_scrape=hours_since,
                is_overdue=is_overdue,
            )
        )

    return DashboardStats(
        total_applications=total_applications,
        new_this_week=new_this_week,
        total_companies=total_companies,
        total_schemes=total_schemes,
        total_pipeline_opportunities=total_pipeline,
        unread_alerts=unread_alerts,
        by_scheme_type=by_scheme_type,
        top_councils=top_councils,
        pipeline_summary=pipeline_summary,
        upcoming_contract_expiries=upcoming_expiries,
        scraper_health=scraper_health_list,
    )
