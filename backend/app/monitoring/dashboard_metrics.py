"""Metrics aggregation for the UK Ops BD Platform front-end dashboard.

All queries target the same SQLAlchemy models used by the rest of the
application and are designed to be called from FastAPI route handlers that
inject a database session via ``get_db``."""

from __future__ import annotations

import datetime
from typing import Any

import structlog
from sqlalchemy import func, case, and_, extract, distinct, text
from sqlalchemy.orm import Session

from app.models.models import (
    Alert,
    Company,
    Contact,
    Council,
    ExistingScheme,
    PipelineOpportunity,
    PlanningApplication,
    ScraperRun,
)

log = structlog.get_logger(__name__)

_PIPELINE_STAGES_ORDERED = [
    "identified",
    "researched",
    "contacted",
    "meeting",
    "proposal",
    "won",
    "lost",
]

_CONVERSION_PAIRS = [
    ("identified", "researched"),
    ("researched", "contacted"),
    ("contacted", "meeting"),
    ("meeting", "proposal"),
    ("proposal", "won"),
]


class MetricsCollector:
    """Aggregates metrics across all domain models for dashboard display."""

    def __init__(self, db: Session) -> None:
        self.db = db

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _utcnow(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    def _start_of_week(self) -> datetime.datetime:
        now = self._utcnow()
        monday = now - datetime.timedelta(days=now.weekday())
        return monday.replace(hour=0, minute=0, second=0, microsecond=0)

    def _start_of_month(self) -> datetime.datetime:
        now = self._utcnow()
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def _start_of_day(self) -> datetime.datetime:
        now = self._utcnow()
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    # -----------------------------------------------------------------
    # Overview
    # -----------------------------------------------------------------

    def get_overview_stats(self) -> dict[str, Any]:
        """Top-level KPI numbers for the dashboard hero section."""
        now = self._utcnow()
        week_start = self._start_of_week()
        month_start = self._start_of_month()

        total_applications = self.db.query(func.count(PlanningApplication.id)).scalar() or 0
        new_this_week = (
            self.db.query(func.count(PlanningApplication.id))
            .filter(PlanningApplication.created_at >= week_start)
            .scalar()
            or 0
        )
        new_this_month = (
            self.db.query(func.count(PlanningApplication.id))
            .filter(PlanningApplication.created_at >= month_start)
            .scalar()
            or 0
        )

        total_companies = self.db.query(func.count(Company.id)).scalar() or 0
        total_contacts = self.db.query(func.count(Contact.id)).scalar() or 0
        total_schemes = self.db.query(func.count(ExistingScheme.id)).scalar() or 0

        six_months = now.date() + datetime.timedelta(days=182)
        contracts_expiring_6m = (
            self.db.query(func.count(ExistingScheme.id))
            .filter(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date <= six_months,
                ExistingScheme.contract_end_date >= now.date(),
            )
            .scalar()
            or 0
        )

        pipeline_total = (
            self.db.query(func.count(PipelineOpportunity.id))
            .filter(PipelineOpportunity.stage.notin_(["won", "lost"]))
            .scalar()
            or 0
        )

        pipeline_by_stage = dict(
            self.db.query(
                PipelineOpportunity.stage,
                func.count(PipelineOpportunity.id),
            )
            .group_by(PipelineOpportunity.stage)
            .all()
        )

        return {
            "total_applications": total_applications,
            "new_this_week": new_this_week,
            "new_this_month": new_this_month,
            "total_companies": total_companies,
            "total_contacts": total_contacts,
            "total_schemes": total_schemes,
            "contracts_expiring_6m": contracts_expiring_6m,
            "pipeline_total": pipeline_total,
            "pipeline_by_stage": pipeline_by_stage,
        }

    # -----------------------------------------------------------------
    # Scraper metrics
    # -----------------------------------------------------------------

    def get_scraper_metrics(self) -> dict[str, Any]:
        """Operational stats for the scraper fleet."""
        today_start = self._start_of_day()

        councils_active = (
            self.db.query(func.count(Council.id))
            .filter(Council.active.is_(True))
            .scalar()
            or 0
        )

        # Healthy = at least one successful run in last 48 h
        healthy_cutoff = self._utcnow() - datetime.timedelta(hours=48)
        councils_healthy = (
            self.db.query(func.count(distinct(ScraperRun.council_id)))
            .filter(
                ScraperRun.status == "success",
                ScraperRun.started_at >= healthy_cutoff,
            )
            .scalar()
            or 0
        )

        councils_failing = max(0, councils_active - councils_healthy)

        runs_today = (
            self.db.query(ScraperRun)
            .filter(ScraperRun.started_at >= today_start)
            .all()
        )
        total_runs_today = len(runs_today)
        success_runs_today = sum(1 for r in runs_today if r.status == "success")
        success_rate_today = (
            round(success_runs_today / total_runs_today * 100, 1)
            if total_runs_today
            else 0.0
        )
        applications_scraped_today = sum(r.applications_found for r in runs_today)

        return {
            "councils_active": councils_active,
            "councils_healthy": councils_healthy,
            "councils_failing": councils_failing,
            "total_runs_today": total_runs_today,
            "success_rate_today": success_rate_today,
            "applications_scraped_today": applications_scraped_today,
        }

    # -----------------------------------------------------------------
    # Pipeline
    # -----------------------------------------------------------------

    def get_pipeline_metrics(self) -> dict[str, Any]:
        """Funnel, priority breakdown, conversion rates, and stage durations."""

        # Funnel by stage
        stage_counts_raw = dict(
            self.db.query(
                PipelineOpportunity.stage,
                func.count(PipelineOpportunity.id),
            )
            .group_by(PipelineOpportunity.stage)
            .all()
        )
        opportunities_by_stage = {
            s: stage_counts_raw.get(s, 0) for s in _PIPELINE_STAGES_ORDERED
        }

        # By priority
        opportunities_by_priority = dict(
            self.db.query(
                PipelineOpportunity.priority,
                func.count(PipelineOpportunity.id),
            )
            .group_by(PipelineOpportunity.priority)
            .all()
        )

        # Conversion rates
        conversion_rates: dict[str, float | None] = {}
        for from_stage, to_stage in _CONVERSION_PAIRS:
            from_count = stage_counts_raw.get(from_stage, 0)
            # Count everything that progressed past from_stage
            to_idx = _PIPELINE_STAGES_ORDERED.index(to_stage)
            progressed = sum(
                stage_counts_raw.get(s, 0)
                for s in _PIPELINE_STAGES_ORDERED[to_idx:]
            )
            rate = round(progressed / from_count * 100, 1) if from_count else None
            conversion_rates[f"{from_stage}_to_{to_stage}"] = rate

        # Average time in stage (approximation: updated_at - created_at for
        # opportunities currently in each stage)
        avg_time_in_stage: dict[str, float | None] = {}
        for stage in _PIPELINE_STAGES_ORDERED:
            avg_seconds = (
                self.db.query(
                    func.avg(
                        extract(
                            "epoch",
                            PipelineOpportunity.updated_at
                            - PipelineOpportunity.created_at,
                        )
                    )
                )
                .filter(PipelineOpportunity.stage == stage)
                .scalar()
            )
            avg_time_in_stage[stage] = (
                round(float(avg_seconds) / 86400, 1) if avg_seconds else None
            )  # days

        return {
            "opportunities_by_stage": opportunities_by_stage,
            "opportunities_by_priority": opportunities_by_priority,
            "conversion_rates": conversion_rates,
            "avg_time_in_stage_days": avg_time_in_stage,
        }

    # -----------------------------------------------------------------
    # Geographic distribution
    # -----------------------------------------------------------------

    def get_geographic_distribution(self) -> list[dict[str, Any]]:
        """Applications and schemes grouped by council region."""
        rows = (
            self.db.query(
                Council.region,
                func.count(distinct(PlanningApplication.id)).label("applications"),
                func.count(distinct(ExistingScheme.id)).label("schemes"),
            )
            .outerjoin(
                PlanningApplication,
                PlanningApplication.council_id == Council.id,
            )
            .outerjoin(
                ExistingScheme,
                ExistingScheme.council_id == Council.id,
            )
            .filter(Council.region.isnot(None))
            .group_by(Council.region)
            .order_by(func.count(distinct(PlanningApplication.id)).desc())
            .all()
        )

        return [
            {
                "region": row.region,
                "applications": row.applications,
                "schemes": row.schemes,
            }
            for row in rows
        ]

    # -----------------------------------------------------------------
    # Trends
    # -----------------------------------------------------------------

    def get_trend_data(self, days: int = 30) -> dict[str, Any]:
        """Daily time-series data for dashboard charts."""
        cutoff = self._utcnow() - datetime.timedelta(days=days)

        # New applications per day
        app_rows = (
            self.db.query(
                func.date(PlanningApplication.created_at).label("day"),
                func.count(PlanningApplication.id).label("count"),
            )
            .filter(PlanningApplication.created_at >= cutoff)
            .group_by(func.date(PlanningApplication.created_at))
            .order_by(func.date(PlanningApplication.created_at))
            .all()
        )
        daily_applications = [
            {"date": str(r.day), "count": r.count} for r in app_rows
        ]

        # New companies per day
        company_rows = (
            self.db.query(
                func.date(Company.created_at).label("day"),
                func.count(Company.id).label("count"),
            )
            .filter(Company.created_at >= cutoff)
            .group_by(func.date(Company.created_at))
            .order_by(func.date(Company.created_at))
            .all()
        )
        daily_companies = [
            {"date": str(r.day), "count": r.count} for r in company_rows
        ]

        # Pipeline changes per day (opportunities created)
        pipeline_rows = (
            self.db.query(
                func.date(PipelineOpportunity.created_at).label("day"),
                func.count(PipelineOpportunity.id).label("count"),
            )
            .filter(PipelineOpportunity.created_at >= cutoff)
            .group_by(func.date(PipelineOpportunity.created_at))
            .order_by(func.date(PipelineOpportunity.created_at))
            .all()
        )
        daily_pipeline = [
            {"date": str(r.day), "count": r.count} for r in pipeline_rows
        ]

        return {
            "period_days": days,
            "daily_applications": daily_applications,
            "daily_companies": daily_companies,
            "daily_pipeline_changes": daily_pipeline,
        }

    # -----------------------------------------------------------------
    # Top opportunities
    # -----------------------------------------------------------------

    def get_top_opportunities(self, limit: int = 10) -> list[dict[str, Any]]:
        """Highest BD-score pipeline opportunities with joined details."""
        opps = (
            self.db.query(PipelineOpportunity)
            .filter(
                PipelineOpportunity.bd_score.isnot(None),
                PipelineOpportunity.stage.notin_(["won", "lost"]),
            )
            .order_by(PipelineOpportunity.bd_score.desc())
            .limit(limit)
            .all()
        )

        results: list[dict[str, Any]] = []
        for opp in opps:
            app = opp.planning_application
            scheme = opp.scheme
            company = opp.company

            result: dict[str, Any] = {
                "opportunity_id": opp.id,
                "source": opp.source,
                "stage": opp.stage,
                "priority": opp.priority,
                "bd_score": opp.bd_score,
                "assigned_to": opp.assigned_to,
                "next_action": opp.next_action,
                "next_action_date": str(opp.next_action_date) if opp.next_action_date else None,
                "company_name": company.name if company else None,
                "company_type": company.company_type if company else None,
            }

            if app:
                result.update(
                    {
                        "application_reference": app.reference,
                        "application_address": app.address,
                        "application_scheme_type": app.scheme_type,
                        "application_num_units": app.num_units,
                        "application_status": app.status,
                    }
                )

            if scheme:
                result.update(
                    {
                        "scheme_name": scheme.name,
                        "scheme_type": scheme.scheme_type,
                        "scheme_num_units": scheme.num_units,
                        "contract_end_date": str(scheme.contract_end_date)
                        if scheme.contract_end_date
                        else None,
                    }
                )

            results.append(result)

        return results

    # -----------------------------------------------------------------
    # Contract expiry timeline
    # -----------------------------------------------------------------

    def get_contract_expiry_timeline(self) -> list[dict[str, Any]]:
        """Schemes grouped by expiry month for the next 24 months."""
        today = datetime.date.today()
        end_date = today + datetime.timedelta(days=730)

        schemes = (
            self.db.query(ExistingScheme)
            .filter(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date >= today,
                ExistingScheme.contract_end_date <= end_date,
            )
            .order_by(ExistingScheme.contract_end_date)
            .all()
        )

        # Bucket by year-month
        buckets: dict[str, list[dict[str, Any]]] = {}
        for scheme in schemes:
            key = scheme.contract_end_date.strftime("%Y-%m")
            entry = {
                "scheme_id": scheme.id,
                "name": scheme.name,
                "scheme_type": scheme.scheme_type,
                "num_units": scheme.num_units,
                "contract_end_date": str(scheme.contract_end_date),
                "operator": None,
                "owner": None,
            }
            if scheme.operator_company:
                entry["operator"] = scheme.operator_company.name
            if scheme.owner_company:
                entry["owner"] = scheme.owner_company.name

            buckets.setdefault(key, []).append(entry)

        return [
            {"month": month, "count": len(items), "schemes": items}
            for month, items in sorted(buckets.items())
        ]
