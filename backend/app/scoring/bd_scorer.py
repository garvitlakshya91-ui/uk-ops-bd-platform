"""Business Development scoring engine.

Calculates composite BD scores for planning applications, existing schemes,
and companies.  Also provides pipeline prioritisation and weekly reporting.

Typical usage::

    from app.scoring.bd_scorer import BDScorer

    scorer = BDScorer(db_session=session)
    score = scorer.score_planning_application(application)
    pipeline = scorer.prioritize_pipeline(opportunities)
"""

from __future__ import annotations

import datetime
from typing import Any, Optional

import structlog
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from app.models.models import (
    Company,
    Contact,
    ExistingScheme,
    PipelineOpportunity,
    PlanningApplication,
    ScraperRun,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

# Scheme type desirability for new-build pipeline.
_SCHEME_TYPE_SCORES: dict[str, float] = {
    "BTR": 100.0,
    "PBSA": 85.0,
    "Co-living": 80.0,
    "Senior": 70.0,
    "Affordable": 60.0,
    "Mixed": 65.0,
    "Residential": 50.0,
    "Unknown": 30.0,
}

# Application status progression scores.
_STATUS_SCORES: dict[str, float] = {
    "approved": 100.0,
    "pending": 70.0,
    "submitted": 60.0,
    "validated": 65.0,
    "appeal": 50.0,
    "refused": 20.0,
    "withdrawn": 10.0,
    "unknown": 40.0,
}

# London and key city premiums.
_LONDON_BOROUGHS: set[str] = {
    "barking and dagenham", "barnet", "bexley", "brent", "bromley",
    "camden", "city of london", "croydon", "ealing", "enfield",
    "greenwich", "hackney", "hammersmith and fulham", "haringey", "harrow",
    "havering", "hillingdon", "hounslow", "islington",
    "kensington and chelsea", "kingston upon thames", "lambeth", "lewisham",
    "merton", "newham", "redbridge", "richmond upon thames", "southwark",
    "sutton", "tower hamlets", "waltham forest", "wandsworth",
    "westminster",
}

_TIER_1_CITIES: set[str] = {
    "manchester", "birmingham", "leeds", "bristol", "edinburgh",
    "glasgow", "liverpool", "sheffield", "nottingham", "cardiff",
}

# Existing scheme scoring weights.
_EXISTING_WEIGHTS: dict[str, float] = {
    "contract_urgency": 0.35,
    "performance_gap": 0.25,
    "satisfaction_risk": 0.15,
    "financial_risk": 0.15,
    "scheme_size": 0.10,
}


class BDScorer:
    """Business Development scoring engine.

    Parameters
    ----------
    db_session : Session
        SQLAlchemy session for database queries.
    """

    def __init__(self, db_session: Session) -> None:
        self._db = db_session

    # ------------------------------------------------------------------
    # Planning application scoring
    # ------------------------------------------------------------------

    def score_planning_application(self, app: PlanningApplication) -> float:
        """Score a planning application on a 0-100 scale.

        Factors
        -------
        * **scheme_type** (weight 0.30) — BTR is most desirable.
        * **num_units** (weight 0.25) — more units = better opportunity.
        * **application_stage** (weight 0.20) — approved > submitted.
        * **location_tier** (weight 0.15) — London premium, tier-1 cities.
        * **developer_track_record** (weight 0.10) — repeat developers score higher.
        """
        # Scheme type.
        scheme_str = (app.scheme_type or "Unknown")
        if hasattr(scheme_str, "value"):
            scheme_str = scheme_str.value
        scheme_score = _SCHEME_TYPE_SCORES.get(scheme_str, 30.0)

        # Unit count.
        units = app.num_units if hasattr(app, "num_units") and app.num_units else None
        if units is None and hasattr(app, "unit_count"):
            units = app.unit_count
        unit_score = self._score_unit_count(units)

        # Application status.
        status = (app.status or "unknown")
        if hasattr(status, "value"):
            status = status.value
        status_lower = status.lower()
        status_score = _STATUS_SCORES.get(status_lower, 40.0)

        # Location tier.
        location_score = self._score_location(app)

        # Developer track record.
        track_record_score = self._score_developer_track_record(app)

        composite = (
            0.30 * scheme_score
            + 0.25 * unit_score
            + 0.20 * status_score
            + 0.15 * location_score
            + 0.10 * track_record_score
        )

        return round(min(max(composite, 0.0), 100.0), 1)

    def _score_unit_count(self, units: int | None) -> float:
        """Convert a unit count to a 0-100 score."""
        if units is None or units <= 0:
            return 20.0
        if units >= 500:
            return 100.0
        if units >= 300:
            return 90.0
        if units >= 200:
            return 80.0
        if units >= 100:
            return 65.0
        if units >= 50:
            return 50.0
        return 30.0

    def _score_location(self, app: PlanningApplication) -> float:
        """Score location based on council/region."""
        council = getattr(app, "council", None)
        region_name = ""
        if council:
            region_name = (getattr(council, "name", "") or "").lower()
            region = (getattr(council, "region", "") or "").lower()
        else:
            region = ""

        if region_name in _LONDON_BOROUGHS or "london" in region:
            return 100.0
        if region_name in _TIER_1_CITIES or any(c in region for c in _TIER_1_CITIES):
            return 80.0
        return 50.0

    def _score_developer_track_record(self, app: PlanningApplication) -> float:
        """Score based on whether the developer has multiple applications."""
        company_id = getattr(app, "applicant_company_id", None)
        if not company_id:
            return 30.0

        count = (
            self._db.query(sa_func.count(PlanningApplication.id))
            .filter(PlanningApplication.applicant_company_id == company_id)
            .scalar()
        ) or 0

        if count >= 10:
            return 100.0
        if count >= 5:
            return 80.0
        if count >= 2:
            return 60.0
        return 40.0

    # ------------------------------------------------------------------
    # Existing scheme scoring
    # ------------------------------------------------------------------

    def score_existing_scheme(self, scheme: ExistingScheme) -> float:
        """Score an existing managed scheme on a 0-100 scale.

        Weighted factors
        ----------------
        * **contract_urgency** (0.35): months until contract_end_date.
          <=6 months = 100, <=12 = 80, <=24 = 50, >24 = 20.
        * **performance_gap** (0.25): inverse of performance_rating.
          Low performance = high opportunity.
        * **satisfaction_risk** (0.15): inverse of satisfaction_score.
        * **financial_risk** (0.15): inverse of financial_health_score.
        * **scheme_size** (0.10): num_units normalised.
          >500 = 100, >200 = 70, >100 = 50, else 30.
        """
        scores: dict[str, float] = {}

        # Prefer SchemeContract end date if available
        from app.models.models import SchemeContract
        current_contract = (
            self._db.query(SchemeContract)
            .filter(
                SchemeContract.scheme_id == scheme.id,
                SchemeContract.is_current.is_(True),
                SchemeContract.contract_end_date.isnot(None),
            )
            .order_by(SchemeContract.contract_end_date.desc())
            .first()
        )
        effective_end_date = (
            current_contract.contract_end_date
            if current_contract
            else scheme.contract_end_date
        )

        # Contract urgency.
        if effective_end_date:
            today = datetime.date.today()
            months_remaining = (
                (effective_end_date.year - today.year) * 12
                + (effective_end_date.month - today.month)
            )
            if months_remaining <= 0:
                scores["contract_urgency"] = 100.0
            elif months_remaining <= 6:
                scores["contract_urgency"] = 100.0
            elif months_remaining <= 12:
                scores["contract_urgency"] = 80.0
            elif months_remaining <= 24:
                scores["contract_urgency"] = 50.0
            else:
                scores["contract_urgency"] = 20.0
        else:
            scores["contract_urgency"] = 30.0  # Unknown = moderate default.

        # Performance gap (lower rating = higher opportunity).
        perf = scheme.performance_rating
        if perf is not None:
            # Assume rating is 0-100 scale.
            scores["performance_gap"] = max(0.0, 100.0 - perf)
        else:
            scores["performance_gap"] = 50.0

        # Satisfaction risk.
        sat = scheme.satisfaction_score
        if sat is not None:
            scores["satisfaction_risk"] = max(0.0, 100.0 - sat)
        else:
            scores["satisfaction_risk"] = 50.0

        # Financial risk.
        fin = scheme.financial_health_score
        if fin is not None:
            scores["financial_risk"] = max(0.0, 100.0 - fin)
        else:
            scores["financial_risk"] = 50.0

        # Scheme size.
        units = scheme.num_units or 0
        if units > 500:
            scores["scheme_size"] = 100.0
        elif units > 200:
            scores["scheme_size"] = 70.0
        elif units > 100:
            scores["scheme_size"] = 50.0
        else:
            scores["scheme_size"] = 30.0

        composite = sum(
            _EXISTING_WEIGHTS[k] * scores[k] for k in _EXISTING_WEIGHTS
        )

        return round(min(max(composite, 0.0), 100.0), 1)

    # ------------------------------------------------------------------
    # Company scoring
    # ------------------------------------------------------------------

    def score_company(self, company: Company) -> dict[str, Any]:
        """Score a company across multiple dimensions.

        Returns a dict with individual factor scores and an overall composite.

        Factors
        -------
        * **portfolio_size** — count of linked schemes and applications.
        * **growth_trajectory** — trend in new applications over time.
        * **relationship_status** — existing contacts? previous interactions?
        """
        scores: dict[str, Any] = {}

        # Portfolio size.
        scheme_count = (
            self._db.query(sa_func.count(ExistingScheme.id))
            .filter(
                (ExistingScheme.operator_company_id == company.id)
                | (ExistingScheme.owner_company_id == company.id)
            )
            .scalar()
        ) or 0

        app_count = (
            self._db.query(sa_func.count(PlanningApplication.id))
            .filter(PlanningApplication.applicant_company_id == company.id)
            .scalar()
        ) or 0

        total_portfolio = scheme_count + app_count
        if total_portfolio >= 20:
            portfolio_score = 100.0
        elif total_portfolio >= 10:
            portfolio_score = 80.0
        elif total_portfolio >= 5:
            portfolio_score = 60.0
        elif total_portfolio >= 1:
            portfolio_score = 40.0
        else:
            portfolio_score = 10.0
        scores["portfolio_size"] = portfolio_score

        # Growth trajectory (applications in last 12 months vs previous 12).
        now = datetime.date.today()
        twelve_months_ago = now - datetime.timedelta(days=365)
        twenty_four_months_ago = now - datetime.timedelta(days=730)

        recent = (
            self._db.query(sa_func.count(PlanningApplication.id))
            .filter(
                PlanningApplication.applicant_company_id == company.id,
                PlanningApplication.submission_date >= twelve_months_ago,
            )
            .scalar()
        ) or 0

        previous = (
            self._db.query(sa_func.count(PlanningApplication.id))
            .filter(
                PlanningApplication.applicant_company_id == company.id,
                PlanningApplication.submission_date >= twenty_four_months_ago,
                PlanningApplication.submission_date < twelve_months_ago,
            )
            .scalar()
        ) or 0

        if recent > previous and previous > 0:
            growth_score = min(100.0, 60.0 + (recent / previous) * 20.0)
        elif recent > 0 and previous == 0:
            growth_score = 80.0
        elif recent == previous and recent > 0:
            growth_score = 50.0
        else:
            growth_score = 20.0
        scores["growth_trajectory"] = round(growth_score, 1)

        # Relationship status.
        contact_count = (
            self._db.query(sa_func.count(Contact.id))
            .filter(Contact.company_id == company.id)
            .scalar()
        ) or 0

        pipeline_count = (
            self._db.query(sa_func.count(PipelineOpportunity.id))
            .filter(PipelineOpportunity.company_id == company.id)
            .scalar()
        ) or 0

        if contact_count > 0 and pipeline_count > 0:
            relationship_score = 80.0
        elif contact_count > 0:
            relationship_score = 50.0
        elif pipeline_count > 0:
            relationship_score = 40.0
        else:
            relationship_score = 10.0
        scores["relationship_status"] = relationship_score

        # Overall composite.
        overall = (
            0.40 * scores["portfolio_size"]
            + 0.35 * scores["growth_trajectory"]
            + 0.25 * scores["relationship_status"]
        )
        scores["overall"] = round(overall, 1)

        scores["details"] = {
            "scheme_count": scheme_count,
            "application_count": app_count,
            "contact_count": contact_count,
            "recent_applications_12m": recent,
            "previous_applications_12m": previous,
        }

        return scores

    # ------------------------------------------------------------------
    # Pipeline prioritisation
    # ------------------------------------------------------------------

    def prioritize_pipeline(
        self,
        opportunities: list[PipelineOpportunity],
    ) -> list[PipelineOpportunity]:
        """Sort and rank pipeline opportunities by BD score.

        Assigns priority labels:

        * Top 10% = **hot**
        * Next 20% = **warm**
        * Remainder = **cold**

        Parameters
        ----------
        opportunities : list[PipelineOpportunity]
            Opportunities to rank.

        Returns
        -------
        list[PipelineOpportunity]
            Sorted (highest score first) with ``priority`` updated.
        """
        # Recalculate scores.
        for opp in opportunities:
            if opp.planning_application_id and opp.planning_application:
                opp.bd_score = self.score_planning_application(opp.planning_application)
            elif opp.scheme_id and opp.scheme:
                opp.bd_score = self.score_existing_scheme(opp.scheme)

        # Sort by score descending.
        scored = sorted(opportunities, key=lambda o: o.bd_score or 0, reverse=True)

        # Assign priority labels.
        total = len(scored)
        if total == 0:
            return scored

        hot_cutoff = max(1, int(total * 0.10))
        warm_cutoff = max(hot_cutoff + 1, int(total * 0.30))

        for i, opp in enumerate(scored):
            if i < hot_cutoff:
                opp.priority = "hot"
            elif i < warm_cutoff:
                opp.priority = "warm"
            else:
                opp.priority = "cold"

        logger.info(
            "pipeline_prioritized",
            total=total,
            hot=hot_cutoff,
            warm=warm_cutoff - hot_cutoff,
            cold=total - warm_cutoff,
        )
        return scored

    # ------------------------------------------------------------------
    # Weekly report
    # ------------------------------------------------------------------

    def generate_weekly_report(self) -> dict[str, Any]:
        """Generate a weekly BD report summary.

        Returns
        -------
        dict
            Contains:
            - ``new_applications_by_type``: counts of new applications this
              week, grouped by scheme type.
            - ``score_changes``: schemes with significant score changes.
            - ``expiring_contracts``: contracts expiring in 3, 6, 12 months.
            - ``top_opportunities``: top 10 pipeline opportunities.
        """
        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)

        report: dict[str, Any] = {}

        # New applications this week by scheme type.
        new_apps = (
            self._db.query(
                PlanningApplication.scheme_type,
                sa_func.count(PlanningApplication.id),
            )
            .filter(PlanningApplication.created_at >= week_ago)
            .group_by(PlanningApplication.scheme_type)
            .all()
        )
        report["new_applications_by_type"] = {
            (st.value if hasattr(st, "value") else str(st)): count
            for st, count in new_apps
        }

        # Contracts expiring within 3, 6, 12 months.
        three_months = today + datetime.timedelta(days=90)
        six_months = today + datetime.timedelta(days=180)
        twelve_months = today + datetime.timedelta(days=365)

        def _expiring_query(deadline: datetime.date) -> list[ExistingScheme]:
            return (
                self._db.query(ExistingScheme)
                .filter(
                    ExistingScheme.contract_end_date.isnot(None),
                    ExistingScheme.contract_end_date <= deadline,
                    ExistingScheme.contract_end_date >= today,
                )
                .order_by(ExistingScheme.contract_end_date)
                .all()
            )

        report["expiring_contracts"] = {
            "within_3_months": [
                {
                    "id": s.id,
                    "name": s.name,
                    "contract_end_date": str(s.contract_end_date),
                    "num_units": s.num_units,
                }
                for s in _expiring_query(three_months)
            ],
            "within_6_months": [
                {
                    "id": s.id,
                    "name": s.name,
                    "contract_end_date": str(s.contract_end_date),
                    "num_units": s.num_units,
                }
                for s in _expiring_query(six_months)
            ],
            "within_12_months": [
                {
                    "id": s.id,
                    "name": s.name,
                    "contract_end_date": str(s.contract_end_date),
                    "num_units": s.num_units,
                }
                for s in _expiring_query(twelve_months)
            ],
        }

        # Top 10 pipeline opportunities.
        top_opps = (
            self._db.query(PipelineOpportunity)
            .filter(PipelineOpportunity.bd_score.isnot(None))
            .order_by(PipelineOpportunity.bd_score.desc())
            .limit(10)
            .all()
        )
        report["top_opportunities"] = [
            {
                "id": o.id,
                "company_id": o.company_id,
                "source": o.source,
                "stage": o.stage,
                "priority": o.priority,
                "bd_score": o.bd_score,
                "next_action": o.next_action,
                "next_action_date": str(o.next_action_date) if o.next_action_date else None,
            }
            for o in top_opps
        ]

        # Score changes — recalculate for all existing schemes and note
        # those where the score differs significantly from stored value.
        score_changes: list[dict[str, Any]] = []
        schemes = self._db.query(ExistingScheme).all()
        for scheme in schemes:
            new_score = self.score_existing_scheme(scheme)
            # Check if there is a linked pipeline opportunity with a stored score.
            if scheme.pipeline_opportunity and scheme.pipeline_opportunity.bd_score is not None:
                old_score = scheme.pipeline_opportunity.bd_score
                delta = new_score - old_score
                if abs(delta) >= 5.0:
                    score_changes.append(
                        {
                            "scheme_id": scheme.id,
                            "scheme_name": scheme.name,
                            "old_score": old_score,
                            "new_score": new_score,
                            "delta": round(delta, 1),
                        }
                    )
        report["score_changes"] = score_changes

        logger.info(
            "weekly_report_generated",
            new_app_count=sum(report["new_applications_by_type"].values()),
            expiring_3m=len(report["expiring_contracts"]["within_3_months"]),
            top_opps=len(report["top_opportunities"]),
            score_changes=len(report["score_changes"]),
        )

        return report
