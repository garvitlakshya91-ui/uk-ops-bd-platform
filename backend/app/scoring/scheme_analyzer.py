"""Detailed scheme analysis for BD decision-making.

Provides in-depth analysis of individual schemes, contract risk assessment,
performance trending, cross-scheme comparison, and regional market analysis.

Typical usage::

    from app.scoring.scheme_analyzer import SchemeAnalyzer

    analyzer = SchemeAnalyzer(db_session=session)
    report = analyzer.generate_scheme_report(scheme_id=42)
    risk = analyzer.analyze_contract_risk(scheme)
"""

from __future__ import annotations

import datetime
from typing import Any

import structlog
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from app.models.models import (
    Company,
    Contact,
    ExistingScheme,
    PipelineOpportunity,
    PlanningApplication,
)
from app.scoring.bd_scorer import BDScorer

logger = structlog.get_logger(__name__)


class SchemeAnalyzer:
    """Detailed analysis engine for existing managed schemes.

    Parameters
    ----------
    db_session : Session
        SQLAlchemy database session.
    """

    def __init__(self, db_session: Session) -> None:
        self._db = db_session
        self._scorer = BDScorer(db_session)

    # ------------------------------------------------------------------
    # Contract risk analysis
    # ------------------------------------------------------------------

    def analyze_contract_risk(self, scheme: ExistingScheme) -> dict[str, Any]:
        """Produce a detailed breakdown of contract expiry risk for *scheme*.

        Returns
        -------
        dict
            Keys: ``risk_level``, ``months_remaining``, ``days_remaining``,
            ``contract_end_date``, ``recommended_action``,
            ``renewal_window_opens``, ``risk_factors``.
        """
        today = datetime.date.today()
        result: dict[str, Any] = {
            "scheme_id": scheme.id,
            "scheme_name": scheme.name,
            "contract_start_date": str(scheme.contract_start_date) if scheme.contract_start_date else None,
            "contract_end_date": str(scheme.contract_end_date) if scheme.contract_end_date else None,
        }

        # Prefer the most recent current SchemeContract end date if available
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

        if not effective_end_date:
            result.update(
                risk_level="unknown",
                months_remaining=None,
                days_remaining=None,
                recommended_action="Confirm contract end date with operations team.",
                renewal_window_opens=None,
                risk_factors=["Contract end date not recorded."],
            )
            return result

        delta = effective_end_date - today
        days_remaining = delta.days
        months_remaining = days_remaining / 30.44  # Average month length.

        risk_factors: list[str] = []

        # Determine risk level.
        if days_remaining <= 0:
            risk_level = "critical"
            recommended_action = "Contract has expired. Immediate action required: confirm holdover terms or initiate new tender."
            risk_factors.append("Contract is past expiry date.")
        elif months_remaining <= 3:
            risk_level = "critical"
            recommended_action = "Engage with client immediately. Prepare proposal for contract renewal or competitive bid."
            risk_factors.append("Less than 3 months until expiry.")
        elif months_remaining <= 6:
            risk_level = "high"
            recommended_action = "Schedule introductory meeting. Prepare competitive analysis and service proposal."
            risk_factors.append("Less than 6 months until expiry.")
        elif months_remaining <= 12:
            risk_level = "medium"
            recommended_action = "Begin relationship building. Research incumbent performance and client satisfaction."
            risk_factors.append("Less than 12 months until expiry.")
        elif months_remaining <= 24:
            risk_level = "low"
            recommended_action = "Monitor and build awareness. Add to long-term pipeline tracking."
        else:
            risk_level = "minimal"
            recommended_action = "No immediate action needed. Review at next quarterly pipeline review."

        # Additional risk factors.
        if scheme.performance_rating is not None and scheme.performance_rating < 50:
            risk_factors.append(f"Low performance rating ({scheme.performance_rating}/100) increases likelihood of operator change.")
        if scheme.satisfaction_score is not None and scheme.satisfaction_score < 50:
            risk_factors.append(f"Low satisfaction score ({scheme.satisfaction_score}/100) suggests client may seek alternatives.")
        if scheme.financial_health_score is not None and scheme.financial_health_score < 40:
            risk_factors.append(f"Poor financial health ({scheme.financial_health_score}/100) may indicate operational difficulties.")
        if scheme.num_units and scheme.num_units >= 200:
            risk_factors.append(f"Large scheme ({scheme.num_units} units) represents significant revenue opportunity.")

        # Renewal window — typically 6-12 months before expiry.
        renewal_window_opens = effective_end_date - datetime.timedelta(days=365)
        if renewal_window_opens < today:
            renewal_window_opens = today

        result.update(
            risk_level=risk_level,
            months_remaining=round(months_remaining, 1),
            days_remaining=days_remaining,
            recommended_action=recommended_action,
            renewal_window_opens=str(renewal_window_opens),
            risk_factors=risk_factors,
        )
        return result

    # ------------------------------------------------------------------
    # Performance trend analysis
    # ------------------------------------------------------------------

    def analyze_performance_trend(self, scheme: ExistingScheme) -> dict[str, Any]:
        """Analyse whether a scheme's performance is improving or declining.

        Uses available rating fields to assess current state and infer trend.
        In a full implementation, this would compare historical snapshots.

        Returns
        -------
        dict
            Keys: ``current_state``, ``trend``, ``factors``,
            ``recommendation``.
        """
        factors: list[dict[str, Any]] = []
        total_score = 0.0
        factor_count = 0

        if scheme.performance_rating is not None:
            factors.append({
                "metric": "performance_rating",
                "value": scheme.performance_rating,
                "assessment": self._assess_metric(scheme.performance_rating),
            })
            total_score += scheme.performance_rating
            factor_count += 1

        if scheme.satisfaction_score is not None:
            factors.append({
                "metric": "satisfaction_score",
                "value": scheme.satisfaction_score,
                "assessment": self._assess_metric(scheme.satisfaction_score),
            })
            total_score += scheme.satisfaction_score
            factor_count += 1

        if scheme.financial_health_score is not None:
            factors.append({
                "metric": "financial_health_score",
                "value": scheme.financial_health_score,
                "assessment": self._assess_metric(scheme.financial_health_score),
            })
            total_score += scheme.financial_health_score
            factor_count += 1

        avg_score = total_score / factor_count if factor_count > 0 else 50.0

        # Determine overall state.
        if avg_score >= 80:
            current_state = "strong"
            trend = "stable"  # Default without historical data.
            recommendation = "Maintain current approach. Low risk of operator change."
        elif avg_score >= 60:
            current_state = "adequate"
            trend = "stable"
            recommendation = "Monitor closely. Prepare competitive positioning in case of decline."
        elif avg_score >= 40:
            current_state = "underperforming"
            trend = "declining"
            recommendation = "Active opportunity. Begin building case for service improvement pitch."
        else:
            current_state = "poor"
            trend = "declining"
            recommendation = "High-priority opportunity. Operator likely to be replaced. Prepare proposal immediately."

        # EPC rating analysis.
        epc_analysis = None
        if scheme.epc_ratings:
            epc_analysis = self._analyze_epc(scheme.epc_ratings)
            if epc_analysis.get("needs_improvement"):
                factors.append({
                    "metric": "epc_rating",
                    "value": epc_analysis.get("predominant_rating", "N/A"),
                    "assessment": "needs_improvement",
                })

        return {
            "scheme_id": scheme.id,
            "scheme_name": scheme.name,
            "current_state": current_state,
            "trend": trend,
            "average_score": round(avg_score, 1),
            "factors": factors,
            "epc_analysis": epc_analysis,
            "recommendation": recommendation,
        }

    # ------------------------------------------------------------------
    # Scheme comparison
    # ------------------------------------------------------------------

    def compare_schemes(self, scheme_ids: list[int]) -> dict[str, Any]:
        """Generate a side-by-side comparison of multiple schemes.

        Parameters
        ----------
        scheme_ids : list[int]
            IDs of schemes to compare.

        Returns
        -------
        dict
            Keys: ``schemes``, ``comparison_summary``.
        """
        schemes = (
            self._db.query(ExistingScheme)
            .filter(ExistingScheme.id.in_(scheme_ids))
            .all()
        )

        if not schemes:
            return {"schemes": [], "comparison_summary": "No schemes found."}

        entries: list[dict[str, Any]] = []
        for scheme in schemes:
            bd_score = self._scorer.score_existing_scheme(scheme)
            contract_risk = self.analyze_contract_risk(scheme)

            entries.append({
                "id": scheme.id,
                "name": scheme.name,
                "postcode": scheme.postcode,
                "scheme_type": scheme.scheme_type,
                "num_units": scheme.num_units,
                "bd_score": bd_score,
                "contract_risk_level": contract_risk["risk_level"],
                "months_to_expiry": contract_risk.get("months_remaining"),
                "performance_rating": scheme.performance_rating,
                "satisfaction_score": scheme.satisfaction_score,
                "financial_health_score": scheme.financial_health_score,
                "operator": self._get_company_name(scheme.operator_company_id),
                "owner": self._get_company_name(scheme.owner_company_id),
                "asset_manager": self._get_company_name(scheme.asset_manager_company_id) if hasattr(scheme, 'asset_manager_company_id') else None,
                "landlord": self._get_company_name(scheme.landlord_company_id) if hasattr(scheme, 'landlord_company_id') else None,
            })

        # Sort by BD score descending for the summary.
        entries.sort(key=lambda e: e["bd_score"], reverse=True)

        # Summary: identify the best opportunity.
        best = entries[0]
        summary_parts = [
            f"Compared {len(entries)} schemes.",
            f"Highest BD score: {best['name']} ({best['bd_score']}).",
        ]

        critical = [e for e in entries if e["contract_risk_level"] in ("critical", "high")]
        if critical:
            summary_parts.append(
                f"{len(critical)} scheme(s) with critical/high contract risk: "
                + ", ".join(e["name"] for e in critical)
                + "."
            )

        return {
            "schemes": entries,
            "comparison_summary": " ".join(summary_parts),
        }

    # ------------------------------------------------------------------
    # Market / regional analysis
    # ------------------------------------------------------------------

    def market_analysis(self, region: str) -> dict[str, Any]:
        """Aggregate statistics for schemes in a region.

        Parameters
        ----------
        region : str
            Region name to filter by (matched against council region).

        Returns
        -------
        dict
            Keys: ``region``, ``total_schemes``, ``total_units``,
            ``avg_performance``, ``avg_satisfaction``, ``contract_expiry_timeline``,
            ``scheme_types``.
        """
        from app.models.models import Council

        council_ids = (
            self._db.query(Council.id)
            .filter(Council.region.ilike(f"%{region}%"))
            .all()
        )
        council_id_set = {cid for (cid,) in council_ids}

        if not council_id_set:
            # Fall back to matching scheme postcode or name.
            schemes = (
                self._db.query(ExistingScheme)
                .filter(ExistingScheme.address.ilike(f"%{region}%"))
                .all()
            )
        else:
            schemes = (
                self._db.query(ExistingScheme)
                .filter(ExistingScheme.council_id.in_(council_id_set))
                .all()
            )

        if not schemes:
            return {
                "region": region,
                "total_schemes": 0,
                "total_units": 0,
                "avg_performance": None,
                "avg_satisfaction": None,
                "contract_expiry_timeline": [],
                "scheme_types": {},
            }

        total_units = sum(s.num_units or 0 for s in schemes)
        perf_ratings = [s.performance_rating for s in schemes if s.performance_rating is not None]
        sat_scores = [s.satisfaction_score for s in schemes if s.satisfaction_score is not None]

        # Contract expiry timeline.
        today = datetime.date.today()
        expiry_buckets: dict[str, int] = {
            "expired": 0,
            "0_6_months": 0,
            "6_12_months": 0,
            "12_24_months": 0,
            "24_plus_months": 0,
            "unknown": 0,
        }
        for s in schemes:
            if not s.contract_end_date:
                expiry_buckets["unknown"] += 1
            elif s.contract_end_date <= today:
                expiry_buckets["expired"] += 1
            elif s.contract_end_date <= today + datetime.timedelta(days=180):
                expiry_buckets["0_6_months"] += 1
            elif s.contract_end_date <= today + datetime.timedelta(days=365):
                expiry_buckets["6_12_months"] += 1
            elif s.contract_end_date <= today + datetime.timedelta(days=730):
                expiry_buckets["12_24_months"] += 1
            else:
                expiry_buckets["24_plus_months"] += 1

        # Scheme type breakdown.
        type_counts: dict[str, int] = {}
        for s in schemes:
            st = s.scheme_type or "Unknown"
            type_counts[st] = type_counts.get(st, 0) + 1

        return {
            "region": region,
            "total_schemes": len(schemes),
            "total_units": total_units,
            "avg_performance": round(sum(perf_ratings) / len(perf_ratings), 1) if perf_ratings else None,
            "avg_satisfaction": round(sum(sat_scores) / len(sat_scores), 1) if sat_scores else None,
            "contract_expiry_timeline": expiry_buckets,
            "scheme_types": type_counts,
        }

    # ------------------------------------------------------------------
    # Full scheme report
    # ------------------------------------------------------------------

    def generate_scheme_report(self, scheme_id: int) -> dict[str, Any]:
        """Generate a comprehensive analysis report for a single scheme.

        Parameters
        ----------
        scheme_id : int
            ID of the scheme to analyse.

        Returns
        -------
        dict
            Full report with BD score, contract risk, performance trend,
            company details, contacts, and pipeline status.
        """
        scheme = self._db.query(ExistingScheme).get(scheme_id)
        if not scheme:
            raise ValueError(f"Scheme {scheme_id} not found")

        bd_score = self._scorer.score_existing_scheme(scheme)
        contract_risk = self.analyze_contract_risk(scheme)
        performance = self.analyze_performance_trend(scheme)

        # Operator and owner details.
        operator_info = self._get_company_info(scheme.operator_company_id)
        owner_info = self._get_company_info(scheme.owner_company_id)

        # Contacts at operator company.
        contacts: list[dict[str, Any]] = []
        if scheme.operator_company_id:
            contact_records = (
                self._db.query(Contact)
                .filter(Contact.company_id == scheme.operator_company_id)
                .all()
            )
            contacts = [
                {
                    "name": c.full_name,
                    "title": c.job_title,
                    "email": c.email,
                    "phone": c.phone,
                    "source": c.source,
                    "confidence": c.confidence_score,
                }
                for c in contact_records
            ]

        # Pipeline status.
        pipeline = None
        if scheme.pipeline_opportunity:
            po = scheme.pipeline_opportunity
            pipeline = {
                "stage": po.stage,
                "priority": po.priority,
                "bd_score": po.bd_score,
                "assigned_to": po.assigned_to,
                "last_contact_date": str(po.last_contact_date) if po.last_contact_date else None,
                "next_action": po.next_action,
                "next_action_date": str(po.next_action_date) if po.next_action_date else None,
            }

        # Contract history
        from app.models.models import SchemeContract
        contracts = (
            self._db.query(SchemeContract)
            .filter(SchemeContract.scheme_id == scheme_id)
            .order_by(SchemeContract.contract_start_date.desc().nullslast())
            .all()
        )
        contract_history = [
            {
                "contract_reference": c.contract_reference,
                "operator": self._get_company_name(c.operator_company_id),
                "client": self._get_company_name(c.client_company_id),
                "start_date": str(c.contract_start_date) if c.contract_start_date else None,
                "end_date": str(c.contract_end_date) if c.contract_end_date else None,
                "value": c.contract_value,
                "source": c.source,
                "is_current": c.is_current,
            }
            for c in contracts
        ]

        report = {
            "scheme": {
                "id": scheme.id,
                "name": scheme.name,
                "address": scheme.address,
                "postcode": scheme.postcode,
                "scheme_type": scheme.scheme_type,
                "num_units": scheme.num_units,
                "regulatory_rating": scheme.regulatory_rating,
            },
            "bd_score": bd_score,
            "contract_risk": contract_risk,
            "performance_trend": performance,
            "operator": operator_info,
            "owner": owner_info,
            "contacts": contacts,
            "pipeline": pipeline,
            "generated_at": datetime.datetime.utcnow().isoformat(),
        }

        report["contract_history"] = contract_history
        report["asset_manager"] = self._get_company_info(scheme.asset_manager_company_id) if hasattr(scheme, 'asset_manager_company_id') else None
        report["landlord"] = self._get_company_info(scheme.landlord_company_id) if hasattr(scheme, 'landlord_company_id') else None

        logger.info("scheme_report_generated", scheme_id=scheme_id, bd_score=bd_score)
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _assess_metric(value: float) -> str:
        """Return a qualitative assessment for a 0-100 metric."""
        if value >= 80:
            return "strong"
        if value >= 60:
            return "adequate"
        if value >= 40:
            return "underperforming"
        return "poor"

    @staticmethod
    def _analyze_epc(epc_ratings: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
        """Analyse EPC ratings for a scheme."""
        if not epc_ratings:
            return {"available": False, "needs_improvement": False}

        # EPC ratings might be stored as a list of rating strings or a dict.
        ratings: list[str] = []
        if isinstance(epc_ratings, dict):
            ratings = list(epc_ratings.values())
        elif isinstance(epc_ratings, list):
            ratings = [str(r) for r in epc_ratings]

        rating_order = {"A": 7, "B": 6, "C": 5, "D": 4, "E": 3, "F": 2, "G": 1}
        valid_ratings = [r.upper().strip() for r in ratings if r.upper().strip() in rating_order]

        if not valid_ratings:
            return {"available": False, "needs_improvement": False}

        avg_score = sum(rating_order.get(r, 0) for r in valid_ratings) / len(valid_ratings)

        # Find predominant rating.
        from collections import Counter
        predominant = Counter(valid_ratings).most_common(1)[0][0]

        return {
            "available": True,
            "total_assessed": len(valid_ratings),
            "predominant_rating": predominant,
            "average_score": round(avg_score, 1),
            "needs_improvement": avg_score < 4,  # Below D.
            "rating_distribution": dict(Counter(valid_ratings)),
        }

    def _get_company_name(self, company_id: int | None) -> str | None:
        """Look up a company name by ID."""
        if not company_id:
            return None
        company = self._db.query(Company).get(company_id)
        return company.name if company else None

    def _get_company_info(self, company_id: int | None) -> dict[str, Any] | None:
        """Get basic company info by ID."""
        if not company_id:
            return None
        company = self._db.query(Company).get(company_id)
        if not company:
            return None
        return {
            "id": company.id,
            "name": company.name,
            "company_type": company.company_type,
            "website": company.website,
            "companies_house_number": company.companies_house_number,
        }
