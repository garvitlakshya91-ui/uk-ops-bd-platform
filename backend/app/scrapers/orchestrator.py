"""
Scraping orchestrator — manages scheduling and execution of all scrapers.

Coordinates scraping jobs across councils, tracks runs via ScraperRun
records, saves results to the database, and generates alerts for new
applications that match BD criteria.
"""

from __future__ import annotations

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Any, Optional, Type

import structlog
from sqlalchemy.orm import Session

from app.config import settings
from app.models.models import (
    Council,
    PlanningApplication,
    ScraperRun,
    Alert,
)
from app.scrapers.base import BaseScraper, ScraperMetrics
from app.scrapers.idox_scraper import IdoxScraper, IdoxCouncilConfig, IDOX_COUNCILS
from app.scrapers.civica_scraper import CivicaScraper, CivicaCouncilConfig, CIVICA_COUNCILS
from app.scrapers.nec_scraper import NECScraper, NECCouncilConfig, NEC_COUNCILS
from app.scrapers.planning_data_api import PlanningDataAPIScraper
from app.scrapers.find_a_tender import FindATenderScraper
from app.scrapers.epc_scraper import EPCScraper
from app.scrapers.rsh_scraper import RSHScraper

logger = structlog.get_logger(__name__)

# Scheme types that are considered high-value BD targets
HIGH_VALUE_SCHEME_TYPES = {"BTR", "PBSA", "Co-living", "Senior"}

# Minimum unit count to trigger an alert
ALERT_MIN_UNITS = 50


class ScraperOrchestrator:
    """
    Manages all scrapers, schedules jobs based on council frequency
    settings, runs scrapers concurrently, persists results, and
    generates alerts.
    """

    def __init__(
        self,
        db_session: Session,
        max_concurrency: int = 5,
        proxy_url: str | None = None,
    ) -> None:
        self.db = db_session
        self.max_concurrency = max_concurrency
        self.proxy_url = proxy_url or settings.PROXY_URL
        self.log = structlog.get_logger(component="ScraperOrchestrator")
        self._semaphore = asyncio.Semaphore(max_concurrency)

    # ------------------------------------------------------------------
    # Scraper factory
    # ------------------------------------------------------------------

    def _build_scraper(self, council: Council) -> BaseScraper | None:
        """
        Instantiate the appropriate scraper for a council based on its
        portal_type.
        """
        portal_type = (council.portal_type or "").lower()

        if portal_type == "idox":
            config = IdoxCouncilConfig(
                name=council.name,
                council_id=council.id,
                base_url=council.portal_url or "",
            )
            return IdoxScraper(
                config=config,
                proxy_url=self.proxy_url,
                use_playwright=True,
            )

        elif portal_type == "civica":
            config = CivicaCouncilConfig(
                name=council.name,
                council_id=council.id,
                base_url=council.portal_url or "",
            )
            return CivicaScraper(config=config, proxy_url=self.proxy_url)

        elif portal_type == "nec":
            config = NECCouncilConfig(
                name=council.name,
                council_id=council.id,
                base_url=council.portal_url or "",
            )
            return NECScraper(config=config, proxy_url=self.proxy_url)

        elif portal_type == "api":
            return PlanningDataAPIScraper(
                council_name=council.name,
                council_id=council.id,
                proxy_url=self.proxy_url,
            )

        else:
            self.log.warning(
                "unknown_portal_type",
                council=council.name,
                portal_type=portal_type,
            )
            return None

    # ------------------------------------------------------------------
    # Determine which councils need scraping
    # ------------------------------------------------------------------

    def get_councils_due(self) -> list[Council]:
        """
        Query the database for active councils whose last_scraped_at
        is older than their scrape_frequency_hours (or have never been
        scraped).
        """
        now = datetime.utcnow()
        councils = (
            self.db.query(Council)
            .filter(Council.active.is_(True))
            .all()
        )

        due: list[Council] = []
        for council in councils:
            if council.last_scraped_at is None:
                due.append(council)
            else:
                threshold = council.last_scraped_at + timedelta(
                    hours=council.scrape_frequency_hours
                )
                if now >= threshold:
                    due.append(council)

        self.log.info(
            "councils_due",
            total_active=len(councils),
            due=len(due),
        )
        return due

    # ------------------------------------------------------------------
    # Create / update ScraperRun records
    # ------------------------------------------------------------------

    def _create_scraper_run(self, council: Council, scraper_name: str) -> ScraperRun:
        run = ScraperRun(
            council_id=council.id,
            started_at=datetime.utcnow(),
            status="running",
            applications_found=0,
            applications_new=0,
            applications_updated=0,
            errors_count=0,
        )
        self.db.add(run)
        self.db.flush()
        return run

    def _complete_scraper_run(
        self,
        run: ScraperRun,
        metrics: ScraperMetrics,
        status: str = "success",
    ) -> None:
        run.status = status
        run.completed_at = datetime.utcnow()
        run.applications_found = metrics.applications_found
        run.applications_new = metrics.applications_new
        run.applications_updated = metrics.applications_updated
        run.errors_count = len(metrics.errors)
        run.error_details = {"errors": metrics.errors} if metrics.errors else None
        run.duration_seconds = metrics.elapsed_seconds
        self.db.flush()

    # ------------------------------------------------------------------
    # Save applications to database
    # ------------------------------------------------------------------

    def _save_applications(
        self,
        council: Council,
        applications: list[dict[str, Any]],
        metrics: ScraperMetrics,
    ) -> list[PlanningApplication]:
        """
        Upsert planning applications into the database.

        For each application:
        - If reference+council_id already exists, update changed fields.
        - Otherwise, insert as a new record.

        Returns the list of newly created PlanningApplication instances.
        """
        new_apps: list[PlanningApplication] = []

        for app_data in applications:
            reference = app_data.get("reference", "")
            if not reference:
                continue

            existing = (
                self.db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == reference,
                    PlanningApplication.council_id == council.id,
                )
                .first()
            )

            if existing:
                # Update changed fields
                updated = False
                for field_name in (
                    "address",
                    "postcode",
                    "description",
                    "applicant_name",
                    "agent_name",
                    "application_type",
                    "status",
                    "decision",
                    "scheme_type",
                    "total_units",
                    "submitted_date",
                    "validated_date",
                    "decision_date",
                    "consultation_end_date",
                    "committee_date",
                    "documents_url",
                    "portal_url",
                    "ward",
                    "source",
                    "is_btr",
                    "is_pbsa",
                    "is_affordable",
                    "raw_data",
                ):
                    new_val = app_data.get(field_name)
                    # Also check legacy field names (num_units -> total_units,
                    # submission_date -> submitted_date)
                    if new_val is None and field_name == "total_units":
                        new_val = app_data.get("num_units")
                    if new_val is None and field_name == "submitted_date":
                        new_val = app_data.get("submission_date")
                    if new_val is not None and new_val != getattr(existing, field_name, None):
                        setattr(existing, field_name, new_val)
                        updated = True

                if updated:
                    metrics.applications_updated += 1
                    self.log.debug(
                        "application_updated",
                        reference=reference,
                        council=council.name,
                    )
            else:
                # Create new record
                new_app = PlanningApplication(
                    reference=reference,
                    council_id=council.id,
                    address=app_data.get("address"),
                    postcode=app_data.get("postcode"),
                    description=app_data.get("description"),
                    applicant_name=app_data.get("applicant_name"),
                    agent_name=app_data.get("agent_name"),
                    application_type=app_data.get("application_type"),
                    status=app_data.get("status", "Unknown"),
                    decision=app_data.get("decision"),
                    scheme_type=app_data.get("scheme_type", "Unknown"),
                    total_units=app_data.get("total_units") or app_data.get("num_units"),
                    submitted_date=app_data.get("submitted_date") or app_data.get("submission_date"),
                    validated_date=app_data.get("validated_date"),
                    decision_date=app_data.get("decision_date"),
                    consultation_end_date=app_data.get("consultation_end_date"),
                    committee_date=app_data.get("committee_date"),
                    documents_url=app_data.get("documents_url"),
                    portal_url=app_data.get("portal_url"),
                    ward=app_data.get("ward"),
                    source=app_data.get("source"),
                    is_btr=app_data.get("is_btr", False),
                    is_pbsa=app_data.get("is_pbsa", False),
                    is_affordable=app_data.get("is_affordable", False),
                    raw_data=app_data.get("raw_data"),
                )
                self.db.add(new_app)
                new_apps.append(new_app)
                metrics.applications_new += 1
                self.log.debug(
                    "application_new",
                    reference=reference,
                    council=council.name,
                    scheme_type=app_data.get("scheme_type"),
                )

        self.db.flush()
        return new_apps

    # ------------------------------------------------------------------
    # Alert generation
    # ------------------------------------------------------------------

    def _create_alerts(
        self, new_applications: list[PlanningApplication]
    ) -> None:
        """
        Generate alerts for new applications that match BD criteria:
        - High-value scheme types (BTR, PBSA, Co-living, Senior)
        - Significant unit counts (>= ALERT_MIN_UNITS)
        """
        for app in new_applications:
            should_alert = False
            reasons: list[str] = []

            if app.scheme_type in HIGH_VALUE_SCHEME_TYPES:
                should_alert = True
                reasons.append(f"scheme_type={app.scheme_type}")

            unit_count = getattr(app, "total_units", None) or getattr(app, "num_units", None)
            if unit_count and unit_count >= ALERT_MIN_UNITS:
                should_alert = True
                reasons.append(f"units={unit_count}")

            if should_alert:
                alert = Alert(
                    type="new_application",
                    title=f"New {app.scheme_type} application: {app.reference}",
                    message=(
                        f"A new {app.scheme_type} planning application has been found.\n\n"
                        f"Reference: {app.reference}\n"
                        f"Address: {app.address or 'N/A'}\n"
                        f"Units: {unit_count or 'N/A'}\n"
                        f"Description: {(app.description or '')[:200]}\n"
                        f"Criteria: {', '.join(reasons)}"
                    ),
                    entity_type="planning_application",
                    entity_id=app.id,
                    is_read=False,
                )
                self.db.add(alert)
                self.log.info(
                    "alert_created",
                    reference=app.reference,
                    scheme_type=app.scheme_type,
                    reasons=reasons,
                )

        self.db.flush()

    # ------------------------------------------------------------------
    # Run a single council scraper
    # ------------------------------------------------------------------

    async def run_council(self, council: Council) -> ScraperMetrics:
        """
        Run the scraper for a single council.

        Creates a ScraperRun record, executes the scraper, saves results,
        generates alerts, and updates the council's last_scraped_at.
        """
        scraper = self._build_scraper(council)
        if scraper is None:
            self.log.error("no_scraper", council=council.name)
            raise ValueError(f"No scraper available for council {council.name}")

        scraper_name = type(scraper).__name__
        run = self._create_scraper_run(council, scraper_name)

        self.log.info(
            "council_scrape_start",
            council=council.name,
            scraper=scraper_name,
            run_id=run.id,
        )

        try:
            async with scraper:
                applications = await scraper.run()

            new_apps = self._save_applications(council, applications, scraper.metrics)
            self._create_alerts(new_apps)

            # Update council last_scraped_at
            council.last_scraped_at = datetime.utcnow()

            status = "success" if not scraper.metrics.errors else "partial"
            self._complete_scraper_run(run, scraper.metrics, status=status)
            self.db.commit()

            self.log.info(
                "council_scrape_complete",
                council=council.name,
                **scraper.metrics.to_dict(),
            )
            return scraper.metrics

        except Exception as exc:
            scraper.metrics.record_error(exc, context="run_council")
            self._complete_scraper_run(run, scraper.metrics, status="failed")

            # Create a scraper failure alert
            alert = Alert(
                type="scraper_failure",
                title=f"Scraper failed: {council.name}",
                message=(
                    f"The {scraper_name} scraper for {council.name} failed.\n\n"
                    f"Error: {str(exc)}\n"
                    f"Traceback: {traceback.format_exc()[:500]}"
                ),
                entity_type="scraper_run",
                entity_id=run.id,
                is_read=False,
            )
            self.db.add(alert)
            self.db.commit()

            self.log.error(
                "council_scrape_failed",
                council=council.name,
                error=str(exc),
            )
            return scraper.metrics

    # ------------------------------------------------------------------
    # Run a single council with concurrency control
    # ------------------------------------------------------------------

    async def _run_council_semaphored(self, council: Council) -> ScraperMetrics:
        """Run a council scraper, respecting the concurrency semaphore."""
        async with self._semaphore:
            return await self.run_council(council)

    # ------------------------------------------------------------------
    # Run all due councils
    # ------------------------------------------------------------------

    async def run_due_councils(self) -> dict[str, Any]:
        """
        Identify all councils due for scraping and run their scrapers
        concurrently (limited by max_concurrency).

        Returns a summary dict of the run.
        """
        councils = self.get_councils_due()
        if not councils:
            self.log.info("no_councils_due")
            return {
                "councils_due": 0,
                "councils_completed": 0,
                "councils_failed": 0,
                "total_applications": 0,
            }

        self.log.info(
            "orchestrator_start",
            councils_due=len(councils),
            max_concurrency=self.max_concurrency,
        )

        tasks = [
            self._run_council_semaphored(council)
            for council in councils
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Summarise
        completed = 0
        failed = 0
        total_found = 0
        total_new = 0

        for council, result in zip(councils, results):
            if isinstance(result, Exception):
                failed += 1
                self.log.error(
                    "council_exception",
                    council=council.name,
                    error=str(result),
                )
            elif isinstance(result, ScraperMetrics):
                if result.errors:
                    failed += 1
                else:
                    completed += 1
                total_found += result.applications_found
                total_new += result.applications_new

        summary = {
            "councils_due": len(councils),
            "councils_completed": completed,
            "councils_failed": failed,
            "total_applications_found": total_found,
            "total_applications_new": total_new,
        }

        self.log.info("orchestrator_complete", **summary)
        return summary

    # ------------------------------------------------------------------
    # Run all scrapers (full sweep)
    # ------------------------------------------------------------------

    async def run_all(self) -> dict[str, Any]:
        """
        Run scrapers for ALL active councils, regardless of schedule.
        Also runs supplementary scrapers (Find a Tender, EPC, RSH).
        """
        councils = (
            self.db.query(Council)
            .filter(Council.active.is_(True))
            .all()
        )

        self.log.info("full_sweep_start", total_councils=len(councils))

        # Run council scrapers
        tasks = [
            self._run_council_semaphored(council)
            for council in councils
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Run supplementary scrapers
        supplementary_results = await self._run_supplementary_scrapers()

        summary = {
            "councils_run": len(councils),
            "supplementary": supplementary_results,
        }
        self.log.info("full_sweep_complete", **summary)
        return summary

    async def _run_supplementary_scrapers(self) -> dict[str, Any]:
        """Run Find a Tender, EPC, and RSH scrapers."""
        results: dict[str, Any] = {}

        # Find a Tender
        try:
            async with FindATenderScraper(proxy_url=self.proxy_url) as scraper:
                tender_results = await scraper.run()
                results["find_a_tender"] = {
                    "status": "success",
                    "count": len(tender_results),
                }
                self.log.info(
                    "supplementary_complete",
                    scraper="FindATender",
                    count=len(tender_results),
                )
        except Exception as exc:
            results["find_a_tender"] = {
                "status": "failed",
                "error": str(exc),
            }
            self.log.error(
                "supplementary_failed",
                scraper="FindATender",
                error=str(exc),
            )

        # RSH
        try:
            async with RSHScraper(proxy_url=self.proxy_url) as scraper:
                rsh_results = await scraper.run()
                results["rsh"] = {
                    "status": "success",
                    "count": len(rsh_results),
                }
                self.log.info(
                    "supplementary_complete",
                    scraper="RSH",
                    count=len(rsh_results),
                )
        except Exception as exc:
            results["rsh"] = {
                "status": "failed",
                "error": str(exc),
            }
            self.log.error(
                "supplementary_failed",
                scraper="RSH",
                error=str(exc),
            )

        return results

    # ------------------------------------------------------------------
    # On-demand single council run
    # ------------------------------------------------------------------

    async def run_single_council(self, council_id: int) -> dict[str, Any]:
        """
        Run the scraper for a specific council by ID, on demand.

        Returns a dict with run results and metrics.
        """
        council = self.db.query(Council).filter(Council.id == council_id).first()
        if not council:
            raise ValueError(f"Council with id={council_id} not found")

        self.log.info("on_demand_run", council=council.name, council_id=council_id)
        metrics = await self.run_council(council)

        return {
            "council": council.name,
            "council_id": council_id,
            "status": "success" if not metrics.errors else "partial",
            **metrics.to_dict(),
        }

    # ------------------------------------------------------------------
    # Seed councils from built-in lists
    # ------------------------------------------------------------------

    def seed_councils(self) -> int:
        """
        Populate the councils table from the comprehensive LPA mapping
        (308 English councils) plus portal scraper lists.

        Upserts: creates new councils and updates organisation_entity/region
        on existing ones.

        Returns the number of councils inserted or updated.
        """
        from app.scrapers.council_mapping import ENGLISH_LPA_MAPPING

        inserted = 0
        updated = 0

        for lpa in ENGLISH_LPA_MAPPING:
            name = lpa["name"]
            existing = (
                self.db.query(Council)
                .filter(Council.name == name)
                .first()
            )

            if existing:
                # Update organisation_entity and region on existing rows
                changed = False
                if lpa.get("organisation_entity") and existing.organisation_entity != lpa["organisation_entity"]:
                    existing.organisation_entity = lpa["organisation_entity"]
                    changed = True
                if lpa.get("region") and existing.region != lpa.get("region"):
                    existing.region = lpa["region"]
                    changed = True
                if changed:
                    updated += 1
            else:
                scraper_class_map = {
                    "idox": "IdoxScraper",
                    "civica": "CivicaScraper",
                    "nec": "NECScraper",
                }
                council = Council(
                    name=name,
                    portal_type=lpa.get("portal_type", "api"),
                    portal_url=lpa.get("portal_url"),
                    scraper_class=scraper_class_map.get(lpa.get("portal_type", ""), None),
                    active=True,
                    region=lpa.get("region"),
                    organisation_entity=lpa.get("organisation_entity"),
                    scrape_frequency_hours=24 if lpa.get("portal_type") == "idox" else 48,
                )
                self.db.add(council)
                inserted += 1

        self.db.commit()
        self.log.info("councils_seeded", inserted=inserted, updated=updated)
        return inserted + updated
