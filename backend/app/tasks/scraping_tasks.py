"""Celery tasks for planning data scraping.

Schedules and executes scraping runs for council planning portals,
the planning.data.gov.uk API, and the Find a Tender service.
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

import structlog
from celery import group
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.models import Alert, Council, ExistingScheme, ScraperRun
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db() -> Session:
    """Create a new database session for use inside a Celery task."""
    return SessionLocal()


def _trigger_enrichment():
    """Dispatch the quick enrichment pipeline after successful scraping."""
    try:
        from app.tasks.scheme_enrichment_pipeline import enrich_new_schemes
        enrich_new_schemes.delay()
        logger.info("post_scrape_enrichment_dispatched")
    except Exception:
        logger.warning("post_scrape_enrichment_dispatch_failed", exc_info=True)


def _run_async(coro):
    """Run an async coroutine from synchronous Celery task context."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


async def _run_scraper(scraper):
    """Execute a scraper using its async context manager and return raw results."""
    async with scraper:
        return await scraper.run()


def _save_planning_applications(
    db: Session,
    council_id: int,
    results: list[dict[str, Any]],
) -> dict[str, int]:
    """Persist scraped planning application results to the database.

    Upserts by (reference, council_id): updates existing records, inserts new
    ones.  Returns summary counts.

    Parameters
    ----------
    db : Session
        Active SQLAlchemy session.
    council_id : int
        ID of the council these applications belong to.
    results : list
        List of normalised application dicts from scraper.run().

    Returns
    -------
    dict with keys: found, new, updated, errors.
    """
    from app.models.models import PlanningApplication

    found = len(results)
    new = 0
    updated = 0
    errors = 0

    for app_data in results:
        try:
            reference = app_data.get("reference") or app_data.get("notice_id", "")
            if not reference:
                errors += 1
                continue

            existing = (
                db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == reference,
                    PlanningApplication.council_id == council_id,
                )
                .first()
            )

            if existing:
                # Update fields that may have changed.
                _update_fields = [
                    "address", "postcode", "description", "applicant_name",
                    "agent_name", "application_type", "status", "decision",
                    "scheme_type", "total_units", "submitted_date",
                    "validated_date", "decision_date",
                    "consultation_end_date", "committee_date",
                    "documents_url", "portal_url", "ward", "source",
                    "is_btr", "is_pbsa", "is_affordable", "raw_data",
                ]
                changed = False
                for field in _update_fields:
                    new_val = app_data.get(field)
                    # Handle legacy field names from older scrapers
                    if new_val is None and field == "total_units":
                        new_val = app_data.get("num_units")
                    if new_val is None and field == "submitted_date":
                        new_val = app_data.get("submission_date")
                    if new_val and new_val != getattr(existing, field, None):
                        setattr(existing, field, new_val)
                        changed = True
                if changed:
                    updated += 1
            else:
                app = PlanningApplication(
                    reference=reference,
                    council_id=council_id,
                    address=app_data.get("address"),
                    postcode=app_data.get("postcode"),
                    description=app_data.get("description"),
                    applicant_name=app_data.get("applicant_name"),
                    agent_name=app_data.get("agent_name"),
                    application_type=app_data.get("application_type"),
                    status=app_data.get("status"),
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
                db.add(app)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_planning_application_failed",
                reference=app_data.get("reference"),
                council_id=council_id,
            )
            errors += 1
            db.rollback()

    return {"found": found, "new": new, "updated": updated, "errors": errors}


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_council",
    max_retries=3,
    default_retry_delay=300,
    acks_late=True,
)
def scrape_council(self, council_id: int) -> dict[str, Any]:
    """Run the scraper for a single council.

    Parameters
    ----------
    council_id : int
        ID of the council to scrape.

    Returns
    -------
    dict
        Summary of the scraping run including counts of applications
        found, created, and updated.
    """
    db = _get_db()
    try:
        council = db.query(Council).get(council_id)
        if not council:
            logger.error("scrape_council_not_found", council_id=council_id)
            return {"error": f"Council {council_id} not found"}

        if not council.active:
            logger.info("scrape_council_inactive", council_id=council_id, name=council.name)
            return {"skipped": True, "reason": "Council inactive"}

        log = logger.bind(council_id=council_id, council_name=council.name)
        log.info("scrape_council_started")

        # Record the scraping run.
        run = ScraperRun(
            council_id=council_id,
            status="running",
            applications_found=0,
            applications_new=0,
            applications_updated=0,
            errors_count=0,
        )
        db.add(run)
        db.commit()

        try:
            # Dynamically load and execute the appropriate scraper.
            scraper_class_name = council.scraper_class or council.portal_type
            scraper = _load_scraper(scraper_class_name, council)

            # Scrapers use async context managers — run via _run_async.
            raw_results = _run_async(_run_scraper(scraper))

            # raw_results is a list of dicts from scraper.run()
            if isinstance(raw_results, list):
                # Persist results via the orchestrator's save logic.
                from app.scrapers.orchestrator import ScraperOrchestrator
                save_result = _save_planning_applications(
                    db, council.id, raw_results
                )
                run.applications_found = save_result["found"]
                run.applications_new = save_result["new"]
                run.applications_updated = save_result["updated"]
                run.errors_count = save_result.get("errors", 0)
            else:
                result = raw_results if isinstance(raw_results, dict) else {}
                run.applications_found = result.get("found", 0)
                run.applications_new = result.get("new", 0)
                run.applications_updated = result.get("updated", 0)
                run.errors_count = result.get("errors", 0)

            run.error_details = None
            run.status = "success" if run.errors_count == 0 else "partial"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)

            if hasattr(run, "duration_seconds") and run.started_at:
                run.duration_seconds = (
                    run.completed_at - run.started_at
                ).total_seconds()

            council.last_scraped_at = datetime.datetime.now(datetime.timezone.utc)

            db.commit()

            log.info(
                "scrape_council_completed",
                found=run.applications_found,
                new=run.applications_new,
                updated=run.applications_updated,
                errors=run.errors_count,
            )

            return {
                "council_id": council_id,
                "status": run.status,
                "found": run.applications_found,
                "new": run.applications_new,
                "updated": run.applications_updated,
                "errors": run.errors_count,
            }

        except Exception as exc:
            run.status = "failed"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            run.errors_count = 1
            run.error_details = {"exception": str(exc)}
            db.commit()

            log.exception("scrape_council_failed")
            raise self.retry(exc=exc)

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.scraping_tasks.scrape_all_councils",
    acks_late=True,
)
def scrape_all_councils() -> dict[str, Any]:
    """Schedule scraping for all active councils.

    Dispatches individual :func:`scrape_council` tasks as a Celery group
    for parallel execution.

    Returns
    -------
    dict
        Summary with count of councils scheduled.
    """
    db = _get_db()
    try:
        active_councils = (
            db.query(Council)
            .filter(Council.active.is_(True))
            .all()
        )

        if not active_councils:
            logger.info("scrape_all_councils_none_active")
            return {"scheduled": 0}

        # Create a group of individual scraping tasks.
        task_group = group(
            scrape_council.s(council.id) for council in active_councils
        )
        result = task_group.apply_async()

        # Schedule a council-id backfill ~30 min after dispatch so any newly
        # ingested NULL-council schemes get resolved without waiting for the
        # hourly beat tick. Uses Postcodes.io + creates missing council rows.
        # Idempotent — does nothing when there's nothing to backfill.
        from app.tasks.enrichment_tasks import backfill_scheme_council_ids
        backfill_scheme_council_ids.apply_async(countdown=1800)

        logger.info(
            "scrape_all_councils_scheduled",
            count=len(active_councils),
            group_id=result.id,
        )

        return {
            "scheduled": len(active_councils),
            "group_id": str(result.id),
            "council_ids": [c.id for c in active_councils],
        }
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_planning_data_api",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def scrape_planning_data_api(self) -> dict[str, Any]:
    """Run the planning.data.gov.uk API scraper.

    Fetches recent planning applications from the centralised government
    API and upserts them into our database.

    Returns
    -------
    dict
        Summary of applications processed.
    """
    db = _get_db()
    try:
        logger.info("scrape_planning_data_api_started")

        run = ScraperRun(
            council_id=None,
            status="running",
            applications_found=0,
            applications_new=0,
            applications_updated=0,
            errors_count=0,
        )
        # ScraperRun requires council_id in some model variants; handle both.
        if hasattr(ScraperRun, "scraper_name"):
            run.scraper_name = "planning_data_api"

        db.add(run)
        db.commit()

        try:
            from app.config import settings

            scraper = _load_scraper("planning_data_api", None)
            result = scraper.scrape()

            run.applications_found = result.get("found", 0)
            run.applications_new = result.get("new", 0)
            run.applications_updated = result.get("updated", 0)
            run.errors_count = result.get("errors", 0)
            run.status = "success" if run.errors_count == 0 else "partial"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            if hasattr(run, "finished_at"):
                run.finished_at = run.completed_at

            db.commit()

            logger.info(
                "scrape_planning_data_api_completed",
                found=run.applications_found,
                new=run.applications_new,
            )

            return {
                "status": run.status,
                "found": run.applications_found,
                "new": run.applications_new,
                "updated": run.applications_updated,
            }

        except Exception as exc:
            run.status = "failed"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            run.error_details = {"exception": str(exc)}
            db.commit()

            logger.exception("scrape_planning_data_api_failed")
            raise self.retry(exc=exc)

    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_find_a_tender",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def scrape_find_a_tender(self) -> dict[str, Any]:
    """Run the Find a Tender scraper and persist results via ingestion pipeline.

    This task scrapes housing management contract notices from the UK
    government Find a Tender service and persists them as ExistingScheme
    and SchemeContract records.
    """
    db = _get_db()
    try:
        logger.info("scrape_find_a_tender_started")
        from app.scrapers.find_a_tender import FindATenderScraper
        from app.scrapers.scheme_ingest import ingest_tender_contracts

        scraper = FindATenderScraper()
        raw_results = _run_async(_run_scraper(scraper))

        # Parse each raw result into contract detail dicts
        parsed = []
        for raw in raw_results:
            try:
                detail = _run_async(scraper.parse_application(raw))
                parsed.append(detail)
            except Exception as exc:
                logger.warning(
                    "tender_parse_failed",
                    notice_id=raw.get("notice_id"),
                    error=str(exc),
                )

        # Persist through the ingestion pipeline
        ingest_stats = ingest_tender_contracts(parsed, db)

        logger.info("scrape_find_a_tender_completed", **ingest_stats)
        _trigger_enrichment()
        return {"status": "success", **ingest_stats}

    except Exception as exc:
        logger.exception("scrape_find_a_tender_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_contracts_finder",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def scrape_contracts_finder(self) -> dict[str, Any]:
    """Run the Contracts Finder scraper and persist results.

    Scrapes housing-related contract notices from the UK Contracts Finder
    service and persists them via the ingestion pipeline.
    """
    db = _get_db()
    try:
        logger.info("scrape_contracts_finder_started")
        from app.scrapers.contracts_finder import ContractsFinderScraper
        from app.scrapers.scheme_ingest import ingest_contracts_finder

        scraper = ContractsFinderScraper()
        raw_results = _run_async(_run_scraper(scraper))

        ingest_stats = ingest_contracts_finder(raw_results, db)

        logger.info("scrape_contracts_finder_completed", **ingest_stats)
        _trigger_enrichment()
        return {"status": "success", **ingest_stats}

    except Exception as exc:
        logger.exception("scrape_contracts_finder_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_rsh_judgements",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def scrape_rsh_judgements(self) -> dict[str, Any]:
    """Run the RSH (Regulator of Social Housing) judgements scraper.

    Scrapes regulatory judgement notices and persists them via the
    ingestion pipeline.
    """
    db = _get_db()
    try:
        logger.info("scrape_rsh_judgements_started")
        from app.scrapers.rsh_scraper import RSHScraper
        from app.scrapers.scheme_ingest import ingest_rsh_judgements

        scraper = RSHScraper()
        raw_results = _run_async(_run_scraper(scraper))

        # Parse each raw result
        parsed = []
        for raw in raw_results:
            try:
                detail = _run_async(scraper.parse_application(raw))
                parsed.append(detail)
            except Exception as exc:
                logger.warning(
                    "rsh_parse_failed",
                    judgement_id=raw.get("judgement_id"),
                    error=str(exc),
                )

        ingest_stats = ingest_rsh_judgements(parsed, db)

        logger.info("scrape_rsh_judgements_completed", **ingest_stats)
        return {"status": "success", **ingest_stats}

    except Exception as exc:
        logger.exception("scrape_rsh_judgements_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.ingest_rsh_registered_providers",
    max_retries=3,
    default_retry_delay=300,
    acks_late=True,
)
def ingest_rsh_registered_providers(self) -> dict[str, Any]:
    """Download and ingest the RSH Registered Providers list.

    Fetches the latest monthly CSV/XLSX from GOV.UK and upserts all
    registered providers into the Company table.  Designed to run monthly.
    """
    db = _get_db()
    try:
        logger.info("ingest_rsh_registered_providers_started")
        from app.scrapers.rsh_registered_providers import RSHRegisteredProvidersScraper
        from app.scrapers.scheme_ingest import ingest_rsh_registered_providers as _ingest

        scraper = RSHRegisteredProvidersScraper()
        providers = _run_async(scraper.fetch_registered_providers())
        logger.info("ingest_rsh_rp_list_fetched", count=len(providers))

        result = _ingest(providers, db)
        logger.info("ingest_rsh_registered_providers_completed", **result)
        return {"status": "success", "providers_fetched": len(providers), **result}

    except Exception as exc:
        logger.exception("ingest_rsh_registered_providers_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.ingest_rsh_sdr",
    max_retries=3,
    default_retry_delay=300,
    acks_late=True,
)
def ingest_rsh_sdr(self) -> dict[str, Any]:
    """Download and ingest the RSH Statistical Data Return stock figures.

    Fetches the latest annual SDR XLSX from GOV.UK and enriches Company
    records with units_owned / units_managed figures.  Also flags providers
    that manage substantially more stock than they own as Operators.
    Designed to run annually (or on-demand).
    """
    db = _get_db()
    try:
        logger.info("ingest_rsh_sdr_started")
        from app.scrapers.rsh_registered_providers import RSHRegisteredProvidersScraper
        from app.scrapers.scheme_ingest import ingest_rsh_sdr as _ingest

        scraper = RSHRegisteredProvidersScraper()
        sdr_rows = _run_async(scraper.fetch_sdr_stock())
        logger.info("ingest_rsh_sdr_fetched", count=len(sdr_rows))

        result = _ingest(sdr_rows, db)
        logger.info("ingest_rsh_sdr_completed", **result)
        return {"status": "success", "rows_fetched": len(sdr_rows), **result}

    except Exception as exc:
        logger.exception("ingest_rsh_sdr_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.ingest_hmlr_ccod",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=7200,  # 2-hour hard limit for the large file processing.
)
def ingest_hmlr_ccod(self, local_path: str | None = None) -> dict[str, Any]:
    """Process the HMLR CCOD dataset to enrich scheme owner information.

    Downloads or reads a pre-downloaded copy of the HM Land Registry
    Corporate and Commercial Ownership Data (CCOD) CSV, matches entries
    against existing schemes by postcode, and sets ``owner_company_id``
    where it is currently unknown.

    Also back-fills Companies House registration numbers onto Company
    records found via the CCOD, enabling the ``enrich_company`` task to
    subsequently resolve PSC chains.

    Parameters
    ----------
    local_path : str | None
        If supplied, read from this local file instead of downloading.
        Useful for manual runs: ``celery call ingest_hmlr_ccod --kwargs
        '{"local_path": "/data/CCOD_FULL_2024_03.zip"}'``

    Designed to run monthly via Celery Beat.
    """
    db = _get_db()
    try:
        logger.info("ingest_hmlr_ccod_started", local_path=local_path)
        from app.scrapers.scheme_ingest import ingest_hmlr_ccod as _ingest

        result = _ingest(db, local_path=local_path)

        logger.info("ingest_hmlr_ccod_completed", **result)
        return {"status": "success", **result}

    except Exception as exc:
        logger.exception("ingest_hmlr_ccod_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.ingest_lahs_data",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def ingest_lahs_data(self) -> dict[str, Any]:
    """Download and ingest Local Authority Housing Statistics.

    Enriches Council records with housing stock counts, waiting list
    sizes, and build activity.  Helps score BD opportunities by
    identifying councils with the most housing activity.
    Designed to run annually.
    """
    db = _get_db()
    try:
        logger.info("ingest_lahs_started")
        from app.scrapers.lahs_scraper import LAHSScraper
        from app.scrapers.scheme_ingest import ingest_lahs_council_data

        scraper = LAHSScraper()
        lahs_data = _run_async(scraper.fetch_lahs_data())
        logger.info("ingest_lahs_fetched", count=len(lahs_data))

        result = ingest_lahs_council_data(lahs_data, db)
        logger.info("ingest_lahs_completed", **result)
        return {"status": "success", "rows_fetched": len(lahs_data), **result}

    except Exception as exc:
        logger.exception("ingest_lahs_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.scrape_brownfield_register",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
)
def scrape_brownfield_register(self) -> dict[str, Any]:
    """Run the Brownfield Land Register scraper.

    Fetches brownfield development sites from planning.data.gov.uk and
    upserts them as PlanningApplication records with scheme_type and
    unit counts.  High-value source for BD — 38,000+ sites nationwide.
    """
    db = _get_db()
    try:
        logger.info("scrape_brownfield_register_started")
        from app.scrapers.brownfield_scraper import BrownfieldScraper
        from app.scrapers.scheme_ingest import ingest_brownfield_sites

        scraper = BrownfieldScraper()
        raw_results = _run_async(_run_scraper(scraper))

        # Parse each result
        parsed = []
        for raw in raw_results:
            try:
                detail = _run_async(scraper.parse_application(raw))
                parsed.append(detail)
            except Exception as exc:
                logger.warning(
                    "brownfield_parse_failed",
                    reference=raw.get("reference"),
                    error=str(exc),
                )

        ingest_stats = ingest_brownfield_sites(parsed, db)

        logger.info("scrape_brownfield_register_completed", **ingest_stats)
        return {"status": "success", **ingest_stats}

    except Exception as exc:
        logger.exception("scrape_brownfield_register_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.run_scheme_data_quality_audit",
    acks_late=True,
)
def run_scheme_data_quality_audit(self) -> dict[str, Any]:
    """Monthly audit of scheme data quality.

    Queries ExistingScheme records to identify data quality issues and
    creates an Alert record summarising the findings.

    Reports two cohorts:
    - ``total_*``: across all schemes (includes individual EPC dwellings
      and housing-association-owned stock that don't conceptually have
      operating contracts — useful for raw data hygiene).
    - ``bd_*``: scoped to the BD-addressable cohort of operating
      schemes (BTR/PBSA/Co-living/Senior sourced from operator-listing
      and tender feeds) — the right baseline for the "% missing"
      dashboard metric.
    """
    db = _get_db()
    try:
        logger.info("scheme_data_quality_audit_started")
        from app.models.models import ExistingScheme, Alert
        from sqlalchemy import and_, or_

        now = datetime.datetime.now(datetime.timezone.utc)
        ninety_days_ago = now - datetime.timedelta(days=90)

        # BD-addressable cohort: operating schemes that should have a
        # third-party management contract with a real end date.
        bd_cohort_filter = and_(
            ExistingScheme.scheme_type.in_(
                ["BTR", "PBSA", "Co-living", "Senior"]
            ),
            ExistingScheme.source.in_(
                ["arl_btr_open_operating", "pbsa_operator", "find_a_tender"]
            ),
        )

        def _count(*filters):
            q = db.query(ExistingScheme)
            for f in filters:
                q = q.filter(f)
            return q.count()

        total_schemes = _count()
        bd_total = _count(bd_cohort_filter)

        never_verified = _count(ExistingScheme.last_verified_at.is_(None))
        stale_90_days = _count(ExistingScheme.last_verified_at < ninety_days_ago)
        missing_operator = _count(ExistingScheme.operator_company_id.is_(None))
        missing_contract_end = _count(ExistingScheme.contract_end_date.is_(None))
        low_confidence = _count(ExistingScheme.data_confidence_score < 0.5)

        bd_missing_operator = _count(
            bd_cohort_filter, ExistingScheme.operator_company_id.is_(None)
        )
        bd_missing_contract_end = _count(
            bd_cohort_filter, ExistingScheme.contract_end_date.is_(None)
        )

        bd_contract_end_fill_pct = (
            round(100 * (bd_total - bd_missing_contract_end) / bd_total)
            if bd_total
            else 0
        )

        audit_results = {
            "total_schemes": total_schemes,
            "never_verified": never_verified,
            "stale_90_days": stale_90_days,
            "missing_operator": missing_operator,
            "missing_contract_end": missing_contract_end,
            "low_confidence": low_confidence,
            "bd_total": bd_total,
            "bd_missing_operator": bd_missing_operator,
            "bd_missing_contract_end": bd_missing_contract_end,
            "bd_contract_end_fill_pct": bd_contract_end_fill_pct,
        }

        # Create an alert with the audit report
        alert = Alert(
            type="data_quality_report",
            title="Monthly Scheme Data Quality Audit",
            message=(
                f"Audit results (total / BD-addressable cohort):\n"
                f"- Total schemes: {total_schemes:,} (BD-addressable: {bd_total:,})\n"
                f"- Never verified: {never_verified:,}\n"
                f"- Stale (>90 days): {stale_90_days:,}\n"
                f"- Missing operator: {missing_operator:,} (BD: {bd_missing_operator:,})\n"
                f"- Missing contract end: {missing_contract_end:,} (BD: {bd_missing_contract_end:,})\n"
                f"- BD contract_end_date fill rate: {bd_contract_end_fill_pct}%\n"
                f"- Low confidence (<0.5): {low_confidence:,}"
            ),
            entity_type="scheme_audit",
            is_read=False,
        )
        db.add(alert)
        db.commit()

        logger.info("scheme_data_quality_audit_completed", **audit_results)
        return {"status": "success", **audit_results}

    except Exception as exc:
        logger.exception("scheme_data_quality_audit_failed")
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.refresh_scheme_data",
    acks_late=True,
)
def refresh_scheme_data(self, scheme_id: int) -> dict[str, Any]:
    """On-demand refresh for a single scheme's data.

    Clears cached EPC ratings, re-enriches via the EPC pipeline,
    updates verification timestamp, and recalculates confidence score.

    Parameters
    ----------
    scheme_id : int
        ID of the ExistingScheme to refresh.
    """
    db = _get_db()
    try:
        logger.info("refresh_scheme_data_started", scheme_id=scheme_id)
        from app.models.models import ExistingScheme
        from app.tasks.enrichment_tasks import enrich_schemes_with_epc

        scheme = db.query(ExistingScheme).get(scheme_id)
        if not scheme:
            logger.error("refresh_scheme_not_found", scheme_id=scheme_id)
            return {"error": f"Scheme {scheme_id} not found"}

        # Clear EPC ratings to force re-fetch
        scheme.epc_ratings = None
        db.commit()

        # Re-enrich with EPC data
        enrich_schemes_with_epc(scheme_id)

        # Refresh the instance after enrichment
        db.refresh(scheme)

        # Update verification timestamp
        scheme.last_verified_at = datetime.datetime.now(datetime.timezone.utc)

        # Recalculate data confidence score
        score = 1.0
        if scheme.operator_company_id is None:
            score -= 0.2
        if scheme.contract_end_date is None:
            score -= 0.2
        if scheme.epc_ratings is None:
            score -= 0.2
        if scheme.last_verified_at is None:
            score -= 0.2
        scheme.data_confidence_score = max(score, 0.0)

        db.commit()

        logger.info(
            "refresh_scheme_data_completed",
            scheme_id=scheme_id,
            confidence_score=scheme.data_confidence_score,
        )
        return {
            "status": "success",
            "scheme_id": scheme_id,
            "data_confidence_score": scheme.data_confidence_score,
        }

    except Exception as exc:
        logger.exception("refresh_scheme_data_failed", scheme_id=scheme_id)
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scraping_tasks.ingest_planit_applications",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
    time_limit=3600,  # 1-hour hard limit — PlanIt has a lot of data
    soft_time_limit=3000,
)
def ingest_planit_applications(
    self,
    days_back: int = 14,
    start_date: str | None = None,
    end_date: str | None = None,
    max_records: int = 10000,
    page_size: int = 200,
    states: tuple[str, ...] = ("Undecided", "Permitted"),
) -> dict[str, Any]:
    """Ingest fresh planning applications from the PlanIt API (planit.org.uk).

    Strategy (designed to finish in <10 min daily without exhausting the
    PlanIt rate budget):

    1. **Flat fetch — no keyword fan-out.** The PlanIt ``q`` filter doesn't
       actually narrow results, so iterating over 10 keywords × weekly date
       chunks just multiplies the request count without adding signal.
    2. **State filter at API level.** Only fetch Undecided + Permitted —
       these are the BD-relevant lifecycle stages. Refused/Withdrawn don't
       feed the pipeline.
    3. **Incremental commits.** Every ``page_size`` records, commit to DB.
       Even if rate-limit eventually kills the run, you keep what you got.
    4. **App-type filter at ingest.** Skip clearly non-BD types (tree work,
       condition discharges, telecoms, listed-building consent, etc.).
    5. **Use the broadened classifier** that matches BTR developer names
       against ``applicant_name`` / ``agent_name``.

    Parameters
    ----------
    days_back : int
        Lookback window (default 14). PlanIt produces ~700 applications per
        day across 417 LPAs, so 14 days ~= 10k records — manageable.
    max_records : int
        Hard cap to avoid runaway runs. Default 10k.
    page_size : int
        Rows per API request and commit batch (default 200).
    states : tuple[str, ...]
        PlanIt app_state values to ingest. Default ("Undecided", "Permitted").
    """
    import time
    import json
    import httpx
    from datetime import date as date_type, datetime as dt, timezone as tz

    PLANIT_BASE = "https://www.planit.org.uk/api/applics/json"
    # PlanIt app_type values that are bureaucratic noise from a BD perspective.
    SKIP_APP_TYPES = {
        "trees", "tree work", "tree preservation",
        "conditions", "discharge condition", "discharge of conditions",
        "discharge of multiple conditions", "non material amendment",
        "non-material amendment", "details reserved by condition",
        "approval of detail", "approval of details",
        "telecoms", "advertis", "listed building consent",
        "listed building", "lawful development", "prior approval",
        "section 106", "s106", "tpo",
    }

    db = _get_db()
    try:
        # Resolve date range
        if start_date:
            d_start = date_type.fromisoformat(start_date)
        else:
            d_start = date_type.today() - datetime.timedelta(days=days_back)
        if end_date:
            d_end = date_type.fromisoformat(end_date)
        else:
            d_end = date_type.today()

        logger.info(
            "ingest_planit_started",
            start_date=d_start.isoformat(),
            end_date=d_end.isoformat(),
            states=list(states),
            max_records=max_records,
        )

        # Note: scraper_runs has a ``source`` column in DB but the ORM model
        # doesn't expose it (schema drift). We set it via raw SQL after insert.
        run = ScraperRun(
            council_id=None,
            status="running",
            applications_found=0,
            applications_new=0,
            applications_updated=0,
            errors_count=0,
        )
        db.add(run)
        db.commit()
        # Tag source via raw SQL (column exists in DB but not on the ORM model)
        db.execute(
            text("UPDATE scraper_runs SET source = 'planit_api' WHERE id = :id"),
            {"id": run.id},
        )
        db.commit()

        # Council lookup
        council_lookup: dict[str, int] = {
            c.name.lower().strip(): c.id for c in db.query(Council).all()
        }

        # Run one fetch per state (Undecided, Permitted) flat over the date range.
        from app.scrapers.base import BaseScraper

        found = 0
        new = 0
        updated = 0
        skipped_noise = 0
        skipped_no_council = 0
        skipped_existing = 0
        errors = 0
        consecutive_429s = 0

        # Wire PROXY_URL through — when set, every PlanIt request goes via the proxy.
        # Rotating proxies (Smartproxy / Bright Data) effectively give us more rate
        # budget by spreading requests across multiple IPs.
        client_kwargs: dict[str, Any] = {
            "timeout": 60.0,
            "headers": {"User-Agent": "ukops-bd-platform/1.0"},
        }
        from app.config import settings as _settings
        if getattr(_settings, "PROXY_URL", None):
            client_kwargs["proxy"] = _settings.PROXY_URL
            logger.info("planit_using_proxy", proxy_redacted=_settings.PROXY_URL.split("@")[-1])
        with httpx.Client(**client_kwargs) as client:
            for state in states:
                page = 1
                while found < max_records:
                    params = {
                        "start_date": d_start.isoformat(),
                        "end_date": d_end.isoformat(),
                        "pg_sz": page_size,
                        "page": page,
                    }
                    # Allow callers to opt out of the app_state filter entirely
                    # (e.g. Birmingham's PlanIt records have app_state=None and
                    # are otherwise silently excluded). Use the literal "all"
                    # or None in the ``states`` tuple to fetch without filter.
                    if state is not None and state != "all":
                        params["app_state"] = state
                    try:
                        r = client.get(PLANIT_BASE, params=params)
                    except Exception as exc:
                        logger.warning("planit_fetch_error", page=page, error=str(exc)[:120])
                        errors += 1
                        time.sleep(5)
                        continue

                    if r.status_code == 429:
                        consecutive_429s += 1
                        retry = int(r.headers.get("Retry-After", "60"))
                        if consecutive_429s >= 3:
                            logger.warning("planit_rate_limited_giving_up", retry=retry)
                            break
                        logger.info("planit_rate_limited_waiting", retry=retry)
                        time.sleep(min(retry, 90))
                        continue
                    consecutive_429s = 0

                    if r.status_code != 200:
                        logger.warning("planit_http_error", status=r.status_code, body=r.text[:200])
                        errors += 1
                        break

                    data = r.json()
                    records = data.get("records", []) or []
                    if not records:
                        break

                    for rec in records:
                        found += 1
                        if found > max_records:
                            break
                        try:
                            res = _save_planit_record_v2(
                                db, rec, council_lookup, SKIP_APP_TYPES, BaseScraper
                            )
                        except Exception as exc:
                            errors += 1
                            logger.debug("planit_save_error", error=str(exc)[:200])
                            continue
                        if res == "new":
                            new += 1
                        elif res == "existing":
                            skipped_existing += 1
                        elif res == "noise":
                            skipped_noise += 1
                        elif res == "no_council":
                            skipped_no_council += 1

                    # Commit at end of every page.
                    db.commit()
                    logger.info(
                        "planit_page_committed",
                        state=state, page=page, found=found, new=new,
                        skipped_noise=skipped_noise,
                        skipped_existing=skipped_existing,
                    )
                    page += 1
                    time.sleep(0.6)  # respect 1-2 req/s soft cap

                if found >= max_records:
                    break

        run.applications_found = found
        run.applications_new = new
        run.applications_updated = updated
        run.errors_count = errors
        run.status = "success" if errors == 0 else "partial"
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
        db.commit()

        logger.info(
            "ingest_planit_completed",
            found=found, new=new, updated=updated,
            skipped_noise=skipped_noise,
            skipped_existing=skipped_existing,
            skipped_no_council=skipped_no_council,
            errors=errors,
        )

        return {
            "status": run.status,
            "found": found,
            "new": new,
            "updated": updated,
            "skipped_noise": skipped_noise,
            "skipped_existing": skipped_existing,
            "skipped_no_council": skipped_no_council,
            "errors": errors,
        }

    except Exception as exc:
        logger.exception("ingest_planit_failed")
        try:
            run.status = "failed"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            run.error_details = {"exception": str(exc)}
            db.commit()
        except Exception:
            pass
        raise self.retry(exc=exc)
    finally:
        db.close()


def _normalise_authority_name(name: str) -> str:
    """Normalise a PlanIt authority name for fuzzy matching to our council names.

    PlanIt uses names like "London Borough of Camden" while our DB may store
    "Camden" or "LB Camden".  This strips common prefixes/suffixes.
    """
    if not name:
        return ""

    n = name.lower().strip()

    # Remove common prefixes
    for prefix in (
        "london borough of ",
        "royal borough of ",
        "city of ",
        "borough of ",
        "metropolitan borough of ",
        "district of ",
        "county of ",
        "council of the ",
        "the ",
    ):
        if n.startswith(prefix):
            n = n[len(prefix):]
            break

    # Remove common suffixes
    for suffix in (
        " borough council",
        " district council",
        " city council",
        " county council",
        " metropolitan district council",
        " metropolitan borough council",
        " council",
        " dc",
        " bc",
        " cc",
        " mbc",
        " mdc",
    ):
        if n.endswith(suffix):
            n = n[: -len(suffix)]
            break

    return n.strip()


def _resolve_council_id(
    authority_name: str,
    council_lookup: dict[str, int],
    _cache: dict[str, int | None] = {},
) -> int | None:
    """Attempt to match a PlanIt authority_name to a council_id.

    Tries exact match first, then normalised match, then substring match.
    Results are cached within the run.
    """
    if authority_name in _cache:
        return _cache[authority_name]

    auth_lower = authority_name.lower().strip()

    # 1. Exact match
    if auth_lower in council_lookup:
        _cache[authority_name] = council_lookup[auth_lower]
        return _cache[authority_name]

    # 2. Normalised match
    norm_auth = _normalise_authority_name(authority_name)
    for council_name, council_id in council_lookup.items():
        norm_council = _normalise_authority_name(council_name)
        if norm_auth == norm_council:
            _cache[authority_name] = council_id
            return council_id

    # 3. Substring match (PlanIt name contains our council name or vice versa)
    for council_name, council_id in council_lookup.items():
        norm_council = _normalise_authority_name(council_name)
        if len(norm_auth) >= 3 and len(norm_council) >= 3:
            if norm_auth in norm_council or norm_council in norm_auth:
                _cache[authority_name] = council_id
                return council_id

    _cache[authority_name] = None
    return None


def _save_planit_record_v2(
    db: "Session",
    rec: dict[str, Any],
    council_lookup: dict[str, int],
    skip_app_types: set[str],
    classifier_cls,
) -> str:
    """Save one PlanIt record. Returns 'new' | 'existing' | 'no_council' | 'noise' | 'error'.

    Used by the rewritten ``ingest_planit_applications`` task — does its own
    insert via raw SQL for speed and to avoid the model-instantiation overhead
    of the legacy path.
    """
    uid = (rec.get("uid") or "").strip()
    area = (rec.get("area_name") or "").strip()
    if not uid or not area:
        return "error"

    council_id = council_lookup.get(area.lower().strip())
    if not council_id:
        return "no_council"

    # Drop noise application types early
    app_type_raw = (rec.get("app_type") or "").lower().strip()
    if any(noise in app_type_raw for noise in skip_app_types):
        return "noise"

    # Deduplicate by (reference, council_id)
    existing = db.execute(
        text("SELECT 1 FROM planning_applications WHERE reference = :r AND council_id = :c"),
        {"r": uid, "c": council_id},
    ).fetchone()
    if existing:
        return "existing"

    # Field mapping
    description = rec.get("description") or ""
    applicant_name = rec.get("applicant_name") or ""
    agent_name = rec.get("agent_name") or ""
    address = rec.get("address") or ""

    # Status mapping
    status_map = {
        "Undecided": "Pending", "Permitted": "Approved", "Conditions": "Approved",
        "Refused": "Refused", "Withdrawn": "Withdrawn", "Appeal": "Appeal",
        "Referred": "Pending", "Other": "Unknown", "Not Available": "Unknown",
    }
    status = status_map.get(rec.get("app_state", ""), "Unknown")

    # Submission date
    submission_date = None
    raw_sd = rec.get("start_date") or rec.get("consulted_date")
    if raw_sd:
        try:
            submission_date = datetime.date.fromisoformat(str(raw_sd)[:10])
        except (ValueError, TypeError):
            pass

    # Coordinates
    lat, lon = None, None
    loc = rec.get("location") or {}
    coords = loc.get("coordinates") if isinstance(loc, dict) else None
    if coords and len(coords) == 2:
        try:
            lon, lat = float(coords[0]), float(coords[1])
        except (TypeError, ValueError):
            pass

    # Classify via broadened classifier (applicant_name + description)
    scheme_type = classifier_cls.classify_scheme_type(
        description, applicant_name=applicant_name, agent_name=agent_name
    )
    num_units = classifier_cls.extract_unit_count(description)

    now = datetime.datetime.now(datetime.timezone.utc)
    import json as _json
    db.execute(
        text("""
            INSERT INTO planning_applications
                (reference, council_id, address, description, applicant_name,
                 agent_name, application_type, status, scheme_type, num_units,
                 latitude, longitude, submission_date, source, raw_data,
                 created_at, updated_at)
            VALUES
                (:ref, :cid, :addr, :desc, :app, :agent, :atype, :status,
                 :stype, :units, :lat, :lon, :sdate, 'planit_api',
                 CAST(:raw AS jsonb), :now, :now)
        """),
        {
            "ref": uid, "cid": council_id,
            "addr": address, "desc": description,
            "app": applicant_name or None,
            "agent": agent_name or None,
            "atype": rec.get("app_type"),
            "status": status, "stype": scheme_type, "units": num_units,
            "lat": lat, "lon": lon, "sdate": submission_date,
            "raw": _json.dumps(rec, default=str),
            "now": now,
        },
    )
    return "new"


def _save_planit_applications(
    db: "Session",
    results: list[dict[str, Any]],
    council_lookup: dict[str, int],
) -> dict[str, int]:
    """Persist PlanIt scraper results to the planning_applications table.

    Resolves authority_name to council_id and upserts by (reference, council_id).

    Returns
    -------
    dict with keys: found, new, updated, skipped, errors,
                    matched_councils, unmatched_councils
    """
    from app.models.models import PlanningApplication

    found = len(results)
    new = 0
    updated = 0
    skipped = 0
    errors = 0
    matched_council_names: set[str] = set()
    unmatched_council_names: set[str] = set()

    # Clear the council resolution cache for this run
    _resolve_council_id.__defaults__[0].clear()  # type: ignore[union-attr]

    batch_count = 0

    for app_data in results:
        try:
            reference = app_data.get("reference", "")
            if not reference:
                errors += 1
                continue

            authority_name = app_data.pop("_authority_name", "")
            council_id = _resolve_council_id(authority_name, council_lookup)

            if council_id is None:
                # Skip applications we cannot match to a council
                unmatched_council_names.add(authority_name)
                skipped += 1
                continue

            matched_council_names.add(authority_name)

            existing = (
                db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == reference,
                    PlanningApplication.council_id == council_id,
                )
                .first()
            )

            if existing:
                # Update fields that may have changed
                _update_fields = [
                    "address", "postcode", "description", "application_type",
                    "status", "decision_date", "submitted_date",
                    "latitude", "longitude", "portal_url", "ward",
                    "scheme_type", "total_units", "source",
                    "is_btr", "is_pbsa", "is_affordable", "raw_data",
                ]
                changed = False
                for field in _update_fields:
                    new_val = app_data.get(field)
                    if new_val is not None and new_val != getattr(existing, field, None):
                        setattr(existing, field, new_val)
                        changed = True
                if changed:
                    updated += 1
            else:
                app = PlanningApplication(
                    reference=reference,
                    council_id=council_id,
                    address=app_data.get("address"),
                    postcode=app_data.get("postcode"),
                    description=app_data.get("description"),
                    application_type=app_data.get("application_type"),
                    status=app_data.get("status", "Unknown"),
                    decision_date=app_data.get("decision_date"),
                    submitted_date=app_data.get("submitted_date"),
                    latitude=app_data.get("latitude"),
                    longitude=app_data.get("longitude"),
                    portal_url=app_data.get("portal_url"),
                    ward=app_data.get("ward"),
                    scheme_type=app_data.get("scheme_type", "Unknown"),
                    total_units=app_data.get("total_units"),
                    source="planit",
                    is_btr=app_data.get("is_btr", False),
                    is_pbsa=app_data.get("is_pbsa", False),
                    is_affordable=app_data.get("is_affordable", False),
                    raw_data=app_data.get("raw_data"),
                )
                db.add(app)
                new += 1

            batch_count += 1

            # Commit in batches to avoid huge transactions
            if batch_count >= 500:
                db.commit()
                batch_count = 0

        except Exception:
            logger.exception(
                "save_planit_application_failed",
                reference=app_data.get("reference"),
            )
            errors += 1
            db.rollback()

    # Final commit for remaining records
    if batch_count > 0:
        try:
            db.commit()
        except Exception:
            logger.exception("save_planit_final_commit_failed")
            errors += 1
            db.rollback()

    if unmatched_council_names:
        logger.info(
            "planit_unmatched_councils",
            count=len(unmatched_council_names),
            sample=sorted(unmatched_council_names)[:20],
        )

    return {
        "found": found,
        "new": new,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "matched_councils": len(matched_council_names),
        "unmatched_councils": len(unmatched_council_names),
    }


# ---------------------------------------------------------------------------
# Scraper loader
# ---------------------------------------------------------------------------

def _build_scraper_config(portal_type: str, council: Council) -> Any:
    """Build a typed scraper config object from a Council ORM model.

    Each scraper expects its own config dataclass (IdoxCouncilConfig,
    NECCouncilConfig, CivicaCouncilConfig).  This function bridges the
    generic Council table row to the specific config shape.
    """
    portal = portal_type.lower()

    if portal == "idox":
        from app.scrapers.idox_scraper import IdoxCouncilConfig
        return IdoxCouncilConfig(
            name=council.name,
            council_id=council.id,
            base_url=council.portal_url or "",
        )
    elif portal == "nec":
        from app.scrapers.nec_scraper import NECCouncilConfig
        return NECCouncilConfig(
            name=council.name,
            council_id=council.id,
            base_url=council.portal_url or "",
        )
    elif portal == "civica":
        from app.scrapers.civica_scraper import CivicaCouncilConfig
        return CivicaCouncilConfig(
            name=council.name,
            council_id=council.id,
            base_url=council.portal_url or "",
        )
    else:
        return None


def _load_scraper(scraper_class_name: str | None, council: Council | None) -> Any:
    """Dynamically load and instantiate the right scraper for a council.

    Builds a typed config object from the Council ORM model and passes it
    to the scraper constructor.  Returns the scraper instance ready to use
    via ``await scraper.run()`` inside an async context manager.

    Raises ``ValueError`` if the portal type is unsupported — no longer
    silently falls back to a stub, since that masked real scraping failures.
    """
    if not scraper_class_name or not council:
        raise ValueError(
            f"Cannot load scraper: class={scraper_class_name}, council={council}"
        )

    portal = scraper_class_name.lower()

    # Map portal types (and legacy class-name aliases) to module paths.
    scraper_map: dict[str, str] = {
        # Council.portal_type values
        "idox": "app.scrapers.idox_scraper.IdoxScraper",
        "civica": "app.scrapers.civica_scraper.CivicaScraper",
        "nec": "app.scrapers.nec_scraper.NECScraper",
        "api": "app.scrapers.planning_data_api.PlanningDataAPIScraper",
        "planning_data_api": "app.scrapers.planning_data_api.PlanningDataAPIScraper",
        "find_a_tender": "app.scrapers.find_a_tender.FindATenderScraper",
        # Council.scraper_class legacy values (set by seed_councils)
        "idoxscraper": "app.scrapers.idox_scraper.IdoxScraper",
        "civicascraper": "app.scrapers.civica_scraper.CivicaScraper",
        "necscraper": "app.scrapers.nec_scraper.NECScraper",
    }

    module_path = scraper_map.get(portal, scraper_class_name)

    import importlib
    parts = module_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid scraper path: {module_path}")

    mod = importlib.import_module(parts[0])
    cls = getattr(mod, parts[1])

    # For web-portal scrapers, build a typed config object. Match by either
    # portal_type ("idox") or the legacy class-name alias ("idoxscraper")
    # — Council.scraper_class is set by seed_councils() to the latter.
    portal_kind = None
    if portal in ("idox", "idoxscraper"):
        portal_kind = "idox"
    elif portal in ("nec", "necscraper"):
        portal_kind = "nec"
    elif portal in ("civica", "civicascraper"):
        portal_kind = "civica"

    if portal_kind:
        config = _build_scraper_config(portal_kind, council)
        return cls(config=config)

    # For API-based scrapers, pass council directly if accepted.
    return cls()
