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
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.models import Alert, Council, ExistingScheme, ScraperRun
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db() -> Session:
    """Create a new database session for use inside a Celery task."""
    return SessionLocal()


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
                    "agent_name", "application_type", "status", "scheme_type",
                    "num_units", "submission_date", "decision_date",
                    "documents_url",
                ]
                changed = False
                for field in _update_fields:
                    new_val = app_data.get(field)
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
                    scheme_type=app_data.get("scheme_type", "Unknown"),
                    num_units=app_data.get("num_units"),
                    submission_date=app_data.get("submission_date"),
                    decision_date=app_data.get("decision_date"),
                    documents_url=app_data.get("documents_url"),
                    raw_html=app_data.get("raw_html"),
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
    """
    db = _get_db()
    try:
        logger.info("scheme_data_quality_audit_started")
        from app.models.models import ExistingScheme, Alert

        now = datetime.datetime.now(datetime.timezone.utc)
        ninety_days_ago = now - datetime.timedelta(days=90)

        never_verified = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.last_verified_at.is_(None))
            .count()
        )
        stale_90_days = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.last_verified_at < ninety_days_ago)
            .count()
        )
        missing_operator = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.operator_company_id.is_(None))
            .count()
        )
        missing_contract_end = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.contract_end_date.is_(None))
            .count()
        )
        low_confidence = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.data_confidence_score < 0.5)
            .count()
        )

        audit_results = {
            "never_verified": never_verified,
            "stale_90_days": stale_90_days,
            "missing_operator": missing_operator,
            "missing_contract_end": missing_contract_end,
            "low_confidence": low_confidence,
        }

        # Create an alert with the audit report
        alert = Alert(
            type="data_quality_report",
            title="Monthly Scheme Data Quality Audit",
            message=(
                f"Audit results:\n"
                f"- Never verified: {never_verified}\n"
                f"- Stale (>90 days): {stale_90_days}\n"
                f"- Missing operator: {missing_operator}\n"
                f"- Missing contract end: {missing_contract_end}\n"
                f"- Low confidence (<0.5): {low_confidence}"
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

    # Map portal types to module paths.
    scraper_map: dict[str, str] = {
        "idox": "app.scrapers.idox_scraper.IdoxScraper",
        "civica": "app.scrapers.civica_scraper.CivicaScraper",
        "nec": "app.scrapers.nec_scraper.NECScraper",
        "planning_data_api": "app.scrapers.planning_data_api.PlanningDataAPIScraper",
        "find_a_tender": "app.scrapers.find_a_tender.FindATenderScraper",
    }

    module_path = scraper_map.get(portal, scraper_class_name)

    import importlib
    parts = module_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid scraper path: {module_path}")

    mod = importlib.import_module(parts[0])
    cls = getattr(mod, parts[1])

    # For web-portal scrapers, build a typed config object.
    if portal in ("idox", "nec", "civica"):
        config = _build_scraper_config(portal, council)
        return cls(config=config)

    # For API-based scrapers, pass council directly if accepted.
    return cls()
