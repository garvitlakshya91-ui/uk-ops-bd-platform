"""Celery tasks for company and contact enrichment.

Orchestrates Companies House lookups, contact discovery, email
re-verification, postcode back-fill, EPC enrichment, and planning-to-scheme
cross-referencing on a scheduled and on-demand basis.
"""

from __future__ import annotations

import asyncio
import datetime
import re
from typing import Any

import structlog
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db() -> Session:
    """Create a new database session for use inside a Celery task."""
    return SessionLocal()


def _run_async(coro):
    """Run an async coroutine from synchronous Celery task context.

    Always creates a fresh event loop to avoid 'Event loop is closed' errors
    that occur when reusing a loop closed by a previous ``asyncio.run()``.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        # Already inside an async context — run in a thread.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()

    # No running loop — create a fresh one.
    return asyncio.run(coro)


@celery_app.task(
    bind=True,
    name="app.tasks.enrichment_tasks.enrich_company",
    max_retries=3,
    default_retry_delay=120,
    acks_late=True,
)
def enrich_company(self, company_id: int) -> dict[str, Any]:
    """Run Companies House and contact enrichment for a single company.

    Parameters
    ----------
    company_id : int
        ID of the :class:`Company` to enrich.

    Returns
    -------
    dict
        Summary of enrichment results including Companies House match
        status and number of contacts discovered.
    """
    db = _get_db()
    try:
        from app.models.models import Company
        from app.enrichment.companies_house import CompaniesHouseEnricher
        from app.enrichment.contact_enrichment import ContactEnrichmentPipeline

        company = db.query(Company).get(company_id)
        if not company:
            logger.error("enrich_company_not_found", company_id=company_id)
            return {"error": f"Company {company_id} not found"}

        log = logger.bind(company_id=company_id, company_name=company.name)
        log.info("enrich_company_started")

        result: dict[str, Any] = {"company_id": company_id, "company_name": company.name}

        # 1. Companies House enrichment.
        if not company.companies_house_number:
            try:
                enricher = CompaniesHouseEnricher()
                ch_data = _run_async(enricher.enrich_company(company.name))
                _run_async(enricher.close())

                if ch_data:
                    company.companies_house_number = ch_data.get("companies_house_number")
                    if not company.registered_address:
                        company.registered_address = ch_data.get("registered_address")
                    if not company.sic_codes:
                        company.sic_codes = ch_data.get("sic_codes")
                    company.is_active = ch_data.get("is_active", True)
                    db.commit()
                    result["companies_house"] = "enriched"
                    log.info("companies_house_enriched", ch_number=company.companies_house_number)

                    # Auto-dispatch PSC chain resolution now that we have a CH number
                    enrich_company_psc.delay(company_id)
                    result["psc_dispatched"] = True
                else:
                    result["companies_house"] = "no_match"
            except Exception as exc:
                log.exception("companies_house_enrichment_failed")
                result["companies_house"] = f"error: {exc}"
        else:
            result["companies_house"] = "already_enriched"

        # 2. Contact enrichment.
        try:
            pipeline = ContactEnrichmentPipeline(db_session=db)
            contacts = _run_async(pipeline.enrich(company))
            db.commit()
            result["contacts_found"] = len(contacts)
            log.info("contact_enrichment_done", contacts=len(contacts))
        except Exception as exc:
            log.exception("contact_enrichment_failed")
            result["contacts_found"] = 0
            result["contact_error"] = str(exc)

        return result

    except Exception as exc:
        logger.exception("enrich_company_task_failed", company_id=company_id)
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.enrich_new_applications",
    acks_late=True,
)
def enrich_new_applications() -> dict[str, Any]:
    """Find recent planning applications without linked companies and run
    matching and enrichment for each.

    This task is designed to run frequently (e.g. hourly) and processes
    applications that have an ``applicant_name`` but no ``applicant_company_id``.

    Returns
    -------
    dict
        Summary with counts of applications processed and companies matched.
    """
    db = _get_db()
    try:
        from app.models.models import PlanningApplication
        from app.matching.company_matcher import CompanyMatcher
        from app.enrichment.companies_house import CompaniesHouseEnricher

        # Find applications with applicant names but no linked company.
        unlinked = (
            db.query(PlanningApplication)
            .filter(
                PlanningApplication.applicant_name.isnot(None),
                PlanningApplication.applicant_name != "",
                PlanningApplication.applicant_company_id.is_(None),
            )
            .order_by(PlanningApplication.created_at.desc())
            .limit(100)  # Process in batches.
            .all()
        )

        if not unlinked:
            logger.info("enrich_new_applications_none_found")
            return {"processed": 0, "matched": 0, "created": 0}

        logger.info("enrich_new_applications_found", count=len(unlinked))

        ch_enricher = CompaniesHouseEnricher()
        matcher = CompanyMatcher(db_session=db, ch_enricher=ch_enricher)

        matched = 0
        created = 0
        errors = 0

        for app in unlinked:
            try:
                company = _run_async(
                    matcher.match_or_create(app.applicant_name, source="planning_scraper")
                )
                app.applicant_company_id = company.id
                db.commit()

                # Trigger enrichment for new companies.
                if company.companies_house_number is None:
                    enrich_company.delay(company.id)
                    created += 1
                else:
                    matched += 1

            except Exception:
                logger.exception(
                    "enrich_application_matching_failed",
                    application_id=app.id,
                    applicant_name=app.applicant_name,
                )
                errors += 1
                db.rollback()

        _run_async(ch_enricher.close())

        # Also handle agent names.
        unlinked_agents = (
            db.query(PlanningApplication)
            .filter(
                PlanningApplication.agent_name.isnot(None),
                PlanningApplication.agent_name != "",
                PlanningApplication.agent_company_id.is_(None),
            )
            .order_by(PlanningApplication.created_at.desc())
            .limit(50)
            .all()
        )

        agent_matched = 0
        for app in unlinked_agents:
            try:
                company = _run_async(
                    matcher.match_or_create(app.agent_name, source="planning_scraper")
                )
                app.agent_company_id = company.id
                db.commit()
                agent_matched += 1
            except Exception:
                logger.exception(
                    "enrich_agent_matching_failed",
                    application_id=app.id,
                    agent_name=app.agent_name,
                )
                db.rollback()

        result = {
            "processed": len(unlinked),
            "matched": matched,
            "created": created,
            "errors": errors,
            "agents_matched": agent_matched,
        }
        logger.info("enrich_new_applications_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.enrichment_tasks.enrich_company_psc",
    max_retries=3,
    default_retry_delay=120,
    acks_late=True,
)
def enrich_company_psc(self, company_id: int) -> dict[str, Any]:
    """Resolve the PSC (Persons with Significant Control) chain for a company.

    Walks the corporate ownership chain upward via the Companies House PSC
    API, creating parent Company records and setting ``parent_company_id``
    FKs along the way.  Stops at individual PSCs, non-UK entities, or after
    3 hops.

    Parameters
    ----------
    company_id : int
        ID of the :class:`Company` whose PSC chain should be resolved.
    """
    db = _get_db()
    try:
        from app.models.models import Company
        from app.enrichment.companies_house import CompaniesHouseEnricher

        company = db.query(Company).get(company_id)
        if not company:
            logger.error("enrich_company_psc_not_found", company_id=company_id)
            return {"error": f"Company {company_id} not found"}

        if not company.companies_house_number:
            logger.info(
                "enrich_company_psc_no_ch_number",
                company_id=company_id,
                company_name=company.name,
            )
            return {"skipped": True, "reason": "no_companies_house_number"}

        log = logger.bind(
            company_id=company_id,
            company_name=company.name,
            ch_number=company.companies_house_number,
        )
        log.info("enrich_company_psc_started")

        enricher = CompaniesHouseEnricher()
        chain = _run_async(
            enricher.resolve_ultimate_owner(company.companies_house_number)
        )
        _run_async(enricher.close())

        if not chain:
            log.info("enrich_company_psc_no_chain")
            return {"company_id": company_id, "chain_length": 0, "parents_linked": 0}

        # Walk the chain and ensure each parent exists in our Company table,
        # then wire up the parent_company_id FK.
        parents_linked = 0
        child = company

        for entry in chain:
            parent_ch = entry["company_number"]
            parent_name = entry["company_name"]

            # Find or create the parent company record.
            parent = (
                db.query(Company)
                .filter(Company.companies_house_number == parent_ch)
                .first()
            )
            if not parent:
                from app.scrapers.scheme_ingest import _normalize_name
                parent = Company(
                    name=parent_name,
                    normalized_name=_normalize_name(parent_name),
                    companies_house_number=parent_ch,
                    registered_address=entry.get("registered_address"),
                    sic_codes=entry.get("sic_codes"),
                    company_type="Investor",  # PSC chain entities are typically investors/holdcos
                    is_active=(entry.get("company_status", "active") == "active"),
                )
                db.add(parent)
                db.flush()
                log.info(
                    "enrich_company_psc_parent_created",
                    parent_ch=parent_ch,
                    parent_name=parent_name,
                )

            # Link child → parent if not already set.
            if child.parent_company_id is None:
                child.parent_company_id = parent.id
                db.commit()
                parents_linked += 1
                log.info(
                    "enrich_company_psc_parent_linked",
                    child_id=child.id,
                    parent_id=parent.id,
                    depth=entry.get("depth", 0),
                )

            child = parent  # Move up the chain.

        # Update any schemes where this company is operator but has no owner
        # Set the ultimate parent as the owner
        from app.models.models import ExistingScheme
        if chain:
            ultimate_parent_id = child.id  # child is now the top of the chain
            schemes_to_update = (
                db.query(ExistingScheme)
                .filter(
                    ExistingScheme.operator_company_id == company_id,
                    ExistingScheme.owner_company_id.is_(None),
                )
                .all()
            )
            for scheme in schemes_to_update:
                scheme.owner_company_id = ultimate_parent_id
                parents_linked += 1
            db.commit()

        result = {
            "company_id": company_id,
            "chain_length": len(chain),
            "parents_linked": parents_linked,
        }
        log.info("enrich_company_psc_completed", **result)
        return result

    except Exception as exc:
        logger.exception("enrich_company_psc_failed", company_id=company_id)
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.enrich_all_companies_psc",
    acks_late=True,
)
def enrich_all_companies_psc() -> dict[str, Any]:
    """Dispatch PSC enrichment for all companies that have a CH number
    but no parent company linked yet.

    Designed to run weekly via Celery Beat.  Batches to avoid overwhelming
    the Companies House rate limit (600 req/5 min).
    """
    db = _get_db()
    try:
        from app.models.models import Company

        candidates = (
            db.query(Company.id)
            .filter(
                Company.companies_house_number.isnot(None),
                Company.companies_house_number != "",
                Company.parent_company_id.is_(None),
            )
            .order_by(Company.updated_at.asc())
            .limit(200)  # Cap per run; rate-limiter handles the rest.
            .all()
        )

        if not candidates:
            logger.info("enrich_all_companies_psc_none_found")
            return {"dispatched": 0}

        for (cid,) in candidates:
            enrich_company_psc.delay(cid)

        logger.info("enrich_all_companies_psc_dispatched", count=len(candidates))
        return {"dispatched": len(candidates)}

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.reverify_contacts",
    acks_late=True,
)
def reverify_contacts() -> dict[str, Any]:
    """Re-check contact emails that have not been verified in over 90 days.

    Uses Hunter.io's email verification API to confirm deliverability.

    Returns
    -------
    dict
        Summary with counts of contacts checked and results.
    """
    db = _get_db()
    try:
        from app.models.models import Contact
        from app.enrichment.contact_enrichment import HunterEnricher

        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=90)

        stale_contacts = (
            db.query(Contact)
            .filter(
                Contact.email.isnot(None),
                Contact.email != "",
                (Contact.last_verified_at.is_(None)) | (Contact.last_verified_at < cutoff),
            )
            .order_by(Contact.last_verified_at.asc().nullsfirst())
            .limit(200)  # Respect Hunter API limits.
            .all()
        )

        if not stale_contacts:
            logger.info("reverify_contacts_none_stale")
            return {"checked": 0}

        logger.info("reverify_contacts_found", count=len(stale_contacts))

        hunter = HunterEnricher()
        verified = 0
        invalid = 0
        errors = 0

        for contact in stale_contacts:
            try:
                result = _run_async(hunter.verify_email(contact.email))

                contact.last_verified_at = datetime.datetime.utcnow()

                status = result.get("status", "unknown")
                if status in ("valid", "accept_all"):
                    verified += 1
                elif status in ("invalid", "disposable"):
                    contact.confidence_score = max(0.0, (contact.confidence_score or 0.5) * 0.3)
                    invalid += 1
                # For "unknown"/"webmail" statuses, keep existing confidence.

                db.commit()
            except Exception:
                logger.exception(
                    "reverify_contact_failed",
                    contact_id=contact.id,
                    email=contact.email,
                )
                errors += 1
                db.rollback()

        result_summary = {
            "checked": len(stale_contacts),
            "verified": verified,
            "invalid": invalid,
            "errors": errors,
        }
        logger.info("reverify_contacts_completed", **result_summary)
        return result_summary

    finally:
        db.close()


# ---------------------------------------------------------------------------
# UK postcode regex (case-insensitive). Captures formats like SW1A 2AA,
# EC1A1BB, M1 1AA, etc.
# ---------------------------------------------------------------------------
_UK_POSTCODE_RE = re.compile(r"([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})", re.IGNORECASE)


def _normalize_postcode(raw: str) -> str:
    """Normalise a UK postcode to 'XX99 9XX' format (uppercase, single space
    before the last three characters)."""
    cleaned = raw.upper().replace(" ", "")
    return f"{cleaned[:-3]} {cleaned[-3:]}"


@celery_app.task(
    name="app.tasks.enrichment_tasks.backfill_scheme_postcodes",
    acks_late=True,
)
def backfill_scheme_postcodes() -> dict[str, Any]:
    """Find ExistingScheme records with a NULL postcode and attempt to extract
    a UK postcode from the address or scheme name fields.

    Designed to run daily via Celery Beat.

    Returns
    -------
    dict
        Summary with counts of schemes scanned and postcodes extracted.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme

        schemes = (
            db.query(ExistingScheme)
            .filter(
                ExistingScheme.postcode.is_(None),
                ExistingScheme.address.isnot(None),
            )
            .all()
        )

        if not schemes:
            logger.info("backfill_scheme_postcodes_none_found")
            return {"scanned": 0, "updated": 0}

        logger.info("backfill_scheme_postcodes_found", count=len(schemes))

        updated = 0
        for scheme in schemes:
            postcode: str | None = None

            # Try address first.
            match = _UK_POSTCODE_RE.search(scheme.address or "")
            if match:
                postcode = _normalize_postcode(match.group(1))

            # Fall back to scheme name.
            if not postcode:
                match = _UK_POSTCODE_RE.search(scheme.name or "")
                if match:
                    postcode = _normalize_postcode(match.group(1))

            if postcode:
                scheme.postcode = postcode
                updated += 1
                logger.debug(
                    "backfill_scheme_postcode_extracted",
                    scheme_id=scheme.id,
                    postcode=postcode,
                )

        db.commit()

        result = {"scanned": len(schemes), "updated": updated}
        logger.info("backfill_scheme_postcodes_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.enrich_schemes_with_epc",
    acks_late=True,
)
def enrich_schemes_with_epc() -> dict[str, Any]:
    """Fetch EPC rating distributions for ExistingScheme records that have a
    postcode but no epc_ratings yet.

    Processes in batches of 50 to respect the EPC API rate limits.
    Designed to run weekly via Celery Beat.

    Returns
    -------
    dict
        Summary with counts of schemes enriched and errors encountered.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme
        from app.scrapers.epc_scraper import EPCScraper

        schemes = (
            db.query(ExistingScheme)
            .filter(
                ExistingScheme.postcode.isnot(None),
                ExistingScheme.postcode != "",
                ExistingScheme.epc_ratings.is_(None),
            )
            .order_by(ExistingScheme.updated_at.asc())
            .limit(50)
            .all()
        )

        if not schemes:
            logger.info("enrich_schemes_with_epc_none_found")
            return {"processed": 0, "enriched": 0, "errors": 0}

        logger.info("enrich_schemes_with_epc_found", count=len(schemes))

        scraper = EPCScraper()
        enriched = 0
        errors = 0

        for scheme in schemes:
            try:
                epc_data = _run_async(
                    scraper.get_rating_distribution(postcode=scheme.postcode)
                )
                if epc_data and epc_data.get("ratings"):
                    scheme.epc_ratings = epc_data
                    db.commit()
                    enriched += 1
                    logger.debug(
                        "enrich_scheme_epc_done",
                        scheme_id=scheme.id,
                        postcode=scheme.postcode,
                    )
                else:
                    # Mark as checked so we don't re-query every run.
                    scheme.epc_ratings = {"ratings": {}, "total": 0, "checked": True}
                    db.commit()
            except Exception:
                logger.exception(
                    "enrich_scheme_epc_failed",
                    scheme_id=scheme.id,
                    postcode=scheme.postcode,
                )
                errors += 1
                db.rollback()

        # Clean up scraper session if it was initialised.
        if hasattr(scraper, "session") and scraper.session:
            _run_async(scraper.session.aclose())

        result = {"processed": len(schemes), "enriched": enriched, "errors": errors}
        logger.info("enrich_schemes_with_epc_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.enrich_all_companies_ch",
    acks_late=True,
)
def enrich_all_companies_ch() -> dict[str, Any]:
    """Dispatch Companies House enrichment for companies that have a name but
    no CH number yet.

    Limits to 100 per run to stay within the Companies House rate limit
    (600 requests / 5 minutes).  Designed to run daily via Celery Beat.

    Returns
    -------
    dict
        Count of enrichment tasks dispatched.
    """
    db = _get_db()
    try:
        from app.models.models import Company

        candidates = (
            db.query(Company.id)
            .filter(
                Company.companies_house_number.is_(None),
                Company.name.isnot(None),
                Company.name != "",
            )
            .order_by(Company.updated_at.asc())
            .limit(100)
            .all()
        )

        if not candidates:
            logger.info("enrich_all_companies_ch_none_found")
            return {"dispatched": 0}

        for (cid,) in candidates:
            enrich_company.delay(cid)

        logger.info("enrich_all_companies_ch_dispatched", count=len(candidates))
        return {"dispatched": len(candidates)}

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.cross_reference_planning_to_schemes",
    acks_late=True,
)
def cross_reference_planning_to_schemes() -> dict[str, Any]:
    """Cross-reference PlanningApplications with ExistingSchemes by postcode.

    For matching schemes where ``num_units`` is NULL but the planning
    application has a value, back-fill the scheme.  Also creates
    PipelineOpportunity records for high-value planning applications near
    schemes whose contracts are expiring.

    Processes in batches of 200.  Designed to run daily via Celery Beat.

    Returns
    -------
    dict
        Summary with counts of matches, updates, and opportunities created.
    """
    db = _get_db()
    try:
        from app.models.models import (
            ExistingScheme,
            PlanningApplication,
            PipelineOpportunity,
        )

        # Gather postcodes from existing schemes.
        scheme_postcodes = (
            db.query(ExistingScheme.postcode)
            .filter(
                ExistingScheme.postcode.isnot(None),
                ExistingScheme.postcode != "",
            )
            .distinct()
            .all()
        )
        postcode_set = {row[0] for row in scheme_postcodes}

        if not postcode_set:
            logger.info("cross_reference_no_scheme_postcodes")
            return {"matched": 0, "units_updated": 0, "opportunities_created": 0}

        # Find planning applications whose postcode matches a scheme postcode.
        matched_apps = (
            db.query(PlanningApplication)
            .filter(
                PlanningApplication.postcode.in_(postcode_set),
            )
            .order_by(PlanningApplication.created_at.desc())
            .limit(200)
            .all()
        )

        if not matched_apps:
            logger.info("cross_reference_no_matching_apps")
            return {"matched": 0, "units_updated": 0, "opportunities_created": 0}

        logger.info("cross_reference_matched_apps", count=len(matched_apps))

        units_updated = 0
        opportunities_created = 0

        for app in matched_apps:
            try:
                schemes = (
                    db.query(ExistingScheme)
                    .filter(ExistingScheme.postcode == app.postcode)
                    .all()
                )

                for scheme in schemes:
                    # Back-fill num_units if the scheme is missing it.
                    if scheme.num_units is None and app.num_units is not None:
                        scheme.num_units = app.num_units
                        units_updated += 1

                    # Create PipelineOpportunity for high-value apps near
                    # schemes with expiring contracts.
                    is_high_value = (app.num_units or 0) >= 50
                    is_expiring = (
                        scheme.contract_end_date is not None
                        and scheme.contract_end_date
                        <= (datetime.date.today() + datetime.timedelta(days=365))
                    )

                    if is_high_value and is_expiring:
                        # Avoid duplicate opportunities.
                        existing_opp = (
                            db.query(PipelineOpportunity)
                            .filter(
                                PipelineOpportunity.planning_application_id == app.id,
                                PipelineOpportunity.scheme_id == scheme.id,
                            )
                            .first()
                        )
                        if not existing_opp and app.applicant_company_id:
                            opp = PipelineOpportunity(
                                source="planning_application",
                                planning_application_id=app.id,
                                scheme_id=scheme.id,
                                company_id=app.applicant_company_id,
                                stage="identified",
                                priority="hot",
                                notes=(
                                    f"Auto-created: {app.num_units}-unit planning app "
                                    f"near expiring scheme (contract ends "
                                    f"{scheme.contract_end_date})"
                                ),
                            )
                            db.add(opp)
                            opportunities_created += 1

                db.commit()

            except Exception:
                logger.exception(
                    "cross_reference_app_failed",
                    application_id=app.id,
                )
                db.rollback()

        result = {
            "matched": len(matched_apps),
            "units_updated": units_updated,
            "opportunities_created": opportunities_created,
        }
        logger.info("cross_reference_planning_to_schemes_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.backfill_contract_dates",
    acks_late=True,
)
def backfill_contract_dates() -> dict[str, Any]:
    """Extract contract dates from description text for SchemeContract records
    that are missing start or end dates.

    The Find a Tender OCDS API frequently omits ``contractPeriod`` from the
    structured data, but the dates are often mentioned in the tender
    description.  This task runs the date-extraction heuristics against the
    ``raw_data.description`` field and back-fills the gaps.

    When a contract's dates are updated the parent
    :class:`ExistingScheme`'s ``contract_start_date`` /
    ``contract_end_date`` are also set if they were previously NULL.

    Designed to run daily via Celery Beat.

    Returns
    -------
    dict
        Summary with counts of contracts scanned, updated, and errors.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme, SchemeContract
        from app.scrapers.date_extractor import (
            extract_contract_dates,
            extract_contract_duration,
        )

        # Find contracts missing at least one date.
        contracts = (
            db.query(SchemeContract)
            .filter(
                or_(
                    SchemeContract.contract_start_date.is_(None),
                    SchemeContract.contract_end_date.is_(None),
                ),
            )
            .order_by(SchemeContract.updated_at.asc())
            .limit(500)
            .all()
        )

        if not contracts:
            logger.info("backfill_contract_dates_none_found")
            return {"scanned": 0, "updated": 0, "schemes_updated": 0, "errors": 0}

        logger.info("backfill_contract_dates_found", count=len(contracts))

        updated = 0
        schemes_updated = 0
        no_description = 0
        no_dates_extracted = 0
        errors = 0

        for contract in contracts:
            try:
                # Extract the description from raw_data JSON.
                raw_data = contract.raw_data or {}

                # If we have the full OCDS release stored, try structured
                # extraction first (contracts→period, awards→contractPeriod)
                raw_release = raw_data.get("raw_release")
                if raw_release and isinstance(raw_release, dict):
                    from app.scrapers.contracts_finder import ContractsFinderScraper
                    s, e, _, _ = ContractsFinderScraper._extract_contract_details(raw_release)
                    if s and e:
                        contract_changed = False
                        if contract.contract_start_date is None:
                            contract.contract_start_date = s
                            contract_changed = True
                        if contract.contract_end_date is None:
                            contract.contract_end_date = e
                            contract_changed = True
                        if contract_changed:
                            updated += 1
                            scheme = (
                                db.query(ExistingScheme)
                                .filter(ExistingScheme.id == contract.scheme_id)
                                .first()
                            )
                            if scheme:
                                if scheme.contract_start_date is None and contract.contract_start_date:
                                    scheme.contract_start_date = contract.contract_start_date
                                    schemes_updated += 1
                                if scheme.contract_end_date is None and contract.contract_end_date:
                                    scheme.contract_end_date = contract.contract_end_date
                            db.commit()
                        continue

                description = raw_data.get("description", "")
                if not description:
                    no_description += 1
                    continue

                dates = extract_contract_dates(description)
                extracted_start = dates.get("start_date")
                extracted_end = dates.get("end_date")

                if not extracted_start and not extracted_end:
                    no_dates_extracted += 1
                    continue

                contract_changed = False

                # Only fill in dates that are currently NULL.
                if contract.contract_start_date is None and extracted_start:
                    contract.contract_start_date = extracted_start
                    contract_changed = True

                if contract.contract_end_date is None and extracted_end:
                    contract.contract_end_date = extracted_end
                    contract_changed = True

                if not contract_changed:
                    continue

                updated += 1
                logger.debug(
                    "backfill_contract_date_extracted",
                    contract_id=contract.id,
                    scheme_id=contract.scheme_id,
                    start_date=str(extracted_start) if extracted_start else None,
                    end_date=str(extracted_end) if extracted_end else None,
                )

                # Propagate to the parent ExistingScheme if its dates are NULL.
                scheme = (
                    db.query(ExistingScheme)
                    .filter(ExistingScheme.id == contract.scheme_id)
                    .first()
                )
                if scheme:
                    scheme_changed = False
                    if scheme.contract_start_date is None and contract.contract_start_date:
                        scheme.contract_start_date = contract.contract_start_date
                        scheme_changed = True
                    if scheme.contract_end_date is None and contract.contract_end_date:
                        scheme.contract_end_date = contract.contract_end_date
                        scheme_changed = True
                    if scheme_changed:
                        schemes_updated += 1

                db.commit()

            except Exception:
                logger.exception(
                    "backfill_contract_date_failed",
                    contract_id=contract.id,
                )
                errors += 1
                db.rollback()

        result = {
            "scanned": len(contracts),
            "updated": updated,
            "schemes_updated": schemes_updated,
            "no_description": no_description,
            "no_dates_extracted": no_dates_extracted,
            "errors": errors,
        }
        logger.info("backfill_contract_dates_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.enrich_companies_charity_status",
    acks_late=True,
)
def enrich_companies_charity_status() -> dict[str, Any]:
    """Cross-reference RP companies against the Charity Commission register.

    For each Company with company_type='RP' that hasn't been checked,
    verify if it's a registered charity and store the charity number.

    Designed to run weekly via Celery Beat.
    """
    db = _get_db()
    try:
        from app.models.models import Company
        from app.scrapers.charity_commission import CharityCommissionClient

        # Find RP companies without charity info
        candidates = (
            db.query(Company)
            .filter(
                Company.company_type.in_(["RP", "LRP"]),
                Company.is_active.is_(True),
            )
            .order_by(Company.updated_at.asc())
            .limit(50)
            .all()
        )

        if not candidates:
            logger.info("enrich_charity_status_none_found")
            return {"checked": 0, "charities_found": 0}

        client = CharityCommissionClient()
        charities_found = 0
        errors = 0

        for company in candidates:
            try:
                # Skip if already has charity info
                sic = company.sic_codes or {}
                if isinstance(sic, dict) and sic.get("charity_number"):
                    continue

                result = _run_async(client.is_registered_charity(company.name))

                if result:
                    existing_sic = dict(company.sic_codes or {})
                    existing_sic["charity_number"] = result.get("charity_number", "")
                    existing_sic["charity_income"] = result.get("income")
                    existing_sic["charity_trustees"] = result.get("trustees", [])[:5]
                    company.sic_codes = existing_sic
                    db.commit()
                    charities_found += 1
                    logger.info(
                        "charity_status_found",
                        company_id=company.id,
                        company_name=company.name,
                        charity_number=result.get("charity_number"),
                    )

            except Exception as exc:
                logger.warning(
                    "charity_enrichment_failed",
                    company_id=company.id,
                    error=str(exc),
                )
                errors += 1
                db.rollback()

        _run_async(client.close())

        result_summary = {
            "checked": len(candidates),
            "charities_found": charities_found,
            "errors": errors,
        }
        logger.info("enrich_charity_status_completed", **result_summary)
        return result_summary

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.backfill_contract_dates_cf_awards",
    acks_late=True,
    soft_time_limit=900,
    time_limit=1200,
)
def backfill_contract_dates_cf_awards() -> dict[str, Any]:
    """Tier 1: Re-query the Contracts Finder API for award-stage releases and
    cross-match with existing SchemeContract records to backfill missing dates.

    The CF OCDS award-stage releases contain full ``contractPeriod`` objects
    with ``startDate`` and ``endDate`` — information that is often absent from
    the tender-stage data originally scraped.

    For each existing contract sourced from ``contracts_finder``, we look up
    its ``source_reference`` (the OCDS release ID) on the award API.  When a
    match is found and it has contract period dates, we update the contract
    and propagate to the parent scheme.

    For contracts without a direct match (e.g. sourced from FAT), we run a
    keyword search for award-stage releases and attempt fuzzy matching by
    title + contracting authority.

    Designed to run daily via Celery Beat.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme, SchemeContract

        # ---- Phase A: Direct match for contracts_finder-sourced contracts ----
        cf_contracts = (
            db.query(SchemeContract)
            .filter(
                SchemeContract.source == "contracts_finder",
                or_(
                    SchemeContract.contract_start_date.is_(None),
                    SchemeContract.contract_end_date.is_(None),
                ),
                SchemeContract.source_reference.isnot(None),
            )
            .all()
        )

        logger.info("backfill_cf_awards_phase_a", cf_contracts=len(cf_contracts))

        phase_a_updated = 0
        phase_a_errors = 0

        if cf_contracts:
            # Build a set of OCID prefixes to search for award releases
            # CF release IDs look like: ocds-b5fd17-<uuid>-<stage>
            # We need the base OCID without the stage suffix
            from app.scrapers.contracts_finder import (
                ContractsFinderScraper,
                SEARCH_TERMS,
            )

            scraper = ContractsFinderScraper()

            # Group contracts by their base OCID for batch lookup
            ref_to_contracts: dict[str, list] = {}
            for c in cf_contracts:
                ref = c.source_reference or ""
                # Normalize: strip stage suffix if present
                base_ref = ref.rsplit("-", 1)[0] if ref.count("-") >= 4 else ref
                ref_to_contracts.setdefault(base_ref, []).append(c)

            # Search for award-stage releases matching these contracts
            # We search in batches using the contract titles as keywords
            try:
                award_releases = _run_async(
                    scraper.search_applications(
                        stages="award",
                        max_pages=100,
                    )
                )

                logger.info(
                    "backfill_cf_awards_fetched",
                    award_releases=len(award_releases),
                )

                # Index awards by their base OCID
                award_by_ref: dict[str, dict] = {}
                for release in award_releases:
                    rid = release.get("id", "")
                    base_rid = rid.rsplit("-", 1)[0] if rid.count("-") >= 4 else rid
                    # Also store by full ID
                    award_by_ref[rid] = release
                    award_by_ref[base_rid] = release

                # Match and update
                for base_ref, contracts in ref_to_contracts.items():
                    award = award_by_ref.get(base_ref)
                    if not award:
                        # Try matching by full source_reference
                        for c in contracts:
                            award = award_by_ref.get(c.source_reference or "")
                            if award:
                                break
                    if not award:
                        continue

                    # Extract dates from the award release
                    start, end, _, _ = ContractsFinderScraper._extract_contract_details(award)

                    if not start and not end:
                        continue

                    for contract in contracts:
                        try:
                            changed = False
                            if contract.contract_start_date is None and start:
                                contract.contract_start_date = start
                                changed = True
                            if contract.contract_end_date is None and end:
                                contract.contract_end_date = end
                                changed = True

                            if changed:
                                phase_a_updated += 1
                                # Store the award data for audit
                                existing_raw = dict(contract.raw_data or {})
                                existing_raw["_cf_award_release"] = award.get("id", "")
                                existing_raw["_date_source"] = "cf_award_api"
                                contract.raw_data = existing_raw

                                # Propagate to parent scheme
                                _propagate_dates_to_scheme(db, contract)

                                db.commit()
                                logger.debug(
                                    "backfill_cf_award_matched",
                                    contract_id=contract.id,
                                    start=str(start),
                                    end=str(end),
                                )
                        except Exception:
                            logger.exception(
                                "backfill_cf_award_update_failed",
                                contract_id=contract.id,
                            )
                            phase_a_errors += 1
                            db.rollback()

            except Exception as exc:
                logger.exception("backfill_cf_awards_search_failed")
                phase_a_errors += 1

            try:
                _run_async(scraper.close())
            except Exception:
                pass

        # ---- Phase B: Title-based fuzzy match for FAT contracts ----
        fat_contracts = (
            db.query(SchemeContract)
            .filter(
                SchemeContract.source == "find_a_tender",
                or_(
                    SchemeContract.contract_start_date.is_(None),
                    SchemeContract.contract_end_date.is_(None),
                ),
            )
            .limit(200)
            .all()
        )

        logger.info("backfill_cf_awards_phase_b", fat_contracts=len(fat_contracts))

        phase_b_updated = 0
        phase_b_errors = 0

        if fat_contracts:
            from app.scrapers.contracts_finder import ContractsFinderScraper

            scraper = ContractsFinderScraper()

            # Build title index from award releases (reuse if available)
            try:
                if not cf_contracts or 'award_releases' not in dir():
                    award_releases = _run_async(
                        scraper.search_applications(stages="award", max_pages=100)
                    )

                # Build a lookup by normalised title for fuzzy matching
                import unicodedata

                def _norm_title(t: str) -> str:
                    t = t.lower().strip()
                    t = re.sub(r"[^a-z0-9\s]", "", t)
                    return re.sub(r"\s+", " ", t).strip()

                award_by_title: dict[str, dict] = {}
                for release in award_releases:
                    title = release.get("tender", {}).get("title", "")
                    if title:
                        award_by_title[_norm_title(title)] = release

                for contract in fat_contracts:
                    try:
                        raw = contract.raw_data or {}
                        title = raw.get("title", "")
                        if not title:
                            continue

                        norm = _norm_title(title)
                        award = award_by_title.get(norm)

                        if not award:
                            # Try partial match — if the FAT title is a substring
                            for at, ar in award_by_title.items():
                                if norm in at or at in norm:
                                    award = ar
                                    break

                        if not award:
                            continue

                        start, end, _, _ = ContractsFinderScraper._extract_contract_details(award)
                        if not start and not end:
                            continue

                        changed = False
                        if contract.contract_start_date is None and start:
                            contract.contract_start_date = start
                            changed = True
                        if contract.contract_end_date is None and end:
                            contract.contract_end_date = end
                            changed = True

                        if changed:
                            phase_b_updated += 1
                            existing_raw = dict(contract.raw_data or {})
                            existing_raw["_cf_award_title_match"] = award.get("id", "")
                            existing_raw["_date_source"] = "cf_award_title_match"
                            contract.raw_data = existing_raw

                            _propagate_dates_to_scheme(db, contract)
                            db.commit()

                    except Exception:
                        logger.exception(
                            "backfill_cf_award_fat_match_failed",
                            contract_id=contract.id,
                        )
                        phase_b_errors += 1
                        db.rollback()

            except Exception:
                logger.exception("backfill_cf_awards_phase_b_search_failed")
                phase_b_errors += 1

            try:
                _run_async(scraper.close())
            except Exception:
                pass

        result = {
            "cf_contracts_scanned": len(cf_contracts),
            "phase_a_updated": phase_a_updated,
            "phase_a_errors": phase_a_errors,
            "fat_contracts_scanned": len(fat_contracts),
            "phase_b_updated": phase_b_updated,
            "phase_b_errors": phase_b_errors,
            "total_updated": phase_a_updated + phase_b_updated,
        }
        logger.info("backfill_contract_dates_cf_awards_completed", **result)
        return result

    finally:
        db.close()


def _propagate_dates_to_scheme(db: Session, contract) -> bool:
    """Copy contract dates to the parent ExistingScheme if its dates are NULL.

    Returns True if the scheme was updated.
    """
    from app.models.models import ExistingScheme

    scheme = (
        db.query(ExistingScheme)
        .filter(ExistingScheme.id == contract.scheme_id)
        .first()
    )
    if not scheme:
        return False

    changed = False
    if scheme.contract_start_date is None and contract.contract_start_date:
        scheme.contract_start_date = contract.contract_start_date
        changed = True
    if scheme.contract_end_date is None and contract.contract_end_date:
        scheme.contract_end_date = contract.contract_end_date
        changed = True
    return changed


@celery_app.task(
    name="app.tasks.enrichment_tasks.backfill_dates_from_duration",
    acks_late=True,
)
def backfill_dates_from_duration() -> dict[str, Any]:
    """Tier 2: Compute end dates from start_date + duration mentioned in
    description, and start dates from published_date + duration.

    Targets contracts that have a start_date but no end_date (or vice versa)
    where the description mentions a duration like "5 years" or "36 months".

    Also handles the case where a contract has no start_date but has a
    published_date or award date — uses that as a proxy for start and computes
    end from duration.

    Designed to run daily after the primary ``backfill_contract_dates`` task.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme, SchemeContract
        from app.scrapers.date_extractor import (
            _add_duration,
            extract_contract_duration,
        )

        # ---- Case A: Has start_date, missing end_date, description has duration ----
        contracts_no_end = (
            db.query(SchemeContract)
            .filter(
                SchemeContract.contract_start_date.isnot(None),
                SchemeContract.contract_end_date.is_(None),
            )
            .all()
        )

        logger.info("backfill_duration_case_a", contracts=len(contracts_no_end))

        case_a_updated = 0
        for contract in contracts_no_end:
            raw = contract.raw_data or {}
            desc = raw.get("description", "")
            if not desc:
                continue

            duration_months = extract_contract_duration(desc)
            if not duration_months:
                continue

            try:
                end = _add_duration(contract.contract_start_date, duration_months, "month")
                contract.contract_end_date = end

                # Mark the source for audit
                existing_raw = dict(contract.raw_data or {})
                existing_raw["_date_source"] = existing_raw.get("_date_source", "") + ",duration_inference"
                existing_raw["_inferred_duration_months"] = duration_months
                contract.raw_data = existing_raw

                _propagate_dates_to_scheme(db, contract)
                db.commit()
                case_a_updated += 1

                logger.debug(
                    "backfill_duration_end_computed",
                    contract_id=contract.id,
                    start=str(contract.contract_start_date),
                    end=str(end),
                    duration_months=duration_months,
                )
            except Exception:
                logger.exception(
                    "backfill_duration_case_a_failed",
                    contract_id=contract.id,
                )
                db.rollback()

        # ---- Case B: No start_date, no end_date, but has duration + published date ----
        contracts_no_dates = (
            db.query(SchemeContract)
            .filter(
                SchemeContract.contract_start_date.is_(None),
                SchemeContract.contract_end_date.is_(None),
            )
            .all()
        )

        logger.info("backfill_duration_case_b", contracts=len(contracts_no_dates))

        case_b_updated = 0
        for contract in contracts_no_dates:
            raw = contract.raw_data or {}
            desc = raw.get("description", "")
            if not desc:
                continue

            duration_months = extract_contract_duration(desc)
            if not duration_months:
                continue

            # Try to find a proxy start date:
            # 1. published_date from raw_data
            # 2. award date from raw_data
            # 3. created_at of the contract record
            proxy_start = None

            published = raw.get("published_date") or raw.get("publishedDate")
            if published:
                try:
                    from app.scrapers.contracts_finder import ContractsFinderScraper
                    proxy_start = ContractsFinderScraper._parse_iso_date(str(published))
                except Exception:
                    pass

            if not proxy_start:
                # Try award date
                awards = raw.get("awards", [])
                if isinstance(awards, list):
                    for award in awards:
                        award_date = award.get("date")
                        if award_date:
                            try:
                                from app.scrapers.contracts_finder import ContractsFinderScraper
                                proxy_start = ContractsFinderScraper._parse_iso_date(str(award_date))
                                if proxy_start:
                                    break
                            except Exception:
                                pass

            if not proxy_start:
                # Use created_at as last resort
                if contract.created_at:
                    proxy_start = contract.created_at.date() if hasattr(contract.created_at, 'date') else contract.created_at

            if not proxy_start:
                continue

            try:
                end = _add_duration(proxy_start, duration_months, "month")
                contract.contract_start_date = proxy_start
                contract.contract_end_date = end

                existing_raw = dict(contract.raw_data or {})
                existing_raw["_date_source"] = "published_date_plus_duration"
                existing_raw["_inferred_duration_months"] = duration_months
                existing_raw["_proxy_start_source"] = "published_date" if published else "created_at"
                contract.raw_data = existing_raw

                _propagate_dates_to_scheme(db, contract)
                db.commit()
                case_b_updated += 1

                logger.debug(
                    "backfill_duration_proxy_computed",
                    contract_id=contract.id,
                    proxy_start=str(proxy_start),
                    end=str(end),
                    duration_months=duration_months,
                )
            except Exception:
                logger.exception(
                    "backfill_duration_case_b_failed",
                    contract_id=contract.id,
                )
                db.rollback()

        result = {
            "case_a_scanned": len(contracts_no_end),
            "case_a_updated": case_a_updated,
            "case_b_scanned": len(contracts_no_dates),
            "case_b_updated": case_b_updated,
            "total_updated": case_a_updated + case_b_updated,
        }
        logger.info("backfill_dates_from_duration_completed", **result)
        return result

    finally:
        db.close()


# ---------------------------------------------------------------------------
# CPV-based typical duration map (months) — Tier 3
# Based on UK public-sector contract norms by CPV code family.
# ---------------------------------------------------------------------------
CPV_TYPICAL_DURATIONS: dict[str, tuple[int, str]] = {
    # Housing management — typically 5-7 year terms
    "70332": (72, "housing_management"),       # Housing management services (owned)
    "70333": (72, "housing_management"),       # Housing management services (rented)
    "70330": (60, "property_management"),      # Property management
    # Facilities management — 3-5 years
    "79993": (48, "facilities_management"),    # Building and FM services
    # Maintenance — 3-5 years
    "50700": (48, "maintenance"),              # Building maintenance & repair
    "45211": (36, "construction"),             # Construction of buildings
    # Accommodation — 1-3 years
    "98341": (24, "accommodation"),            # Accommodation services
    "55100": (24, "hotel_accommodation"),      # Hotel services
    "55200": (24, "camping"),                  # Camping/caravan sites
    "55250": (24, "short_lets"),               # Short-let accommodation
}

# Keyword-based fallback durations when no CPV match
KEYWORD_TYPICAL_DURATIONS: list[tuple[str, int, str]] = [
    ("housing management", 72, "housing_management"),
    ("tenant management", 72, "housing_management"),
    ("estate management", 60, "property_management"),
    ("property management", 60, "property_management"),
    ("block management", 60, "property_management"),
    ("facilities management", 48, "facilities_management"),
    ("concierge", 36, "concierge"),
    ("repairs and maintenance", 48, "maintenance"),
    ("housing maintenance", 48, "maintenance"),
    ("maintenance", 36, "maintenance"),
    ("cleaning", 36, "cleaning"),
    ("landscaping", 36, "landscaping"),
    ("insurance", 24, "insurance"),
    ("temporary accommodation", 24, "temporary_accommodation"),
    ("student accommodation", 60, "student_accommodation"),
    ("build to rent", 60, "btr_management"),
    ("care home", 60, "care_home"),
    ("supported housing", 60, "supported_housing"),
    ("sheltered housing", 60, "sheltered_housing"),
]


@celery_app.task(
    name="app.tasks.enrichment_tasks.estimate_contract_dates_cpv",
    acks_late=True,
)
def estimate_contract_dates_cpv() -> dict[str, Any]:
    """Tier 3: Estimate contract end dates using CPV-code or keyword-based
    typical durations for the sector.

    This is a last-resort heuristic for contracts that still have no dates
    after Tier 1 (CF award API) and Tier 2 (duration extraction).

    Estimated dates are flagged with ``_date_source: "cpv_estimate"`` and a
    ``_date_confidence: "low"`` marker in ``raw_data`` so downstream systems
    can treat them as estimates rather than confirmed dates.

    Designed to run weekly via Celery Beat (after Tier 1 and 2 have run).
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme, SchemeContract

        # Only target contracts still missing end dates after Tiers 1 & 2
        contracts = (
            db.query(SchemeContract)
            .filter(SchemeContract.contract_end_date.is_(None))
            .all()
        )

        if not contracts:
            logger.info("estimate_cpv_dates_none_found")
            return {"scanned": 0, "estimated": 0}

        logger.info("estimate_cpv_dates_found", count=len(contracts))

        estimated = 0
        no_match = 0

        for contract in contracts:
            raw = contract.raw_data or {}

            # Try to determine typical duration
            duration_months = None
            duration_category = None

            # Strategy A: CPV code match
            cpv_codes = raw.get("cpv_codes", [])
            if isinstance(cpv_codes, list):
                for cpv in cpv_codes:
                    cpv_prefix = str(cpv)[:5]
                    if cpv_prefix in CPV_TYPICAL_DURATIONS:
                        duration_months, duration_category = CPV_TYPICAL_DURATIONS[cpv_prefix]
                        break

            # Strategy B: Keyword match on title/description
            if not duration_months:
                title = raw.get("title", "")
                desc = raw.get("description", "")
                combined = f"{title} {desc}".lower()

                for keyword, months, category in KEYWORD_TYPICAL_DURATIONS:
                    if keyword in combined:
                        duration_months = months
                        duration_category = category
                        break

            if not duration_months:
                no_match += 1
                continue

            # Determine a start date proxy
            start = contract.contract_start_date
            if not start:
                # Try published date, award date, or created_at
                published = raw.get("published_date") or raw.get("publishedDate")
                if published:
                    try:
                        from app.scrapers.contracts_finder import ContractsFinderScraper
                        start = ContractsFinderScraper._parse_iso_date(str(published))
                    except Exception:
                        pass

                if not start:
                    awards = raw.get("awards", [])
                    if isinstance(awards, list):
                        for award in awards:
                            ad = award.get("date")
                            if ad:
                                try:
                                    from app.scrapers.contracts_finder import ContractsFinderScraper
                                    start = ContractsFinderScraper._parse_iso_date(str(ad))
                                    if start:
                                        break
                                except Exception:
                                    pass

                if not start and contract.created_at:
                    start = contract.created_at.date() if hasattr(contract.created_at, 'date') else contract.created_at

            if not start:
                no_match += 1
                continue

            try:
                from app.scrapers.date_extractor import _add_duration

                end = _add_duration(start, duration_months, "month")

                if contract.contract_start_date is None:
                    contract.contract_start_date = start
                contract.contract_end_date = end

                # Mark as estimate with low confidence
                existing_raw = dict(contract.raw_data or {})
                existing_raw["_date_source"] = "cpv_estimate"
                existing_raw["_date_confidence"] = "low"
                existing_raw["_estimated_duration_months"] = duration_months
                existing_raw["_duration_category"] = duration_category
                contract.raw_data = existing_raw

                _propagate_dates_to_scheme(db, contract)
                db.commit()
                estimated += 1

                logger.debug(
                    "estimate_cpv_date_set",
                    contract_id=contract.id,
                    start=str(start),
                    end=str(end),
                    category=duration_category,
                    duration_months=duration_months,
                )
            except Exception:
                logger.exception(
                    "estimate_cpv_date_failed",
                    contract_id=contract.id,
                )
                db.rollback()

        result = {
            "scanned": len(contracts),
            "estimated": estimated,
            "no_cpv_keyword_match": no_match,
        }
        logger.info("estimate_contract_dates_cpv_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.enrichment_tasks.reprocess_operator_extraction",
    acks_late=True,
)
def reprocess_operator_extraction() -> dict[str, Any]:
    """Re-run operator and asset-manager extraction on existing contracts.

    Clears bad operator links (those that look like sentence fragments) and
    re-extracts using the tightened regex patterns.  Also extracts asset
    managers for schemes that don't have one yet.

    Designed to run once after a regex fix, or weekly to catch improvements.
    """
    db = _get_db()
    try:
        from app.models.models import Company, ExistingScheme, SchemeContract
        from app.scrapers.scheme_ingest import (
            _extract_asset_manager_from_text,
            _extract_operator_from_text,
            _find_or_create_company,
            _is_valid_company_name,
        )

        # ---- 1. Clean up bad operator names on contracts ----
        contracts_with_op = (
            db.query(SchemeContract)
            .filter(SchemeContract.operator_company_id.isnot(None))
            .all()
        )

        cleaned = 0
        for contract in contracts_with_op:
            if contract.operator_company_id:
                company = db.query(Company).get(contract.operator_company_id)
                if company and not _is_valid_company_name(company.name):
                    contract.operator_company_id = None
                    cleaned += 1

        if cleaned:
            db.commit()
            logger.info("reprocess_operator_cleaned_bad_links", cleaned=cleaned)

        # ---- 2. Re-extract operators from description for contracts without one ----
        contracts_no_op = (
            db.query(SchemeContract)
            .filter(SchemeContract.operator_company_id.is_(None))
            .all()
        )

        operators_extracted = 0
        for contract in contracts_no_op:
            raw_data = contract.raw_data or {}
            description = raw_data.get("description", "")
            title = raw_data.get("title", "")

            supplier = _extract_operator_from_text(description)
            if not supplier:
                supplier = _extract_operator_from_text(title)
            if not supplier:
                continue

            company = _find_or_create_company(supplier, db, company_type="Operator")
            if company:
                contract.operator_company_id = company.id
                operators_extracted += 1

                # Also set on scheme if missing
                scheme = db.query(ExistingScheme).get(contract.scheme_id)
                if scheme and not scheme.operator_company_id:
                    scheme.operator_company_id = company.id

        db.commit()

        # ---- 3. Extract asset managers ----
        schemes_no_am = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.asset_manager_company_id.is_(None))
            .all()
        )

        am_extracted = 0
        for scheme in schemes_no_am:
            # Get the latest contract for this scheme to read its description
            contract = (
                db.query(SchemeContract)
                .filter(SchemeContract.scheme_id == scheme.id)
                .order_by(SchemeContract.created_at.desc())
                .first()
            )
            if not contract:
                continue

            raw_data = contract.raw_data or {}
            description = raw_data.get("description", "")

            am_name = _extract_asset_manager_from_text(description)
            if not am_name:
                continue

            am_company = _find_or_create_company(am_name, db, company_type="Asset Manager")
            if am_company:
                scheme.asset_manager_company_id = am_company.id
                am_extracted += 1

        db.commit()

        result = {
            "bad_operators_cleaned": cleaned,
            "contracts_scanned": len(contracts_no_op),
            "operators_extracted": operators_extracted,
            "schemes_scanned_for_am": len(schemes_no_am),
            "asset_managers_extracted": am_extracted,
        }
        logger.info("reprocess_operator_extraction_completed", **result)
        return result

    finally:
        db.close()
