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
    """Run an async coroutine from synchronous Celery task context."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If there is already a running loop (e.g. in some test
            # frameworks), create a new one in a thread.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
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
            except Exception:
                logger.exception(
                    "enrich_scheme_epc_failed",
                    scheme_id=scheme.id,
                    postcode=scheme.postcode,
                )
                errors += 1
                db.rollback()

        _run_async(scraper.close())

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
