"""Celery tasks for tracking developer SPV incorporations and land ownership.

These tasks combine Companies House API data with Land Registry CCOD data
to provide early pipeline intelligence:

1. **SPV Tracking**: Major UK developers incorporate SPV companies 6-18 months
   before submitting planning applications. Tracking new SPVs gives early
   warning of upcoming development activity.

2. **CCOD Cross-referencing**: When an SPV acquires land, it appears in the
   Land Registry CCOD dataset. Matching SPV company numbers against CCOD
   reveals which sites a developer controls.

3. **Pipeline Enrichment**: Combining SPV data with existing planning
   applications and scheme data creates richer pipeline opportunities.

Schedules:
    - ``track_developer_spvs``: Weekly (Sundays at 01:00)
    - ``ingest_land_registry_ccod_ownership``: Monthly (10th at 02:00)
    - ``enrich_company_ownership``: Weekly (Mondays at 03:00)
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

import structlog
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


# -------------------------------------------------------------------------
# Top 30 UK residential developers (and major BTR/PBSA operators)
# -------------------------------------------------------------------------
TARGET_DEVELOPERS: list[dict[str, str]] = [
    # Volume housebuilders
    {"name": "Barratt", "full_name": "Barratt Developments PLC"},
    {"name": "Persimmon", "full_name": "Persimmon PLC"},
    {"name": "Taylor Wimpey", "full_name": "Taylor Wimpey PLC"},
    {"name": "Bellway", "full_name": "Bellway PLC"},
    {"name": "Berkeley", "full_name": "Berkeley Group Holdings PLC"},
    {"name": "Redrow", "full_name": "Redrow PLC"},
    {"name": "Vistry", "full_name": "Vistry Group PLC"},
    {"name": "Crest Nicholson", "full_name": "Crest Nicholson Holdings PLC"},
    {"name": "Countryside", "full_name": "Countryside Partnerships PLC"},
    {"name": "Miller Homes", "full_name": "Miller Homes Group Ltd"},
    {"name": "Bloor Homes", "full_name": "Bloor Holdings Ltd"},
    {"name": "Keepmoat", "full_name": "Keepmoat Homes Ltd"},
    # BTR developers/operators
    {"name": "Greystar", "full_name": "Greystar Real Estate Partners"},
    {"name": "Legal & General", "full_name": "Legal and General Group PLC"},
    {"name": "Grainger", "full_name": "Grainger PLC"},
    {"name": "Get Living", "full_name": "Get Living PLC"},
    {"name": "Quintain", "full_name": "Quintain Ltd"},
    {"name": "Moda Living", "full_name": "Moda Living Ltd"},
    {"name": "Essential Living", "full_name": "Essential Living Ltd"},
    {"name": "Fizzy Living", "full_name": "Fizzy Living Ltd"},
    {"name": "Way of Life", "full_name": "Way of Life Ltd"},
    {"name": "Packaged Living", "full_name": "Packaged Living Ltd"},
    # PBSA developers/operators
    {"name": "Unite Students", "full_name": "Unite Group PLC"},
    {"name": "iQ Student", "full_name": "iQ Student Accommodation"},
    {"name": "Empiric Student", "full_name": "Empiric Student Property PLC"},
    {"name": "Fresh Student", "full_name": "Fresh Student Living"},
    {"name": "CRM Students", "full_name": "CRM Students Ltd"},
    # Mixed / major developers
    {"name": "L&Q", "full_name": "L&Q Group"},
    {"name": "Peabody", "full_name": "Peabody Trust"},
    {"name": "Lendlease", "full_name": "Lendlease Corporation Ltd"},
]


def _get_db() -> Session:
    """Create a new database session for use inside a Celery task."""
    return SessionLocal()


def _run_async(coro):
    """Run an async coroutine from synchronous Celery task context."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()

    return asyncio.run(coro)


# -------------------------------------------------------------------------
# Task 1: Track Developer SPVs
# -------------------------------------------------------------------------

async def _track_spvs_async(
    days_back: int = 30,
) -> dict[str, Any]:
    """Search Companies House for new SPVs from target developers."""
    from app.scrapers.companies_house_scraper import CompaniesHouseScraper, PROPERTY_SIC_CODES

    since_date = datetime.date.today() - datetime.timedelta(days=days_back)
    all_spvs: list[dict[str, Any]] = []

    async with CompaniesHouseScraper() as ch:
        for dev in TARGET_DEVELOPERS:
            try:
                spvs = await ch.search_developer_spvs(
                    dev["name"],
                    since_date=since_date,
                )
                for spv in spvs:
                    spv["_parent_developer"] = dev["name"]
                    spv["_parent_developer_full"] = dev["full_name"]
                all_spvs.extend(spvs)

                logger.info(
                    "developer_spv_search",
                    developer=dev["name"],
                    spvs_found=len(spvs),
                )
            except Exception:
                logger.exception(
                    "developer_spv_search_failed",
                    developer=dev["name"],
                )

        # Also enrich top results with officer/PSC details
        enriched: list[dict[str, Any]] = []
        for spv in all_spvs[:100]:  # Limit enrichment to top 100 to respect rate limits
            company_number = spv.get("company_number")
            if not company_number:
                enriched.append(spv)
                continue

            try:
                details = await ch.enrich_spv_details(company_number)
                spv["_enriched"] = details
                # Try to identify the parent developer from PSC data
                parent = ch.extract_parent_developer(
                    details.get("pscs", []),
                    details.get("officers", []),
                )
                if parent:
                    spv["_confirmed_parent"] = parent
            except Exception:
                logger.warning(
                    "spv_enrichment_failed",
                    company_number=company_number,
                )
            enriched.append(spv)

    return {
        "since_date": since_date.isoformat(),
        "developers_searched": len(TARGET_DEVELOPERS),
        "total_spvs_found": len(all_spvs),
        "enriched_count": len(enriched),
        "spvs": all_spvs,
    }


def _persist_spvs(db: Session, spv_data: dict[str, Any]) -> dict[str, int]:
    """Persist discovered SPVs to the database.

    Creates Company records for new SPVs and PipelineOpportunity entries
    for promising finds.

    Returns summary counts.
    """
    from app.models.models import Company, CompanyAlias, PipelineOpportunity
    from app.matching.company_matcher import normalize_company_name, CompanyMatcher

    spvs = spv_data.get("spvs", [])
    new_companies = 0
    updated_companies = 0
    new_opportunities = 0
    errors = 0

    for spv in spvs:
        try:
            company_number = spv.get("company_number", "")
            company_name = spv.get("title") or spv.get("company_name", "")
            if not company_number or not company_name:
                continue

            # Check if company already exists by CH number
            existing = (
                db.query(Company)
                .filter(Company.companies_house_number == company_number)
                .first()
            )

            if existing:
                # Update SIC codes and metadata if changed
                sic_codes = spv.get("sic_codes")
                if sic_codes and existing.sic_codes != sic_codes:
                    existing.sic_codes = sic_codes
                    updated_companies += 1
                continue

            # Check by normalised name
            norm_name = normalize_company_name(company_name)
            existing = (
                db.query(Company)
                .filter(Company.normalized_name == norm_name)
                .first()
            )
            if existing:
                if not existing.companies_house_number:
                    existing.companies_house_number = company_number
                    existing.sic_codes = spv.get("sic_codes")
                    updated_companies += 1
                continue

            # Create new company record
            registered_address = ""
            address_data = spv.get("registered_office_address") or spv.get("address", {})
            if isinstance(address_data, dict):
                from app.scrapers.companies_house_scraper import CompaniesHouseScraper
                registered_address = CompaniesHouseScraper.format_registered_address(address_data)
            elif isinstance(address_data, str):
                registered_address = address_data

            incorporation_date = spv.get("date_of_creation", "")
            company = Company(
                name=company_name,
                normalized_name=norm_name,
                companies_house_number=company_number,
                registered_address=registered_address,
                sic_codes=spv.get("sic_codes"),
                company_type="Developer",
                is_active=True,
            )

            # Store parent developer info in a notes-like way via the
            # parent_company relationship if we can find the parent
            parent_dev_name = spv.get("_parent_developer_full") or spv.get("_parent_developer")
            if parent_dev_name:
                parent = (
                    db.query(Company)
                    .filter(Company.normalized_name == normalize_company_name(parent_dev_name))
                    .first()
                )
                if parent:
                    company.parent_company_id = parent.id

            db.add(company)
            db.flush()
            new_companies += 1

            # Add the parent developer name as an alias source note
            if parent_dev_name:
                db.add(CompanyAlias(
                    company_id=company.id,
                    alias_name=f"SPV of {parent_dev_name}",
                    source="companies_house_spv_tracker",
                ))

            # Create a pipeline opportunity for new SPV
            opportunity = PipelineOpportunity(
                title=f"New SPV: {company_name}",
                description=(
                    f"New SPV incorporated by {parent_dev_name or 'unknown developer'}. "
                    f"Company number: {company_number}. "
                    f"SIC codes: {', '.join(spv.get('sic_codes', []))}. "
                    f"Incorporation date: {incorporation_date}. "
                    f"This may indicate an upcoming development project."
                ),
                company_id=company.id,
                source="companies_house_spv_tracker",
                stage="Lead",
                priority="Medium",
            )
            db.add(opportunity)
            new_opportunities += 1

            db.commit()

        except Exception:
            logger.exception(
                "persist_spv_failed",
                company_number=spv.get("company_number"),
            )
            errors += 1
            db.rollback()

    # Final commit for any remaining updates
    try:
        db.commit()
    except Exception:
        db.rollback()

    return {
        "new_companies": new_companies,
        "updated_companies": updated_companies,
        "new_opportunities": new_opportunities,
        "errors": errors,
    }


@celery_app.task(
    bind=True,
    name="app.tasks.developer_tracking_tasks.track_developer_spvs",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=7200,  # 2 hours max
    soft_time_limit=6600,
)
def track_developer_spvs(self, days_back: int = 30) -> dict[str, Any]:
    """Track new SPV incorporations by major UK developers.

    Searches Companies House for recently incorporated companies
    whose names match known developers and whose SIC codes indicate
    property development activity.

    Parameters
    ----------
    days_back : int
        Number of days to search back for new incorporations.

    Returns
    -------
    dict
        Summary of SPVs found and persisted.
    """
    db = _get_db()
    try:
        logger.info("track_developer_spvs_started", days_back=days_back)

        # Search Companies House for new SPVs
        spv_data = _run_async(_track_spvs_async(days_back=days_back))

        # Persist results
        persist_result = _persist_spvs(db, spv_data)

        result = {
            "status": "success",
            "developers_searched": spv_data["developers_searched"],
            "total_spvs_found": spv_data["total_spvs_found"],
            "enriched_count": spv_data.get("enriched_count", 0),
            **persist_result,
        }
        logger.info("track_developer_spvs_completed", **result)
        return result

    except Exception as exc:
        logger.exception("track_developer_spvs_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# -------------------------------------------------------------------------
# Task 2: Ingest Land Registry CCOD for ownership tracking
# -------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.developer_tracking_tasks.ingest_land_registry_ccod_ownership",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=14400,  # 4 hours (CCOD is a large file)
    soft_time_limit=13800,
)
def ingest_land_registry_ccod_ownership(
    self,
    local_path: str | None = None,
) -> dict[str, Any]:
    """Download and process CCOD data to match against known companies.

    Cross-references all company registration numbers in our database
    against the CCOD dataset to discover which land titles they own.
    Updates Company records with ownership data and creates pipeline
    opportunities for developers found to own sites.

    Parameters
    ----------
    local_path : str | None
        Pre-downloaded CCOD file path. If None, uses configured path.

    Returns
    -------
    dict
        Summary of CCOD processing results.
    """
    db = _get_db()
    try:
        logger.info(
            "ingest_ccod_ownership_started",
            local_path=local_path,
        )
        from app.models.models import Company, PipelineOpportunity
        from app.scrapers.land_registry_scraper import LandRegistryCCODIndex

        # Gather all company numbers from our database
        companies_with_ch = (
            db.query(Company.id, Company.companies_house_number, Company.name)
            .filter(
                Company.companies_house_number.isnot(None),
                Company.companies_house_number != "",
                Company.is_active.is_(True),
            )
            .all()
        )

        if not companies_with_ch:
            logger.info("no_companies_with_ch_numbers")
            return {"status": "success", "message": "No companies with CH numbers to match"}

        # Build mapping from CH number to company ID
        ch_to_company: dict[str, tuple[int, str]] = {}
        ch_numbers: set[str] = set()
        for cid, ch_num, name in companies_with_ch:
            normalised = ch_num.strip().upper()
            if normalised.isdigit():
                normalised = normalised.zfill(8)
            ch_to_company[normalised] = (cid, name)
            ch_numbers.add(normalised)

        logger.info("ccod_companies_to_match", count=len(ch_numbers))

        # Build partial CCOD index for just our companies
        index = LandRegistryCCODIndex(local_path=local_path)
        build_stats = index.build_index_for_companies(ch_numbers)

        # Cross-reference and create pipeline entries
        matches = index.cross_reference_companies(list(ch_numbers))
        new_opportunities = 0
        companies_with_property = 0

        for ch_num, portfolio in matches.items():
            company_id, company_name = ch_to_company.get(ch_num, (None, None))
            if not company_id:
                continue

            companies_with_property += 1

            # For companies with significant property holdings,
            # create or update a pipeline opportunity
            if portfolio.title_count >= 1:
                # Check if we already have a CCOD-sourced opportunity for this company
                existing_opp = (
                    db.query(PipelineOpportunity)
                    .filter(
                        PipelineOpportunity.company_id == company_id,
                        PipelineOpportunity.source == "land_registry_ccod",
                    )
                    .first()
                )

                postcodes_str = ", ".join(sorted(portfolio.postcodes)[:20])
                description = (
                    f"Land Registry CCOD shows {company_name} (CH: {ch_num}) "
                    f"owns {portfolio.title_count} title(s) "
                    f"({portfolio.freehold_count} freehold, {portfolio.leasehold_count} leasehold). "
                    f"Postcodes: {postcodes_str}"
                )

                if existing_opp:
                    existing_opp.description = description
                    existing_opp.notes = (
                        f"Updated {datetime.date.today().isoformat()}. "
                        f"Titles: {portfolio.title_count}"
                    )
                else:
                    opp = PipelineOpportunity(
                        title=f"Land ownership: {company_name} ({portfolio.title_count} titles)",
                        description=description,
                        company_id=company_id,
                        source="land_registry_ccod",
                        stage="Research",
                        priority="Low" if portfolio.title_count < 5 else "Medium",
                    )
                    db.add(opp)
                    new_opportunities += 1

            # Commit in batches
            if companies_with_property % 50 == 0:
                try:
                    db.commit()
                except Exception:
                    db.rollback()
                    logger.exception("ccod_batch_commit_failed")

        try:
            db.commit()
        except Exception:
            db.rollback()

        result = {
            "status": "success",
            "companies_checked": len(ch_numbers),
            "companies_with_property": companies_with_property,
            "new_opportunities": new_opportunities,
            **build_stats,
        }
        logger.info("ingest_ccod_ownership_completed", **result)
        return result

    except Exception as exc:
        logger.exception("ingest_ccod_ownership_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# -------------------------------------------------------------------------
# Task 3: Enrich Company Ownership (cross-reference SPVs with CCOD)
# -------------------------------------------------------------------------

async def _enrich_ownership_async(
    spv_company_numbers: list[str],
) -> dict[str, Any]:
    """Enrich SPV records with full Companies House details."""
    from app.scrapers.companies_house_scraper import CompaniesHouseScraper

    results: list[dict[str, Any]] = []

    async with CompaniesHouseScraper() as ch:
        for cn in spv_company_numbers:
            try:
                details = await ch.enrich_spv_details(cn)
                results.append(details)
            except Exception:
                logger.warning("ownership_enrichment_failed", company_number=cn)
                results.append({"company_number": cn, "error": "enrichment_failed"})

    return {"enriched": results}


@celery_app.task(
    bind=True,
    name="app.tasks.developer_tracking_tasks.enrich_company_ownership",
    max_retries=2,
    default_retry_delay=900,
    acks_late=True,
    time_limit=7200,
    soft_time_limit=6600,
)
def enrich_company_ownership(self) -> dict[str, Any]:
    """Cross-reference recently discovered SPVs with CCOD data.

    Finds company records tagged as developer SPVs that have not yet
    been checked against CCOD, runs the cross-reference, and updates
    pipeline opportunities with property ownership details.

    Returns
    -------
    dict
        Summary of enrichment results.
    """
    db = _get_db()
    try:
        logger.info("enrich_company_ownership_started")
        from app.models.models import Company, PipelineOpportunity
        from app.matching.company_matcher import normalize_company_name
        from app.scrapers.land_registry_scraper import LandRegistryCCODIndex

        # Find SPV companies (those with a parent_company_id or created by
        # our tracker, and that have a CH number)
        spv_companies = (
            db.query(Company)
            .filter(
                Company.companies_house_number.isnot(None),
                Company.companies_house_number != "",
                Company.is_active.is_(True),
                Company.parent_company_id.isnot(None),
            )
            .all()
        )

        if not spv_companies:
            logger.info("no_spv_companies_to_enrich")
            return {"status": "success", "message": "No SPV companies to enrich"}

        spv_numbers = [c.companies_house_number for c in spv_companies if c.companies_house_number]
        ch_to_company = {
            c.companies_house_number: c for c in spv_companies if c.companies_house_number
        }

        logger.info("enriching_spv_companies", count=len(spv_numbers))

        # Step 1: Get fresh details from Companies House for all SPVs
        enrichment_data = _run_async(_enrich_ownership_async(spv_numbers))

        # Update company records with enriched data
        enriched_count = 0
        for item in enrichment_data.get("enriched", []):
            if item.get("error"):
                continue

            cn = item.get("company_number", "")
            company = ch_to_company.get(cn)
            if not company:
                continue

            profile = item.get("profile", {})
            if profile:
                # Update registered address
                address = profile.get("registered_office_address")
                if address:
                    from app.scrapers.companies_house_scraper import CompaniesHouseScraper
                    company.registered_address = CompaniesHouseScraper.format_registered_address(address)

                # Update SIC codes
                sic_codes = profile.get("sic_codes")
                if sic_codes:
                    company.sic_codes = sic_codes

                enriched_count += 1

        try:
            db.commit()
        except Exception:
            db.rollback()

        # Step 2: Cross-reference with CCOD
        ccod_index = LandRegistryCCODIndex()
        ccod_stats = ccod_index.build_index_for_companies(set(spv_numbers))
        ccod_matches = ccod_index.cross_reference_companies(spv_numbers)

        new_opportunities = 0
        spvs_with_property = 0

        for cn, portfolio in ccod_matches.items():
            company = ch_to_company.get(cn)
            if not company:
                continue

            spvs_with_property += 1

            # Check for existing opportunity
            existing_opp = (
                db.query(PipelineOpportunity)
                .filter(
                    PipelineOpportunity.company_id == company.id,
                    PipelineOpportunity.source == "spv_ccod_crossref",
                )
                .first()
            )

            parent_name = ""
            if company.parent_company_id:
                parent = db.query(Company).get(company.parent_company_id)
                parent_name = parent.name if parent else ""

            postcodes_str = ", ".join(sorted(portfolio.postcodes)[:20])
            description = (
                f"SPV {company.name} (CH: {cn})"
                + (f" linked to {parent_name}" if parent_name else "")
                + f" owns {portfolio.title_count} land title(s). "
                f"Postcodes: {postcodes_str}. "
                f"This indicates active site acquisition and potential development."
            )

            if existing_opp:
                existing_opp.description = description
                existing_opp.last_activity_date = datetime.date.today()
            else:
                priority = "High" if portfolio.title_count >= 3 else "Medium"
                opp = PipelineOpportunity(
                    title=f"SPV site acquisition: {company.name}",
                    description=description,
                    company_id=company.id,
                    source="spv_ccod_crossref",
                    stage="Lead",
                    priority=priority,
                    last_activity_date=datetime.date.today(),
                )
                db.add(opp)
                new_opportunities += 1

        try:
            db.commit()
        except Exception:
            db.rollback()

        result = {
            "status": "success",
            "spvs_checked": len(spv_numbers),
            "enriched_count": enriched_count,
            "spvs_with_property": spvs_with_property,
            "new_opportunities": new_opportunities,
            **ccod_stats,
        }
        logger.info("enrich_company_ownership_completed", **result)
        return result

    except Exception as exc:
        logger.exception("enrich_company_ownership_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# -------------------------------------------------------------------------
# Task 4: Find new incorporations with property SIC codes (broad scan)
# -------------------------------------------------------------------------

async def _scan_new_incorporations_async(
    days_back: int = 30,
) -> list[dict[str, Any]]:
    """Search for all new property-related incorporations."""
    from app.scrapers.companies_house_scraper import CompaniesHouseScraper

    async with CompaniesHouseScraper() as ch:
        since_date = datetime.date.today() - datetime.timedelta(days=days_back)
        return await ch.find_new_incorporations(since_date=since_date)


@celery_app.task(
    bind=True,
    name="app.tasks.developer_tracking_tasks.scan_new_property_incorporations",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=3600,
)
def scan_new_property_incorporations(self, days_back: int = 14) -> dict[str, Any]:
    """Broad scan for all new property-development incorporations.

    Unlike ``track_developer_spvs`` which focuses on known developers,
    this task searches for ALL new companies with property development
    SIC codes. Useful for discovering smaller or new-entrant developers.

    Parameters
    ----------
    days_back : int
        Number of days to search back.

    Returns
    -------
    dict
        Summary of new incorporations found.
    """
    db = _get_db()
    try:
        logger.info("scan_new_property_incorporations_started", days_back=days_back)

        incorporations = _run_async(_scan_new_incorporations_async(days_back=days_back))

        # Persist as company records (without parent linkage)
        from app.models.models import Company
        from app.matching.company_matcher import normalize_company_name

        new_count = 0
        existing_count = 0

        for inc in incorporations:
            company_number = inc.get("company_number", "")
            company_name = inc.get("title") or inc.get("company_name", "")
            if not company_number:
                continue

            existing = (
                db.query(Company)
                .filter(Company.companies_house_number == company_number)
                .first()
            )
            if existing:
                existing_count += 1
                continue

            norm_name = normalize_company_name(company_name)
            existing = (
                db.query(Company)
                .filter(Company.normalized_name == norm_name)
                .first()
            )
            if existing:
                if not existing.companies_house_number:
                    existing.companies_house_number = company_number
                existing_count += 1
                continue

            address_data = inc.get("registered_office_address") or inc.get("address", {})
            registered_address = ""
            if isinstance(address_data, dict):
                from app.scrapers.companies_house_scraper import CompaniesHouseScraper
                registered_address = CompaniesHouseScraper.format_registered_address(address_data)

            company = Company(
                name=company_name,
                normalized_name=norm_name,
                companies_house_number=company_number,
                registered_address=registered_address,
                sic_codes=inc.get("sic_codes"),
                company_type="Developer",
                is_active=True,
            )
            db.add(company)
            new_count += 1

            if new_count % 100 == 0:
                try:
                    db.commit()
                except Exception:
                    db.rollback()

        try:
            db.commit()
        except Exception:
            db.rollback()

        result = {
            "status": "success",
            "total_incorporations": len(incorporations),
            "new_companies": new_count,
            "already_known": existing_count,
        }
        logger.info("scan_new_property_incorporations_completed", **result)
        return result

    except Exception as exc:
        logger.exception("scan_new_property_incorporations_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()
