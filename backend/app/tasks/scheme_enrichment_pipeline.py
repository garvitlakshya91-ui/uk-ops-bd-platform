"""
Unified scheme enrichment pipeline.

Chains multiple owner/operator resolution strategies to maximise coverage
on existing_schemes records. Designed to run both:
  - Automatically after any scrape/ingestion completes
  - On a daily schedule to catch stragglers

Strategies (in order of confidence):
  S1: CCOD postcode match            — Land Registry corporate freeholder
  S2: ARL BTR postcode match         — Developer/operator from ARL data
  S3: Planning applicant postcode     — Planning application applicant name
  S4: Companies House SPV postcode    — Developer SPV registered address
  S5: Contract/tender same scheme     — Inherit from linked contracts
  S6: Outward code proximity          — Nearby postcode corporate owner
  S7: Council fallback                — Resolve council_id from postcode

Each strategy only targets schemes still missing the relevant field,
so re-running is safe and idempotent.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db() -> Session:
    return SessionLocal()


# ---------------------------------------------------------------------------
# Strategy helpers
# ---------------------------------------------------------------------------

def _normalise_company_name(name: str) -> str:
    n = name.lower().strip()
    for suffix in (" limited", " ltd", " plc", " llp", " inc", " lp", " cic"):
        n = n.replace(suffix, "")
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return re.sub(r"\s+", " ", n).strip()


def _find_or_create_company(
    db: Session,
    name: str,
    companies_house_number: str | None = None,
    company_type: str = "Developer",
) -> Optional[int]:
    """Find existing company by normalized name or CH number, or create new."""
    from app.models.models import Company

    if not name or len(name.strip()) < 3:
        return None

    norm = _normalise_company_name(name)
    if not norm:
        return None

    # Try CH number first
    if companies_house_number:
        existing = (
            db.query(Company)
            .filter(Company.companies_house_number == companies_house_number)
            .first()
        )
        if existing:
            return existing.id

    # Try normalized name
    existing = (
        db.query(Company)
        .filter(Company.normalized_name == norm)
        .first()
    )
    if existing:
        return existing.id

    # Fuzzy match
    existing = (
        db.query(Company)
        .filter(Company.normalized_name.ilike(f"%{norm}%"))
        .first()
    )
    if existing:
        return existing.id

    # Create new
    new_company = Company(
        name=name.strip()[:500],
        normalized_name=norm[:500],
        companies_house_number=companies_house_number,
        company_type=company_type,
        is_active=True,
    )
    db.add(new_company)
    db.flush()
    return new_company.id


# ---------------------------------------------------------------------------
# S1: CCOD Postcode Match
# ---------------------------------------------------------------------------

def _enrich_from_ccod(db: Session) -> dict[str, int]:
    """Match schemes to Land Registry CCOD by postcode."""
    import os
    from app.config import settings

    # Find the CCOD file
    local_path = settings.HMLR_CCOD_LOCAL_PATH
    if not local_path or not os.path.exists(local_path):
        # Check common locations
        for candidate in [
            "CCOD_FULL_2026_03.zip",
            "C:/Users/garvi/uk-ops-bd-platform/backend/CCOD_FULL_2026_03.zip",
        ]:
            if os.path.exists(candidate):
                local_path = candidate
                break

    if not local_path or not os.path.exists(local_path):
        logger.info("ccod_enrichment_skipped", reason="no CCOD file found")
        return {"ccod_matched": 0}

    from app.scrapers.hmlr_ccod_scraper import HMLRCCODScraper

    # Get postcodes needing owner
    rows = db.execute(text(
        "SELECT DISTINCT postcode FROM existing_schemes "
        "WHERE owner_company_id IS NULL AND postcode IS NOT NULL AND postcode != ''"
    )).fetchall()
    postcodes = {r[0] for r in rows}

    if not postcodes:
        return {"ccod_matched": 0}

    logger.info("ccod_enrichment_starting", postcodes=len(postcodes))

    # Build postcode -> best proprietor map
    scraper = HMLRCCODScraper(local_path=local_path)
    postcode_owners: dict[str, dict] = {}  # postcode -> {name, ch_number, count}

    for row in scraper.filter_by_postcodes(postcodes):
        pc = row.postcode
        prop = row.primary_proprietor
        if not prop or not prop.get("name"):
            continue

        if pc not in postcode_owners:
            postcode_owners[pc] = {
                "name": prop["name"],
                "ch_number": prop.get("registration_number", ""),
                "title": row.title_number,
                "tenure": row.tenure,
                "count": 1,
            }
        else:
            postcode_owners[pc]["count"] += 1

    # Apply to schemes
    matched = 0
    for pc, owner_info in postcode_owners.items():
        company_id = _find_or_create_company(
            db, owner_info["name"], owner_info.get("ch_number"), "Freeholder"
        )
        if not company_id:
            continue

        updated = db.execute(text(
            "UPDATE existing_schemes SET owner_company_id = :cid, "
            "hmlr_title_number = :title, hmlr_tenure = :tenure "
            "WHERE postcode = :pc AND owner_company_id IS NULL"
        ), {
            "cid": company_id,
            "title": owner_info.get("title", ""),
            "tenure": owner_info.get("tenure", ""),
            "pc": pc,
        })
        matched += updated.rowcount

    db.commit()
    logger.info("ccod_enrichment_done", matched=matched)
    return {"ccod_matched": matched}


# ---------------------------------------------------------------------------
# S2: ARL BTR Postcode Cross-Reference
# ---------------------------------------------------------------------------

def _enrich_from_arl(db: Session) -> dict[str, int]:
    """Copy owner/operator from ARL BTR schemes to EPC schemes at same postcode."""
    result = db.execute(text("""
        UPDATE existing_schemes target
        SET owner_company_id = arl.owner_company_id
        FROM existing_schemes arl
        WHERE arl.source = 'arl_btr_open_operating'
          AND arl.owner_company_id IS NOT NULL
          AND target.owner_company_id IS NULL
          AND target.postcode IS NOT NULL
          AND target.postcode = arl.postcode
          AND target.id != arl.id
    """))
    owner_count = result.rowcount

    result2 = db.execute(text("""
        UPDATE existing_schemes target
        SET operator_company_id = arl.operator_company_id
        FROM existing_schemes arl
        WHERE arl.source = 'arl_btr_open_operating'
          AND arl.operator_company_id IS NOT NULL
          AND target.operator_company_id IS NULL
          AND target.postcode IS NOT NULL
          AND target.postcode = arl.postcode
          AND target.id != arl.id
    """))
    operator_count = result2.rowcount

    db.commit()
    logger.info("arl_crossref_done", owners=owner_count, operators=operator_count)
    return {"arl_owners": owner_count, "arl_operators": operator_count}


# ---------------------------------------------------------------------------
# S3: Planning Application Applicant Match
# ---------------------------------------------------------------------------

def _enrich_from_planning(db: Session) -> dict[str, int]:
    """Match schemes to planning application applicants by postcode."""
    # Find schemes missing owner that have a postcode
    schemes = db.execute(text("""
        SELECT es.id, es.postcode
        FROM existing_schemes es
        WHERE es.owner_company_id IS NULL
          AND es.postcode IS NOT NULL AND es.postcode != ''
        LIMIT 5000
    """)).fetchall()

    if not schemes:
        return {"planning_matched": 0}

    # Build postcode -> applicant_company_id from planning_applications
    postcodes = list({s[1] for s in schemes})

    # Batch query planning apps with applicants at these postcodes
    matched = 0
    batch_size = 500
    for i in range(0, len(postcodes), batch_size):
        batch = postcodes[i:i + batch_size]
        apps = db.execute(text("""
            SELECT DISTINCT ON (postcode) postcode, applicant_company_id
            FROM planning_applications
            WHERE postcode IN :pcs
              AND applicant_company_id IS NOT NULL
            ORDER BY postcode, created_at DESC
        """), {"pcs": tuple(batch)}).fetchall()

        pc_to_company = {a[0]: a[1] for a in apps}

        for scheme_id, pc in schemes:
            if pc in pc_to_company:
                db.execute(text(
                    "UPDATE existing_schemes SET owner_company_id = :cid "
                    "WHERE id = :sid AND owner_company_id IS NULL"
                ), {"cid": pc_to_company[pc], "sid": scheme_id})
                matched += 1

    db.commit()
    logger.info("planning_crossref_done", matched=matched)
    return {"planning_matched": matched}


# ---------------------------------------------------------------------------
# S4: Companies House SPV Address Match
# ---------------------------------------------------------------------------

def _enrich_from_spv_address(db: Session) -> dict[str, int]:
    """Match schemes to developer SPV companies by registered address postcode."""
    result = db.execute(text("""
        UPDATE existing_schemes es
        SET owner_company_id = c.id
        FROM companies c
        WHERE es.owner_company_id IS NULL
          AND es.postcode IS NOT NULL
          AND c.registered_address IS NOT NULL
          AND es.postcode = UPPER(TRIM(
              SUBSTRING(c.registered_address FROM '[A-Za-z]{1,2}[0-9][A-Za-z0-9]?\\s*[0-9][A-Za-z]{2}$')
          ))
          AND c.company_type IN ('Developer', 'SPV', 'Housebuilder')
    """))
    matched = result.rowcount
    db.commit()
    logger.info("spv_address_match_done", matched=matched)
    return {"spv_matched": matched}


# ---------------------------------------------------------------------------
# S5: Inherit Owner from Linked Contracts
# ---------------------------------------------------------------------------

def _enrich_from_contracts(db: Session) -> dict[str, int]:
    """Copy owner from SchemeContract to parent ExistingScheme if missing."""
    result = db.execute(text("""
        UPDATE existing_schemes es
        SET owner_company_id = sc.client_company_id
        FROM scheme_contracts sc
        WHERE sc.scheme_id = es.id
          AND es.owner_company_id IS NULL
          AND sc.client_company_id IS NOT NULL
    """))
    owners = result.rowcount

    result2 = db.execute(text("""
        UPDATE existing_schemes es
        SET operator_company_id = sc.operator_company_id
        FROM scheme_contracts sc
        WHERE sc.scheme_id = es.id
          AND es.operator_company_id IS NULL
          AND sc.operator_company_id IS NOT NULL
    """))
    operators = result2.rowcount

    db.commit()
    logger.info("contract_inherit_done", owners=owners, operators=operators)
    return {"contract_owners": owners, "contract_operators": operators}


# ---------------------------------------------------------------------------
# S6: Outward Code Proximity Match
# ---------------------------------------------------------------------------

def _enrich_from_nearby_postcode(db: Session) -> dict[str, int]:
    """
    For schemes still missing owner, find corporate owners at nearby postcodes
    (same outward code) from CCOD-enriched schemes.
    Only use when the same company owns multiple schemes in the same outward code.
    """
    result = db.execute(text("""
        WITH outward_owners AS (
            SELECT
                SPLIT_PART(postcode, ' ', 1) AS outward_code,
                owner_company_id,
                COUNT(*) as scheme_count
            FROM existing_schemes
            WHERE owner_company_id IS NOT NULL
              AND postcode IS NOT NULL
            GROUP BY SPLIT_PART(postcode, ' ', 1), owner_company_id
            HAVING COUNT(*) >= 3
        ),
        ranked AS (
            SELECT outward_code, owner_company_id, scheme_count,
                   ROW_NUMBER() OVER (PARTITION BY outward_code ORDER BY scheme_count DESC) as rn
            FROM outward_owners
        )
        UPDATE existing_schemes es
        SET owner_company_id = r.owner_company_id
        FROM ranked r
        WHERE es.owner_company_id IS NULL
          AND es.postcode IS NOT NULL
          AND SPLIT_PART(es.postcode, ' ', 1) = r.outward_code
          AND r.rn = 1
    """))
    matched = result.rowcount
    db.commit()
    logger.info("nearby_postcode_match_done", matched=matched)
    return {"nearby_matched": matched}


# ---------------------------------------------------------------------------
# S7: Council Resolution
# ---------------------------------------------------------------------------

def _resolve_councils(db: Session) -> dict[str, int]:
    """Resolve council_id for schemes that have postcode but no council."""
    from app.models.models import Council

    schemes = db.execute(text("""
        SELECT id, postcode FROM existing_schemes
        WHERE council_id IS NULL AND postcode IS NOT NULL AND postcode != ''
        LIMIT 5000
    """)).fetchall()

    if not schemes:
        return {"councils_resolved": 0}

    # Build postcode outward code -> council mapping from existing data
    council_map = db.execute(text("""
        SELECT DISTINCT ON (SPLIT_PART(postcode, ' ', 1))
            SPLIT_PART(postcode, ' ', 1) as outward,
            council_id
        FROM existing_schemes
        WHERE council_id IS NOT NULL AND postcode IS NOT NULL
        ORDER BY SPLIT_PART(postcode, ' ', 1), COUNT(*) OVER (
            PARTITION BY SPLIT_PART(postcode, ' ', 1), council_id
        ) DESC
    """)).fetchall()

    outward_to_council = {r[0]: r[1] for r in council_map}

    resolved = 0
    for scheme_id, postcode in schemes:
        outward = postcode.split(" ")[0] if " " in postcode else postcode[:-3]
        council_id = outward_to_council.get(outward)
        if council_id:
            db.execute(text(
                "UPDATE existing_schemes SET council_id = :cid WHERE id = :sid"
            ), {"cid": council_id, "sid": scheme_id})
            resolved += 1

    db.commit()
    logger.info("council_resolution_done", resolved=resolved)
    return {"councils_resolved": resolved}


# ---------------------------------------------------------------------------
# S8: Postcode Backfill
# ---------------------------------------------------------------------------

def _backfill_postcodes(db: Session) -> dict[str, int]:
    """Extract postcodes from address field for schemes missing postcode."""
    UK_PC_RE = re.compile(r'[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}', re.I)

    schemes = db.execute(text("""
        SELECT id, address, name FROM existing_schemes
        WHERE (postcode IS NULL OR postcode = '')
          AND (address IS NOT NULL AND address != '')
        LIMIT 2000
    """)).fetchall()

    filled = 0
    for sid, address, name in schemes:
        text_to_search = f"{address or ''} {name or ''}"
        match = UK_PC_RE.search(text_to_search)
        if match:
            pc = match.group(0).upper().strip()
            # Normalize spacing
            if len(pc) > 3 and " " not in pc:
                pc = f"{pc[:-3]} {pc[-3:]}"
            db.execute(text(
                "UPDATE existing_schemes SET postcode = :pc WHERE id = :sid"
            ), {"pc": pc, "sid": sid})
            filled += 1

    db.commit()
    logger.info("postcode_backfill_done", filled=filled)
    return {"postcodes_filled": filled}


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.scheme_enrichment_pipeline.enrich_schemes_full",
    acks_late=True,
    time_limit=7200,
    soft_time_limit=6600,
)
def enrich_schemes_full(
    self,
    skip_ccod: bool = False,
    source_filter: str | None = None,
) -> dict[str, Any]:
    """
    Run the full scheme enrichment pipeline.

    Chains all strategies in order of confidence:
    S0: Postcode backfill (prerequisite)
    S1: CCOD postcode match
    S2: ARL BTR cross-reference
    S3: Planning applicant match
    S4: SPV address match
    S5: Contract inheritance
    S6: Outward code proximity
    S7: Council resolution

    Parameters
    ----------
    skip_ccod : bool
        Skip the CCOD step (useful for quick re-runs when CCOD hasn't changed).
    source_filter : str | None
        Only enrich schemes from this source (e.g. 'epc_new_dwelling').
    """
    db = _get_db()
    try:
        logger.info("enrichment_pipeline_starting", skip_ccod=skip_ccod, source=source_filter)

        # Snapshot before
        before = db.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(owner_company_id) as has_owner,
                COUNT(operator_company_id) as has_operator,
                COUNT(council_id) as has_council
            FROM existing_schemes
        """)).fetchone()

        results: dict[str, Any] = {}

        # S0: Postcode backfill (prerequisite for all postcode-based strategies)
        results.update(_backfill_postcodes(db))

        # S1: CCOD (highest confidence — registered freeholder)
        if not skip_ccod:
            results.update(_enrich_from_ccod(db))

        # S2: ARL BTR cross-reference
        results.update(_enrich_from_arl(db))

        # S3: Planning applicant match
        results.update(_enrich_from_planning(db))

        # S4: SPV address match
        results.update(_enrich_from_spv_address(db))

        # S5: Contract inheritance
        results.update(_enrich_from_contracts(db))

        # S6: Outward code proximity
        results.update(_enrich_from_nearby_postcode(db))

        # S7: Council resolution
        results.update(_resolve_councils(db))

        # Snapshot after
        after = db.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(owner_company_id) as has_owner,
                COUNT(operator_company_id) as has_operator,
                COUNT(council_id) as has_council
            FROM existing_schemes
        """)).fetchone()

        results["before"] = {
            "total": before[0], "owners": before[1],
            "operators": before[2], "councils": before[3],
        }
        results["after"] = {
            "total": after[0], "owners": after[1],
            "operators": after[2], "councils": after[3],
        }
        results["improvement"] = {
            "new_owners": after[1] - before[1],
            "new_operators": after[2] - before[2],
            "new_councils": after[3] - before[3],
            "owner_pct": round(after[1] * 100 / after[0], 1) if after[0] else 0,
        }

        logger.info("enrichment_pipeline_complete", **results["improvement"])
        return results

    except Exception as exc:
        logger.exception("enrichment_pipeline_failed")
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Post-scrape hook — lightweight version (skip CCOD)
# ---------------------------------------------------------------------------

@celery_app.task(
    name="app.tasks.scheme_enrichment_pipeline.enrich_new_schemes",
    acks_late=True,
    time_limit=600,
)
def enrich_new_schemes() -> dict[str, Any]:
    """
    Quick enrichment for newly scraped schemes.
    Skips CCOD (slow, runs separately) and uses fast strategies only.
    Called automatically after each scrape task completes.
    """
    db = _get_db()
    try:
        logger.info("enrich_new_schemes_starting")
        results: dict[str, Any] = {}

        results.update(_backfill_postcodes(db))
        results.update(_enrich_from_arl(db))
        results.update(_enrich_from_planning(db))
        results.update(_enrich_from_contracts(db))
        results.update(_enrich_from_nearby_postcode(db))
        results.update(_resolve_councils(db))

        logger.info("enrich_new_schemes_done", **results)
        return results

    except Exception:
        logger.exception("enrich_new_schemes_failed")
        raise
    finally:
        db.close()
