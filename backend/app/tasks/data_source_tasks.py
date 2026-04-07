"""
Celery tasks for ingesting data from external property data sources.

Tasks:
- ingest_price_paid_data      — Land Registry Price Paid (monthly new-build CSV)
- ingest_gla_planning         — GLA Planning London Datahub (daily)
- ingest_bpf_btr_pipeline     — BPF Build-to-Rent pipeline (quarterly)
- ingest_epc_new_dwellings    — EPC new-dwelling scheme discovery (weekly)
- ingest_arl_btr_schemes      — ARL/REalyse BTR Open & Operating list (monthly)
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

import structlog

from app.database import SessionLocal
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db():
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


# ---------------------------------------------------------------------------
# 1. Land Registry Price Paid Data
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.data_source_tasks.ingest_price_paid_data",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=3600,  # 1 hour — CSV can be large
)
def ingest_price_paid_data(
    self,
    use_monthly: bool = True,
    local_csv_path: str | None = None,
) -> dict[str, Any]:
    """
    Download the Land Registry Price Paid CSV, filter for new builds,
    cluster by postcode to detect multi-unit developments, and save
    detected schemes to existing_schemes.

    Parameters
    ----------
    use_monthly : bool
        If True (default), download the small monthly update file.
        Set to False for a full backfill with the complete dataset.
    local_csv_path : str | None
        If provided, read from a local file instead of downloading.

    Designed to run monthly via Celery Beat.
    """
    db = _get_db()
    try:
        logger.info(
            "ingest_price_paid_started",
            use_monthly=use_monthly,
            local_csv_path=local_csv_path,
        )

        from app.scrapers.price_paid_scraper import PricePaidScraper, save_price_paid_schemes

        scraper = PricePaidScraper(
            use_monthly=use_monthly,
            local_csv_path=local_csv_path,
        )
        schemes = scraper.fetch_and_cluster()

        logger.info(
            "ingest_price_paid_clustered",
            schemes_detected=len(schemes),
        )

        if not schemes:
            logger.info("ingest_price_paid_no_schemes")
            return {
                "status": "success",
                "schemes_detected": 0,
                "new": 0,
                "updated": 0,
            }

        result = save_price_paid_schemes(schemes, db)

        logger.info("ingest_price_paid_completed", **result)
        return {"status": "success", "schemes_detected": len(schemes), **result}

    except Exception as exc:
        logger.exception("ingest_price_paid_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 2. GLA Planning London Datahub
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.data_source_tasks.ingest_gla_planning",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def ingest_gla_planning(self) -> dict[str, Any]:
    """
    Fetch planning application data from the GLA Planning London Datahub
    and save residential applications (10+ units) to planning_applications.

    Covers all 33 London boroughs.  Resolves borough names to council_id.

    Designed to run daily via Celery Beat.
    """
    db = _get_db()
    try:
        logger.info("ingest_gla_planning_started")

        from app.scrapers.gla_scraper import GLAPlanningDatahubScraper, save_gla_planning_applications

        async def _fetch():
            async with GLAPlanningDatahubScraper() as scraper:
                return await scraper.fetch_all()

        records = _run_async(_fetch())

        logger.info("ingest_gla_planning_fetched", records=len(records))

        if not records:
            logger.info("ingest_gla_planning_no_records")
            return {
                "status": "success",
                "records_fetched": 0,
                "new": 0,
                "updated": 0,
            }

        result = save_gla_planning_applications(records, db)

        logger.info("ingest_gla_planning_completed", **result)
        return {"status": "success", "records_fetched": len(records), **result}

    except Exception as exc:
        logger.exception("ingest_gla_planning_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 3. BPF Build-to-Rent Pipeline
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.data_source_tasks.ingest_bpf_btr_pipeline",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
    time_limit=1800,
)
def ingest_bpf_btr_pipeline(self) -> dict[str, Any]:
    """
    Fetch BTR pipeline data from the BPF interactive map API and save
    discovered schemes to existing_schemes with source="bpf_btr_pipeline".

    The BPF publishes quarterly updates to their BTR pipeline data.

    Designed to run quarterly via Celery Beat (with more frequent
    checks to catch mid-quarter updates).
    """
    db = _get_db()
    try:
        logger.info("ingest_bpf_btr_pipeline_started")

        from app.scrapers.bpf_scraper import BPFBTRScraper, save_bpf_btr_schemes

        async def _fetch():
            async with BPFBTRScraper() as scraper:
                return await scraper.fetch_and_normalise()

        schemes = _run_async(_fetch())

        logger.info("ingest_bpf_btr_pipeline_fetched", schemes=len(schemes))

        if not schemes:
            logger.info("ingest_bpf_btr_pipeline_no_schemes")
            return {
                "status": "success",
                "schemes_fetched": 0,
                "new": 0,
                "updated": 0,
            }

        result = save_bpf_btr_schemes(schemes, db)

        logger.info("ingest_bpf_btr_pipeline_completed", **result)
        return {"status": "success", "schemes_fetched": len(schemes), **result}

    except Exception as exc:
        logger.exception("ingest_bpf_btr_pipeline_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 4. EPC New-Dwelling Scheme Discovery
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.data_source_tasks.ingest_epc_new_dwellings",
    max_retries=2,
    default_retry_delay=1800,
    acks_late=True,
    time_limit=7200,  # 2 hours — nationwide scan
)
def ingest_epc_new_dwellings(
    self,
    days_back: int = 365,
    min_cluster_size: int = 10,
) -> dict[str, Any]:
    """
    Discover new-build residential schemes from the EPC register.

    Queries the EPC API for all new-dwelling transactions in the given
    period, clusters by postcode, and saves clusters of 10+ units as
    schemes in existing_schemes.

    Every new dwelling in England & Wales requires an EPC, making this
    the most comprehensive free source for completed/near-completion
    private development schemes.

    Designed to run weekly via Celery Beat.
    """
    db = _get_db()
    try:
        logger.info(
            "ingest_epc_new_dwellings_started",
            days_back=days_back,
            min_cluster_size=min_cluster_size,
        )

        from app.scrapers.epc_new_dwelling_scraper import (
            EPCNewDwellingScraper,
            save_epc_discovered_schemes,
        )

        async def _discover():
            async with EPCNewDwellingScraper(
                days_back=days_back,
                min_cluster_size=min_cluster_size,
            ) as scraper:
                return await scraper.discover_schemes()

        schemes = _run_async(_discover())

        logger.info(
            "ingest_epc_new_dwellings_discovered",
            schemes_found=len(schemes),
            total_units=sum(s.get("num_units", 0) for s in schemes),
        )

        if not schemes:
            logger.info("ingest_epc_new_dwellings_no_schemes")
            return {
                "status": "success",
                "schemes_discovered": 0,
                "new": 0,
                "updated": 0,
            }

        result = save_epc_discovered_schemes(schemes, db)

        logger.info("ingest_epc_new_dwellings_completed", **result)
        return {
            "status": "success",
            "schemes_discovered": len(schemes),
            **result,
        }

    except Exception as exc:
        logger.exception("ingest_epc_new_dwellings_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 5. ARL/REalyse BTR Open & Operating List
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.data_source_tasks.ingest_arl_btr_schemes",
    max_retries=3,
    default_retry_delay=600,
    acks_late=True,
    time_limit=600,  # 10 min — single JS bundle download + parse
)
def ingest_arl_btr_schemes(self) -> dict[str, Any]:
    """
    Fetch BTR scheme data from the ARL/REalyse interactive map and save
    to existing_schemes with source="arl_btr_open_operating".

    The ARL publishes ~1,200 BTR schemes with rich metadata including
    developer, funder, operator, unit counts, tenure type, planning refs,
    and coordinates.  Data is embedded in a JS bundle and decoded via
    Node.js.

    Designed to run monthly via Celery Beat.
    """
    db = _get_db()
    try:
        logger.info("ingest_arl_btr_schemes_started")

        from app.scrapers.arl_btr_scraper import ARLBTRScraper, save_arl_btr_schemes

        async def _fetch():
            async with ARLBTRScraper() as scraper:
                return await scraper.fetch_and_normalise()

        schemes = _run_async(_fetch())

        logger.info(
            "ingest_arl_btr_schemes_fetched",
            schemes=len(schemes),
        )

        if not schemes:
            logger.info("ingest_arl_btr_schemes_no_data")
            return {
                "status": "success",
                "schemes_fetched": 0,
                "new": 0,
                "updated": 0,
            }

        result = save_arl_btr_schemes(schemes, db)

        logger.info("ingest_arl_btr_schemes_completed", **result)
        return {"status": "success", "schemes_fetched": len(schemes), **result}

    except Exception as exc:
        logger.exception("ingest_arl_btr_schemes_failed")
        raise self.retry(exc=exc)
    finally:
        db.close()
