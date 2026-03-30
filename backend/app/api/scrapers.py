import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.models import Council, ScraperRun
from app.api.auth import require_role
from app.models.user import User

router = APIRouter(prefix="/api/scrapers", tags=["Scrapers"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class CouncilBase(BaseModel):
    name: str
    portal_type: str
    portal_url: Optional[str] = None
    scraper_class: Optional[str] = None
    active: bool = True
    region: Optional[str] = None
    scrape_frequency_hours: int = 24


class CouncilCreate(CouncilBase):
    pass


class CouncilUpdate(BaseModel):
    name: Optional[str] = None
    portal_type: Optional[str] = None
    portal_url: Optional[str] = None
    scraper_class: Optional[str] = None
    active: Optional[bool] = None
    region: Optional[str] = None
    scrape_frequency_hours: Optional[int] = None


class CouncilResponse(CouncilBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    last_scraped_at: Optional[datetime.datetime] = None


class CouncilListResponse(BaseModel):
    items: list[CouncilResponse]
    total: int
    skip: int
    limit: int


class ScraperRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    council_id: int
    started_at: datetime.datetime
    completed_at: Optional[datetime.datetime] = None
    status: str
    applications_found: int
    applications_new: int
    applications_updated: int
    errors_count: int
    error_details: Optional[dict] = None
    duration_seconds: Optional[float] = None


class ScraperRunListResponse(BaseModel):
    items: list[ScraperRunResponse]
    total: int
    skip: int
    limit: int


class TriggerScrapeRequest(BaseModel):
    council_id: int


class TriggerScrapeResponse(BaseModel):
    message: str
    council_id: int
    council_name: str
    scraper_run_id: int


class ScraperHealthMetric(BaseModel):
    council_id: int
    council_name: str
    portal_type: str
    region: Optional[str] = None
    active: bool
    last_scraped_at: Optional[datetime.datetime] = None
    scrape_frequency_hours: int
    hours_since_last_scrape: Optional[float] = None
    is_overdue: bool
    last_run_status: Optional[str] = None
    recent_success_rate: Optional[float] = None
    avg_duration_seconds: Optional[float] = None
    total_runs_last_7_days: int


class ScraperHealthResponse(BaseModel):
    total_councils: int
    active_councils: int
    overdue_scrapers: int
    failed_last_24h: int
    metrics: list[ScraperHealthMetric]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/councils", response_model=CouncilListResponse)
def list_councils(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    active: Optional[bool] = None,
    region: Optional[str] = None,
    portal_type: Optional[str] = None,
    search: Optional[str] = None,
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    query = db.query(Council)

    if active is not None:
        query = query.filter(Council.active == active)
    if region is not None:
        query = query.filter(Council.region == region)
    if portal_type is not None:
        query = query.filter(Council.portal_type == portal_type)
    if search is not None:
        query = query.filter(Council.name.ilike(f"%{search}%"))

    total = query.count()
    items = query.order_by(Council.name).offset(skip).limit(limit).all()

    return CouncilListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/councils/{council_id}", response_model=CouncilResponse)
def get_council(council_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")
    return council


@router.post("/councils", response_model=CouncilResponse, status_code=status.HTTP_201_CREATED)
def create_council(data: CouncilCreate, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    existing = db.query(Council).filter(Council.name == data.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Council with this name already exists")

    council = Council(**data.model_dump())
    db.add(council)
    db.commit()
    db.refresh(council)
    return council


@router.put("/councils/{council_id}", response_model=CouncilResponse)
def update_council(
    council_id: int,
    data: CouncilUpdate,
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(council, field, value)

    db.commit()
    db.refresh(council)
    return council


@router.delete("/councils/{council_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_council(council_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")
    db.delete(council)
    db.commit()


class SeedCouncilsResponse(BaseModel):
    message: str
    inserted: int


@router.post("/seed-councils", response_model=SeedCouncilsResponse)
def seed_councils(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Populate councils table from built-in scraper council lists."""
    from app.scrapers.orchestrator import ScraperOrchestrator

    orchestrator = ScraperOrchestrator(db_session=db)
    inserted = orchestrator.seed_councils()
    return SeedCouncilsResponse(
        message=f"Seeded {inserted} councils",
        inserted=inserted,
    )


@router.post("/trigger", response_model=TriggerScrapeResponse)
def trigger_scrape(data: TriggerScrapeRequest, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Trigger a scraper run for a specific council via Celery."""
    council = db.query(Council).filter(Council.id == data.council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")
    if not council.active:
        raise HTTPException(status_code=400, detail="Council scraper is not active")

    # Check for already-running scrape
    running = (
        db.query(ScraperRun)
        .filter(
            ScraperRun.council_id == council.id,
            ScraperRun.status == "running",
        )
        .first()
    )
    if running:
        raise HTTPException(
            status_code=409,
            detail="A scrape is already running for this council",
        )

    run = ScraperRun(council_id=council.id, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    # Dispatch Celery task
    from app.tasks.scraping_tasks import scrape_council
    scrape_council.delay(council.id)

    return TriggerScrapeResponse(
        message="Scraper run initiated",
        council_id=council.id,
        council_name=council.name,
        scraper_run_id=run.id,
    )


class TriggerAllResponse(BaseModel):
    message: str
    scheduled: int
    council_ids: list[int]


@router.post("/trigger-all", response_model=TriggerAllResponse)
def trigger_all_scrapers(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Trigger scrapers for all active councils via Celery."""
    from app.tasks.scraping_tasks import scrape_all_councils

    active = db.query(Council).filter(Council.active.is_(True)).all()
    if not active:
        raise HTTPException(status_code=404, detail="No active councils found")

    result = scrape_all_councils.delay()

    return TriggerAllResponse(
        message=f"Scheduled scraping for {len(active)} councils",
        scheduled=len(active),
        council_ids=[c.id for c in active],
    )


@router.post("/scrape-direct/{council_id}")
async def scrape_direct(council_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Run a scraper directly (async, no Celery) for testing."""
    from app.scrapers.orchestrator import ScraperOrchestrator

    council = db.query(Council).filter(Council.id == council_id).first()
    if not council:
        raise HTTPException(status_code=404, detail="Council not found")

    orchestrator = ScraperOrchestrator(db_session=db)
    try:
        metrics = await orchestrator.run_council(council)
        return {
            "status": "success" if not metrics.errors else "partial",
            "council": council.name,
            "applications_found": metrics.applications_found,
            "applications_new": metrics.applications_new,
            "applications_updated": metrics.applications_updated,
            "errors": metrics.errors,
            "elapsed_seconds": metrics.elapsed_seconds,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "council": council.name,
            "error": str(exc),
        }


@router.post("/scrape-planning-api")
async def scrape_planning_api(
    limit: int = Query(500, ge=1, le=10000),
    days: int = Query(90, ge=1, le=365),
    start_offset: int = Query(0, ge=0),
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Run the Planning Data API scraper directly (bypasses Celery).
    Use start_offset to skip already-scraped records and get different councils."""
    from app.scrapers.planning_data_api import PlanningDataAPIScraper
    from app.scrapers.orchestrator import ScraperOrchestrator
    from datetime import date, timedelta

    scraper = PlanningDataAPIScraper()
    orchestrator = ScraperOrchestrator(db_session=db)

    try:
        async with scraper:
            applications = await scraper.run(
                date_from=date.today() - timedelta(days=days),
                date_to=date.today(),
                max_pages=limit // 100 + 1,
                start_offset=start_offset,
            )

        from app.models.models import PlanningApplication

        # Build org-entity → council_id lookup
        council_lookup = {}
        councils = db.query(Council).filter(Council.organisation_entity.isnot(None)).all()
        for c in councils:
            council_lookup[c.organisation_entity] = c.id

        new_count = 0
        updated_count = 0
        skipped_count = 0
        for app_data in applications:
            ref = app_data.get("reference", "")
            if not ref:
                continue

            # Resolve council_id from organisation_entity
            org_entity = app_data.get("organisation_entity", "")
            council_id = council_lookup.get(org_entity)
            if not council_id:
                skipped_count += 1
                continue

            existing = (
                db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == ref,
                    PlanningApplication.council_id == council_id,
                )
                .first()
            )
            if existing:
                updated_count += 1
            else:
                app_obj = PlanningApplication(
                    reference=ref,
                    council_id=council_id,
                    address=app_data.get("address"),
                    postcode=app_data.get("postcode"),
                    description=app_data.get("description"),
                    applicant_name=app_data.get("applicant_name"),
                    agent_name=app_data.get("agent_name"),
                    application_type=app_data.get("application_type"),
                    status=app_data.get("status", "Unknown"),
                    scheme_type=app_data.get("scheme_type", "Unknown"),
                    num_units=app_data.get("num_units"),
                    submission_date=app_data.get("submission_date"),
                    decision_date=app_data.get("decision_date"),
                    documents_url=app_data.get("documents_url"),
                    latitude=app_data.get("latitude"),
                    longitude=app_data.get("longitude"),
                )
                db.add(app_obj)
                new_count += 1

        db.commit()

        return {
            "status": "success",
            "applications_found": len(applications),
            "applications_new": new_count,
            "applications_updated": updated_count,
            "applications_skipped_unmapped": skipped_count,
            "councils_mapped": len(council_lookup),
            "elapsed_seconds": scraper.metrics.elapsed_seconds,
            "errors": scraper.metrics.errors,
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


@router.post("/scrape-brownfield")
async def scrape_brownfield(
    min_dwellings: int = Query(5, ge=1),
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    """Scrape brownfield land register — nationwide development sites with addresses,
    unit counts, and coordinates across 190+ councils.
    Filter by min_dwellings (default 5) for BD relevance."""
    from app.scrapers.brownfield_scraper import BrownfieldScraper
    from app.models.models import PlanningApplication

    scraper = BrownfieldScraper(min_dwellings=min_dwellings)

    try:
        async with scraper:
            sites = await scraper.run()

        # Build org-entity → council_id lookup
        council_lookup = {}
        councils = db.query(Council).filter(Council.organisation_entity.isnot(None)).all()
        for c in councils:
            council_lookup[c.organisation_entity] = c.id

        new_count = 0
        updated_count = 0
        skipped_unmapped = 0
        for site in sites:
            ref = site.get("reference", "")
            if not ref:
                continue

            org_entity = site.get("organisation_entity", "")
            council_id = council_lookup.get(org_entity)
            if not council_id:
                skipped_unmapped += 1
                continue

            ref = ref[:100]
            existing = (
                db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == ref,
                    PlanningApplication.council_id == council_id,
                )
                .first()
            )
            if existing:
                # Update with brownfield data if it has better fields
                changed = False
                if site.get("address") and not existing.address:
                    existing.address = site["address"]
                    changed = True
                if site.get("postcode") and not existing.postcode:
                    existing.postcode = site["postcode"]
                    changed = True
                if site.get("latitude") and not existing.latitude:
                    existing.latitude = site["latitude"]
                    existing.longitude = site["longitude"]
                    changed = True
                if site.get("num_units") and not existing.num_units:
                    existing.num_units = site["num_units"]
                    changed = True
                if changed:
                    updated_count += 1
            else:
                try:
                    app_obj = PlanningApplication(
                        reference=ref[:100],
                        council_id=council_id,
                        address=site.get("address"),
                        postcode=site.get("postcode"),
                        description=(site.get("description") or "")[:5000],
                        application_type=site.get("application_type"),
                        status=site.get("status", "Unknown"),
                        scheme_type=site.get("scheme_type", "Residential"),
                        num_units=site.get("num_units"),
                        submission_date=site.get("submission_date"),
                        decision_date=site.get("decision_date"),
                        latitude=site.get("latitude"),
                        longitude=site.get("longitude"),
                    )
                    db.add(app_obj)
                    db.flush()
                    new_count += 1
                except Exception:
                    db.rollback()
                    skipped_unmapped += 1

        db.commit()

        return {
            "status": "success",
            "source": "brownfield-land",
            "sites_found": len(sites),
            "applications_new": new_count,
            "applications_updated": updated_count,
            "skipped_unmapped": skipped_unmapped,
            "councils_mapped": len(council_lookup),
            "elapsed_seconds": scraper.metrics.elapsed_seconds,
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


@router.post("/scrape-tenders")
async def scrape_tenders(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Run Find a Tender scraper and ingest housing management contracts as schemes."""
    from app.scrapers.find_a_tender import FindATenderScraper
    from app.scrapers.scheme_ingest import ingest_tender_contracts

    scraper = FindATenderScraper()
    try:
        async with scraper:
            results = await scraper.run()

        counts = ingest_tender_contracts(results, db)
        return {
            "status": "success",
            "tenders_found": len(results),
            "schemes_created": counts["created"],
            "schemes_updated": counts["updated"],
            "skipped": counts["skipped"],
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


@router.post("/scrape-rsh")
async def scrape_rsh(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Run RSH scraper and ingest regulatory judgements for social housing providers."""
    from app.scrapers.rsh_scraper import RSHScraper
    from app.scrapers.scheme_ingest import ingest_rsh_judgements

    scraper = RSHScraper()
    try:
        async with scraper:
            results = await scraper.run()

        counts = ingest_rsh_judgements(results, db)
        return {
            "status": "success",
            "judgements_found": len(results),
            "schemes_created": counts["created"],
            "schemes_enriched": counts["enriched"],
            "skipped": counts["skipped"],
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


@router.post("/enrich-epc")
async def enrich_epc(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    """Enrich existing schemes with EPC ratings for schemes that have postcodes."""
    from app.scrapers.scheme_ingest import enrich_schemes_with_epc

    try:
        counts = await enrich_schemes_with_epc(db)
        return {
            "status": "success",
            "schemes_enriched": counts["enriched"],
            "total_with_postcode": counts["total_with_postcode"],
        }
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


@router.post("/trigger-enrichment")
def trigger_enrichment(current_user: User = Depends(require_role("admin"))):
    """Trigger company matching and enrichment for unlinked applications."""
    from app.tasks.enrichment_tasks import enrich_new_applications
    result = enrich_new_applications.delay()
    return {"message": "Enrichment task queued", "task_id": str(result.id)}


@router.post("/trigger-scoring")
def trigger_scoring(current_user: User = Depends(require_role("admin"))):
    """Trigger BD scoring recalculation and pipeline creation."""
    from app.tasks.scoring_tasks import recalculate_all_scores
    recalculate_all_scores.delay()
    return {"message": "Scoring tasks queued"}


@router.get("/runs", response_model=ScraperRunListResponse)
def list_scraper_runs(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    council_id: Optional[int] = None,
    run_status: Optional[str] = None,
    current_user: User = Depends(require_role("admin", "bd_manager")),
    db: Session = Depends(get_db),
):
    query = db.query(ScraperRun)

    if council_id is not None:
        query = query.filter(ScraperRun.council_id == council_id)
    if run_status is not None:
        query = query.filter(ScraperRun.status == run_status)

    total = query.count()
    items = query.order_by(ScraperRun.started_at.desc()).offset(skip).limit(limit).all()

    return ScraperRunListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/runs/{run_id}", response_model=ScraperRunResponse)
def get_scraper_run(run_id: int, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    run = db.query(ScraperRun).filter(ScraperRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Scraper run not found")
    return run


@router.get("/health", response_model=ScraperHealthResponse)
def scraper_health(current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    """Return health metrics for all scrapers."""
    now = datetime.datetime.now(datetime.timezone.utc)
    seven_days_ago = now - datetime.timedelta(days=7)
    twenty_four_hours_ago = now - datetime.timedelta(hours=24)

    councils = db.query(Council).order_by(Council.name).all()
    total_councils = len(councils)
    active_councils = sum(1 for c in councils if c.active)
    overdue_count = 0
    failed_24h = 0

    metrics: list[ScraperHealthMetric] = []

    for council in councils:
        # Hours since last scrape
        hours_since: Optional[float] = None
        is_overdue = False
        if council.last_scraped_at:
            delta = now - council.last_scraped_at
            hours_since = round(delta.total_seconds() / 3600, 1)
            is_overdue = council.active and hours_since > council.scrape_frequency_hours

        if is_overdue:
            overdue_count += 1

        # Latest run
        latest_run = (
            db.query(ScraperRun)
            .filter(ScraperRun.council_id == council.id)
            .order_by(ScraperRun.started_at.desc())
            .first()
        )

        # Recent runs (7 days)
        recent_runs = (
            db.query(ScraperRun)
            .filter(
                ScraperRun.council_id == council.id,
                ScraperRun.started_at >= seven_days_ago,
            )
            .all()
        )

        total_recent = len(recent_runs)
        success_count = sum(1 for r in recent_runs if r.status == "success")
        success_rate: Optional[float] = None
        if total_recent > 0:
            success_rate = round(success_count / total_recent * 100, 1)

        avg_duration: Optional[float] = None
        durations = [r.duration_seconds for r in recent_runs if r.duration_seconds is not None]
        if durations:
            avg_duration = round(sum(durations) / len(durations), 1)

        # Failed in last 24h
        failed_recent = (
            db.query(func.count(ScraperRun.id))
            .filter(
                ScraperRun.council_id == council.id,
                ScraperRun.started_at >= twenty_four_hours_ago,
                ScraperRun.status == "failed",
            )
            .scalar()
            or 0
        )
        failed_24h += failed_recent

        metrics.append(
            ScraperHealthMetric(
                council_id=council.id,
                council_name=council.name,
                portal_type=council.portal_type,
                region=council.region,
                active=council.active,
                last_scraped_at=council.last_scraped_at,
                scrape_frequency_hours=council.scrape_frequency_hours,
                hours_since_last_scrape=hours_since,
                is_overdue=is_overdue,
                last_run_status=latest_run.status if latest_run else None,
                recent_success_rate=success_rate,
                avg_duration_seconds=avg_duration,
                total_runs_last_7_days=total_recent,
            )
        )

    return ScraperHealthResponse(
        total_councils=total_councils,
        active_councils=active_councils,
        overdue_scrapers=overdue_count,
        failed_last_24h=failed_24h,
        metrics=metrics,
    )
