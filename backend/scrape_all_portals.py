"""
Batch scrape all portal councils (Idox + NEC + Civica + API).

Runs scrapes sequentially per council to respect rate limits. Each council
scrape searches for housing-related keywords and fetches detail pages for
applicant names.

Usage:
    python scrape_all_portals.py [--type idox|nec|civica|api|all] [--limit N] [--council-id ID]
"""

import asyncio
import os
import sys
import argparse
import datetime
import traceback

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from app.database import SessionLocal
from app.tasks.scraping_tasks import _save_planning_applications


KEYWORDS = [
    "build to rent",
    "BTR",
    "student accommodation",
    "PBSA",
    "co-living",
    "retirement",
    "affordable housing",
    "residential",
    "flats",
    "apartments",
]

ALL_PORTAL_TYPES = ["idox", "nec", "civica", "api"]


async def scrape_idox_council(council_id: int, name: str, portal_url: str) -> dict:
    """Scrape a single Idox council."""
    from app.scrapers.idox_scraper import IdoxScraper, IdoxCouncilConfig

    config = IdoxCouncilConfig(
        name=name,
        council_id=council_id,
        base_url=portal_url,
    )
    scraper = IdoxScraper(config=config, use_playwright=True)

    async with scraper:
        results = await scraper.run(
            keywords=KEYWORDS,
            max_pages=3,
        )

    return {"results": results, "count": len(results)}


async def scrape_nec_council(council_id: int, name: str, portal_url: str) -> dict:
    """Scrape a single NEC/Northgate council."""
    from app.scrapers.nec_scraper import NECScraper, NECCouncilConfig

    config = NECCouncilConfig(
        name=name,
        council_id=council_id,
        base_url=portal_url,
    )
    scraper = NECScraper(config=config)

    async with scraper:
        results = await scraper.run(
            keywords=KEYWORDS,
            max_pages=3,
        )

    return {"results": results, "count": len(results)}


async def scrape_civica_council(council_id: int, name: str, portal_url: str) -> dict:
    """Scrape a single Civica/OcellaWeb council."""
    from app.scrapers.civica_scraper import CivicaScraper, CivicaCouncilConfig

    config = CivicaCouncilConfig(
        name=name,
        council_id=council_id,
        base_url=portal_url,
    )
    scraper = CivicaScraper(config=config)

    async with scraper:
        results = await scraper.run(
            keywords=KEYWORDS,
            max_pages=3,
        )

    return {"results": results, "count": len(results)}


async def scrape_api_council(council_id: int, name: str, organisation_entity: str | None) -> dict:
    """Scrape a single council via the Planning Data API."""
    from app.scrapers.planning_data_api import PlanningDataAPIScraper
    from datetime import date, timedelta

    scraper = PlanningDataAPIScraper(
        council_name=name,
        council_id=council_id,
    )

    # Search for recent applications (last 90 days)
    date_from = date.today() - timedelta(days=90)

    async with scraper:
        raw_results = await scraper.search_applications(
            date_from=date_from,
            lad_code=organisation_entity,
            max_pages=5,
        )

        # Parse raw API entities into our model field format
        parsed = []
        for raw in raw_results:
            try:
                detail = await scraper.parse_application(raw)
                parsed.append(detail)
            except Exception:
                pass  # skip unparseable

    return {"results": parsed, "count": len(parsed)}


def save_results(council_id: int, results: list) -> dict:
    """Persist scraped results to DB."""
    db = SessionLocal()
    try:
        return _save_planning_applications(db, council_id, results)
    finally:
        db.close()


def record_run(council_id: int, status: str, found: int, new: int, updated: int, errors: int, error_details=None):
    """Record a scraper run in the database."""
    db = SessionLocal()
    try:
        from app.models.models import ScraperRun, Council
        run = ScraperRun(
            council_id=council_id,
            status=status,
            applications_found=found,
            applications_new=new,
            applications_updated=updated,
            errors_count=errors,
        )
        if error_details:
            run.error_details = {"exception": str(error_details)}
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
        db.add(run)

        # Update council last_scraped_at
        council = db.query(Council).get(council_id)
        if council:
            council.last_scraped_at = datetime.datetime.now(datetime.timezone.utc)

        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["idox", "nec", "civica", "api", "all"], default="all")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--council-id", type=int, default=None)
    args = parser.parse_args()

    engine = create_engine(os.environ["DATABASE_URL"])

    with engine.connect() as conn:
        if args.council_id:
            councils = conn.execute(text(
                "SELECT id, name, portal_url, portal_type, organisation_entity "
                "FROM councils WHERE id = :id"
            ), {"id": args.council_id}).fetchall()
        else:
            if args.type == "all":
                types = ALL_PORTAL_TYPES
            else:
                types = [args.type]

            councils = conn.execute(text(
                "SELECT id, name, portal_url, portal_type, organisation_entity "
                "FROM councils "
                "WHERE portal_type = ANY(:types) AND active = true ORDER BY portal_type, id"
            ), {"types": types}).fetchall()

    if args.limit:
        councils = councils[:args.limit]

    print(f"\n{'='*60}")
    print(f"Scraping {len(councils)} councils")
    portal_counts = {}
    for _, _, _, ptype, _ in councils:
        portal_counts[ptype] = portal_counts.get(ptype, 0) + 1
    for ptype, count in sorted(portal_counts.items()):
        print(f"  {ptype}: {count}")
    print(f"{'='*60}\n")

    total_apps = 0
    total_new = 0
    success = 0
    failed = 0
    skipped = 0

    for i, (cid, name, url, ptype, org_entity) in enumerate(councils, 1):
        print(f"[{i}/{len(councils)}] {name} ({ptype}, id={cid})...")

        try:
            if ptype == "idox":
                result = await scrape_idox_council(cid, name, url)
            elif ptype == "nec":
                result = await scrape_nec_council(cid, name, url)
            elif ptype == "civica":
                result = await scrape_civica_council(cid, name, url)
            elif ptype == "api":
                result = await scrape_api_council(cid, name, org_entity)
            else:
                print(f"  Skipping unsupported type: {ptype}")
                skipped += 1
                continue

            apps = result["results"]
            count = result["count"]
            print(f"  Scraped {count} applications")

            if apps:
                save = save_results(cid, apps)
                print(f"  Saved: {save}")
                total_apps += save.get("found", 0)
                total_new += save.get("new", 0)
                record_run(cid, "success", save.get("found", 0), save.get("new", 0), save.get("updated", 0), save.get("errors", 0))
            else:
                record_run(cid, "success", 0, 0, 0, 0)

            success += 1

        except Exception as exc:
            print(f"  FAILED: {exc}")
            traceback.print_exc()
            record_run(cid, "failed", 0, 0, 0, 1, error_details=exc)
            failed += 1

    print(f"\n{'='*60}")
    print(f"COMPLETE: {success} succeeded, {failed} failed, {skipped} skipped")
    print(f"Total applications: {total_apps} ({total_new} new)")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
