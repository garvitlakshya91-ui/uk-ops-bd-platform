"""
Re-run NEC (Northgate PlanningExplorer) scrapers for the 5 NEC councils.

This bypasses the celery `_load_scraper` plumbing (which has an unrelated
config-passing bug for the legacy ``NECScraper`` class-name path) and
directly instantiates ``NECScraper`` with a ``NECCouncilConfig`` for each
council, then saves via ``_save_planning_applications``.

Run inside the backend container so playwright/chromium are available:

    docker exec uk-ops-bd-platform-backend-1 python /app/run_nec_birmingham.py
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
import traceback

# Ensure imports work whether run from container (/app) or host (backend/).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@host.docker.internal:5432/uk_ops_bd",
)

from app.database import SessionLocal
from app.scrapers.nec_scraper import NECCouncilConfig, NECScraper
from app.tasks.scraping_tasks import _save_planning_applications

# (council_id, name, base_url, search_path) — IDs are the real DB IDs.
NEC_TARGETS: list[tuple[int, str, str, str]] = [
    (18, "Birmingham", "https://eplanning.birmingham.gov.uk",
     "/Northgate/PlanningExplorer/GeneralSearch.aspx"),
    (291, "Wandsworth", "https://planning.wandsworth.gov.uk",
     "/Northgate/PlanningExplorer/GeneralSearch.aspx"),
    (163, "Merton", "https://planning.merton.gov.uk",
     "/Northgate/PlanningExplorerAA/GeneralSearch.aspx"),
    (148, "Liverpool", "http://northgate.liverpool.gov.uk",
     "/PlanningExplorer/generalsearch.aspx"),
    (199, "Reading", "https://planning.reading.gov.uk",
     "/fastweb_PL/search.asp"),
]


async def scrape_one(
    council_id: int, name: str, base_url: str, search_path: str
) -> dict:
    """Run NECScraper for a single council and persist results."""
    config = NECCouncilConfig(
        name=name,
        council_id=council_id,
        base_url=base_url,
        search_path=search_path,
    )
    scraper = NECScraper(config=config)
    async with scraper:
        results = await scraper.run(date_range_lookback_days=180)
    if not results:
        return {"found": 0, "new": 0, "updated": 0, "errors": 0}
    db = SessionLocal()
    try:
        return _save_planning_applications(db, council_id, results)
    finally:
        db.close()


async def main() -> None:
    started = datetime.datetime.now(datetime.timezone.utc)
    summary: dict[str, dict] = {}
    for cid, name, base_url, search_path in NEC_TARGETS:
        print(f"\n=== {name} (id={cid}) ===", flush=True)
        try:
            res = await scrape_one(cid, name, base_url, search_path)
            print(f"  result: {res}", flush=True)
            summary[name] = res
        except Exception as exc:  # noqa: BLE001
            print(f"  FAILED: {exc}", flush=True)
            traceback.print_exc()
            summary[name] = {"error": str(exc)}
    elapsed = (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds()
    print(f"\n=== SUMMARY (elapsed {elapsed:.1f}s) ===", flush=True)
    for name, res in summary.items():
        print(f"  {name}: {res}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
