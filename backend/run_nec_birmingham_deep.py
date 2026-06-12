"""Deep Birmingham NEC crawl — 18-month backlog.

The default ``run_nec_birmingham.py`` uses 180-day lookback. We currently
have only 65 Birmingham apps from the NEC source and 1,544 metadata-thin
PlanIt rows. Expanding the lookback to 540 days should recover ~1,500-
2,000 detail-page-rich applications, unblocking the BTR classifier.

Runs against Birmingham only, ignoring the other NEC councils. Must run
inside the backend container so playwright/chromium are available:

    docker exec uk-ops-bd-platform-backend-1 python /app/run_nec_birmingham_deep.py
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@host.docker.internal:5432/uk_ops_bd",
)

from app.database import SessionLocal
from app.scrapers.nec_scraper import NECCouncilConfig, NECScraper
from app.tasks.scraping_tasks import _save_planning_applications


COUNCIL_ID = 18
COUNCIL_NAME = "Birmingham"
BASE_URL = "https://eplanning.birmingham.gov.uk"
SEARCH_PATH = "/Northgate/PlanningExplorer/GeneralSearch.aspx"
LOOKBACK_DAYS = 540  # 18 months


async def main() -> None:
    started = datetime.datetime.now(datetime.timezone.utc)
    print(
        f"=== Birmingham deep NEC crawl ===\n"
        f"  council_id={COUNCIL_ID}\n"
        f"  base_url={BASE_URL}\n"
        f"  lookback_days={LOOKBACK_DAYS} (18 months)\n",
        flush=True,
    )

    config = NECCouncilConfig(
        name=COUNCIL_NAME,
        council_id=COUNCIL_ID,
        base_url=BASE_URL,
        search_path=SEARCH_PATH,
    )
    scraper = NECScraper(config=config)
    try:
        async with scraper:
            results = await scraper.run(
                keyword_lookback_days=LOOKBACK_DAYS,
                date_range_lookback_days=LOOKBACK_DAYS,
            )
        print(f"\n  scraper returned {len(results)} raw rows", flush=True)
        if not results:
            print("  nothing to save.", flush=True)
            return
        db = SessionLocal()
        try:
            res = _save_planning_applications(db, COUNCIL_ID, results)
            print(f"  save result: {res}", flush=True)
        finally:
            db.close()
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED: {exc}", flush=True)
        traceback.print_exc()
        raise
    elapsed = (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds()
    print(f"\n=== DONE — elapsed {elapsed:.1f}s ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
