"""Standalone test of the new Idox date-range fallback.

Runs against Manchester (council_id=153) without touching celery, so the
CH arrears task running there isn't disturbed.
"""
import asyncio, json, os, sys
from datetime import date, datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)

from sqlalchemy import create_engine, text
from app.scrapers.idox_scraper import IdoxScraper, IdoxCouncilConfig


async def main() -> int:
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
    target = sys.argv[1] if len(sys.argv) > 1 else "Manchester"
    with engine.connect() as c:
        row = c.execute(text(
            "SELECT id, name, portal_url FROM councils WHERE name=:n"
        ), {"n": target}).fetchone()
        if not row:
            print(f"Council not found: {target}")
            return 1
        cid, name, url = row
    print(f"Council: {name} (id={cid}) url={url}")

    config = IdoxCouncilConfig(
        name=name,
        council_id=cid,
        base_url=url,
    )

    use_pw = "--no-playwright" not in sys.argv
    print(f"  use_playwright={use_pw}")
    async with IdoxScraper(config=config, use_playwright=use_pw) as scraper:
        # Use last 60 days for a quick test
        date_from = date.today() - timedelta(days=60)
        date_to = date.today()
        print(f"Searching {date_from} -> {date_to} (date-range pass first, "
              f"keyword fallback if <5)...")
        try:
            results = await scraper.search_applications(
                date_from=date_from, date_to=date_to, max_pages=10,
            )
        except Exception as exc:
            print(f"FAILED: {exc}")
            import traceback; traceback.print_exc()
            return 1

    print(f"Returned: {len(results)} results")
    print()
    # Show first 10
    for r in results[:10]:
        ref = r.get("reference", "?")
        addr = (r.get("address") or "")[:50]
        desc = (r.get("description") or "")[:60]
        url = r.get("detail_url") or ""
        print(f"  {ref:<20s} {addr:<52s} {desc}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
