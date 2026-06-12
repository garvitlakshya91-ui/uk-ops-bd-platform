"""Scrape iQ Student Accommodation only and persist."""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.scrapers.pbsa_scraper import PBSAScraper, save_pbsa_schemes


async def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting iQ scrape...")
    async with PBSAScraper(rate_limit_seconds=1.0) as scraper:
        scraped = await scraper.scrape_all(["iq_student"])
    counts = {k: len(v) for k, v in scraped.items()}
    print(f"[{time.strftime('%H:%M:%S')}] Scrape complete. Per-operator: {counts}")

    db = SessionLocal()
    try:
        stats = save_pbsa_schemes(scraped, db)
        print(f"[{time.strftime('%H:%M:%S')}] Persist stats: {stats}")
    finally:
        db.close()
    print(f"[{time.strftime('%H:%M:%S')}] Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
