"""
Full-run PBSA operator scrape — all 13 operators, persist to DB, run CCOD enrichment.

Usage:
    python scripts/run_pbsa_scrape.py
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.scrapers.pbsa_scraper import (
    PBSAScraper,
    OPERATOR_CONFIGS,
    save_pbsa_schemes,
)


async def main() -> None:
    start = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting PBSA scrape for {len(OPERATOR_CONFIGS)} operators...")

    async with PBSAScraper(rate_limit_seconds=1.5) as scraper:
        scraped = await scraper.scrape_all()

    per_operator = {k: len(v) for k, v in scraped.items()}
    total = sum(per_operator.values())
    print(f"[{time.strftime('%H:%M:%S')}] Scrape complete. Per-operator counts:")
    for k, n in per_operator.items():
        print(f"  {k:30s}  {n:>5}")
    print(f"  {'TOTAL':30s}  {total:>5}")

    # Persist
    print(f"[{time.strftime('%H:%M:%S')}] Persisting to database...")
    db = SessionLocal()
    try:
        stats = save_pbsa_schemes(scraped, db)
        print(f"[{time.strftime('%H:%M:%S')}] Persist stats: {stats}")

        # CCOD enrichment
        if stats.get("new", 0) > 0:
            print(f"[{time.strftime('%H:%M:%S')}] Running CCOD enrichment to attach owners...")
            try:
                from app.tasks.scheme_enrichment_pipeline import _enrich_from_ccod
                ccod = _enrich_from_ccod(db)
                print(f"[{time.strftime('%H:%M:%S')}] CCOD enrichment result: {ccod}")
            except Exception as exc:
                print(f"[{time.strftime('%H:%M:%S')}] CCOD enrichment failed: {exc}")
    finally:
        db.close()

    elapsed = time.time() - start
    print(f"[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s ({elapsed/60:.1f} min)")


if __name__ == "__main__":
    asyncio.run(main())
