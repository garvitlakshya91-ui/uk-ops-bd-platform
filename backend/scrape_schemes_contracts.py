"""
Batch scrape schemes, contracts, and supplementary data sources.

Runs without Celery — invokes scrapers and ingestion functions directly.
Populates: existing_schemes, scheme_contracts, companies, planning_applications (brownfield).

Data sources (all public, no API keys needed):
1. Find a Tender — above-threshold government housing contracts
2. Contracts Finder — below-threshold housing contracts
3. RSH Registered Providers — social housing provider list
4. RSH SDR — stock data per provider
5. Brownfield Register — 38,000+ development sites

Usage:
    python scrape_schemes_contracts.py [--source fat|cf|rsh|brownfield|all]
"""

import asyncio
import os
import sys
import argparse
import traceback
import datetime

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal


async def run_find_a_tender(db):
    """Scrape Find a Tender and ingest housing contracts."""
    print("\n" + "=" * 60)
    print("FIND A TENDER — Housing management contracts")
    print("=" * 60)

    from app.scrapers.find_a_tender import FindATenderScraper
    from app.scrapers.scheme_ingest import ingest_tender_contracts

    scraper = FindATenderScraper()
    async with scraper:
        # run() calls search_applications() which pre-filters for housing
        # and calls _parse_release() on each result, then parse_application()
        # via BaseScraper.run() — returns already-parsed flat dicts
        results = await scraper.run()

    print(f"  Fetched {len(results)} housing contracts")

    if results:
        stats = ingest_tender_contracts(results, db)
        print(f"  Ingested: {stats}")
        return stats
    return {"schemes_created": 0, "contracts_created": 0}


async def run_contracts_finder(db):
    """Scrape Contracts Finder and ingest housing contracts."""
    print("\n" + "=" * 60)
    print("CONTRACTS FINDER — Below-threshold housing contracts")
    print("=" * 60)

    from app.scrapers.contracts_finder import ContractsFinderScraper
    from app.scrapers.scheme_ingest import ingest_contracts_finder

    scraper = ContractsFinderScraper()
    async with scraper:
        # CF run() returns already-parsed flat dicts (title, cpv_codes, etc. at top level)
        raw_results = await scraper.run()

    print(f"  Fetched {len(raw_results)} results")

    if raw_results:
        stats = ingest_contracts_finder(raw_results, db)
        print(f"  Ingested: {stats}")
        return stats
    return {"schemes_created": 0, "contracts_created": 0}


async def run_rsh(db):
    """Scrape RSH registered providers and SDR stock data."""
    print("\n" + "=" * 60)
    print("RSH — Registered Providers + Stock Data")
    print("=" * 60)

    from app.scrapers.rsh_registered_providers import RSHRegisteredProvidersScraper
    from app.scrapers.scheme_ingest import (
        ingest_rsh_registered_providers,
        ingest_rsh_sdr,
    )

    scraper = RSHRegisteredProvidersScraper()

    # 1. Registered Providers list
    print("  Fetching registered providers...")
    providers = await scraper.fetch_registered_providers()
    print(f"  Fetched {len(providers)} providers")

    if providers:
        rp_stats = ingest_rsh_registered_providers(providers, db)
        print(f"  RP ingested: {rp_stats}")

    # 2. SDR stock data
    print("  Fetching SDR stock data...")
    try:
        sdr_rows = await scraper.fetch_sdr_stock()
        print(f"  Fetched {len(sdr_rows)} SDR rows")

        if sdr_rows:
            sdr_stats = ingest_rsh_sdr(sdr_rows, db)
            print(f"  SDR ingested: {sdr_stats}")
    except Exception as exc:
        print(f"  SDR fetch failed (may not be available): {exc}")

    return {"providers": len(providers)}


async def run_brownfield(db):
    """Scrape the Brownfield Land Register."""
    print("\n" + "=" * 60)
    print("BROWNFIELD REGISTER — Development sites")
    print("=" * 60)

    from app.scrapers.brownfield_scraper import BrownfieldScraper
    from app.scrapers.scheme_ingest import ingest_brownfield_sites

    scraper = BrownfieldScraper()
    async with scraper:
        raw_results = await scraper.run()

    print(f"  Fetched {len(raw_results)} raw results")

    # BrownfieldScraper.run() already returns parsed data — no need to re-parse
    if raw_results:
        stats = ingest_brownfield_sites(raw_results, db)
        print(f"  Ingested: {stats}")
        return stats
    return {"created": 0}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["fat", "cf", "rsh", "brownfield", "all"],
        default="all",
    )
    args = parser.parse_args()

    db = SessionLocal()

    results = {}
    sources = {
        "fat": ("Find a Tender", run_find_a_tender),
        "cf": ("Contracts Finder", run_contracts_finder),
        "rsh": ("RSH Providers", run_rsh),
        "brownfield": ("Brownfield Register", run_brownfield),
    }

    to_run = sources.keys() if args.source == "all" else [args.source]

    for key in to_run:
        name, func = sources[key]
        try:
            result = await func(db)
            results[key] = {"status": "success", **result}
        except Exception as exc:
            print(f"\n  FAILED: {name}: {exc}")
            traceback.print_exc()
            results[key] = {"status": "failed", "error": str(exc)}

    db.close()

    print("\n" + "=" * 60)
    print("SCHEME/CONTRACT SCRAPE COMPLETE")
    print("=" * 60)
    for key, res in results.items():
        print(f"  {key}: {res}")


if __name__ == "__main__":
    asyncio.run(main())
