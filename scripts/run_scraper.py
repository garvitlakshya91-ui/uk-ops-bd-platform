#!/usr/bin/env python3
"""CLI script to manually run scrapers.

Usage:
    python scripts/run_scraper.py --all
    python scripts/run_scraper.py --council "Manchester City Council"
    python scripts/run_scraper.py --source planning_data_api
    python scripts/run_scraper.py --council "London Borough of Tower Hamlets" --source idox_scraper
"""

import argparse
import os
import sys
import time
from datetime import datetime

# Allow running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models.council import Council
from app.models.scraper_run import ScraperRun

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/uk_ops_bd",
)

VALID_SOURCES = ["idox_scraper", "planning_data_api", "find_a_tender", "epc"]


def get_councils(session: Session, council_name: str | None = None) -> list[Council]:
    """Retrieve councils to scrape."""
    stmt = select(Council).where(Council.is_active == True)  # noqa: E712
    if council_name:
        stmt = stmt.where(Council.name == council_name)
    result = session.execute(stmt).scalars().all()
    return list(result)


def run_council_scraper(
    session: Session,
    council: Council,
    source: str,
) -> ScraperRun:
    """Run a scraper for a specific council and source.

    In a full implementation this would invoke the actual scraper logic
    (Playwright-based portal scraping, API calls, etc.). For now it
    creates a ScraperRun record and simulates progress output.
    """
    print(f"\n  [{source}] Starting scrape for: {council.name}")
    print(f"    Portal: {council.portal_url or 'N/A'}")
    print(f"    Portal type: {council.portal_type or 'Unknown'}")

    run = ScraperRun(
        council_id=council.id,
        source=source,
        status="running",
        started_at=datetime.utcnow(),
        records_found=0,
        records_created=0,
        records_updated=0,
        records_skipped=0,
        errors_count=0,
    )
    session.add(run)
    session.flush()

    start_time = time.time()

    try:
        # ----- Scraper dispatch -----
        if source == "idox_scraper":
            _run_idox_scraper(council, run)
        elif source == "planning_data_api":
            _run_planning_data_api_scraper(council, run)
        elif source == "find_a_tender":
            _run_find_a_tender_scraper(council, run)
        elif source == "epc":
            _run_epc_scraper(council, run)
        else:
            raise ValueError(f"Unknown source: {source}")

        run.status = "completed"
        run.completed_at = datetime.utcnow()
        run.duration_seconds = time.time() - start_time

        council.last_scraped_at = datetime.utcnow()

        print(f"    Results: found={run.records_found}, created={run.records_created}, "
              f"updated={run.records_updated}, skipped={run.records_skipped}")
        print(f"    Duration: {run.duration_seconds:.1f}s")
        print(f"    Status: COMPLETED")

    except Exception as e:
        run.status = "failed"
        run.completed_at = datetime.utcnow()
        run.duration_seconds = time.time() - start_time
        run.errors_count = 1
        run.error_details = {
            "errors": [
                {
                    "type": type(e).__name__,
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ]
        }
        print(f"    ERROR: {e}")
        print(f"    Status: FAILED")

    session.flush()
    return run


def _run_idox_scraper(council: Council, run: ScraperRun) -> None:
    """Run Idox planning portal scraper.

    In production, this would use Playwright to navigate the Idox portal,
    search for recent applications, and extract planning application data.
    """
    if not council.portal_url:
        raise ValueError(f"No portal URL configured for {council.name}")

    print(f"    Connecting to Idox portal: {council.portal_url}")
    print("    Searching for BTR/PBSA applications from last 30 days...")

    # Simulate scraper results
    # In production, this would be replaced with actual Playwright scraping logic:
    #   1. Launch browser and navigate to portal_url
    #   2. Set search filters (date range, application type)
    #   3. Paginate through results
    #   4. Extract application details
    #   5. Match against BTR/PBSA keywords
    #   6. Upsert into database
    print("    [STUB] Scraper not yet implemented - recording dry run")
    run.records_found = 0
    run.records_created = 0
    run.records_updated = 0
    run.records_skipped = 0


def _run_planning_data_api_scraper(council: Council, run: ScraperRun) -> None:
    """Run Planning Data API scraper.

    Uses the Planning Data API (https://www.planning.data.gov.uk/api/v1)
    to fetch planning application data.
    """
    api_url = os.getenv(
        "PLANNING_DATA_API_URL",
        "https://www.planning.data.gov.uk/api/v1",
    )
    print(f"    Querying Planning Data API: {api_url}")
    print(f"    Filtering for council: {council.name}")

    # In production:
    #   1. Query /planning-application endpoint with council filter
    #   2. Filter for residential applications > 50 units
    #   3. Score relevance for BTR/PBSA
    #   4. Upsert into database
    print("    [STUB] API scraper not yet implemented - recording dry run")
    run.records_found = 0
    run.records_created = 0
    run.records_updated = 0
    run.records_skipped = 0


def _run_find_a_tender_scraper(council: Council, run: ScraperRun) -> None:
    """Run Find a Tender (FTS) scraper.

    Searches the UK Find a Tender service for public sector housing
    management contracts and procurement opportunities.
    """
    print("    Querying Find a Tender service...")
    print("    Searching CPV codes: 70330000 (Property management), 70332000 (Facilities management)")

    # In production:
    #   1. Query FTS API for relevant CPV codes
    #   2. Filter by council area / region
    #   3. Extract opportunity details
    #   4. Create pipeline opportunities
    print("    [STUB] FTS scraper not yet implemented - recording dry run")
    run.records_found = 0
    run.records_created = 0
    run.records_updated = 0
    run.records_skipped = 0


def _run_epc_scraper(council: Council, run: ScraperRun) -> None:
    """Run EPC (Energy Performance Certificate) data scraper.

    Fetches EPC data from the Open EPC API to enrich property records
    with energy efficiency ratings.
    """
    print("    Querying Open EPC API...")
    print(f"    Filtering for council area: {council.name}")

    # In production:
    #   1. Query domestic EPC API by local authority
    #   2. Match against known schemes by address/postcode
    #   3. Update scheme records with EPC ratings
    print("    [STUB] EPC scraper not yet implemented - recording dry run")
    run.records_found = 0
    run.records_created = 0
    run.records_updated = 0
    run.records_skipped = 0


def run_global_source(session: Session, source: str) -> list[ScraperRun]:
    """Run a global (non-council-specific) scraper source."""
    print(f"\n  [{source}] Starting global scrape...")

    run = ScraperRun(
        source=source,
        status="running",
        started_at=datetime.utcnow(),
        records_found=0,
        records_created=0,
        records_updated=0,
        records_skipped=0,
        errors_count=0,
    )
    session.add(run)
    session.flush()

    start_time = time.time()

    try:
        if source == "planning_data_api":
            print("    Querying Planning Data API for all regions...")
            print("    [STUB] Global API scraper not yet implemented")
        elif source == "find_a_tender":
            print("    Querying Find a Tender for all housing management contracts...")
            print("    [STUB] Global FTS scraper not yet implemented")
        elif source == "epc":
            print("    Querying Open EPC API for bulk data...")
            print("    [STUB] Global EPC scraper not yet implemented")

        run.status = "completed"
        run.completed_at = datetime.utcnow()
        run.duration_seconds = time.time() - start_time
        print(f"    Duration: {run.duration_seconds:.1f}s")
        print(f"    Status: COMPLETED")

    except Exception as e:
        run.status = "failed"
        run.completed_at = datetime.utcnow()
        run.duration_seconds = time.time() - start_time
        run.errors_count = 1
        run.error_details = {
            "errors": [
                {
                    "type": type(e).__name__,
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ]
        }
        print(f"    ERROR: {e}")
        print(f"    Status: FAILED")

    session.flush()
    return [run]


def main():
    parser = argparse.ArgumentParser(
        description="UK Ops BD Platform - Manual Scraper Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                                    Run all scrapers for all active councils
  %(prog)s --council "Manchester City Council"       Run all scrapers for Manchester
  %(prog)s --source planning_data_api               Run Planning Data API scraper globally
  %(prog)s --council "Leeds City Council" --source idox_scraper
                                                     Run Idox scraper for Leeds only
        """,
    )
    parser.add_argument(
        "--council",
        type=str,
        help="Name of a specific council to scrape",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run scrapers for all active councils",
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=VALID_SOURCES,
        help="Specific scraper source to run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be scraped without running",
    )

    args = parser.parse_args()

    if not args.all and not args.council and not args.source:
        parser.error("Specify --all, --council, or --source (or a combination)")

    print("=" * 60)
    print("UK Ops BD Platform - Scraper Runner")
    print("=" * 60)
    print(f"  Started at: {datetime.utcnow().isoformat()}")
    print(f"  Database:   {DATABASE_URL}")

    engine = create_engine(DATABASE_URL, echo=False)
    total_runs = []

    with Session(engine) as session:
        try:
            # Determine sources to run
            sources = [args.source] if args.source else VALID_SOURCES

            # Determine if we need council-specific scraping
            if args.all or args.council:
                councils = get_councils(session, args.council)

                if not councils:
                    if args.council:
                        print(f"\n  ERROR: Council not found: {args.council}")
                        print("  Use an exact council name from the database.")
                        sys.exit(1)
                    else:
                        print("\n  WARNING: No active councils found in database.")
                        sys.exit(0)

                print(f"\n  Councils to scrape: {len(councils)}")
                print(f"  Sources: {', '.join(sources)}")

                if args.dry_run:
                    print("\n  DRY RUN - would scrape:")
                    for council in councils:
                        for source in sources:
                            print(f"    - {council.name} [{source}]")
                    sys.exit(0)

                # Council-specific sources (idox_scraper)
                council_sources = [s for s in sources if s == "idox_scraper"]
                # Global sources that can be filtered by council
                filterable_sources = [s for s in sources if s != "idox_scraper"]

                for council in councils:
                    print(f"\n{'—' * 50}")
                    print(f"  Council: {council.name}")
                    print(f"{'—' * 50}")

                    for source in council_sources:
                        run = run_council_scraper(session, council, source)
                        total_runs.append(run)

                    for source in filterable_sources:
                        run = run_council_scraper(session, council, source)
                        total_runs.append(run)

            else:
                # Source-only mode (global scrape)
                print(f"\n  Running global scrape for source: {args.source}")

                if args.dry_run:
                    print(f"\n  DRY RUN - would run global {args.source} scraper")
                    sys.exit(0)

                runs = run_global_source(session, args.source)
                total_runs.extend(runs)

            session.commit()

        except Exception as e:
            session.rollback()
            print(f"\n  FATAL ERROR: {e}")
            sys.exit(1)

    # Summary
    completed = sum(1 for r in total_runs if r.status == "completed")
    failed = sum(1 for r in total_runs if r.status == "failed")
    total_found = sum(r.records_found or 0 for r in total_runs)
    total_created = sum(r.records_created or 0 for r in total_runs)
    total_updated = sum(r.records_updated or 0 for r in total_runs)

    print(f"\n{'=' * 60}")
    print("  SCRAPER RUN SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total runs:      {len(total_runs)}")
    print(f"  Completed:       {completed}")
    print(f"  Failed:          {failed}")
    print(f"  Records found:   {total_found}")
    print(f"  Records created: {total_created}")
    print(f"  Records updated: {total_updated}")
    print(f"  Finished at:     {datetime.utcnow().isoformat()}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
