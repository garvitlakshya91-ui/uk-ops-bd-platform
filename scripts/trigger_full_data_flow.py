#!/usr/bin/env python3
"""Trigger the complete scheme data extraction pipeline.

Runs all scrapers → ingestion → scoring → audit in sequence,
printing progress at every step. Designed to run inside the backend
Docker container:

    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py

Or selectively:

    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py --step contracts_finder
    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py --step find_a_tender
    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py --step rsh
    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py --step score
    docker exec uk-ops-bd-platform-backend-1 python scripts/trigger_full_data_flow.py --step audit
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import os
import time
import traceback
from datetime import datetime, timezone

# Ensure app is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from app.database import SessionLocal
from app.models.models import ExistingScheme, SchemeContract, SchemeChangeLog, Company


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(msg: str) -> None:
    print(f"\n{'='*65}")
    print(f"  {msg}")
    print(f"{'='*65}")


def _step(msg: str) -> None:
    print(f"\n  >> {msg}")


def _ok(msg: str) -> None:
    print(f"     OK  {msg}")


def _err(msg: str) -> None:
    print(f"     ERR {msg}")


def _summary(label: str, stats: dict) -> None:
    parts = [f"{k}={v}" for k, v in stats.items()]
    print(f"     {label}: {', '.join(parts)}")


# ---------------------------------------------------------------------------
# Step 1: Contracts Finder (REST API — most reliable source)
# ---------------------------------------------------------------------------

def run_contracts_finder() -> dict:
    _banner("STEP 1: Contracts Finder API (JSON / OCDS)")
    _step("Initialising ContractsFinderScraper...")

    from app.scrapers.contracts_finder import ContractsFinderScraper
    from app.scrapers.scheme_ingest import ingest_contracts_finder

    scraper = ContractsFinderScraper()

    _step("Searching for housing management contracts...")
    t0 = time.time()
    raw_results = asyncio.run(_run(scraper))
    elapsed = time.time() - t0
    _ok(f"Scraped {len(raw_results)} raw results in {elapsed:.1f}s")

    _step("Ingesting into database...")
    db = SessionLocal()
    try:
        stats = ingest_contracts_finder(raw_results, db)
        _summary("Contracts Finder", stats)
        return stats
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Step 2: Find a Tender (HTML scraping — supplementary source)
# ---------------------------------------------------------------------------

def run_find_a_tender() -> dict:
    _banner("STEP 2: Find a Tender (HTML scraping)")
    _step("Initialising FindATenderScraper...")

    from app.scrapers.find_a_tender import FindATenderScraper
    from app.scrapers.scheme_ingest import ingest_tender_contracts

    scraper = FindATenderScraper()

    _step("Searching for housing management tender notices...")
    t0 = time.time()
    raw_results = asyncio.run(_run(scraper))
    elapsed = time.time() - t0
    _ok(f"Found {len(raw_results)} search results in {elapsed:.1f}s")

    # Parse detail pages
    _step("Fetching detail pages for each notice...")
    parsed = []
    for i, raw in enumerate(raw_results):
        try:
            detail = asyncio.run(scraper.parse_application(raw))
            parsed.append(detail)
            if (i + 1) % 10 == 0:
                _ok(f"Parsed {i+1}/{len(raw_results)}")
        except Exception as exc:
            _err(f"Parse failed for {raw.get('notice_id', '?')}: {exc}")

    _ok(f"Successfully parsed {len(parsed)}/{len(raw_results)} notices")

    _step("Ingesting into database...")
    db = SessionLocal()
    try:
        stats = ingest_tender_contracts(parsed, db)
        _summary("Find a Tender", stats)
        return stats
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Step 3: RSH Regulatory Judgements
# ---------------------------------------------------------------------------

def run_rsh() -> dict:
    _banner("STEP 3: RSH Regulatory Judgements (GOV.UK)")
    _step("Initialising RSHScraper...")

    from app.scrapers.rsh_scraper import RSHScraper
    from app.scrapers.scheme_ingest import ingest_rsh_judgements

    scraper = RSHScraper()

    _step("Searching for regulatory judgement publications...")
    t0 = time.time()
    raw_results = asyncio.run(_run(scraper))
    elapsed = time.time() - t0
    _ok(f"Found {len(raw_results)} judgement listings in {elapsed:.1f}s")

    # Parse each judgement
    _step("Fetching judgement detail pages...")
    parsed = []
    for i, raw in enumerate(raw_results):
        try:
            detail = asyncio.run(scraper.parse_application(raw))
            parsed.append(detail)
            if (i + 1) % 10 == 0:
                _ok(f"Parsed {i+1}/{len(raw_results)}")
        except Exception as exc:
            _err(f"Parse failed for {raw.get('provider_name', '?')}: {exc}")

    _ok(f"Successfully parsed {len(parsed)}/{len(raw_results)} judgements")

    _step("Ingesting into database...")
    db = SessionLocal()
    try:
        stats = ingest_rsh_judgements(parsed, db)
        _summary("RSH Judgements", stats)
        return stats
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Step 4: Scoring & Analysis
# ---------------------------------------------------------------------------

def run_scoring() -> dict:
    _banner("STEP 4: BD Scoring & Contract Risk Analysis")

    from app.scoring.bd_scorer import BDScorer
    from app.scoring.scheme_analyzer import SchemeAnalyzer

    db = SessionLocal()
    try:
        schemes = db.query(ExistingScheme).all()
        _step(f"Scoring {len(schemes)} schemes...")

        scorer = BDScorer(db)
        analyzer = SchemeAnalyzer(db)

        scored = 0
        risk_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "minimal": 0, "unknown": 0}

        for scheme in schemes:
            try:
                bd_score = scorer.score_existing_scheme(scheme)
                risk = analyzer.analyze_contract_risk(scheme)
                risk_level = risk.get("risk_level", "unknown")
                risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
                scored += 1
            except Exception as exc:
                _err(f"Scoring failed for {scheme.name}: {exc}")

        _ok(f"Scored {scored}/{len(schemes)} schemes")
        _summary("Risk levels", risk_counts)
        return {"scored": scored, **risk_counts}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Step 5: Data Quality Audit
# ---------------------------------------------------------------------------

def run_audit() -> dict:
    _banner("STEP 5: Data Quality Audit")

    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        from datetime import timedelta
        ninety_days_ago = now - timedelta(days=90)

        total = db.query(ExistingScheme).count()
        _step(f"Auditing {total} scheme records...")

        never_verified = db.query(ExistingScheme).filter(ExistingScheme.last_verified_at.is_(None)).count()
        missing_operator = db.query(ExistingScheme).filter(ExistingScheme.operator_company_id.is_(None)).count()
        missing_owner = db.query(ExistingScheme).filter(ExistingScheme.owner_company_id.is_(None)).count()
        missing_contract = db.query(ExistingScheme).filter(ExistingScheme.contract_end_date.is_(None)).count()
        missing_postcode = db.query(ExistingScheme).filter(ExistingScheme.postcode.is_(None)).count()

        contracts = db.query(SchemeContract).count()
        change_logs = db.query(SchemeChangeLog).count()
        companies = db.query(Company).count()

        stats = {
            "total_schemes": total,
            "never_verified": never_verified,
            "missing_operator": missing_operator,
            "missing_owner": missing_owner,
            "missing_contract_end": missing_contract,
            "missing_postcode": missing_postcode,
            "total_contracts": contracts,
            "total_change_logs": change_logs,
            "total_companies": companies,
        }

        _ok(f"Total schemes:        {total}")
        _ok(f"Never verified:       {never_verified}")
        _ok(f"Missing operator:     {missing_operator}")
        _ok(f"Missing owner:        {missing_owner}")
        _ok(f"Missing contract end: {missing_contract}")
        _ok(f"Missing postcode:     {missing_postcode}")
        _ok(f"SchemeContract rows:  {contracts}")
        _ok(f"SchemeChangeLog rows: {change_logs}")
        _ok(f"Companies:            {companies}")

        return stats
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Async helper
# ---------------------------------------------------------------------------

async def _run(scraper):
    """Execute a scraper inside its async context manager."""
    async with scraper:
        return await scraper.run()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_STEPS = {
    "contracts_finder": run_contracts_finder,
    "find_a_tender": run_find_a_tender,
    "rsh": run_rsh,
    "score": run_scoring,
    "audit": run_audit,
}


def main():
    parser = argparse.ArgumentParser(
        description="Trigger the full scheme data extraction pipeline",
    )
    parser.add_argument(
        "--step",
        choices=list(ALL_STEPS.keys()),
        help="Run only a specific step",
    )
    args = parser.parse_args()

    _banner("UK OPS BD PLATFORM  -  FULL DATA FLOW TRIGGER")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")

    if args.step:
        steps = {args.step: ALL_STEPS[args.step]}
    else:
        steps = ALL_STEPS

    results = {}
    for name, fn in steps.items():
        try:
            results[name] = fn()
        except Exception as exc:
            _err(f"STEP FAILED: {name}")
            _err(traceback.format_exc()[:500])
            results[name] = {"error": str(exc)}

    # Final summary
    _banner("PIPELINE COMPLETE")
    print(f"  Finished: {datetime.now(timezone.utc).isoformat()}")
    for step_name, stats in results.items():
        if "error" in stats:
            print(f"  {step_name}: FAILED - {stats['error'][:80]}")
        else:
            parts = [f"{k}={v}" for k, v in stats.items()]
            print(f"  {step_name}: {', '.join(parts)}")
    print()


if __name__ == "__main__":
    main()
