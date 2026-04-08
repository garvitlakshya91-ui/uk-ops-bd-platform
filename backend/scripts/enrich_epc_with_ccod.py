"""Enrich EPC new-dwelling schemes with HMLR CCOD owner company data.

Streams through the CCOD ZIP file, matches by postcode to existing_schemes
where source='epc_new_dwelling' and owner_company_id IS NULL, then creates
or looks up the proprietor Company and links it.
"""

import sys
import os

# Ensure we can import the app package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from collections import defaultdict

from sqlalchemy import func
from app.database import SessionLocal
from app.models.models import ExistingScheme, Company
from app.matching.company_matcher import normalize_company_name
from app.scrapers.hmlr_ccod_scraper import HMLRCCODScraper

CCOD_PATH = r"C:/Users/garvi/uk-ops-bd-platform/backend/CCOD_FULL_2026_03.zip"


def main():
    db = SessionLocal()
    try:
        # Step 1: Get all EPC new-dwelling schemes needing owner attribution
        schemes = (
            db.query(ExistingScheme)
            .filter(
                ExistingScheme.source == "epc_new_dwelling",
                ExistingScheme.owner_company_id.is_(None),
            )
            .all()
        )
        total_eligible = len(schemes)
        print(f"Found {total_eligible} EPC new-dwelling schemes without owner_company_id")

        if total_eligible == 0:
            print("Nothing to do.")
            return

        # Build postcode -> list of schemes mapping
        postcode_to_schemes = defaultdict(list)
        for s in schemes:
            if s.postcode:
                # Normalise postcode to match CCOD format: uppercase, single space
                pc = s.postcode.strip().upper().replace("  ", " ")
                # Also store a fully normalised version (no space then re-add)
                cleaned = pc.replace(" ", "")
                if len(cleaned) > 3:
                    pc_norm = f"{cleaned[:-3]} {cleaned[-3:]}"
                else:
                    pc_norm = cleaned
                postcode_to_schemes[pc_norm].append(s)

        postcodes = set(postcode_to_schemes.keys())
        print(f"Unique postcodes to match: {len(postcodes)}")

        # Step 2: Build a normalised-name -> Company cache from existing companies
        company_cache = {}  # normalized_name -> Company
        existing_companies = db.query(Company).all()
        for c in existing_companies:
            if c.normalized_name:
                company_cache[c.normalized_name] = c
        print(f"Loaded {len(existing_companies)} existing companies into cache")

        # Step 3: Stream CCOD and collect matches per postcode
        # We want the proprietor with the most title appearances at each postcode
        # Structure: postcode -> proprietor_name -> {count, ccod_row, proprietor}
        postcode_matches = defaultdict(lambda: defaultdict(lambda: {"count": 0, "row": None, "proprietor": None}))

        scraper = HMLRCCODScraper(local_path=CCOD_PATH)
        print("Streaming CCOD file...")
        matched_rows = 0
        for row in scraper.filter_by_postcodes(postcodes):
            matched_rows += 1
            if row.primary_proprietor:
                prop = row.primary_proprietor
                name = prop["name"]
                pc = row.postcode
                entry = postcode_matches[pc][name]
                entry["count"] += 1
                if entry["row"] is None:
                    entry["row"] = row
                    entry["proprietor"] = prop

        print(f"CCOD rows matched: {matched_rows}")
        print(f"Postcodes with matches: {len(postcode_matches)}")

        # Step 4: For each postcode, pick the proprietor with the most titles
        schemes_updated = 0
        companies_created = 0

        for pc, proprietor_map in postcode_matches.items():
            if pc not in postcode_to_schemes:
                continue

            # Pick the proprietor with the highest count at this postcode
            best_name = max(proprietor_map, key=lambda n: proprietor_map[n]["count"])
            best = proprietor_map[best_name]
            best_prop = best["proprietor"]
            best_row = best["row"]

            # Look up or create the Company
            norm_name = normalize_company_name(best_prop["name"])
            if not norm_name:
                continue

            company = company_cache.get(norm_name)
            if company is None:
                # Check DB by companies_house_number if available
                ch_number = best_prop.get("registration_number", "").strip()
                if ch_number:
                    company = (
                        db.query(Company)
                        .filter(Company.companies_house_number == ch_number)
                        .first()
                    )
                    if company:
                        company_cache[norm_name] = company

            if company is None:
                # Create new company
                ch_number = best_prop.get("registration_number", "").strip() or None
                company = Company(
                    name=best_prop["name"],
                    normalized_name=norm_name,
                    companies_house_number=ch_number,
                    company_type="Developer",
                    is_active=True,
                )
                db.add(company)
                db.flush()  # get the ID
                company_cache[norm_name] = company
                companies_created += 1

            # Update all schemes at this postcode
            for scheme in postcode_to_schemes[pc]:
                scheme.owner_company_id = company.id
                scheme.hmlr_title_number = best_row.title_number
                scheme.hmlr_tenure = best_row.tenure
                schemes_updated += 1

        db.commit()

        # Step 5: Report
        total_with_owner = (
            db.query(func.count(ExistingScheme.id))
            .filter(
                ExistingScheme.source == "epc_new_dwelling",
                ExistingScheme.owner_company_id.isnot(None),
            )
            .scalar()
        )
        total_epc = (
            db.query(func.count(ExistingScheme.id))
            .filter(ExistingScheme.source == "epc_new_dwelling")
            .scalar()
        )

        pct = (total_with_owner / total_epc * 100) if total_epc > 0 else 0

        print("\n" + "=" * 60)
        print("CCOD ENRICHMENT RESULTS")
        print("=" * 60)
        print(f"EPC schemes eligible (no owner):  {total_eligible}")
        print(f"EPC schemes updated this run:     {schemes_updated}")
        print(f"New companies created:             {companies_created}")
        print(f"Total EPC schemes:                {total_epc}")
        print(f"Total with owner_company_id:      {total_with_owner}")
        print(f"Owner attribution rate:            {pct:.1f}%")
        print("=" * 60)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
