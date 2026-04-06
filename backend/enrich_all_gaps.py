"""
Comprehensive data quality enrichment script.
Fills gaps across all tables using existing data, regex extraction, and free APIs.

Usage: python enrich_all_gaps.py [--step N] [--dry-run]
"""
import os, sys, re, json, time, math, argparse
from collections import Counter, defaultdict
from datetime import date

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text, update
from sqlalchemy.orm import Session

engine = create_engine(os.environ["DATABASE_URL"])

# ---------------------------------------------------------------------------
# UK postcode regex
# ---------------------------------------------------------------------------
PC_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# SIC code → sector mapping
# ---------------------------------------------------------------------------
SIC_SECTOR_MAP = {
    "01": "Agriculture", "02": "Forestry", "03": "Fishing",
    "05": "Mining", "06": "Oil & Gas", "07": "Mining", "08": "Mining", "09": "Mining",
    "10": "Food & Beverage", "11": "Food & Beverage", "12": "Tobacco",
    "13": "Textiles", "14": "Textiles", "15": "Textiles",
    "16": "Wood & Paper", "17": "Wood & Paper", "18": "Printing",
    "19": "Petroleum", "20": "Chemicals", "21": "Pharmaceuticals",
    "22": "Plastics & Rubber", "23": "Building Materials",
    "24": "Metals", "25": "Metal Products", "26": "Electronics",
    "27": "Electrical Equipment", "28": "Machinery",
    "29": "Automotive", "30": "Transport Equipment",
    "31": "Furniture", "32": "Other Manufacturing", "33": "Repair & Installation",
    "35": "Energy & Utilities", "36": "Water Supply", "37": "Sewerage",
    "38": "Waste Management", "39": "Remediation",
    "41": "Construction", "42": "Civil Engineering", "43": "Specialised Construction",
    "45": "Motor Vehicles", "46": "Wholesale Trade", "47": "Retail Trade",
    "49": "Transport", "50": "Water Transport", "51": "Air Transport",
    "52": "Warehousing & Logistics", "53": "Postal & Courier",
    "55": "Accommodation", "56": "Food & Beverage Services",
    "58": "Publishing", "59": "Media & Film", "60": "Broadcasting",
    "61": "Telecommunications", "62": "IT & Software", "63": "Information Services",
    "64": "Financial Services", "65": "Insurance", "66": "Financial Services",
    "68": "Real Estate", "69": "Legal & Accounting",
    "70": "Management Consultancy", "71": "Architecture & Engineering",
    "72": "Scientific R&D", "73": "Advertising & Marketing",
    "74": "Professional Services", "75": "Veterinary",
    "77": "Rental & Leasing", "78": "Recruitment",
    "79": "Travel & Tourism", "80": "Security", "81": "Facilities Management",
    "82": "Office Administration",
    "84": "Public Administration", "85": "Education",
    "86": "Healthcare", "87": "Residential Care", "88": "Social Work",
    "90": "Arts & Entertainment", "91": "Libraries & Museums",
    "92": "Gambling", "93": "Sports & Recreation",
    "94": "Membership Organisations", "95": "Repair Services",
    "96": "Other Personal Services", "97": "Households as Employers",
    "98": "Households", "99": "International Organisations",
}

# ---------------------------------------------------------------------------
# Contract type classification keywords
# ---------------------------------------------------------------------------
CONTRACT_TYPE_KEYWORDS = {
    "maintenance": ["maintenance", "repairs", "responsive repair", "planned maintenance",
                     "servicing", "upkeep", "refurbishment", "retrofit"],
    "construction": ["construction", "building", "new build", "development",
                     "demolition", "erection", "groundworks", "civil engineering"],
    "cleaning": ["cleaning", "janitorial", "housekeeping", "window cleaning",
                 "deep clean", "waste collection"],
    "management": ["management", "property management", "housing management",
                   "estate management", "asset management", "facilities management"],
    "security": ["security", "CCTV", "concierge", "door entry", "surveillance"],
    "IT_services": ["software", "IT ", "digital", "technology", "system",
                    "platform", "cloud", "hosting", "data", "network"],
    "consultancy": ["consultancy", "advisory", "consultant", "professional services",
                    "specialist advice"],
    "design": ["architect", "design", "planning", "surveying", "landscape"],
    "energy": ["energy", "heating", "gas", "electricity", "boiler", "solar",
               "insulation", "EPC", "decarbonisation", "retrofit"],
    "care_support": ["care", "support", "assisted living", "sheltered",
                     "extra care", "warden", "supported housing", "domiciliary"],
    "temporary_accommodation": ["temporary accommodation", "homelessness",
                                "rough sleeping", "emergency accommodation", "hostel"],
    "legal_financial": ["legal", "solicitor", "insurance", "audit", "financial",
                        "accounting", "valuation"],
    "recruitment": ["recruitment", "staffing", "agency worker", "temporary staff"],
    "grounds": ["grounds", "landscaping", "gardening", "tree", "grass"],
    "furniture_supplies": ["furniture", "furnishing", "supplies", "equipment",
                           "goods", "food", "catering"],
    "telecom": ["telephony", "broadband", "internet", "connectivity", "telecoms"],
    "transport": ["transport", "fleet", "vehicle", "removal", "logistics"],
}

# BTR / PBSA keywords
BTR_KEYWORDS = [
    "build to rent", "btr", "private rented", "purpose built rental",
    "co-living", "coliving", "multifamily", "multi-family",
    "rental scheme", "institutional rent", "prs",
]
PBSA_KEYWORDS = [
    "student accommodation", "pbsa", "purpose built student",
    "student housing", "student hall", "student residence",
    "halls of residence", "university accommodation",
]

# Units extraction patterns
UNITS_PATTERNS = [
    re.compile(r"(\d{1,5})\s*(?:residential\s+)?(?:units?|dwellings?|flats?|apartments?|homes?|houses?|beds?|bedrooms?|rooms?)", re.I),
    re.compile(r"(?:provision of|comprising|containing|creating|delivering|erection of)\s+(\d{1,5})\s+(?:new\s+)?(?:units?|dwellings?|flats?|apartments?|homes?)", re.I),
    re.compile(r"(\d{1,5})\s*(?:bed|bedroom)\s+(?:scheme|development|block|property)", re.I),
]


def step_1_extract_postcodes_from_contracts(dry_run=False):
    """Pull postcodes and addresses from contract raw_data into scheme records."""
    print("\n" + "=" * 70)
    print("STEP 1: Extract postcodes/addresses from contract raw_data")
    print("=" * 70)
    with engine.connect() as c:
        # Get schemes missing postcode that have contracts with postcodes
        rows = c.execute(text("""
            SELECT DISTINCT es.id, sc.raw_data->>'postcode' as pc, sc.raw_data->>'address' as addr
            FROM existing_schemes es
            JOIN scheme_contracts sc ON sc.scheme_id = es.id
            WHERE (es.postcode IS NULL OR es.postcode = '')
              AND sc.raw_data IS NOT NULL
              AND sc.raw_data->>'postcode' IS NOT NULL
              AND sc.raw_data->>'postcode' != ''
        """)).fetchall()
        print(f"  Found {len(rows)} schemes that can get postcodes from contracts")

        if dry_run:
            return len(rows)

        updated = 0
        for r in rows:
            pc = r[1].strip().upper() if r[1] else None
            addr = r[2].strip() if r[2] else None
            if pc:
                c.execute(text(
                    "UPDATE existing_schemes SET postcode = :pc, address = COALESCE(address, :addr) WHERE id = :id"
                ), {"pc": pc, "addr": addr, "id": r[0]})
                updated += 1
        c.commit()
        print(f"  Updated {updated} scheme postcodes from contract raw_data")

        # Also try regex on contract descriptions
        rows2 = c.execute(text("""
            SELECT DISTINCT es.id, sc.raw_data->>'description' as desc_text
            FROM existing_schemes es
            JOIN scheme_contracts sc ON sc.scheme_id = es.id
            WHERE (es.postcode IS NULL OR es.postcode = '')
              AND sc.raw_data IS NOT NULL
              AND sc.raw_data->>'description' IS NOT NULL
        """)).fetchall()
        regex_updated = 0
        for r in rows2:
            m = PC_RE.search(r[1] or "")
            if m:
                pc = m.group(1).strip().upper()
                # Basic validation — skip obviously bad ones
                if len(pc) >= 5:
                    c.execute(text(
                        "UPDATE existing_schemes SET postcode = :pc WHERE id = :id AND (postcode IS NULL OR postcode = '')"
                    ), {"pc": pc, "id": r[0]})
                    regex_updated += 1
        c.commit()
        print(f"  Updated {regex_updated} more from description regex")
        return updated + regex_updated


def step_2_link_schemes_to_councils(dry_run=False):
    """Match schemes to councils using contracting_authority name."""
    print("\n" + "=" * 70)
    print("STEP 2: Link schemes to councils")
    print("=" * 70)
    with engine.connect() as c:
        # Get all council names
        councils = c.execute(text("SELECT id, name FROM councils")).fetchall()
        council_map = {}  # normalized name -> id
        for cid, cname in councils:
            norm = cname.lower().strip()
            council_map[norm] = cid
            # Also add without "council", "borough", etc. for fuzzy matching
            for suffix in [" council", " borough council", " district council",
                           " city council", " county council", " metropolitan borough council"]:
                if norm.endswith(suffix):
                    council_map[norm[:-len(suffix)].strip()] = cid

        # Get contracting authorities from contracts
        rows = c.execute(text("""
            SELECT DISTINCT es.id, sc.raw_data->>'contracting_authority' as ca
            FROM existing_schemes es
            JOIN scheme_contracts sc ON sc.scheme_id = es.id
            WHERE es.council_id IS NULL
              AND sc.raw_data IS NOT NULL
              AND sc.raw_data->>'contracting_authority' IS NOT NULL
        """)).fetchall()
        print(f"  Found {len(rows)} schemes to match against {len(councils)} councils")

        if dry_run:
            return 0

        matched = 0
        for r in rows:
            ca = (r[1] or "").lower().strip()
            # Direct match
            council_id = council_map.get(ca)
            if not council_id:
                # Try partial match — check if council name is in contracting authority
                for cname, cid in council_map.items():
                    if len(cname) > 5 and cname in ca:
                        council_id = cid
                        break
            if council_id:
                c.execute(text(
                    "UPDATE existing_schemes SET council_id = :cid WHERE id = :id AND council_id IS NULL"
                ), {"cid": council_id, "id": r[0]})
                matched += 1
        c.commit()
        print(f"  Linked {matched} schemes to councils")
        return matched


def step_3_derive_contract_type(dry_run=False):
    """Classify contract type from scheme name + contract descriptions."""
    print("\n" + "=" * 70)
    print("STEP 3: Derive contract_type from descriptions")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT es.id, es.name,
                   (SELECT string_agg(COALESCE(sc.raw_data->>'description', ''), ' ')
                    FROM scheme_contracts sc WHERE sc.scheme_id = es.id) as descs
            FROM existing_schemes es
            WHERE es.contract_type IS NULL OR es.contract_type = ''
        """)).fetchall()
        print(f"  Found {len(rows)} schemes needing contract_type")

        if dry_run:
            return 0

        updated = 0
        for r in rows:
            combined = ((r[1] or "") + " " + (r[2] or "")).lower()
            best_type = None
            best_count = 0
            for ctype, keywords in CONTRACT_TYPE_KEYWORDS.items():
                count = sum(1 for kw in keywords if kw.lower() in combined)
                if count > best_count:
                    best_count = count
                    best_type = ctype
            if best_type and best_count >= 1:
                c.execute(text(
                    "UPDATE existing_schemes SET contract_type = :ct WHERE id = :id"
                ), {"ct": best_type, "id": r[0]})
                updated += 1
        c.commit()
        print(f"  Classified {updated} schemes")
        # Show distribution
        dist = c.execute(text(
            "SELECT contract_type, COUNT(*) FROM existing_schemes WHERE contract_type IS NOT NULL GROUP BY contract_type ORDER BY COUNT(*) DESC"
        )).fetchall()
        for d in dist:
            print(f"    {d[0]:30s} {d[1]:,}")
        return updated


def step_4_extract_total_units(dry_run=False):
    """Extract total_units from scheme names and contract descriptions."""
    print("\n" + "=" * 70)
    print("STEP 4: Extract total_units from descriptions")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT es.id, es.name,
                   (SELECT string_agg(COALESCE(sc.raw_data->>'description', ''), ' ')
                    FROM scheme_contracts sc WHERE sc.scheme_id = es.id) as descs
            FROM existing_schemes es
            WHERE es.total_units IS NULL
        """)).fetchall()
        print(f"  Found {len(rows)} schemes needing total_units")

        if dry_run:
            return 0

        updated = 0
        for r in rows:
            combined = (r[1] or "") + " " + (r[2] or "")
            for pat in UNITS_PATTERNS:
                m = pat.search(combined)
                if m:
                    units = int(m.group(1))
                    if 1 <= units <= 10000:  # sanity check
                        c.execute(text(
                            "UPDATE existing_schemes SET total_units = :u WHERE id = :id"
                        ), {"u": units, "id": r[0]})
                        updated += 1
                        break
        c.commit()
        print(f"  Extracted units for {updated} schemes")
        return updated


def step_5_populate_sector_from_sic(dry_run=False):
    """Map SIC codes to business sectors."""
    print("\n" + "=" * 70)
    print("STEP 5: Populate sector from SIC codes")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, sic_codes FROM companies
            WHERE (sector IS NULL OR sector = '')
              AND sic_codes IS NOT NULL
        """)).fetchall()
        print(f"  Found {len(rows)} companies with SIC codes but no sector")

        if dry_run:
            return 0

        updated = 0
        for r in rows:
            sic_data = r[1]
            # Handle both list and dict formats
            codes = []
            if isinstance(sic_data, list):
                codes = sic_data
            elif isinstance(sic_data, dict):
                # RSH registration or other format — skip
                continue
            else:
                continue

            # Find first matching sector
            sector = None
            for code in codes:
                code_str = str(code).strip()
                prefix = code_str[:2]
                sector = SIC_SECTOR_MAP.get(prefix)
                if sector:
                    break

            if sector:
                c.execute(text(
                    "UPDATE companies SET sector = :s WHERE id = :id"
                ), {"s": sector, "id": r[0]})
                updated += 1
        c.commit()
        print(f"  Updated sector for {updated} companies")
        # Show distribution
        dist = c.execute(text(
            "SELECT sector, COUNT(*) FROM companies WHERE sector IS NOT NULL GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 15"
        )).fetchall()
        for d in dist:
            print(f"    {d[0]:30s} {d[1]:,}")
        return updated


def step_6_classify_btr_pbsa(dry_run=False):
    """Classify planning applications as BTR or PBSA."""
    print("\n" + "=" * 70)
    print("STEP 6: Classify planning apps as BTR/PBSA")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, description, application_type
            FROM planning_applications
            WHERE (is_btr IS NULL OR is_pbsa IS NULL)
              AND description IS NOT NULL
        """)).fetchall()
        print(f"  Found {len(rows):,} planning apps to classify")

        if dry_run:
            return 0

        btr_count = 0
        pbsa_count = 0
        batch_size = 5000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for r in batch:
                desc = (r[1] or "").lower() + " " + (r[2] or "").lower()
                is_btr = any(kw in desc for kw in BTR_KEYWORDS)
                is_pbsa = any(kw in desc for kw in PBSA_KEYWORDS)
                if is_btr or is_pbsa:
                    c.execute(text(
                        "UPDATE planning_applications SET is_btr = :btr, is_pbsa = :pbsa WHERE id = :id"
                    ), {"btr": is_btr, "pbsa": is_pbsa, "id": r[0]})
                    if is_btr:
                        btr_count += 1
                    if is_pbsa:
                        pbsa_count += 1
                else:
                    c.execute(text(
                        "UPDATE planning_applications SET is_btr = false, is_pbsa = false WHERE id = :id"
                    ), {"btr": False, "pbsa": False, "id": r[0]})
            c.commit()
            if (i + batch_size) % 50000 == 0:
                print(f"    Processed {i + batch_size:,}...")

        print(f"  BTR classified: {btr_count:,}")
        print(f"  PBSA classified: {pbsa_count:,}")
        return btr_count + pbsa_count


def step_7_extract_planning_postcodes(dry_run=False):
    """Extract postcodes from planning application addresses and descriptions."""
    print("\n" + "=" * 70)
    print("STEP 7: Extract planning app postcodes from text")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, address, description
            FROM planning_applications
            WHERE (postcode IS NULL OR postcode = '')
              AND (address IS NOT NULL OR description IS NOT NULL)
        """)).fetchall()
        print(f"  Found {len(rows):,} planning apps needing postcodes")

        if dry_run:
            return 0

        updated = 0
        batch_size = 5000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for r in batch:
                combined = (r[1] or "") + " " + (r[2] or "")
                m = PC_RE.search(combined)
                if m:
                    pc = m.group(1).strip().upper()
                    if len(pc) >= 5:
                        c.execute(text(
                            "UPDATE planning_applications SET postcode = :pc WHERE id = :id"
                        ), {"pc": pc, "id": r[0]})
                        updated += 1
            c.commit()
            if (i + batch_size) % 50000 == 0:
                print(f"    Processed {i + batch_size:,}, extracted {updated:,}...")

        print(f"  Extracted postcodes for {updated:,} planning apps")
        return updated


def step_8_populate_operator_from_raw(dry_run=False):
    """Match supplier names from raw_data to companies and set operator_company_id."""
    print("\n" + "=" * 70)
    print("STEP 8: Populate operator_company_id from raw_data supplier")
    print("=" * 70)
    with engine.connect() as c:
        # Build company name lookup
        companies = c.execute(text("SELECT id, normalized_name FROM companies")).fetchall()
        co_map = {}
        for cid, norm in companies:
            co_map[norm.lower().strip()] = cid

        # Get contracts with supplier but no operator
        rows = c.execute(text("""
            SELECT sc.id, sc.raw_data->>'supplier' as supplier
            FROM scheme_contracts sc
            WHERE sc.operator_company_id IS NULL
              AND sc.raw_data IS NOT NULL
              AND sc.raw_data->>'supplier' IS NOT NULL
              AND sc.raw_data->>'supplier' != ''
        """)).fetchall()
        print(f"  Found {len(rows)} contracts with supplier name, no operator linked")
        print(f"  Company lookup has {len(co_map)} entries")

        if dry_run:
            return 0

        # Normalize function
        def normalize(name):
            name = name.lower().strip()
            for suffix in [" ltd", " limited", " plc", " llp", " inc",
                           " group", " holdings", " uk", " (uk)"]:
                name = name.replace(suffix, "")
            return re.sub(r"[^a-z0-9 ]", "", name).strip()

        matched = 0
        for r in rows:
            supplier = r[1]
            norm = normalize(supplier)
            cid = co_map.get(norm)
            if not cid:
                # Try partial match
                for co_norm, co_id in co_map.items():
                    if len(norm) > 5 and (norm in co_norm or co_norm in norm):
                        cid = co_id
                        break
            if cid:
                c.execute(text(
                    "UPDATE scheme_contracts SET operator_company_id = :cid WHERE id = :id"
                ), {"cid": cid, "id": r[0]})
                matched += 1
        c.commit()
        print(f"  Linked {matched} contracts to operators")
        return matched


def step_9_annual_revenue_from_contracts(dry_run=False):
    """Calculate annual_revenue_gbp from contract value / duration."""
    print("\n" + "=" * 70)
    print("STEP 9: Calculate annual revenue from contract values")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT es.id,
                   SUM(sc.contract_value) as total_value,
                   MIN(sc.contract_start_date) as start_d,
                   MAX(sc.contract_end_date) as end_d
            FROM existing_schemes es
            JOIN scheme_contracts sc ON sc.scheme_id = es.id
            WHERE es.annual_revenue_gbp IS NULL
              AND sc.contract_value IS NOT NULL
              AND sc.contract_value > 0
            GROUP BY es.id
        """)).fetchall()
        print(f"  Found {len(rows)} schemes with contract values to derive revenue")

        if dry_run:
            return 0

        updated = 0
        for r in rows:
            sid, total_val, start_d, end_d = r
            if start_d and end_d and end_d > start_d:
                years = (end_d - start_d).days / 365.25
                if years > 0:
                    annual = int(total_val / years)
                    c.execute(text(
                        "UPDATE existing_schemes SET annual_revenue_gbp = :rev WHERE id = :id"
                    ), {"rev": annual, "id": sid})
                    updated += 1
            elif total_val:
                # Assume 3-year contract if no dates
                annual = int(total_val / 3)
                c.execute(text(
                    "UPDATE existing_schemes SET annual_revenue_gbp = :rev WHERE id = :id"
                ), {"rev": annual, "id": sid})
                updated += 1
        c.commit()
        print(f"  Calculated annual revenue for {updated} schemes")
        return updated


def step_10_performance_rating(dry_run=False):
    """Calculate performance_rating from available data (EPC, contract value, etc.)."""
    print("\n" + "=" * 70)
    print("STEP 10: Calculate performance ratings")
    print("=" * 70)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT es.id, es.epc_ratings, es.annual_revenue_gbp,
                   es.data_confidence_score, es.contract_start_date, es.contract_end_date,
                   es.total_units, es.postcode
            FROM existing_schemes es
            WHERE es.performance_rating IS NULL
        """)).fetchall()
        print(f"  Found {len(rows)} schemes needing performance_rating")

        if dry_run:
            return 0

        updated = 0
        for r in rows:
            sid = r[0]
            epc = r[1]
            revenue = r[2]
            confidence = r[3]
            start_d = r[4]
            end_d = r[5]
            units = r[6]
            postcode = r[7]

            scores = []

            # EPC score (0-100)
            if epc and isinstance(epc, dict):
                ratings = epc.get("ratings", {})
                if ratings and not epc.get("checked"):
                    weights = {"A": 100, "B": 85, "C": 70, "D": 55, "E": 40, "F": 25, "G": 10}
                    total_certs = sum(ratings.values())
                    if total_certs > 0:
                        weighted = sum(weights.get(k, 50) * v for k, v in ratings.items())
                        scores.append(weighted / total_certs)

            # Data completeness score (0-100)
            completeness = 0
            if revenue:
                completeness += 20
            if start_d:
                completeness += 15
            if end_d:
                completeness += 15
            if units:
                completeness += 15
            if postcode:
                completeness += 15
            if epc:
                completeness += 20
            scores.append(completeness)

            # Confidence score
            if confidence:
                scores.append(confidence * 100)

            if scores:
                rating = sum(scores) / len(scores)
                c.execute(text(
                    "UPDATE existing_schemes SET performance_rating = :r WHERE id = :id"
                ), {"r": round(rating, 1), "id": sid})
                updated += 1
        c.commit()
        print(f"  Calculated performance_rating for {updated} schemes")
        return updated


def step_11_geocode_schemes(dry_run=False):
    """Geocode schemes with postcodes using postcodes.io (free, no key)."""
    print("\n" + "=" * 70)
    print("STEP 11: Geocode schemes via postcodes.io")
    print("=" * 70)
    import httpx

    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, postcode FROM existing_schemes
            WHERE postcode IS NOT NULL AND postcode != ''
              AND (latitude IS NULL OR longitude IS NULL)
        """)).fetchall()
        print(f"  Found {len(rows)} schemes to geocode")

        if dry_run:
            return 0

        # postcodes.io supports bulk lookup of 100 at a time
        updated = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            postcodes = [r[1].strip().upper() for r in batch]
            id_map = {r[1].strip().upper(): r[0] for r in batch}

            try:
                resp = httpx.post(
                    "https://api.postcodes.io/postcodes",
                    json={"postcodes": postcodes},
                    timeout=30,
                )
                if resp.status_code != 200:
                    print(f"    Batch {i//100 + 1} failed: {resp.status_code}")
                    continue

                results = resp.json().get("result", [])
                for item in results:
                    query_pc = item.get("query", "").strip().upper()
                    result = item.get("result")
                    if result and result.get("latitude"):
                        sid = id_map.get(query_pc)
                        if sid:
                            c.execute(text(
                                "UPDATE existing_schemes SET latitude = :lat, longitude = :lng WHERE id = :id"
                            ), {"lat": result["latitude"], "lng": result["longitude"], "id": sid})
                            updated += 1
                c.commit()
                time.sleep(0.5)  # Be nice to the free API
            except Exception as e:
                print(f"    Batch error: {e}")
                continue

        print(f"  Geocoded {updated} schemes")
        return updated


def step_12_geocode_planning_apps(dry_run=False):
    """Geocode planning apps with postcodes using postcodes.io."""
    print("\n" + "=" * 70)
    print("STEP 12: Geocode planning apps via postcodes.io")
    print("=" * 70)
    import httpx

    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, postcode FROM planning_applications
            WHERE postcode IS NOT NULL AND postcode != ''
              AND (latitude IS NULL OR longitude IS NULL)
            LIMIT 10000
        """)).fetchall()
        print(f"  Found {len(rows):,} planning apps to geocode (max 10k per run)")

        if dry_run:
            return 0

        updated = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            postcodes = [r[1].strip().upper() for r in batch]
            id_map = {}
            for r in batch:
                pc = r[1].strip().upper()
                if pc not in id_map:
                    id_map[pc] = []
                id_map[pc].append(r[0])

            try:
                unique_pcs = list(set(postcodes))
                resp = httpx.post(
                    "https://api.postcodes.io/postcodes",
                    json={"postcodes": unique_pcs},
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue

                results = resp.json().get("result", [])
                for item in results:
                    query_pc = item.get("query", "").strip().upper()
                    result = item.get("result")
                    if result and result.get("latitude"):
                        for aid in id_map.get(query_pc, []):
                            c.execute(text(
                                "UPDATE planning_applications SET latitude = :lat, longitude = :lng WHERE id = :id"
                            ), {"lat": result["latitude"], "lng": result["longitude"], "id": aid})
                            updated += 1
                c.commit()
                time.sleep(0.3)
            except Exception as e:
                print(f"    Batch error: {e}")
                continue

            if (i + 100) % 2000 == 0:
                print(f"    Processed {i + 100:,}, geocoded {updated:,}...")

        print(f"  Geocoded {updated:,} planning apps")
        return updated


def step_13_enrich_companies_ch(dry_run=False):
    """Re-fire CH enrichment for companies missing CH numbers."""
    print("\n" + "=" * 70)
    print("STEP 13: Companies House enrichment (via Celery)")
    print("=" * 70)
    if dry_run:
        return 0

    try:
        sys.path.insert(0, ".")
        from app.tasks import celery_app
        celery_app.loader.import_default_modules()
        task = celery_app.tasks["app.tasks.enrichment_tasks.enrich_all_companies_ch"]
        result = task.delay()
        print(f"  Fired enrich_all_companies_ch: {result.id}")
        # Also fire PSC
        task2 = celery_app.tasks["app.tasks.enrichment_tasks.enrich_all_companies_psc"]
        result2 = task2.delay()
        print(f"  Fired enrich_all_companies_psc: {result2.id}")
        return 2
    except Exception as e:
        print(f"  Could not fire Celery task: {e}")
        print("  Run manually: celery worker must be running")
        return 0


def step_14_hunter_contacts(dry_run=False):
    """Use Hunter.io to find contacts at top companies."""
    print("\n" + "=" * 70)
    print("STEP 14: Contact enrichment via Hunter.io")
    print("=" * 70)
    import httpx

    api_key = os.environ.get("HUNTER_API_KEY", "")
    if not api_key:
        # Try loading from settings
        try:
            from app.config import settings
            api_key = settings.HUNTER_API_KEY
        except Exception:
            pass

    if not api_key:
        print("  No Hunter API key found, skipping")
        return 0

    with engine.connect() as c:
        # Get companies with CH numbers (most likely to have a website / domain)
        companies = c.execute(text("""
            SELECT co.id, co.name, co.website, co.companies_house_number
            FROM companies co
            LEFT JOIN contacts ct ON ct.company_id = co.id
            WHERE ct.id IS NULL
              AND co.companies_house_number IS NOT NULL
              AND co.companies_house_number != ''
            ORDER BY (SELECT COUNT(*) FROM scheme_contracts sc WHERE sc.client_company_id = co.id) DESC
            LIMIT 30
        """)).fetchall()
        print(f"  Found {len(companies)} top companies to search for contacts")
        print(f"  (Using 30 of 50 available Hunter searches)")

        if dry_run:
            return 0

        contacts_found = 0
        for co in companies:
            co_id, co_name, website, ch_num = co

            # Get domain - try website first, fall back to domain search
            domain = None
            if website:
                domain = website.replace("https://", "").replace("http://", "").split("/")[0]
            else:
                # Use Hunter domain search
                try:
                    resp = httpx.get(
                        "https://api.hunter.io/v2/domain-search",
                        params={"company": co_name, "api_key": api_key, "limit": 5},
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json().get("data", {})
                        domain = data.get("domain")
                        emails = data.get("emails", [])
                        if not domain and emails:
                            domain = emails[0].get("domain")
                except Exception:
                    continue

            if not domain:
                continue

            # Search for contacts at this domain
            try:
                resp = httpx.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={"domain": domain, "api_key": api_key, "limit": 5},
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue

                data = resp.json().get("data", {})
                emails = data.get("emails", [])

                # Update company website if missing
                if not website and domain:
                    c.execute(text(
                        "UPDATE companies SET website = :w WHERE id = :id AND (website IS NULL OR website = '')"
                    ), {"w": f"https://{domain}", "id": co_id})

                for email_data in emails[:3]:  # Max 3 contacts per company
                    first = email_data.get("first_name", "")
                    last = email_data.get("last_name", "")
                    full_name = f"{first} {last}".strip()
                    if not full_name:
                        continue

                    c.execute(text("""
                        INSERT INTO contacts (company_id, full_name, job_title, email, source, confidence_score, created_at)
                        VALUES (:co_id, :name, :title, :email, 'hunter.io', :conf, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "co_id": co_id,
                        "name": full_name,
                        "title": email_data.get("position", ""),
                        "email": email_data.get("value", ""),
                        "conf": (email_data.get("confidence", 50)) / 100.0,
                    })
                    contacts_found += 1

                c.commit()
                time.sleep(2)  # Rate limit
            except Exception as e:
                print(f"    Error for {co_name}: {e}")
                continue

        print(f"  Found {contacts_found} contacts across {len(companies)} companies")
        return contacts_found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", type=int, help="Run only step N")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    steps = [
        (1, step_1_extract_postcodes_from_contracts),
        (2, step_2_link_schemes_to_councils),
        (3, step_3_derive_contract_type),
        (4, step_4_extract_total_units),
        (5, step_5_populate_sector_from_sic),
        (6, step_6_classify_btr_pbsa),
        (7, step_7_extract_planning_postcodes),
        (8, step_8_populate_operator_from_raw),
        (9, step_9_annual_revenue_from_contracts),
        (10, step_10_performance_rating),
        (11, step_11_geocode_schemes),
        (12, step_12_geocode_planning_apps),
        (13, step_13_enrich_companies_ch),
        (14, step_14_hunter_contacts),
    ]

    results = {}
    for num, func in steps:
        if args.step and args.step != num:
            continue
        try:
            result = func(dry_run=args.dry_run)
            results[num] = result
        except Exception as e:
            print(f"\n  ERROR in step {num}: {e}")
            import traceback
            traceback.print_exc()
            results[num] = f"ERROR: {e}"

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for num, result in results.items():
        step_name = steps[num - 1][1].__doc__.split("\n")[0] if num <= len(steps) else "?"
        print(f"  Step {num:2d}: {result:>8} | {step_name}")


if __name__ == "__main__":
    main()
