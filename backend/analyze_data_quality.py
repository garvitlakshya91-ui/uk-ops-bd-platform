"""Comprehensive data quality analysis across all tables."""
import os, sys
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text

e = create_engine(os.environ["DATABASE_URL"])

def pct(filled, total):
    return filled * 100 // total if total else 0

def analyze_field(c, table, label, cond, total):
    filled = c.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {cond}")).scalar()
    p = pct(filled, total)
    miss = total - filled
    print(f"  {label:30s} {filled:>6,}/{total:,} ({p:>3d}%)  missing={miss:,}")

print("=" * 80)
print("DATA QUALITY ANALYSIS")
print("=" * 80)

with e.connect() as c:
    # === EXISTING SCHEMES ===
    total = c.execute(text("SELECT COUNT(*) FROM existing_schemes")).scalar()
    print(f"\n--- EXISTING SCHEMES ({total:,} rows) ---")
    for label, cond in [
        ("name", "name IS NOT NULL AND name != ''"),
        ("address", "address IS NOT NULL AND address != ''"),
        ("postcode", "postcode IS NOT NULL AND postcode != ''"),
        ("council_id", "council_id IS NOT NULL"),
        ("owner_company_id", "owner_company_id IS NOT NULL"),
        ("operator_company_id", "operator_company_id IS NOT NULL"),
        ("scheme_type", "scheme_type IS NOT NULL AND scheme_type != ''"),
        ("status", "status IS NOT NULL AND status != ''"),
        ("total_units", "total_units IS NOT NULL"),
        ("contract_start_date", "contract_start_date IS NOT NULL"),
        ("contract_end_date", "contract_end_date IS NOT NULL"),
        ("contract_type", "contract_type IS NOT NULL AND contract_type != ''"),
        ("annual_revenue_gbp", "annual_revenue_gbp IS NOT NULL"),
        ("latitude/longitude", "latitude IS NOT NULL AND longitude IS NOT NULL"),
        ("source", "source IS NOT NULL AND source != ''"),
        ("source_reference", "source_reference IS NOT NULL AND source_reference != ''"),
        ("epc_ratings", "epc_ratings IS NOT NULL"),
        ("performance_rating", "performance_rating IS NOT NULL"),
        ("data_confidence_score", "data_confidence_score IS NOT NULL"),
    ]:
        analyze_field(c, "existing_schemes", label, cond, total)

    # === SCHEME CONTRACTS ===
    total2 = c.execute(text("SELECT COUNT(*) FROM scheme_contracts")).scalar()
    print(f"\n--- SCHEME CONTRACTS ({total2:,} rows) ---")
    for label, cond in [
        ("contract_reference", "contract_reference IS NOT NULL AND contract_reference != ''"),
        ("contract_type", "contract_type IS NOT NULL AND contract_type != ''"),
        ("operator_company_id", "operator_company_id IS NOT NULL"),
        ("client_company_id", "client_company_id IS NOT NULL"),
        ("contract_start_date", "contract_start_date IS NOT NULL"),
        ("contract_end_date", "contract_end_date IS NOT NULL"),
        ("contract_value", "contract_value IS NOT NULL"),
        ("source", "source IS NOT NULL AND source != ''"),
    ]:
        analyze_field(c, "scheme_contracts", label, cond, total2)

    # === COMPANIES ===
    total3 = c.execute(text("SELECT COUNT(*) FROM companies")).scalar()
    print(f"\n--- COMPANIES ({total3:,} rows) ---")
    for label, cond in [
        ("name", "name IS NOT NULL AND name != ''"),
        ("companies_house_number", "companies_house_number IS NOT NULL AND companies_house_number != ''"),
        ("company_type", "company_type IS NOT NULL AND company_type != ''"),
        ("sector", "sector IS NOT NULL AND sector != ''"),
        ("website", "website IS NOT NULL AND website != ''"),
        ("registered_address", "registered_address IS NOT NULL AND registered_address != ''"),
        ("sic_codes", "sic_codes IS NOT NULL"),
        ("employee_count", "employee_count IS NOT NULL"),
        ("key_contact_name", "key_contact_name IS NOT NULL AND key_contact_name != ''"),
        ("key_contact_email", "key_contact_email IS NOT NULL AND key_contact_email != ''"),
    ]:
        analyze_field(c, "companies", label, cond, total3)

    # === PLANNING APPLICATIONS ===
    total4 = c.execute(text("SELECT COUNT(*) FROM planning_applications")).scalar()
    print(f"\n--- PLANNING APPLICATIONS ({total4:,} rows) ---")
    for label, cond in [
        ("reference", "reference IS NOT NULL AND reference != ''"),
        ("description", "description IS NOT NULL AND description != ''"),
        ("address", "address IS NOT NULL AND address != ''"),
        ("postcode", "postcode IS NOT NULL AND postcode != ''"),
        ("applicant_name", "applicant_name IS NOT NULL AND applicant_name != ''"),
        ("applicant_company_id", "applicant_company_id IS NOT NULL"),
        ("status", "status IS NOT NULL AND status != ''"),
        ("decision", "decision IS NOT NULL AND decision != ''"),
        ("submitted_date", "submitted_date IS NOT NULL"),
        ("decision_date", "decision_date IS NOT NULL"),
        ("total_units", "total_units IS NOT NULL"),
        ("latitude/longitude", "latitude IS NOT NULL AND longitude IS NOT NULL"),
        ("is_btr", "is_btr IS NOT NULL"),
    ]:
        analyze_field(c, "planning_applications", label, cond, total4)

    # === EMPTY TABLES ===
    print("\n--- EMPTY/UNDERUSED TABLES ---")
    for t in ["contacts", "pipeline_opportunities", "scheme_change_log"]:
        cnt = c.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
        print(f"  {t}: {cnt} rows")

    # === CROSS-REFERENCE QUALITY ===
    print("\n--- CROSS-REFERENCE QUALITY ---")
    linked = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE operator_company_id IS NOT NULL OR owner_company_id IS NOT NULL")).scalar()
    print(f"  Schemes linked to companies: {linked}/{total}")
    linked_pa = c.execute(text("SELECT COUNT(*) FROM planning_applications WHERE applicant_company_id IS NOT NULL")).scalar()
    print(f"  Planning apps linked to companies: {linked_pa}/{total4}")
    contracts_linked = c.execute(text("SELECT COUNT(*) FROM scheme_contracts WHERE operator_company_id IS NOT NULL")).scalar()
    print(f"  Contracts with operator: {contracts_linked}/{total2}")

    # Source distribution
    print("\n--- SCHEME SOURCE DISTRIBUTION ---")
    rows = c.execute(text("SELECT source, COUNT(*) as cnt FROM existing_schemes GROUP BY source ORDER BY cnt DESC")).fetchall()
    for r in rows:
        print(f"  {str(r[0]):30s} {r[1]:,}")

    # Contract value stats
    print("\n--- CONTRACT VALUE STATS ---")
    stats = c.execute(text("SELECT COUNT(*), AVG(contract_value), MIN(contract_value), MAX(contract_value) FROM scheme_contracts WHERE contract_value IS NOT NULL")).fetchone()
    print(f"  With value: {stats[0]:,}")
    if stats[1]:
        print(f"  Avg: GBP {stats[1]:,.0f}")
        print(f"  Range: GBP {stats[2]:,.0f} - GBP {stats[3]:,.0f}")

    # Date quality
    print("\n--- DATE QUALITY ---")
    both = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_start_date IS NOT NULL AND contract_end_date IS NOT NULL")).scalar()
    start_only = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_start_date IS NOT NULL AND contract_end_date IS NULL")).scalar()
    end_only = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_start_date IS NULL AND contract_end_date IS NOT NULL")).scalar()
    neither = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_start_date IS NULL AND contract_end_date IS NULL")).scalar()
    print(f"  Both dates: {both}")
    print(f"  Start only: {start_only}")
    print(f"  End only: {end_only}")
    print(f"  Neither: {neither}")

    # Council coverage
    print("\n--- COUNCIL COVERAGE ---")
    active = c.execute(text("SELECT COUNT(*) FROM councils WHERE active = true")).scalar()
    scraped = c.execute(text("SELECT COUNT(*) FROM councils WHERE last_scraped_at IS NOT NULL")).scalar()
    total_councils = c.execute(text("SELECT COUNT(*) FROM councils")).scalar()
    print(f"  Total councils: {total_councils}")
    print(f"  Active: {active}")
    print(f"  Successfully scraped: {scraped}")

    # Planning by council
    print("\n--- TOP 10 COUNCILS BY PLANNING APPS ---")
    rows = c.execute(text("""
        SELECT co.name, COUNT(pa.id) as cnt
        FROM planning_applications pa
        JOIN councils co ON pa.council_id = co.id
        GROUP BY co.name ORDER BY cnt DESC LIMIT 10
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]:40s} {r[1]:,}")
