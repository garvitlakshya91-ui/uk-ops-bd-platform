"""Run all contract date backfill enrichment tasks manually.

These tasks are normally scheduled via Celery Beat but can be run directly.
Tier 1: Extract dates from raw_data description text
Tier 2: Infer end dates from start_date + duration text
Tier 3: Estimate from CPV-based typical durations (low confidence)
"""
import os
import sys
import traceback

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])


def count_missing():
    with e.connect() as c:
        no_end = c.execute(text(
            "SELECT COUNT(*) FROM scheme_contracts WHERE contract_end_date IS NULL"
        )).scalar()
        no_start = c.execute(text(
            "SELECT COUNT(*) FROM scheme_contracts WHERE contract_start_date IS NULL"
        )).scalar()
        total = c.execute(text("SELECT COUNT(*) FROM scheme_contracts")).scalar()
    return total, no_start, no_end


print("=" * 60)
print("CONTRACT DATE BACKFILL")
print("=" * 60)

total, no_start, no_end = count_missing()
print(f"Before: {total} contracts, {no_start} missing start ({no_start*100//total}%), {no_end} missing end ({no_end*100//total}%)")

# Tier 1: Text-based extraction from description
print("\n--- Tier 1: Description text extraction ---")
try:
    from app.tasks.enrichment_tasks import backfill_contract_dates
    # The function processes 500 at a time, run in a loop
    total_updated = 0
    for batch in range(20):  # Up to 10,000 contracts
        result = backfill_contract_dates()
        updated = result.get("updated", 0)
        total_updated += updated
        print(f"  Batch {batch+1}: scanned={result.get('scanned', 0)}, updated={updated}, schemes_updated={result.get('schemes_updated', 0)}")
        if result.get("scanned", 0) == 0 or updated == 0:
            break
    print(f"  Tier 1 total updated: {total_updated}")
except Exception as exc:
    print(f"  Tier 1 FAILED: {exc}")
    traceback.print_exc()

# Tier 1b: CF awards extraction
print("\n--- Tier 1b: CF awards structured extraction ---")
try:
    from app.tasks.enrichment_tasks import backfill_contract_dates_cf_awards
    result = backfill_contract_dates_cf_awards()
    print(f"  Result: {result}")
except Exception as exc:
    print(f"  Tier 1b FAILED: {exc}")
    traceback.print_exc()

# Tier 2: Duration inference
print("\n--- Tier 2: Duration inference (start + N years -> end) ---")
try:
    from app.tasks.enrichment_tasks import backfill_dates_from_duration
    result = backfill_dates_from_duration()
    print(f"  Result: {result}")
except Exception as exc:
    print(f"  Tier 2 FAILED: {exc}")
    traceback.print_exc()

# Tier 3: CPV-based estimates
print("\n--- Tier 3: CPV-based typical duration estimates ---")
try:
    from app.tasks.enrichment_tasks import estimate_contract_dates_cpv
    result = estimate_contract_dates_cpv()
    print(f"  Result: {result}")
except Exception as exc:
    print(f"  Tier 3 FAILED: {exc}")
    traceback.print_exc()

# Final counts
print("\n" + "=" * 60)
total, no_start, no_end = count_missing()
print(f"After: {total} contracts, {no_start} missing start ({no_start*100//total}%), {no_end} missing end ({no_end*100//total}%)")

# Also update scheme dates from contracts
print("\n--- Propagating contract dates to schemes ---")
with e.connect() as c:
    updated = c.execute(text("""
        UPDATE existing_schemes es
        SET contract_start_date = sc.contract_start_date,
            contract_end_date = sc.contract_end_date
        FROM scheme_contracts sc
        WHERE sc.scheme_id = es.id
          AND sc.is_current = true
          AND (es.contract_start_date IS NULL OR es.contract_end_date IS NULL)
          AND (sc.contract_start_date IS NOT NULL OR sc.contract_end_date IS NOT NULL)
    """))
    c.commit()
    print(f"  Propagated dates to {updated.rowcount} schemes")

# Final scheme counts
with e.connect() as c:
    total_s = c.execute(text("SELECT COUNT(*) FROM existing_schemes")).scalar()
    no_end_s = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_end_date IS NULL")).scalar()
    print(f"\nSchemes: {total_s} total, {no_end_s} missing end_date ({no_end_s*100//total_s}%)")
