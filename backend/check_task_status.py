"""Check status of all fired tasks and DB counts."""
import os, sys
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.tasks import celery_app
from celery.result import AsyncResult
from sqlalchemy import create_engine, text

celery_app.loader.import_default_modules()

task_ids = {
    "recalculate_all_scores": "b294563f-a5be-404d-a29a-ba1ea7b6638a",
    "generate_alerts": "96f99cd4-4c9f-4b7b-956b-76c3ab7ffe55",
    "backfill_scheme_postcodes": "09e2d0f2-d8d3-487c-ae31-9667c0596f42",
    "cross_reference_planning_to_schemes": "3a426abd-7d93-42c8-b621-4c87c632c277",
    "reprocess_operator_extraction": "92a2e61b-d4b6-4bcb-8194-fcd511a83798",
    "enrich_all_companies_ch": "4688cf35-4006-4664-a848-112b19b03841",
    "enrich_all_companies_psc": "48f8c997-1d36-4e44-b67c-00bbf3425018",
    "enrich_new_applications": "8b05b678-c3ae-4eeb-996a-32bda007572c",
    "backfill_contract_dates": "0a23c227-ccda-43ad-bc1a-946f01001535",
    "backfill_dates_from_duration": "e627104b-600a-416b-adfb-7100ed54a503",
    "estimate_contract_dates_cpv": "82d9f37f-2fb7-4065-a9bb-bc248c2601d0",
    "run_scheme_data_quality_audit": "3329f83d-dc27-47cb-b0ae-8316ce9be56f",
}

print("=" * 70)
print("TASK STATUS")
print("=" * 70)

for name, tid in task_ids.items():
    result = AsyncResult(tid, app=celery_app)
    status = result.status
    info = ""
    if status == "SUCCESS":
        r = result.result
        if isinstance(r, dict):
            info = ", ".join(f"{k}={v}" for k, v in list(r.items())[:4])
        else:
            info = str(r)[:80]
    elif status == "FAILURE":
        info = str(result.result)[:80]

    icon = {"SUCCESS": "OK", "PENDING": "..", "STARTED": ">>", "FAILURE": "XX"}.get(status, status)
    print(f"  [{icon}] {name:42s} {info[:60]}")

# DB counts
print()
print("=" * 70)
print("DATABASE COUNTS")
print("=" * 70)
e = create_engine(os.environ["DATABASE_URL"])
with e.connect() as c:
    for t in ["existing_schemes", "scheme_contracts", "companies", "planning_applications", "alerts"]:
        try:
            cnt = c.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t}: {cnt:,}")
        except:
            print(f"  {t}: (table missing)")

    # Contract dates
    total = c.execute(text("SELECT COUNT(*) FROM existing_schemes")).scalar()
    no_end = c.execute(text("SELECT COUNT(*) FROM existing_schemes WHERE contract_end_date IS NULL")).scalar()
    print(f"\n  Schemes missing contract_end_date: {no_end}/{total} ({no_end*100//total if total else 0}%)")

    # Companies with CH number
    enriched = c.execute(text("SELECT COUNT(*) FROM companies WHERE companies_house_number IS NOT NULL AND companies_house_number != ''")).scalar()
    total_co = c.execute(text("SELECT COUNT(*) FROM companies")).scalar()
    print(f"  Companies with CH number: {enriched}/{total_co}")
