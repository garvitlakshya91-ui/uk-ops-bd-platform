"""Fire all enrichment and scoring tasks that haven't been run yet."""
import os, sys
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.tasks import celery_app

# Force task import
celery_app.loader.import_default_modules()

# Define all tasks to fire, in dependency order
tasks_to_fire = [
    # Scoring (fast, no external deps)
    "app.tasks.scoring_tasks.recalculate_all_scores",
    "app.tasks.scoring_tasks.generate_alerts",

    # Enrichment - no external API needed
    "app.tasks.enrichment_tasks.backfill_scheme_postcodes",
    "app.tasks.enrichment_tasks.cross_reference_planning_to_schemes",
    "app.tasks.enrichment_tasks.reprocess_operator_extraction",

    # Enrichment - Companies House (key works)
    "app.tasks.enrichment_tasks.enrich_all_companies_ch",
    "app.tasks.enrichment_tasks.enrich_all_companies_psc",

    # Enrichment - Hunter.io (key works, 50 searches)
    "app.tasks.enrichment_tasks.enrich_new_applications",

    # Enrichment - contract dates (no API needed)
    "app.tasks.enrichment_tasks.backfill_contract_dates",
    "app.tasks.enrichment_tasks.backfill_dates_from_duration",
    "app.tasks.enrichment_tasks.estimate_contract_dates_cpv",

    # Data quality
    "app.tasks.scraping_tasks.run_scheme_data_quality_audit",
]

print("Firing all enrichment tasks...")
print("=" * 60)

fired = {}
for task_name in tasks_to_fire:
    try:
        task = celery_app.tasks[task_name]
        result = task.delay()
        fired[task_name] = result.id
        short = task_name.split(".")[-1]
        print(f"  FIRED: {short} -> {result.id}")
    except Exception as e:
        print(f"  FAILED: {task_name} -> {e}")

print(f"\n{len(fired)} tasks fired")
print("\nTask IDs for monitoring:")
for name, tid in fired.items():
    print(f"  {name.split('.')[-1]}: {tid}")
