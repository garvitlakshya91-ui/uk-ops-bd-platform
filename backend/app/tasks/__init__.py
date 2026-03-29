"""Celery application configuration for the UK Ops BD Platform."""

from celery import Celery
from celery.schedules import crontab

from app.config import settings

celery_app = Celery(
    "uk_ops_bd",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/London",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=600,
    task_time_limit=900,
    task_default_queue="default",
    task_routes={
        "app.tasks.scraping_tasks.*": {"queue": "scraping"},
        "app.tasks.enrichment_tasks.*": {"queue": "enrichment"},
        "app.tasks.scoring_tasks.*": {"queue": "scoring"},
    },
)

celery_app.conf.beat_schedule = {
    # Scraping schedules
    "scrape-all-councils-daily": {
        "task": "app.tasks.scraping_tasks.scrape_all_councils",
        "schedule": crontab(hour=2, minute=0),
        "options": {"queue": "scraping"},
    },
    "scrape-planning-data-api-every-6h": {
        "task": "app.tasks.scraping_tasks.scrape_planning_data_api",
        "schedule": crontab(hour="*/6", minute=15),
        "options": {"queue": "scraping"},
    },
    # Enrichment schedules
    "enrich-new-applications-hourly": {
        "task": "app.tasks.enrichment_tasks.enrich_new_applications",
        "schedule": crontab(minute=30),
        "options": {"queue": "enrichment"},
    },
    "reverify-contacts-weekly": {
        "task": "app.tasks.enrichment_tasks.reverify_contacts",
        "schedule": crontab(hour=6, minute=0, day_of_week="monday"),
        "options": {"queue": "enrichment"},
    },
    # Scoring schedules
    "recalculate-scores-daily": {
        "task": "app.tasks.scoring_tasks.recalculate_all_scores",
        "schedule": crontab(hour=4, minute=0),
        "options": {"queue": "scoring"},
    },
    "generate-alerts-hourly": {
        "task": "app.tasks.scoring_tasks.generate_alerts",
        "schedule": crontab(minute=0),
        "options": {"queue": "scoring"},
    },
    # Scheme data scraping schedules
    "scrape-find-a-tender-daily": {
        "task": "app.tasks.scraping_tasks.scrape_find_a_tender",
        "schedule": crontab(hour=2, minute=15),
        "options": {"queue": "scraping"},
    },
    "scrape-contracts-finder-daily": {
        "task": "app.tasks.scraping_tasks.scrape_contracts_finder",
        "schedule": crontab(hour=2, minute=30),
        "options": {"queue": "scraping"},
    },
    "scrape-rsh-judgements-weekly": {
        "task": "app.tasks.scraping_tasks.scrape_rsh_judgements",
        "schedule": crontab(hour=3, minute=0, day_of_week="tuesday"),
        "options": {"queue": "scraping"},
    },
    "scheme-data-quality-audit-monthly": {
        "task": "app.tasks.scraping_tasks.run_scheme_data_quality_audit",
        "schedule": crontab(hour=5, minute=0, day_of_month="1"),
        "options": {"queue": "scoring"},
    },
    # Postcode back-fill and data enrichment
    "backfill-scheme-postcodes-daily": {
        "task": "app.tasks.enrichment_tasks.backfill_scheme_postcodes",
        "schedule": crontab(hour=3, minute=30),
        "options": {"queue": "enrichment"},
    },
    "enrich-schemes-epc-weekly": {
        "task": "app.tasks.enrichment_tasks.enrich_schemes_with_epc",
        "schedule": crontab(hour=4, minute=30, day_of_week="wednesday"),
        "options": {"queue": "enrichment"},
    },
    "enrich-all-companies-ch-daily": {
        "task": "app.tasks.enrichment_tasks.enrich_all_companies_ch",
        "schedule": crontab(hour=3, minute=0),
        "options": {"queue": "enrichment"},
    },
    "enrich-all-companies-psc-weekly": {
        "task": "app.tasks.enrichment_tasks.enrich_all_companies_psc",
        "schedule": crontab(hour=5, minute=0, day_of_week="thursday"),
        "options": {"queue": "enrichment"},
    },
    "cross-reference-planning-schemes-daily": {
        "task": "app.tasks.enrichment_tasks.cross_reference_planning_to_schemes",
        "schedule": crontab(hour=5, minute=30),
        "options": {"queue": "enrichment"},
    },
    "backfill-contract-dates-daily": {
        "task": "app.tasks.enrichment_tasks.backfill_contract_dates",
        "schedule": crontab(hour=4, minute=15),
        "options": {"queue": "enrichment"},
    },
    # RSH and HMLR data ingestion
    "ingest-rsh-registered-providers-monthly": {
        "task": "app.tasks.scraping_tasks.ingest_rsh_registered_providers",
        "schedule": crontab(hour=3, minute=15, day_of_month="15"),
        "options": {"queue": "scraping"},
    },
    "ingest-rsh-sdr-quarterly": {
        "task": "app.tasks.scraping_tasks.ingest_rsh_sdr",
        "schedule": crontab(hour=3, minute=45, day_of_month="1", month_of_year="1,4,7,10"),
        "options": {"queue": "scraping"},
    },
    "ingest-hmlr-ccod-monthly": {
        "task": "app.tasks.scraping_tasks.ingest_hmlr_ccod",
        "schedule": crontab(hour=1, minute=0, day_of_month="5"),
        "options": {"queue": "scraping"},
    },
    # Brownfield and LAHS data
    "scrape-brownfield-register-weekly": {
        "task": "app.tasks.scraping_tasks.scrape_brownfield_register",
        "schedule": crontab(hour=2, minute=45, day_of_week="wednesday"),
        "options": {"queue": "scraping"},
    },
    "ingest-lahs-data-annually": {
        "task": "app.tasks.scraping_tasks.ingest_lahs_data",
        "schedule": crontab(hour=3, minute=0, day_of_month="1", month_of_year="4"),
        "options": {"queue": "scraping"},
    },
}

celery_app.autodiscover_tasks([
    "app.tasks.scraping_tasks",
    "app.tasks.enrichment_tasks",
    "app.tasks.scoring_tasks",
])
