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
        "app.tasks.developer_tracking_tasks.*": {"queue": "scraping"},
        "app.tasks.data_source_tasks.*": {"queue": "scraping"},
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
    "enrich-all-companies-psc-daily": {
        "task": "app.tasks.enrichment_tasks.enrich_all_companies_psc",
        "schedule": crontab(hour=5, minute=0),
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
    # Tier 1: CF award-stage date backfill (runs after text-based extraction)
    "backfill-contract-dates-cf-awards-daily": {
        "task": "app.tasks.enrichment_tasks.backfill_contract_dates_cf_awards",
        "schedule": crontab(hour=4, minute=30),
        "options": {"queue": "enrichment"},
    },
    # Tier 2: Duration inference (start + "N years" -> end)
    "backfill-dates-from-duration-daily": {
        "task": "app.tasks.enrichment_tasks.backfill_dates_from_duration",
        "schedule": crontab(hour=4, minute=45),
        "options": {"queue": "enrichment"},
    },
    # Tier 3: CPV/keyword-based typical duration estimates (weekly, low confidence)
    "estimate-contract-dates-cpv-weekly": {
        "task": "app.tasks.enrichment_tasks.estimate_contract_dates_cpv",
        "schedule": crontab(hour=6, minute=30, day_of_week="thursday"),
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
    "ingest-planit-applications-daily": {
        "task": "app.tasks.scraping_tasks.ingest_planit_applications",
        "schedule": crontab(hour=1, minute=30),
        "options": {"queue": "scraping"},
    },
    "enrich-companies-charity-weekly": {
        "task": "app.tasks.enrichment_tasks.enrich_companies_charity_status",
        "schedule": crontab(hour=6, minute=0, day_of_week="saturday"),
        "options": {"queue": "enrichment"},
    },
    "reprocess-operator-extraction-weekly": {
        "task": "app.tasks.enrichment_tasks.reprocess_operator_extraction",
        "schedule": crontab(hour=5, minute=30, day_of_week="sunday"),
        "options": {"queue": "enrichment"},
    },
    # Developer SPV tracking and land ownership
    "track-developer-spvs-weekly": {
        "task": "app.tasks.developer_tracking_tasks.track_developer_spvs",
        "schedule": crontab(hour=1, minute=0, day_of_week="sunday"),
        "options": {"queue": "scraping"},
    },
    "ingest-ccod-ownership-monthly": {
        "task": "app.tasks.developer_tracking_tasks.ingest_land_registry_ccod_ownership",
        "schedule": crontab(hour=2, minute=0, day_of_month="10"),
        "options": {"queue": "scraping"},
    },
    "enrich-company-ownership-weekly": {
        "task": "app.tasks.developer_tracking_tasks.enrich_company_ownership",
        "schedule": crontab(hour=3, minute=0, day_of_week="monday"),
        "options": {"queue": "enrichment"},
    },
    "scan-new-property-incorporations-weekly": {
        "task": "app.tasks.developer_tracking_tasks.scan_new_property_incorporations",
        "schedule": crontab(hour=1, minute=30, day_of_week="sunday"),
        "options": {"queue": "scraping"},
    },
    # ------------------------------------------------------------------
    # New data source integrations
    # ------------------------------------------------------------------
    "ingest-price-paid-monthly": {
        "task": "app.tasks.data_source_tasks.ingest_price_paid_data",
        "schedule": crontab(hour=1, minute=30, day_of_month="10"),
        "options": {"queue": "scraping"},
    },
    "ingest-gla-planning-daily": {
        "task": "app.tasks.data_source_tasks.ingest_gla_planning",
        "schedule": crontab(hour=2, minute=50),
        "options": {"queue": "scraping"},
    },
    "ingest-bpf-btr-pipeline-quarterly": {
        "task": "app.tasks.data_source_tasks.ingest_bpf_btr_pipeline",
        "schedule": crontab(hour=3, minute=30, day_of_month="1", month_of_year="1,4,7,10"),
        "options": {"queue": "scraping"},
    },
}

celery_app.autodiscover_tasks([
    "app.tasks.scraping_tasks",
    "app.tasks.enrichment_tasks",
    "app.tasks.scoring_tasks",
    "app.tasks.developer_tracking_tasks",
    "app.tasks.data_source_tasks",
])
