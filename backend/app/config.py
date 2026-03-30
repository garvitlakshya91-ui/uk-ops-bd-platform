from pydantic_settings import BaseSettings
from pydantic import model_validator
from typing import Optional


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    # Application
    APP_NAME: str = "UK Ops BD Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"

    # Redis / Celery
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = ""
    CELERY_RESULT_BACKEND: str = ""

    @model_validator(mode="after")
    def _derive_celery_urls(self) -> "Settings":
        """Derive Celery broker/backend URLs from REDIS_URL if not explicitly set."""
        base = self.REDIS_URL.rsplit("/", 1)[0]  # strip db number
        if not self.CELERY_BROKER_URL:
            self.CELERY_BROKER_URL = f"{base}/1"
        if not self.CELERY_RESULT_BACKEND:
            self.CELERY_RESULT_BACKEND = f"{base}/2"
        return self

    # External API Keys
    COMPANIES_HOUSE_API_KEY: str = ""
    APOLLO_API_KEY: str = ""
    HUNTER_API_KEY: str = ""

    # EPC Open Data Communities API
    EPC_API_KEY: str = ""  # Register at https://epc.opendatacommunities.org/

    # HM Land Registry CCOD dataset
    # Download the latest file from:
    #   https://use-land-property-data.service.gov.uk/datasets/ccod
    # and set HMLR_CCOD_LOCAL_PATH to the downloaded CSV/ZIP path to avoid
    # re-downloading (~120 MB) on every run.
    HMLR_CCOD_DOWNLOAD_URL: str = "https://use-land-property-data.service.gov.uk/datasets/ccod/download"
    HMLR_CCOD_LOCAL_PATH: str = ""  # e.g. /data/hmlr/CCOD_FULL_2024_03.zip

    # Elasticsearch
    ELASTICSEARCH_URL: str = "http://localhost:9200"

    # Planning Data API
    PLANNING_DATA_API_URL: str = "https://www.planning.data.gov.uk/api/v1"

    # Contracts Finder API
    CONTRACTS_FINDER_API_URL: str = "https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search"

    # SMTP / Email Alerts
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_EMAIL: str = "alerts@ukopsbd.com"
    SMTP_USE_TLS: bool = True

    # Slack
    SLACK_WEBHOOK_URL: str = ""

    # AWS S3
    S3_BUCKET_NAME: str = "uk-ops-bd-documents"
    S3_REGION: str = "eu-west-2"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""

    # Scraper settings
    SCRAPER_RATE_LIMIT_REQUESTS: int = 10
    SCRAPER_RATE_LIMIT_PERIOD_SECONDS: int = 60
    SCRAPER_DEFAULT_TIMEOUT_SECONDS: int = 30
    SCRAPER_MAX_RETRIES: int = 3
    SCRAPER_RETRY_DELAY_SECONDS: int = 5

    # Proxy
    PROXY_URL: Optional[str] = None

    # Sentry
    SENTRY_DSN: str = ""

    # JWT / Auth
    JWT_SECRET_KEY: str = "change-me-in-production-use-openssl-rand-hex-32"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 480  # 8 hours
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:3010", "http://localhost:5173"]

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
    }


settings = Settings()
