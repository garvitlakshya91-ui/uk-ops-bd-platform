import structlog
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import engine, Base

from app.api.applications import router as applications_router
from app.api.companies import router as companies_router
from app.api.pipeline import router as pipeline_router
from app.api.schemes import router as schemes_router
from app.api.dashboard import router as dashboard_router
from app.api.alerts import router as alerts_router
from app.api.scrapers import router as scrapers_router
from app.api.frontend_adapters import router as frontend_adapters_router
from app.api.auth import router as auth_router

logger = structlog.get_logger()


def _init_sentry() -> None:
    if settings.SENTRY_DSN:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            integrations=[FastApiIntegration(), SqlalchemyIntegration()],
            traces_sample_rate=0.2,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    _init_sentry()

    # Import all models so Base.metadata is complete, then create tables
    import app.models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    logger.info("application_started", version=settings.APP_VERSION)
    yield
    logger.info("application_shutdown")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Backend API for the UK Operations BD Platform — planning applications, "
    "company intelligence, pipeline management and scraper orchestration.",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(applications_router)
app.include_router(companies_router)
app.include_router(pipeline_router)
app.include_router(schemes_router)
app.include_router(dashboard_router)
app.include_router(alerts_router)
app.include_router(scrapers_router)
app.include_router(frontend_adapters_router)
app.include_router(auth_router)


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(
        "unhandled_exception",
        path=str(request.url),
        method=request.method,
        error=str(exc),
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred."},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Health"])
def health_check():
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "service": settings.APP_NAME,
    }


@app.get("/", tags=["Health"])
def root():
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
    }
