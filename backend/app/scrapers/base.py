"""
Abstract base scraper providing common functionality for all planning portal scrapers.

Includes rate limiting, retry logic, proxy rotation, user-agent rotation,
response caching, error tracking, scheme type classification, and unit count extraction.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# User-Agent rotation pool
# ---------------------------------------------------------------------------
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
]

# ---------------------------------------------------------------------------
# Keyword dictionaries for scheme classification
# ---------------------------------------------------------------------------
SCHEME_KEYWORDS: dict[str, list[str]] = {
    "BTR": [
        "build to rent",
        "btr",
        "private rented sector",
        "prs",
        "build-to-rent",
        "rental apartments",
        "purpose built rental",
        "rental scheme",
        "private rent",
    ],
    "PBSA": [
        "student",
        "pbsa",
        "purpose built student",
        "student accommodation",
        "university accommodation",
        "student housing",
        "student halls",
        "student bedrooms",
    ],
    "Co-living": [
        "co-living",
        "co living",
        "shared living",
        "coliving",
    ],
    "Senior": [
        "retirement",
        "extra care",
        "assisted living",
        "c2 use",
        "later living",
        "care home",
        "sheltered",
        "elderly",
        "over 55",
        "over 60",
        "nursing home",
        "dementia",
        "age restricted",
        "senior living",
        "retirement village",
    ],
    "Affordable": [
        "affordable hous",
        "social rent",
        "shared ownership",
        "council housing",
        "housing association",
        "affordable units",
        "social housing",
        "affordable rent",
    ],
}

# Regex patterns for extracting unit counts from descriptions
UNIT_COUNT_PATTERNS: list[re.Pattern] = [
    re.compile(
        r"(\d[\d,]*)\s*(?:no\.?\s*)?(?:residential\s*)?(?:units|flats|apartments|dwellings|homes|rooms|bed\s*spaces|houses|bungalows|maisonettes)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:erection|construction|development|provision|creation|demolition and erection)\s+of\s+(\d[\d,]*)\s",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\d[\d,]*)\s*(?:x\s*)?(?:\d+\s*bed(?:room)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"comprising\s+(\d[\d,]*)\s",
        re.IGNORECASE,
    ),
    re.compile(
        r"up\s+to\s+(\d[\d,]*)\s*(?:residential\s*)?(?:units|flats|apartments|dwellings|homes)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\d[\d,]*)\s*-?\s*unit",
        re.IGNORECASE,
    ),
    re.compile(
        r"for\s+(\d[\d,]*)\s+(?:houses|dwellings|homes|flats|apartments|residential)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\d[\d,]*)\s*(?:one|two|three|four|1|2|3|4)\s*bed(?:room)?",
        re.IGNORECASE,
    ),
]


@dataclass
class ScraperMetrics:
    """Track scraper performance metrics for a single run."""

    requests_made: int = 0
    requests_successful: int = 0
    requests_failed: int = 0
    applications_found: int = 0
    applications_new: int = 0
    applications_updated: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    @property
    def success_rate(self) -> float:
        if self.requests_made == 0:
            return 0.0
        return self.requests_successful / self.requests_made

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.start_time

    def record_error(self, error: Exception, context: str = "") -> None:
        self.errors.append(
            {
                "error": str(error),
                "type": type(error).__name__,
                "context": context,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "requests_made": self.requests_made,
            "requests_successful": self.requests_successful,
            "requests_failed": self.requests_failed,
            "success_rate": round(self.success_rate, 4),
            "applications_found": self.applications_found,
            "applications_new": self.applications_new,
            "applications_updated": self.applications_updated,
            "errors_count": len(self.errors),
            "elapsed_seconds": round(self.elapsed_seconds, 2),
        }


class BaseScraper(ABC):
    """
    Abstract base class for all planning portal scrapers.

    Provides:
    - Async HTTP client with proxy support and user-agent rotation
    - Rate limiting via a token-bucket approach
    - Automatic retries with exponential back-off (tenacity)
    - In-memory response cache keyed by URL + params
    - Scheme-type classification from description text
    - Unit-count extraction from description text
    - Structured logging via structlog
    - Error tracking and metrics collection
    """

    council_name: str
    council_id: int
    portal_url: str

    def __init__(
        self,
        council_name: str,
        council_id: int,
        portal_url: str,
        rate_limit: float | None = None,
        proxy_url: str | None = None,
    ) -> None:
        self.council_name = council_name
        self.council_id = council_id
        self.portal_url = portal_url.rstrip("/")
        self.rate_limit = rate_limit or (
            settings.SCRAPER_RATE_LIMIT_PERIOD_SECONDS
            / settings.SCRAPER_RATE_LIMIT_REQUESTS
        )
        self.proxy_url = proxy_url or settings.PROXY_URL
        self._ua_index = 0
        self._last_request_time: float = 0.0
        self._cache: dict[str, httpx.Response] = {}
        self.metrics = ScraperMetrics()
        self.log = structlog.get_logger(
            scraper=type(self).__name__,
            council=council_name,
        )
        self.session: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _build_client(self) -> httpx.AsyncClient:
        transport_kwargs: dict[str, Any] = {}
        if self.proxy_url:
            transport_kwargs["proxy"] = self.proxy_url

        return httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.SCRAPER_DEFAULT_TIMEOUT_SECONDS, connect=10.0
            ),
            follow_redirects=True,
            verify=False,  # Many council portals have expired/invalid SSL certs
            headers={"User-Agent": self._next_user_agent()},
            **transport_kwargs,
        )

    async def __aenter__(self) -> "BaseScraper":
        self.session = await self._build_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[override]
        if self.session:
            await self.session.aclose()
            self.session = None

    # ------------------------------------------------------------------
    # User-Agent rotation
    # ------------------------------------------------------------------

    def _next_user_agent(self) -> str:
        ua = USER_AGENTS[self._ua_index % len(USER_AGENTS)]
        self._ua_index += 1
        return ua

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    async def _respect_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit:
            wait = self.rate_limit - elapsed
            self.log.debug("rate_limit_wait", wait_seconds=round(wait, 2))
            await asyncio.sleep(wait)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Cached, retried, rate-limited HTTP request
    # ------------------------------------------------------------------

    def _cache_key(self, method: str, url: str, params: dict | None) -> str:
        raw = f"{method}:{url}:{sorted((params or {}).items())}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @retry(
        retry=retry_if_exception_type(
            (httpx.TransportError, httpx.TimeoutException)
        ),
        stop=stop_after_attempt(settings.SCRAPER_MAX_RETRIES),
        wait=wait_exponential(
            multiplier=settings.SCRAPER_RETRY_DELAY_SECONDS,
            min=2,
            max=60,
        ),
        reraise=True,
    )
    async def fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json_payload: dict | None = None,
        headers: dict | None = None,
        use_cache: bool = True,
    ) -> httpx.Response:
        """
        Perform an HTTP request with rate limiting, retries, caching, and
        user-agent rotation.
        """
        if self.session is None:
            self.session = await self._build_client()

        cache_key = self._cache_key(method, url, params)
        if use_cache and method == "GET" and cache_key in self._cache:
            self.log.debug("cache_hit", url=url)
            return self._cache[cache_key]

        await self._respect_rate_limit()

        self.metrics.requests_made += 1
        merged_headers = {"User-Agent": self._next_user_agent()}
        if headers:
            merged_headers.update(headers)

        self.log.info("http_request", method=method, url=url, params=params)

        try:
            response = await self.session.request(
                method,
                url,
                params=params,
                data=data,
                json=json_payload,
                headers=merged_headers,
            )

            # Respect Retry-After header
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "30"))
                self.log.warning("rate_limited", retry_after=retry_after, url=url)
                await asyncio.sleep(retry_after)
                raise httpx.TransportError(
                    f"Rate limited, retrying after {retry_after}s"
                )

            response.raise_for_status()
            self.metrics.requests_successful += 1

            if use_cache and method == "GET":
                self._cache[cache_key] = response

            return response

        except Exception as exc:
            self.metrics.requests_failed += 1
            self.metrics.record_error(exc, context=f"{method} {url}")
            self.log.error("http_error", url=url, error=str(exc))
            raise

    # ------------------------------------------------------------------
    # Scheme-type classification
    # ------------------------------------------------------------------

    @staticmethod
    def classify_scheme_type(description: str | None) -> str:
        """
        Classify a planning application into a scheme type by matching
        keywords against the description text.  Returns the first matching
        type string or 'Unknown'.

        Return values align with the PlanningApplication.scheme_type column:
        'BTR', 'PBSA', 'Co-living', 'Senior', 'Affordable', 'Mixed',
        'Residential', 'Unknown'.
        """
        if not description:
            return "Unknown"

        text = description.lower()
        matched_types: list[str] = []

        for scheme_type, keywords in SCHEME_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    matched_types.append(scheme_type)
                    break

        if len(matched_types) == 0:
            residential_kw = [
                "residential",
                "dwellings",
                "flats",
                "apartments",
                "housing",
                "houses",
                "bungalows",
                "maisonettes",
                "bedroom",
                "bed ",
                "new build",
                "new dwelling",
                "new homes",
                "storey",
            ]
            for kw in residential_kw:
                if kw in text:
                    return "Residential"
            return "Unknown"
        elif len(matched_types) == 1:
            return matched_types[0]
        else:
            return "Mixed"

    # ------------------------------------------------------------------
    # Unit count extraction
    # ------------------------------------------------------------------

    @staticmethod
    def extract_unit_count(description: str | None) -> int | None:
        """
        Attempt to extract the number of residential units from a planning
        application description using regex patterns.
        """
        if not description:
            return None

        for pattern in UNIT_COUNT_PATTERNS:
            match = pattern.search(description)
            if match:
                raw = match.group(1).replace(",", "")
                try:
                    count = int(raw)
                    if 1 <= count <= 50_000:
                        return count
                except ValueError:
                    continue

        return None

    # ------------------------------------------------------------------
    # Status normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def normalise_status(raw_status: str | None) -> str:
        """Map a raw status string from a portal to a canonical string."""
        if not raw_status:
            return "Unknown"

        status_lower = raw_status.strip().lower()
        mapping: dict[str, str] = {
            "pending": "Pending",
            "pending consideration": "Pending",
            "pending decision": "Pending",
            "under consideration": "Pending",
            "registered": "Submitted",
            "submitted": "Submitted",
            "received": "Submitted",
            "validated": "Validated",
            "valid": "Validated",
            "approved": "Approved",
            "granted": "Approved",
            "permitted": "Approved",
            "permit": "Approved",
            "decided": "Decided",
            "refused": "Refused",
            "refuse": "Refused",
            "rejected": "Refused",
            "withdrawn": "Withdrawn",
            "appeal": "Appeal",
            "appeal lodged": "Appeal",
            "appeal in progress": "Appeal",
        }
        for key, value in mapping.items():
            if key in status_lower:
                return value
        return "Unknown"

    # ------------------------------------------------------------------
    # Postcode extraction
    # ------------------------------------------------------------------

    _POSTCODE_RE = re.compile(
        r"([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})", re.IGNORECASE
    )

    @classmethod
    def extract_postcode(cls, address: str | None) -> str | None:
        """Extract a UK postcode from an address string."""
        if not address:
            return None
        match = cls._POSTCODE_RE.search(address)
        if match:
            pc = match.group(1).upper()
            # Normalise spacing: ensure one space before last 3 chars
            pc = re.sub(r"\s+", "", pc)
            if len(pc) >= 5:
                return f"{pc[:-3]} {pc[-3:]}"
            return pc
        return None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def search_applications(
        self,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Search the portal for planning applications matching the given
        criteria.  Returns a list of raw result dicts (at minimum containing
        a reference and detail URL).
        """
        ...

    @abstractmethod
    async def parse_application(
        self,
        raw: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Convert a raw search result dict into a dict of fields matching
        the PlanningApplication model columns.
        """
        ...

    @abstractmethod
    async def get_application_detail(
        self,
        detail_url: str,
    ) -> dict[str, Any]:
        """
        Fetch the detail page for a single application and return a dict
        of extracted fields.
        """
        ...

    # ------------------------------------------------------------------
    # Convenience: run full scrape cycle
    # ------------------------------------------------------------------

    async def run(self, **search_kwargs: Any) -> list[dict[str, Any]]:
        """
        Execute a complete scrape: search -> fetch details -> parse.
        Returns a list of dicts ready to be persisted as PlanningApplication
        rows.
        """
        self.log.info("scrape_start", council=self.council_name)
        self.metrics = ScraperMetrics()

        results: list[dict[str, Any]] = []
        try:
            raw_results = await self.search_applications(**search_kwargs)
            self.metrics.applications_found = len(raw_results)
            self.log.info(
                "search_complete",
                results_count=len(raw_results),
            )

            for raw in raw_results:
                try:
                    # If search result has a detail_url, fetch the detail
                    # page first to get applicant/agent/full status
                    detail_url = raw.get("detail_url")
                    if detail_url:
                        try:
                            detail = await self.get_application_detail(detail_url)
                            if detail:
                                # Merge detail fields into raw (detail overrides)
                                raw = {**raw, **{k: v for k, v in detail.items() if v}}
                        except Exception as detail_exc:
                            self.log.warning(
                                "detail_fetch_failed",
                                reference=raw.get("reference", "")[:80],
                                error=str(detail_exc)[:120],
                            )
                            # Continue with search-result data only

                    parsed = await self.parse_application(raw)
                    results.append(parsed)
                except Exception as exc:
                    self.metrics.record_error(
                        exc,
                        context=f"parse: {raw.get('reference', 'unknown')}",
                    )
                    self.log.warning(
                        "parse_error",
                        reference=raw.get("reference"),
                        error=str(exc),
                    )
        except Exception as exc:
            self.metrics.record_error(exc, context="search_applications")
            self.log.error("scrape_failed", error=str(exc))
            raise

        self.log.info(
            "scrape_complete",
            **self.metrics.to_dict(),
        )
        return results
