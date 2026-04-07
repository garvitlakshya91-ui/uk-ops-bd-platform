"""
Scraper for the PlanIt API (planit.org.uk).

PlanIt aggregates 20M+ planning applications across 417 UK local authorities.
The API is free, requires no authentication, and provides JSON/GeoJSON endpoints.

Endpoint: GET https://www.planit.org.uk/api/applics/json
Docs:     https://www.planit.org.uk/api/

We use this to supplement council-portal and planning.data.gov.uk data with
broader nationwide coverage, filtering for residential/BTR-relevant applications
and all major (10+ unit) applications.
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timedelta
from typing import Any

import httpx
import structlog

from app.config import settings
from app.scrapers.base import BaseScraper, ScraperMetrics

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# PlanIt API constants
# ---------------------------------------------------------------------------

PLANIT_API_BASE = "https://www.planit.org.uk"
PLANIT_APPLICS_JSON = f"{PLANIT_API_BASE}/api/applics/json"

# Maximum page size the PlanIt API will accept
PLANIT_MAX_PAGE_SIZE = 100

# Keywords used to filter residential/BTR-relevant applications via the
# PlanIt `q` (search query) parameter.  We run separate queries per keyword
# group so that PlanIt performs server-side filtering, reducing bandwidth.
RESIDENTIAL_KEYWORDS: list[str] = [
    "residential",
    "dwelling",
    "flat",
    "apartment",
    "BTR",
    "build-to-rent",
    "student accommodation",
    "PBSA",
    "co-living",
    "housing",
]

# PlanIt app_size values that indicate "major" applications (10+ dwellings)
MAJOR_APP_SIZES: set[str] = {"major", "large"}

# PlanIt app_state values mapped to our canonical status strings
PLANIT_STATUS_MAP: dict[str, str] = {
    "Undecided": "Pending",
    "Permitted": "Approved",
    "Conditions": "Approved",
    "Refused": "Refused",
    "Withdrawn": "Withdrawn",
    "Appeal": "Appeal",
    "Referred": "Pending",
    "Not Available": "Unknown",
    "Other": "Unknown",
}


class PlanItScraper(BaseScraper):
    """
    Scraper for the PlanIt planning application aggregator API.

    Fetches applications in date-range batches, filtering for:
    1. Residential/BTR-relevant applications (keyword-based).
    2. ALL major applications (10+ units) regardless of keywords.

    Results are deduplicated by (uid, authority_name) and mapped to
    PlanningApplication model fields.
    """

    def __init__(
        self,
        rate_limit: float | None = 1.0,  # 1 request per second — be polite
        proxy_url: str | None = None,
        days_back: int = 30,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        super().__init__(
            council_name="PlanIt API",
            council_id=0,  # Will be resolved per-application
            portal_url=PLANIT_API_BASE,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        self.days_back = days_back
        self._start_date = start_date
        self._end_date = end_date
        # Track UIDs already seen within a run to avoid cross-keyword dups
        self._seen_uids: set[str] = set()

    @property
    def effective_start_date(self) -> date:
        if self._start_date:
            return self._start_date
        return date.today() - timedelta(days=self.days_back)

    @property
    def effective_end_date(self) -> date:
        if self._end_date:
            return self._end_date
        return date.today()

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    async def _fetch_page(
        self,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fetch a single page from the PlanIt API and return the records list."""
        try:
            response = await self.fetch(
                PLANIT_APPLICS_JSON,
                params=params,
                use_cache=False,
            )
            data = response.json()

            # The PlanIt JSON endpoint returns {"records": [...]} or a bare list.
            if isinstance(data, dict):
                return data.get("records", [])
            elif isinstance(data, list):
                return data
            else:
                self.log.warning("planit_unexpected_response_type", type=type(data).__name__)
                return []

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                # No results for this query — not an error
                return []
            raise
        except Exception as exc:
            self.log.error("planit_fetch_page_error", error=str(exc), params=params)
            raise

    async def _fetch_all_pages(
        self,
        base_params: dict[str, Any],
        *,
        label: str = "",
    ) -> list[dict[str, Any]]:
        """Paginate through all results for the given query parameters."""
        all_records: list[dict[str, Any]] = []
        page = 1  # PlanIt API uses 1-based pagination
        pg_sz = PLANIT_MAX_PAGE_SIZE

        while True:
            params = {
                **base_params,
                "pg_sz": pg_sz,
                "page": page,
            }

            records = await self._fetch_page(params)

            if not records:
                break

            all_records.extend(records)

            self.log.debug(
                "planit_page_fetched",
                label=label,
                page=page,
                records_on_page=len(records),
                total_so_far=len(all_records),
            )

            # If we got fewer than pg_sz records, we have reached the last page
            if len(records) < pg_sz:
                break

            page += 1

            # Safety cap to avoid runaway pagination (100 pages = 10,000 records)
            if page >= 100:
                self.log.warning(
                    "planit_pagination_cap_reached",
                    label=label,
                    total_records=len(all_records),
                )
                break

        return all_records

    # ------------------------------------------------------------------
    # Date range chunking
    # ------------------------------------------------------------------

    def _date_chunks(self, chunk_days: int = 7) -> list[tuple[date, date]]:
        """Split the overall date range into smaller chunks to stay within
        API result limits and avoid timeouts."""
        chunks: list[tuple[date, date]] = []
        current = self.effective_start_date
        end = self.effective_end_date

        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)

        return chunks

    # ------------------------------------------------------------------
    # Main fetch logic
    # ------------------------------------------------------------------

    async def _fetch_keyword_applications(self) -> list[dict[str, Any]]:
        """Fetch residential/BTR-relevant applications using keyword queries."""
        results: list[dict[str, Any]] = []

        for chunk_start, chunk_end in self._date_chunks():
            for keyword in RESIDENTIAL_KEYWORDS:
                base_params = {
                    "q": keyword,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    # pg_sz + page used for pagination
                }

                records = await self._fetch_all_pages(
                    base_params,
                    label=f"keyword={keyword} {chunk_start}..{chunk_end}",
                )

                # Deduplicate within this run
                for record in records:
                    uid = record.get("uid", "")
                    if uid and uid not in self._seen_uids:
                        self._seen_uids.add(uid)
                        results.append(record)

        return results

    async def _fetch_major_applications(self) -> list[dict[str, Any]]:
        """Fetch ALL major applications (10+ units) regardless of keyword."""
        results: list[dict[str, Any]] = []

        for chunk_start, chunk_end in self._date_chunks():
            for app_size in ("large", "major"):
                base_params = {
                    "app_size": app_size,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    # pg_sz + page used for pagination
                }

                records = await self._fetch_all_pages(
                    base_params,
                    label=f"app_size={app_size} {chunk_start}..{chunk_end}",
                )

                for record in records:
                    uid = record.get("uid", "")
                    if uid and uid not in self._seen_uids:
                        self._seen_uids.add(uid)
                        results.append(record)

        return results

    # ------------------------------------------------------------------
    # Field mapping
    # ------------------------------------------------------------------

    def _map_status(self, raw_state: str | None) -> str:
        """Map PlanIt app_state to our canonical status."""
        if not raw_state:
            return "Unknown"
        return PLANIT_STATUS_MAP.get(raw_state, self.normalise_status(raw_state))

    def _estimate_units_from_app_size(
        self,
        app_size: str | None,
        description: str | None,
    ) -> int | None:
        """Estimate total_units from the description first, falling back to
        app_size heuristics."""
        # Try extracting from description first
        units = self.extract_unit_count(description)
        if units is not None:
            return units

        # Heuristic estimates based on PlanIt app_size
        if not app_size:
            return None

        size_lower = app_size.lower()
        if size_lower == "large":
            return 50  # Reasonable estimate for large-scale applications
        elif size_lower == "major":
            return 15  # "Major" in planning = 10+ dwellings
        elif size_lower == "small":
            return None  # Too variable to estimate

        return None

    def _parse_date(self, date_str: str | None) -> date | None:
        """Parse a date string from the PlanIt API (various formats)."""
        if not date_str:
            return None

        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except (ValueError, AttributeError):
                continue

        return None

    def _map_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Map a single PlanIt API record to PlanningApplication model fields."""
        uid = record.get("uid", "")
        address = record.get("address", "") or ""
        description = record.get("description", "") or ""
        app_type = record.get("app_type", "")
        app_state = record.get("app_state", "")
        app_size = record.get("app_size", "")
        authority_name = record.get("authority_name", "")

        # Postcode: PlanIt sometimes provides it, otherwise extract from address
        postcode = record.get("postcode") or self.extract_postcode(address)

        # Coordinates
        lat = record.get("lat")
        lng = record.get("lng")
        if lat is not None:
            try:
                lat = float(lat)
            except (ValueError, TypeError):
                lat = None
        if lng is not None:
            try:
                lng = float(lng)
            except (ValueError, TypeError):
                lng = None

        # Unit estimation
        total_units = self._estimate_units_from_app_size(app_size, description)

        # Scheme classification
        scheme_type = self.classify_scheme_type(description)

        # BTR / PBSA flags
        is_btr = scheme_type == "BTR"
        is_pbsa = scheme_type == "PBSA"
        is_affordable = scheme_type == "Affordable"

        # Build the portal URL from PlanIt's url field or construct one
        portal_url = record.get("url", "")
        if not portal_url and uid:
            portal_url = f"https://www.planit.org.uk/planapplic/{uid}"

        return {
            "reference": uid,
            "address": address,
            "postcode": postcode,
            "description": description,
            "application_type": app_type,
            "status": self._map_status(app_state),
            "decision_date": self._parse_date(record.get("decided_date")),
            "submitted_date": self._parse_date(record.get("start_date")),
            "latitude": lat,
            "longitude": lng,
            "portal_url": portal_url,
            "scheme_type": scheme_type,
            "total_units": total_units,
            "is_btr": is_btr,
            "is_pbsa": is_pbsa,
            "is_affordable": is_affordable,
            "source": "planit",
            "ward": record.get("ward", ""),
            # authority_name is stored in raw_data so ingestion can resolve council_id
            "_authority_name": authority_name,
            "raw_data": {
                "planit_uid": uid,
                "app_type": app_type,
                "app_state": app_state,
                "app_size": app_size,
                "authority_name": authority_name,
                "decided_date": record.get("decided_date"),
                "start_date": record.get("start_date"),
                "lat": lat,
                "lng": lng,
                "url": record.get("url"),
            },
        }

    # ------------------------------------------------------------------
    # BaseScraper abstract methods
    # ------------------------------------------------------------------

    async def search_applications(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Fetch applications from PlanIt API across all keywords + major apps."""
        self.log.info(
            "planit_search_start",
            start_date=self.effective_start_date.isoformat(),
            end_date=self.effective_end_date.isoformat(),
        )

        # Reset dedup tracker
        self._seen_uids.clear()

        # Fetch keyword-based residential applications
        keyword_results = await self._fetch_keyword_applications()
        self.log.info(
            "planit_keyword_results",
            count=len(keyword_results),
        )

        # Fetch all major applications
        major_results = await self._fetch_major_applications()
        self.log.info(
            "planit_major_results",
            count=len(major_results),
        )

        total = len(keyword_results) + len(major_results)
        self.log.info(
            "planit_search_complete",
            keyword_count=len(keyword_results),
            major_count=len(major_results),
            total_unique=total,
        )

        return keyword_results + major_results

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Map a PlanIt record to PlanningApplication fields."""
        return self._map_record(raw)

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """PlanIt API records contain all fields inline -- no detail page needed."""
        return {}

    # ------------------------------------------------------------------
    # Override run() for efficiency
    # ------------------------------------------------------------------

    async def run(self, **search_kwargs: Any) -> list[dict[str, Any]]:
        """Execute a complete PlanIt scrape.

        Overrides BaseScraper.run() to skip the per-record detail fetch
        (unnecessary for PlanIt since all data comes from the API response).
        """
        self.log.info("planit_scrape_start")
        self.metrics = ScraperMetrics()

        try:
            raw_results = await self.search_applications(**search_kwargs)
            self.metrics.applications_found = len(raw_results)

            results: list[dict[str, Any]] = []
            for raw in raw_results:
                try:
                    parsed = self._map_record(raw)
                    results.append(parsed)
                except Exception as exc:
                    self.metrics.record_error(
                        exc,
                        context=f"parse: {raw.get('uid', 'unknown')}",
                    )
                    self.log.warning(
                        "planit_parse_error",
                        uid=raw.get("uid"),
                        error=str(exc),
                    )

            self.log.info("planit_scrape_complete", **self.metrics.to_dict())
            return results

        except Exception as exc:
            self.metrics.record_error(exc, context="planit_search")
            self.log.error("planit_scrape_failed", error=str(exc))
            raise
