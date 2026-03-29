"""
Scraper for the Planning Data API (planning.data.gov.uk).

This is the primary data source — a REST API maintained by DLUHC providing
nationwide planning application data. The API is free, rate-limited, and
returns JSON.

Endpoint: GET /api/v1/planning-application
Docs:     https://www.planning.data.gov.uk/docs
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx
import structlog

from app.config import settings
from app.scrapers.base import BaseScraper, ScraperMetrics

logger = structlog.get_logger(__name__)

# Residential application type codes relevant to our pipeline
RESIDENTIAL_APPLICATION_TYPES = [
    "full",
    "outline",
    "reserved-matters",
    "hybrid",
    "planning-permission",
]

# Map API status values to our canonical statuses
STATUS_MAP: dict[str, str] = {
    "not-started": "Submitted",
    "in-progress": "Pending",
    "determined": "Decided",
    "withdrawn": "Withdrawn",
    "appealed": "Appeal",
}


class PlanningDataAPIScraper(BaseScraper):
    """
    Scraper for the planning.data.gov.uk REST API.

    This is a nationwide data source and does not belong to a single council,
    but results are filtered per-council using the local-authority-district
    (LAD) statistical geography codes.
    """

    BASE_URL = "https://www.planning.data.gov.uk"

    def __init__(
        self,
        council_name: str = "Planning Data API",
        council_id: int = 0,
        portal_url: str = "",
        rate_limit: float | None = 2.0,
        proxy_url: str | None = None,
        lad_code: str | None = None,
    ) -> None:
        super().__init__(
            council_name=council_name,
            council_id=council_id,
            portal_url=portal_url or self.BASE_URL,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        self.lad_code = lad_code

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    async def _api_get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue a GET request to the Planning Data API and return JSON."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        response = await self.fetch(url, params=params, use_cache=False)
        return response.json()

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        lad_code: str | None = None,
        max_pages: int = 50,
        page_size: int = 100,
        start_offset: int = 0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Paginate through the Planning Data API searching for residential
        planning applications.

        Args:
            date_from: Earliest submission date (defaults to 90 days ago).
            date_to:   Latest submission date (defaults to today).
            lad_code:  Local Authority District code to filter by.
            max_pages: Safety limit on page iterations.
            page_size: Results per page (API maximum is typically 100).

        Returns:
            List of raw application dicts from the API.
        """
        if date_from is None:
            date_from = date.today() - timedelta(days=90)
        if date_to is None:
            date_to = date.today()

        effective_lad = lad_code or self.lad_code
        all_results: list[dict[str, Any]] = []
        page = 1

        while page <= max_pages:
            params: dict[str, Any] = {
                "dataset": "planning-application",
                "entry_date__gte": date_from.isoformat(),
                "entry_date__lte": date_to.isoformat(),
                "limit": page_size,
                "offset": start_offset + (page - 1) * page_size,
            }

            if effective_lad:
                params["organisation-entity"] = effective_lad

            self.log.info(
                "api_page_request",
                page=page,
                params=params,
            )

            try:
                data = await self._api_get("entity.json", params=params)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    retry_after = int(
                        exc.response.headers.get("Retry-After", "60")
                    )
                    self.log.warning(
                        "api_rate_limit",
                        retry_after=retry_after,
                        page=page,
                    )
                    await asyncio.sleep(retry_after)
                    continue  # retry same page
                raise

            entities = data.get("entities", data.get("results", []))
            if not entities:
                self.log.info("api_pagination_done", total=len(all_results))
                break

            all_results.extend(entities)
            self.log.info(
                "api_page_received",
                page=page,
                page_count=len(entities),
                cumulative=len(all_results),
            )

            # If we received fewer than page_size, we are on the last page
            if len(entities) < page_size:
                break

            page += 1

        self.metrics.applications_found = len(all_results)
        return all_results

    # ------------------------------------------------------------------
    # Detail
    # ------------------------------------------------------------------

    async def get_application_detail(
        self,
        detail_url: str,
    ) -> dict[str, Any]:
        """
        Fetch a single planning application by its API detail URL / entity
        reference.
        """
        response = await self.fetch(detail_url, use_cache=True)
        return response.json()

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def parse_application(
        self,
        raw: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Map an API response entity to a dict matching our
        PlanningApplication model fields.
        """
        description = raw.get("description", "") or ""
        address = raw.get("address", "") or raw.get("site-address", "") or ""
        reference = (
            raw.get("reference", "")
            or raw.get("planning-application", "")
            or raw.get("entity", "")
        )

        # Attempt to get the raw status and normalise
        raw_status = raw.get("status", "") or raw.get("planning-decision", "")
        status = STATUS_MAP.get(raw_status, self.normalise_status(raw_status))

        # Parse dates
        submission_date = self._parse_date(
            raw.get("entry-date")
            or raw.get("start-date")
            or raw.get("submission-date")
        )
        decision_date = self._parse_date(
            raw.get("decision-date") or raw.get("end-date")
        )

        # Scheme classification and unit extraction
        scheme_type = self.classify_scheme_type(description)
        num_units = self.extract_unit_count(description)
        postcode = self.extract_postcode(address)

        return {
            "reference": str(reference),
            "council_id": self.council_id,
            "organisation_entity": str(raw.get("organisation-entity", "")),
            "address": address,
            "postcode": postcode,
            "description": description,
            "applicant_name": raw.get("applicant-name"),
            "agent_name": raw.get("agent-name"),
            "application_type": raw.get("planning-application-type", ""),
            "status": status,
            "scheme_type": scheme_type,
            "num_units": num_units,
            "submission_date": submission_date,
            "decision_date": decision_date,
            "documents_url": raw.get("document-url"),
            "raw_html": None,
            "latitude": self._safe_float(raw.get("point", {}).get("lat") if isinstance(raw.get("point"), dict) else raw.get("latitude")),
            "longitude": self._safe_float(raw.get("point", {}).get("lng") if isinstance(raw.get("point"), dict) else raw.get("longitude")),
        }

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Bulk: search multiple LADs
    # ------------------------------------------------------------------

    async def search_multiple_lads(
        self,
        lad_codes: list[str],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Convenience method to search across several local authority
        district codes sequentially, aggregating all results.
        """
        all_results: list[dict[str, Any]] = []
        for lad in lad_codes:
            self.log.info("search_lad", lad_code=lad)
            results = await self.search_applications(lad_code=lad, **kwargs)
            all_results.extend(results)
        return all_results
