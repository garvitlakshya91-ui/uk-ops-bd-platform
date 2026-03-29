"""
Scraper for the Brownfield Land Register (planning.data.gov.uk).

This is the highest-value nationwide data source — 38,000+ development sites
across 190+ councils with addresses, coordinates, dwelling counts, and
planning permission status.

Dataset: brownfield-land
Endpoint: GET https://www.planning.data.gov.uk/entity.json?dataset=brownfield-land
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
import structlog

from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

# BD relevance filter: minimum dwellings to be worth tracking
MIN_DWELLINGS_BD_RELEVANT = 5

# Permission status mapping → our canonical status
PERMISSION_STATUS_MAP: dict[str, str] = {
    "permissioned": "Approved",
    "not-permissioned": "Pre-Application",
    "pending-decision": "Pending",
}

PERMISSION_TYPE_MAP: dict[str, str] = {
    "full-planning-permission": "Full",
    "outline-planning-permission": "Outline",
    "reserved-matters-approval": "Reserved Matters",
    "permission-in-principle": "Permission in Principle",
    "technical-details-consent": "Technical Details",
    "other": "Other",
}


class BrownfieldScraper(BaseScraper):
    """
    Scraper for the brownfield-land dataset on planning.data.gov.uk.

    This dataset contains development sites registered by councils as
    brownfield land suitable for housing. Each record includes address,
    coordinates, dwelling counts, and planning permission details.
    """

    BASE_URL = "https://www.planning.data.gov.uk"

    def __init__(
        self,
        rate_limit: float | None = 1.0,
        proxy_url: str | None = None,
        min_dwellings: int = MIN_DWELLINGS_BD_RELEVANT,
    ) -> None:
        super().__init__(
            council_name="Brownfield Register",
            council_id=0,
            portal_url=self.BASE_URL,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        self.min_dwellings = min_dwellings

    async def _api_get(
        self,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.BASE_URL}/entity.json"
        response = await self.fetch(url, params=params, use_cache=False)
        return response.json()

    async def search_applications(
        self,
        *,
        max_pages: int = 400,
        page_size: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Paginate through the brownfield-land dataset, filtering for
        sites with sufficient dwelling counts.
        """
        all_results: list[dict[str, Any]] = []
        page = 1

        while page <= max_pages:
            params: dict[str, Any] = {
                "dataset": "brownfield-land",
                "limit": page_size,
                "offset": (page - 1) * page_size,
            }

            self.log.info("brownfield_page_request", page=page, offset=params["offset"])

            try:
                data = await self._api_get(params=params)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    import asyncio
                    retry_after = int(exc.response.headers.get("Retry-After", "60"))
                    self.log.warning("api_rate_limit", retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                raise

            entities = data.get("entities", [])
            if not entities:
                break

            # Filter for BD-relevant sites (min dwelling threshold)
            for entity in entities:
                try:
                    max_dwellings = int(entity.get("maximum-net-dwellings") or 0)
                except (ValueError, TypeError):
                    max_dwellings = 0

                if max_dwellings >= self.min_dwellings:
                    all_results.append(entity)

            self.log.info(
                "brownfield_page_received",
                page=page,
                raw_count=len(entities),
                filtered_count=len(all_results),
            )

            if len(entities) < page_size:
                break

            page += 1

        self.metrics.applications_found = len(all_results)
        return all_results

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """Not needed — all data is in the search results."""
        return {}

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Map a brownfield-land entity to PlanningApplication fields.
        """
        address = raw.get("site-address", "") or ""
        postcode = self.extract_postcode(address)
        notes = raw.get("notes", "") or ""

        # Parse dwellings
        try:
            max_dwellings = int(raw.get("maximum-net-dwellings") or 0)
        except (ValueError, TypeError):
            max_dwellings = None
        try:
            min_dwellings = int(raw.get("minimum-net-dwellings") or 0)
        except (ValueError, TypeError):
            min_dwellings = None

        num_units = max_dwellings or min_dwellings

        # Parse coordinates from POINT string
        lat, lng = None, None
        point = raw.get("point", "")
        if point:
            m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", point)
            if m:
                lng = float(m.group(1))
                lat = float(m.group(2))

        # Permission status
        perm_status = raw.get("planning-permission-status", "")
        status = PERMISSION_STATUS_MAP.get(perm_status, "Unknown")

        perm_type = raw.get("planning-permission-type", "")
        app_type = PERMISSION_TYPE_MAP.get(perm_type, perm_type)

        # Permission date
        perm_date = self._parse_date(raw.get("planning-permission-date"))

        # Build description from available fields
        ownership = raw.get("ownership-status", "")
        deliverable = raw.get("deliverable", "")
        hectares = raw.get("hectares", "")
        description_parts = []
        if num_units:
            description_parts.append(f"Brownfield site for {num_units} dwellings")
        else:
            description_parts.append("Brownfield development site")
        if hectares:
            description_parts.append(f"({hectares} hectares)")
        if ownership:
            description_parts.append(f"- {ownership.replace('-', ' ')}")
        if deliverable == "yes":
            description_parts.append("- deliverable")
        if notes:
            description_parts.append(f". {notes}")
        description = " ".join(description_parts)

        # Classify scheme type from notes/description
        scheme_type = self.classify_scheme_type(description + " " + notes)
        if scheme_type == "Unknown" and num_units:
            scheme_type = "Residential"

        return {
            "reference": raw.get("reference", str(raw.get("entity", ""))),
            "organisation_entity": str(raw.get("organisation-entity", "")),
            "address": address,
            "postcode": postcode,
            "description": description,
            "application_type": app_type,
            "status": status,
            "scheme_type": scheme_type,
            "num_units": num_units,
            "submission_date": perm_date,
            "decision_date": perm_date,
            "latitude": lat,
            "longitude": lng,
            "source": "brownfield-register",
        }

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except (ValueError, TypeError):
            try:
                return datetime.strptime(str(value), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                return None
