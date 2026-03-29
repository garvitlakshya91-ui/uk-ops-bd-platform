"""
Scraper for Find a Tender (https://www.find-tender.service.gov.uk).

Uses the official FAT OCDS JSON API instead of HTML scraping.
The HTML detail pages return 403 Forbidden; the OCDS API returns
complete structured data without restrictions.

API endpoint:
    GET https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages

Supported parameters (verified against live API):
    updatedFrom   — ISO 8601 datetime, e.g. "2025-01-01T00:00:00Z"
    updatedTo     — ISO 8601 datetime
    stages        — "award", "tender", "planning", "contract" (comma-separated)
    limit         — results per page (max 100)
    cursor        — opaque pagination token from links.next

The API does NOT support keyword search — filtering is done client-side
against CPV codes and title/description keywords.

OCDS release structure:
    releases[].ocid                          — unique notice identifier
    releases[].date                          — publication date (ISO 8601)
    releases[].tag[]                         — ["award"], ["tender"], etc.
    releases[].tender.title
    releases[].tender.description
    releases[].tender.value.amount
    releases[].tender.contractPeriod.startDate / endDate
    releases[].tender.items[].classification.id   — CPV code
    releases[].buyer.name                    — contracting authority
    releases[].awards[].suppliers[].name    — awarded supplier
    releases[].awards[].contractPeriod      — actual contract dates (awards)

CPV codes of interest:
- 70330000 — Property management services
- 70332000 — Housing management services for owned property
- 70333000 — Housing management services for rented property
- 79993000 — Building and facilities management services
- 50700000 — Maintenance and repair services for building installations
- 98341000 — Accommodation services
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import structlog

from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

OCDS_API_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
BASE_URL = "https://www.find-tender.service.gov.uk"

# CPV codes relevant to housing/property management (used for client-side filtering)
HOUSING_CPV_CODES: set[str] = {
    "70330000",  # Property management services
    "70332000",  # Housing management services (owned)
    "70333000",  # Housing management services (rented)
    "70300000",  # Agency services for real estate
    "70332200",  # Commercial property management
    "79993000",  # Building and facilities management
    "50700000",  # Building maintenance and repair
    "98341000",  # Accommodation services
    "85311000",  # Social welfare services with accommodation
    "85320000",  # Social services without accommodation
}

# Keywords used for client-side title/description filtering (if no CPV match)
_HOUSING_KEYWORDS = [
    "housing management",
    "property management",
    "social housing",
    "affordable housing",
    "registered provider",
    "housing association",
    "sheltered housing",
    "supported housing",
    "extra care",
    "tenant management",
    "estate management",
    "facilities management",
    "leasehold management",
    "void management",
]

# Stages to fetch — award has supplier info; valid values: planning, tender, award
_STAGES = ["award", "tender"]

# Max results per API page
_PAGE_SIZE = 100

# Max pages per run (safety cap — ~100*500 = 50,000 records max)
_MAX_PAGES = 500


class FindATenderScraper(BaseScraper):
    """
    Scraper for the Find a Tender service using the official OCDS JSON API.

    Paginates through all award/contract releases updated since published_from,
    then filters client-side for housing-related CPV codes and keywords.
    """

    def __init__(
        self,
        rate_limit: float | None = 2.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            council_name="Find a Tender",
            council_id=0,
            portal_url=BASE_URL,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        search_terms: list[str] | None = None,  # kept for API compatibility, unused
        cpv_codes: list[str] | None = None,
        published_from: date | None = None,
        published_to: date | None = None,
        max_pages: int = _MAX_PAGES,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Fetch housing-related contract notices from the FAT OCDS JSON API.

        Paginates through award/contract releases within the date window and
        filters client-side for our CPV codes and housing keywords.
        """
        if published_from is None:
            published_from = date.today() - timedelta(days=180)
        if published_to is None:
            published_to = date.today()

        housing_cpvs = set(cpv_codes) if cpv_codes else HOUSING_CPV_CODES

        updated_from_str = f"{published_from.isoformat()}T00:00:00Z"
        updated_to_str = f"{published_to.isoformat()}T23:59:59Z"

        all_results: list[dict[str, Any]] = []
        seen_ocids: set[str] = set()
        pages = 0
        total_checked = 0

        # Start with the initial URL
        # httpx serialises list values as repeated params: stages=award&stages=contract
        params: dict[str, Any] = {
            "updatedFrom": updated_from_str,
            "updatedTo": updated_to_str,
            "stages": _STAGES,
            "limit": _PAGE_SIZE,
        }
        next_url: str | None = OCDS_API_URL

        while next_url and pages < max_pages:
            if pages == 0:
                resp = await self.fetch(next_url, params=params, use_cache=False)
            else:
                # Follow the cursor URL directly (contains all params + cursor)
                resp = await self.fetch(next_url, use_cache=False)

            try:
                data = resp.json()
            except Exception:
                self.log.warning("fat_ocds_json_error", page=pages)
                break

            releases = data.get("releases") or []
            if not releases:
                break

            total_checked += len(releases)

            for release in releases:
                ocid = release.get("ocid", "")
                if not ocid or ocid in seen_ocids:
                    continue
                seen_ocids.add(ocid)

                if _is_housing_release(release, housing_cpvs):
                    parsed = _parse_release(release)
                    if parsed:
                        all_results.append(parsed)

            pages += 1
            next_url = (data.get("links") or {}).get("next")

            self.log.info(
                "fat_ocds_page",
                page=pages,
                checked=total_checked,
                matched=len(all_results),
            )

        self.metrics.applications_found = len(all_results)
        self.log.info(
            "fat_search_complete",
            total_checked=total_checked,
            matched=len(all_results),
            pages=pages,
        )
        return all_results

    # ------------------------------------------------------------------
    # Parse / Detail
    # ------------------------------------------------------------------

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Return structured contract fields from an OCDS search result."""
        return {
            "notice_id": raw.get("notice_id", ""),
            "title": raw.get("title", ""),
            "contracting_authority": raw.get("contracting_authority", ""),
            "supplier": raw.get("supplier", ""),
            "contract_value": raw.get("contract_value"),
            "currency": raw.get("currency", "GBP"),
            "start_date": raw.get("start_date"),
            "end_date": raw.get("end_date"),
            "cpv_codes": raw.get("cpv_codes", []),
            "description": raw.get("description", ""),
            "notice_type": raw.get("notice_type", ""),
            "published_date": raw.get("published_date", ""),
            "detail_url": raw.get("detail_url", ""),
        }

    async def get_application_detail(self, notice_id_or_url: str) -> dict[str, Any]:
        """Look up a single OCDS release by OCID via the API."""
        if notice_id_or_url.startswith("http"):
            ref = _extract_notice_ref_from_url(notice_id_or_url)
        else:
            ref = notice_id_or_url

        if not ref:
            return {}

        try:
            resp = await self.fetch(
                OCDS_API_URL,
                params={"updatedFrom": "2020-01-01T00:00:00Z", "limit": 1},
                use_cache=True,
            )
            data = resp.json()
            releases = data.get("releases") or []
            # Filter for matching OCID
            for release in releases:
                if ref in (release.get("ocid", "") or ""):
                    return _parse_release(release) or {}
        except Exception as exc:
            self.log.warning("fat_ocds_detail_failed", ref=ref, error=str(exc))

        return {}


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _is_housing_release(release: dict[str, Any], housing_cpvs: set[str]) -> bool:
    """Return True if this OCDS release is housing/FM related."""
    tender = release.get("tender") or {}

    # Check CPV codes first (most reliable)
    for item in tender.get("items") or []:
        cpv_id = (item.get("classification") or {}).get("id", "")
        if cpv_id in housing_cpvs:
            return True

    # Fall back to keyword match in title/description
    title = (tender.get("title") or "").lower()
    description = (tender.get("description") or "").lower()
    combined = f"{title} {description}"

    return any(kw in combined for kw in _HOUSING_KEYWORDS)


def _parse_release(release: dict[str, Any]) -> dict[str, Any] | None:
    """Extract flat contract fields from an OCDS release dict."""
    ocid = release.get("ocid", "")
    if not ocid:
        return None

    tender = release.get("tender") or {}
    buyer = release.get("buyer") or {}
    awards = release.get("awards") or []

    title = tender.get("title") or ""
    contracting_authority = buyer.get("name", "")

    # Supplier and value — prefer most recent award
    supplier = ""
    contract_value: float | None = None
    start_date: date | None = None
    end_date: date | None = None

    if awards:
        award = awards[-1]
        suppliers = award.get("suppliers") or []
        if suppliers:
            supplier = suppliers[0].get("name", "")

        award_val = (award.get("value") or {}).get("amount")
        if award_val is not None:
            try:
                contract_value = float(award_val)
            except (TypeError, ValueError):
                pass

        # Award period takes precedence (actual vs estimated)
        award_period = award.get("contractPeriod") or {}
        start_date = _parse_iso_date(award_period.get("startDate"))
        end_date = _parse_iso_date(award_period.get("endDate"))

    # Tender value as fallback
    if contract_value is None:
        tender_val = (tender.get("value") or {}).get("amount")
        if tender_val is not None:
            try:
                contract_value = float(tender_val)
            except (TypeError, ValueError):
                pass

    # Tender period as fallback
    if start_date is None or end_date is None:
        tender_period = tender.get("contractPeriod") or {}
        start_date = start_date or _parse_iso_date(tender_period.get("startDate"))
        end_date = end_date or _parse_iso_date(tender_period.get("endDate"))

    # CPV codes
    cpv_codes: list[str] = []
    for item in tender.get("items") or []:
        cpv_id = (item.get("classification") or {}).get("id", "")
        if cpv_id and re.match(r"^\d{8}$", cpv_id):
            cpv_codes.append(cpv_id)

    # Notice type
    tags = release.get("tag") or []
    notice_type = ", ".join(tags)

    # Detail URL
    notice_ref = _extract_notice_ref(ocid)
    detail_url = f"{BASE_URL}/Notice/{notice_ref}" if notice_ref else ""

    published_date = (release.get("date") or "")[:10]

    return {
        "notice_id": ocid,
        "notice_ref": notice_ref,
        "title": title,
        "contracting_authority": contracting_authority,
        "supplier": supplier,
        "contract_value": contract_value,
        "currency": "GBP",
        "start_date": start_date,
        "end_date": end_date,
        "cpv_codes": cpv_codes,
        "description": tender.get("description") or "",
        "notice_type": notice_type,
        "published_date": published_date,
        "detail_url": detail_url,
    }


def _parse_iso_date(value: str | None) -> date | None:
    """Parse an ISO 8601 date string."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None


def _extract_notice_ref(ocid: str) -> str:
    """Extract the notice reference from a FAT OCID."""
    prefix = "ocds-h6vhtk-"
    if ocid.startswith(prefix):
        return ocid[len(prefix):]
    parts = ocid.split("-", 3)
    return parts[-1] if len(parts) >= 3 else ocid


def _extract_notice_ref_from_url(url: str) -> str:
    """Extract notice reference from a FAT detail URL."""
    match = re.search(r"/Notice/([^?#/]+)", url, re.IGNORECASE)
    return match.group(1) if match else ""
