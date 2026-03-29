"""
Scraper for the Contracts Finder API (https://www.contractsfinder.service.gov.uk).

Searches for housing management, property management, and facilities
management contracts published on Contracts Finder. Unlike Find a Tender
(which covers higher-value above-threshold contracts), Contracts Finder
covers lower-value below-threshold opportunities published by UK public
sector bodies.

The API returns data in OCDS (Open Contracting Data Standard) format.
Each release contains tender, award, and contract information which we
map to our internal scheme/contract model.

CPV codes of interest (same set as find_a_tender.py):
- 70330000 -- Property management services
- 70332000 -- Housing management services for owned property
- 70333000 -- Housing management services for rented property
- 79993000 -- Building and facilities management services
- 50700000 -- Maintenance and repair services for building installations
- 98341000 -- Accommodation services
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any

import structlog

from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Contracts Finder API endpoint
# ---------------------------------------------------------------------------
API_BASE_URL = "https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search"

# ---------------------------------------------------------------------------
# Housing-related keyword and CPV filters
# (Aligned with scheme_ingest.py HOUSING_KEYWORDS and HOUSING_CPV_CODES)
# ---------------------------------------------------------------------------
HOUSING_KEYWORDS: list[str] = [
    "housing", "tenant", "residential", "dwelling", "sheltered",
    "supported living", "social housing", "affordable", "rented",
    "lettings", "property management", "estate management",
    "housing management", "housing maintenance", "registered provider",
    "housing association", "accommodation", "homelessness",
    "repairs and maintenance", "voids", "care home", "extra care",
    "retirement", "supported housing", "temporary accommodation",
]

HOUSING_CPV_CODES: set[str] = {
    "70330000", "70332000", "70333000", "79993000", "98341000",
    "50700000", "45211000", "45211341", "45211340",
}

# Search terms used to query the API -- broad enough to capture relevant
# housing/FM contracts while keeping the result set manageable.
SEARCH_TERMS: list[str] = [
    "housing management",
    "property management",
    "facilities management housing",
    "tenant management",
    "estate management",
    "registered provider",
    "affordable housing management",
    "sheltered housing",
    "supported housing",
    "housing association",
]

# Maximum pages to fetch per search term (100 results per page)
DEFAULT_MAX_PAGES: int = 10
DEFAULT_PAGE_SIZE: int = 100


class ContractsFinderScraper(BaseScraper):
    """
    Scraper for the UK Contracts Finder OCDS API.

    Searches for housing and property management contracts, parses OCDS
    release data, and returns normalised contract records suitable for
    ingestion into the scheme_contracts table.

    Usage::

        async with ContractsFinderScraper() as scraper:
            results = await scraper.run()
            # results is a list of dicts with contract fields
    """

    def __init__(
        self,
        rate_limit: float | None = 2.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            council_name="Contracts Finder",
            council_id=0,
            portal_url=API_BASE_URL,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        search_terms: list[str] | None = None,
        published_from: date | None = None,
        published_to: date | None = None,
        stages: str = "tender,award",
        max_pages: int = DEFAULT_MAX_PAGES,
        page_size: int = DEFAULT_PAGE_SIZE,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Search Contracts Finder for OCDS releases matching housing/FM
        keywords.

        Iterates over each search term, paginates through all results,
        deduplicates by release ID, and filters to housing-related
        notices only.

        Args:
            search_terms: Keywords to search for. Defaults to SEARCH_TERMS.
            published_from: Earliest publication date. Defaults to 180 days ago.
            published_to: Latest publication date. Defaults to today.
            stages: Comma-separated OCDS stages to include.
            max_pages: Maximum pages to fetch per search term.
            page_size: Number of results per page (max 100).

        Returns:
            List of raw OCDS release dicts that passed the housing filter.
        """
        if search_terms is None:
            search_terms = SEARCH_TERMS
        if published_from is None:
            published_from = date.today() - timedelta(days=180)
        if published_to is None:
            published_to = date.today()

        all_results: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for term in search_terms:
            self.log.info("contracts_finder_search", term=term)
            try:
                releases = await self._search_term(
                    term=term,
                    published_from=published_from,
                    published_to=published_to,
                    stages=stages,
                    max_pages=max_pages,
                    page_size=page_size,
                )
                for release in releases:
                    release_id = release.get("id", "")
                    if not release_id or release_id in seen_ids:
                        continue
                    seen_ids.add(release_id)

                    # Apply housing relevance filter
                    if self._is_housing_related(release):
                        all_results.append(release)
                    else:
                        self.log.debug(
                            "release_filtered_non_housing",
                            release_id=release_id,
                        )
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"contracts_finder_search:{term}"
                )
                self.log.warning(
                    "contracts_finder_search_failed",
                    term=term,
                    error=str(exc),
                )

        self.metrics.applications_found = len(all_results)
        self.log.info(
            "contracts_finder_search_complete",
            total_results=len(all_results),
            unique_ids=len(seen_ids),
        )
        return all_results

    async def _search_term(
        self,
        term: str,
        published_from: date,
        published_to: date,
        stages: str,
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """
        Paginate through Contracts Finder API results for a single
        search term.

        Args:
            term: Keyword to search for.
            published_from: Start of publication date range.
            published_to: End of publication date range.
            stages: OCDS stages filter (e.g. "tender,award").
            max_pages: Maximum number of pages to retrieve.
            page_size: Number of results per page.

        Returns:
            List of OCDS release dicts from all pages.
        """
        releases: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "keyword": term,
                "publishedFrom": published_from.strftime("%Y-%m-%d"),
                "publishedTo": published_to.strftime("%Y-%m-%d"),
                "stages": stages,
                "size": min(page_size, 100),
                "page": page,
            }

            try:
                resp = await self.fetch(
                    API_BASE_URL,
                    params=params,
                    use_cache=False,
                )
                data = resp.json()
            except Exception as exc:
                self.log.warning(
                    "contracts_finder_page_error",
                    term=term,
                    page=page,
                    error=str(exc),
                )
                break

            page_releases = data.get("releases", [])
            if not page_releases:
                self.log.debug(
                    "contracts_finder_no_more_results",
                    term=term,
                    page=page,
                )
                break

            releases.extend(page_releases)
            self.log.info(
                "contracts_finder_page",
                term=term,
                page=page,
                count=len(page_releases),
                cumulative=len(releases),
            )

            # If we received fewer results than requested, we are on the
            # last page -- no need to request more.
            if len(page_releases) < page_size:
                break

        return releases

    # ------------------------------------------------------------------
    # Housing relevance filter
    # ------------------------------------------------------------------

    @staticmethod
    def _is_housing_related(release: dict[str, Any]) -> bool:
        """
        Determine whether an OCDS release is related to housing or
        property management.

        Checks CPV classification codes first (fast, precise), then
        falls back to keyword matching on title and description text.

        Args:
            release: A single OCDS release dict.

        Returns:
            True if the release is housing-related.
        """
        # Check CPV codes from tender items
        tender = release.get("tender", {})
        items = tender.get("items", [])
        for item in items:
            classification = item.get("classification", {})
            cpv_id = classification.get("id", "")
            if cpv_id in HOUSING_CPV_CODES:
                return True
            # Also check the broader prefix (first 5 digits)
            if cpv_id and cpv_id[:5] in {c[:5] for c in HOUSING_CPV_CODES}:
                return True

        # Fall back to keyword matching on title + description
        title = tender.get("title", "")
        description = tender.get("description", "")
        text = f"{title} {description}".lower()

        return any(kw in text for kw in HOUSING_KEYWORDS)

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def parse_application(
        self,
        raw: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Convert a raw OCDS release dict into a normalised contract dict
        suitable for persistence.

        Extracts: notice ID, title, description, buyer (contracting
        authority), supplier names from awards, contract dates, value,
        currency, and CPV codes.

        Args:
            raw: A single OCDS release dict.

        Returns:
            A normalised dict of contract fields.
        """
        release_id = raw.get("id", "")
        tender = raw.get("tender", {})
        buyer = raw.get("buyer", {})

        title = tender.get("title", "")
        description = tender.get("description", "")
        contracting_authority = buyer.get("name", "")

        # Extract address from parties (buyer party)
        address = ""
        postcode = ""
        buyer_id = buyer.get("id", "")
        for party in raw.get("parties", []):
            if party.get("id") == buyer_id or "buyer" in (party.get("roles") or []):
                addr_obj = party.get("address", {})
                parts = [
                    addr_obj.get("streetAddress", ""),
                    addr_obj.get("locality", ""),
                    addr_obj.get("region", ""),
                ]
                address = ", ".join(p for p in parts if p).strip(", ")
                postcode = addr_obj.get("postalCode", "")
                break

        # Extract CPV codes from items
        cpv_codes = self._extract_cpv_codes(tender)

        # Extract supplier name from awards
        supplier = self._extract_supplier(raw)

        # Extract contract dates and value
        contract_start, contract_end, contract_value, currency = (
            self._extract_contract_details(raw)
        )

        return {
            "notice_id": release_id,
            "title": title,
            "description": description,
            "contracting_authority": contracting_authority,
            "supplier": supplier,
            "contract_start_date": contract_start,
            "contract_end_date": contract_end,
            "contract_value": contract_value,
            "currency": currency or "GBP",
            "cpv_codes": cpv_codes,
            "address": address,
            "postcode": postcode,
            "source": "contracts_finder",
            "source_reference": release_id,
            "detail_url": f"https://www.contractsfinder.service.gov.uk/Notice/{release_id}",
            "raw_release": raw,
        }

    # ------------------------------------------------------------------
    # Detail page (not applicable for API-based scraper)
    # ------------------------------------------------------------------

    async def get_application_detail(
        self,
        detail_url: str,
    ) -> dict[str, Any]:
        """
        Fetch additional detail for a notice.

        For the Contracts Finder API scraper, all required data is
        already present in the OCDS release returned by the search
        endpoint, so this is a no-op that returns an empty dict.

        Args:
            detail_url: URL of the notice detail page (unused).

        Returns:
            Empty dict -- detail is already in the search response.
        """
        return {}

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_cpv_codes(tender: dict[str, Any]) -> list[str]:
        """
        Extract CPV classification codes from OCDS tender items.

        Args:
            tender: The ``tender`` object from an OCDS release.

        Returns:
            List of CPV code strings (e.g. ``["70332000"]``).
        """
        cpv_codes: list[str] = []
        items = tender.get("items", [])
        for item in items:
            classification = item.get("classification", {})
            cpv_id = classification.get("id", "")
            if cpv_id and re.match(r"^\d{8}$", cpv_id):
                cpv_codes.append(cpv_id)
        return cpv_codes

    @staticmethod
    def _extract_supplier(release: dict[str, Any]) -> str:
        """
        Extract the primary supplier name from OCDS award data.

        Looks in the ``awards`` array for the first supplier entry.

        Args:
            release: A single OCDS release dict.

        Returns:
            Supplier name string, or empty string if not found.
        """
        awards = release.get("awards", [])
        for award in awards:
            suppliers = award.get("suppliers", [])
            if suppliers:
                name = suppliers[0].get("name", "")
                if name and name.strip():
                    return name.strip()
        return ""

    @staticmethod
    def _parse_iso_date(date_str: str | None) -> date | None:
        """
        Parse an ISO 8601 datetime string into a date object.

        Handles formats like ``2024-01-01T00:00:00Z`` and
        ``2024-01-01``.

        Args:
            date_str: ISO date/datetime string, or None.

        Returns:
            A ``date`` object, or None if parsing fails.
        """
        if not date_str or not date_str.strip():
            return None

        date_str = date_str.strip()

        # Try ISO datetime with timezone
        for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(date_str[:26].rstrip("Z") + "Z" if "T" in date_str else date_str, fmt).date()
            except ValueError:
                continue

        # Last resort: extract YYYY-MM-DD prefix
        match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d").date()
            except ValueError:
                pass

        return None

    @classmethod
    def _extract_contract_details(
        cls,
        release: dict[str, Any],
    ) -> tuple[date | None, date | None, float | None, str | None]:
        """
        Extract contract period dates and value from an OCDS release.

        Looks first in the ``contracts`` array, falling back to the
        ``awards`` array for value data.

        Args:
            release: A single OCDS release dict.

        Returns:
            Tuple of (start_date, end_date, value, currency).
        """
        start_date: date | None = None
        end_date: date | None = None
        contract_value: float | None = None
        currency: str | None = None

        # Try contracts array first
        contracts = release.get("contracts", [])
        for contract in contracts:
            period = contract.get("period", {})

            parsed_start = cls._parse_iso_date(period.get("startDate"))
            if parsed_start and start_date is None:
                start_date = parsed_start

            parsed_end = cls._parse_iso_date(period.get("endDate"))
            if parsed_end and end_date is None:
                end_date = parsed_end

            value_obj = contract.get("value", {})
            amount = value_obj.get("amount")
            if amount is not None and contract_value is None:
                try:
                    parsed_value = float(amount)
                    if parsed_value > 0:
                        contract_value = parsed_value
                        currency = value_obj.get("currency", "GBP")
                except (ValueError, TypeError):
                    pass

        # Fall back to tender.contractPeriod for dates
        if start_date is None or end_date is None:
            tender_period = release.get("tender", {}).get("contractPeriod", {})
            if start_date is None:
                start_date = cls._parse_iso_date(tender_period.get("startDate"))
            if end_date is None:
                end_date = cls._parse_iso_date(tender_period.get("endDate"))

        # Fall back to awards for dates and value
        awards = release.get("awards", [])
        for award in awards:
            # Dates from award.contractPeriod
            if start_date is None or end_date is None:
                award_period = award.get("contractPeriod", {})
                if start_date is None:
                    start_date = cls._parse_iso_date(award_period.get("startDate"))
                if end_date is None:
                    end_date = cls._parse_iso_date(award_period.get("endDate"))

            # Value from award
            if contract_value is None:
                value_obj = award.get("value", {})
                amount = value_obj.get("amount")
                if amount is not None:
                    try:
                        parsed_value = float(amount)
                        if parsed_value > 0:
                            contract_value = parsed_value
                            currency = value_obj.get("currency", "GBP")
                    except (ValueError, TypeError):
                        continue

        return start_date, end_date, contract_value, currency

    # ------------------------------------------------------------------
    # Convenience: run full pipeline
    # ------------------------------------------------------------------

    async def run(self, **search_kwargs: Any) -> list[dict[str, Any]]:
        """
        Execute a full scrape: search -> parse -> return normalised
        contract records.

        Overrides the base ``run()`` method because the Contracts Finder
        API returns all needed data in the search response (no separate
        detail fetch is required).

        Args:
            **search_kwargs: Passed through to ``search_applications``.

        Returns:
            List of normalised contract dicts ready for persistence.
        """
        self.log.info("scrape_start", source="contracts_finder")
        from app.scrapers.base import ScraperMetrics
        self.metrics = ScraperMetrics()

        results: list[dict[str, Any]] = []
        try:
            raw_releases = await self.search_applications(**search_kwargs)
            self.log.info(
                "search_complete",
                results_count=len(raw_releases),
            )

            for release in raw_releases:
                release_id = release.get("id", "unknown")
                try:
                    parsed = await self.parse_application(release)
                    results.append(parsed)
                except Exception as exc:
                    self.metrics.record_error(
                        exc,
                        context=f"parse:{release_id}",
                    )
                    self.log.warning(
                        "parse_error",
                        release_id=release_id,
                        error=str(exc),
                    )

        except Exception as exc:
            self.metrics.record_error(exc, context="search_applications")
            self.log.error("scrape_failed", error=str(exc))
            raise

        self.metrics.applications_found = len(results)
        self.log.info(
            "scrape_complete",
            **self.metrics.to_dict(),
        )
        return results
