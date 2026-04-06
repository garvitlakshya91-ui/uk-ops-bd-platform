"""Companies House API integration for tracking developer SPVs.

Uses the free Companies House REST API to search for companies, retrieve
details/officers/filings, and identify new SPV incorporations by major
UK residential developers.

API docs: https://developer.company-information.service.gov.uk/
Auth:     Basic auth with API key as username, empty password.
Rate:     600 requests per 5-minute window (2 req/sec sustained).

Usage::

    from app.scrapers.companies_house_scraper import CompaniesHouseScraper

    scraper = CompaniesHouseScraper()
    results = await scraper.search_developer_spvs("Barratt Developments")
    company = await scraper.get_company("12345678")
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import Any

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

# -------------------------------------------------------------------------
# SIC codes relevant to property development and real estate
# -------------------------------------------------------------------------
PROPERTY_SIC_CODES: set[str] = {
    "41100",  # Development of building projects
    "41201",  # Construction of commercial buildings
    "41202",  # Construction of domestic buildings
    "68100",  # Buying and selling of own real estate
    "68201",  # Renting and operating of Housing Association real estate
    "68202",  # Letting and operating of conference and exhibition centres
    "68209",  # Other letting and operating of own or leased real estate
    "68310",  # Real estate agencies
    "68320",  # Management of real estate on a fee or contract basis
}

# Base URL for the Companies House API
CH_API_BASE = "https://api.company-information.service.gov.uk"

# Rate limit: 600 requests per 300 seconds = 0.5s minimum between requests
_MIN_REQUEST_INTERVAL = 0.5


class CompaniesHouseRateLimiter:
    """Token-bucket rate limiter for the Companies House API.

    Allows a burst of requests but enforces the 600-per-5-minutes ceiling.
    """

    def __init__(self, max_tokens: int = 600, refill_seconds: float = 300.0):
        self._max_tokens = max_tokens
        self._refill_seconds = refill_seconds
        self._tokens = float(max_tokens)
        self._last_refill = time.monotonic()
        self._last_request = 0.0

    async def acquire(self) -> None:
        """Wait until a request token is available."""
        now = time.monotonic()

        # Refill tokens based on elapsed time
        elapsed = now - self._last_refill
        refill = elapsed * (self._max_tokens / self._refill_seconds)
        self._tokens = min(self._max_tokens, self._tokens + refill)
        self._last_refill = now

        # Wait if no tokens available
        if self._tokens < 1.0:
            wait_time = (1.0 - self._tokens) * (self._refill_seconds / self._max_tokens)
            logger.debug("ch_rate_limit_wait", wait_seconds=round(wait_time, 2))
            await asyncio.sleep(wait_time)
            self._tokens = 1.0

        # Also enforce minimum interval between requests
        since_last = now - self._last_request
        if since_last < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - since_last)

        self._tokens -= 1.0
        self._last_request = time.monotonic()


class CompaniesHouseScraper:
    """Client for the Companies House REST API.

    Parameters
    ----------
    api_key : str | None
        Companies House API key. Defaults to ``settings.COMPANIES_HOUSE_API_KEY``.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.COMPANIES_HOUSE_API_KEY
        if not self._api_key:
            raise ValueError(
                "COMPANIES_HOUSE_API_KEY is not configured. "
                "Register for a free key at https://developer.company-information.service.gov.uk/"
            )
        self._rate_limiter = CompaniesHouseRateLimiter()
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=CH_API_BASE,
                auth=(self._api_key, ""),
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={"Accept": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "CompaniesHouseScraper":
        await self._get_client()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Low-level request with rate limiting and retry
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """Make a rate-limited request to the Companies House API.

        Returns parsed JSON or None if 404.
        Raises on non-retryable errors after exhausting retries.
        """
        client = await self._get_client()

        for attempt in range(max_retries):
            await self._rate_limiter.acquire()

            try:
                response = await client.request(method, path, params=params)

                if response.status_code == 404:
                    return None

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logger.warning(
                        "ch_api_rate_limited",
                        retry_after=retry_after,
                        attempt=attempt + 1,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (500, 502, 503, 504) and attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "ch_api_server_error",
                        status=exc.response.status_code,
                        attempt=attempt + 1,
                        retry_in=wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "ch_api_transport_error",
                        error=str(exc),
                        attempt=attempt + 1,
                        retry_in=wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

        return None

    # ------------------------------------------------------------------
    # Core API methods
    # ------------------------------------------------------------------

    async def search_companies(
        self,
        query: str,
        *,
        items_per_page: int = 50,
        start_index: int = 0,
    ) -> list[dict[str, Any]]:
        """Search for companies by name.

        Parameters
        ----------
        query : str
            Company name or keyword to search for.
        items_per_page : int
            Number of results per page (max 100).
        start_index : int
            Starting index for pagination.

        Returns
        -------
        list[dict]
            List of company search result items.
        """
        data = await self._request(
            "GET",
            "/search/companies",
            params={
                "q": query,
                "items_per_page": min(items_per_page, 100),
                "start_index": start_index,
            },
        )
        if not data:
            return []
        return data.get("items", [])

    async def get_company(self, company_number: str) -> dict[str, Any] | None:
        """Get full details for a specific company.

        Parameters
        ----------
        company_number : str
            Companies House registration number (e.g. "12345678").

        Returns
        -------
        dict | None
            Company profile data, or None if not found.
        """
        return await self._request("GET", f"/company/{company_number}")

    async def get_officers(
        self,
        company_number: str,
        *,
        items_per_page: int = 50,
        start_index: int = 0,
    ) -> list[dict[str, Any]]:
        """Get directors and officers for a company.

        Parameters
        ----------
        company_number : str
            Companies House registration number.

        Returns
        -------
        list[dict]
            List of officer records.
        """
        data = await self._request(
            "GET",
            f"/company/{company_number}/officers",
            params={
                "items_per_page": min(items_per_page, 100),
                "start_index": start_index,
            },
        )
        if not data:
            return []
        return data.get("items", [])

    async def get_filing_history(
        self,
        company_number: str,
        *,
        items_per_page: int = 25,
        start_index: int = 0,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get filing history for a company.

        Parameters
        ----------
        company_number : str
            Companies House registration number.
        category : str | None
            Filter by filing category (e.g. "incorporation", "accounts").

        Returns
        -------
        list[dict]
            List of filing history items.
        """
        params: dict[str, Any] = {
            "items_per_page": min(items_per_page, 100),
            "start_index": start_index,
        }
        if category:
            params["category"] = category

        data = await self._request(
            "GET",
            f"/company/{company_number}/filing-history",
            params=params,
        )
        if not data:
            return []
        return data.get("items", [])

    async def get_persons_with_significant_control(
        self,
        company_number: str,
        *,
        items_per_page: int = 25,
        start_index: int = 0,
    ) -> list[dict[str, Any]]:
        """Get PSC (Persons with Significant Control) for a company.

        Useful for identifying the parent developer behind an SPV.

        Returns
        -------
        list[dict]
            List of PSC records.
        """
        data = await self._request(
            "GET",
            f"/company/{company_number}/persons-with-significant-control",
            params={
                "items_per_page": min(items_per_page, 100),
                "start_index": start_index,
            },
        )
        if not data:
            return []
        return data.get("items", [])

    async def advanced_search(
        self,
        *,
        company_name: str | None = None,
        incorporated_from: str | None = None,
        incorporated_to: str | None = None,
        sic_codes: list[str] | None = None,
        company_status: str = "active",
        size: int = 100,
        start_index: int = 0,
    ) -> list[dict[str, Any]]:
        """Use the advanced company search endpoint.

        Parameters
        ----------
        company_name : str | None
            Name to search for.
        incorporated_from : str | None
            ISO date string (YYYY-MM-DD) for earliest incorporation date.
        incorporated_to : str | None
            ISO date string (YYYY-MM-DD) for latest incorporation date.
        sic_codes : list[str] | None
            Filter by SIC code(s).
        company_status : str
            Filter by status (default "active").
        size : int
            Number of results (max 500).

        Returns
        -------
        list[dict]
            List of matching company records.
        """
        params: dict[str, Any] = {
            "size": min(size, 500),
            "start_index": start_index,
        }
        if company_name:
            params["company_name_includes"] = company_name
        if incorporated_from:
            params["incorporated_from"] = incorporated_from
        if incorporated_to:
            params["incorporated_to"] = incorporated_to
        if sic_codes:
            params["sic_codes"] = ",".join(sic_codes)
        if company_status:
            params["company_status"] = company_status

        data = await self._request(
            "GET",
            "/advanced-search/companies",
            params=params,
        )
        if not data:
            return []
        return data.get("items", [])

    # ------------------------------------------------------------------
    # Higher-level methods for developer SPV tracking
    # ------------------------------------------------------------------

    async def search_developer_spvs(
        self,
        developer_name: str,
        *,
        since_date: date | None = None,
        sic_codes: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find SPVs likely linked to a developer.

        Searches Companies House for companies whose name contains the
        developer name and whose SIC codes indicate property development.

        Parameters
        ----------
        developer_name : str
            Name of the parent developer (e.g. "Barratt", "Berkeley").
        since_date : date | None
            Only return companies incorporated on or after this date.
        sic_codes : set[str] | None
            SIC codes to filter by. Defaults to PROPERTY_SIC_CODES.

        Returns
        -------
        list[dict]
            List of company dicts that match the criteria.
        """
        target_sics = sic_codes or PROPERTY_SIC_CODES
        log = logger.bind(developer=developer_name)

        # Use advanced search if we have date filters
        if since_date:
            results = await self.advanced_search(
                company_name=developer_name,
                incorporated_from=since_date.isoformat(),
                company_status="active",
                size=100,
            )
        else:
            # Fall back to basic search and paginate
            results = []
            for start in range(0, 200, 50):
                page = await self.search_companies(
                    developer_name,
                    items_per_page=50,
                    start_index=start,
                )
                results.extend(page)
                if len(page) < 50:
                    break

        # Filter by SIC codes
        spvs = []
        for company in results:
            company_sics = set(company.get("sic_codes") or [])
            if company_sics & target_sics:
                spvs.append(company)

        log.info(
            "developer_spv_search_complete",
            total_results=len(results),
            spv_matches=len(spvs),
        )
        return spvs

    async def find_new_incorporations(
        self,
        since_date: date | None = None,
        sic_codes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find recently incorporated companies with property development SIC codes.

        Parameters
        ----------
        since_date : date | None
            Search from this date. Defaults to 30 days ago.
        sic_codes : list[str] | None
            SIC codes to search. Defaults to all PROPERTY_SIC_CODES.

        Returns
        -------
        list[dict]
            Recently incorporated companies matching the SIC criteria.
        """
        if since_date is None:
            since_date = date.today() - timedelta(days=30)

        target_sics = list(sic_codes or PROPERTY_SIC_CODES)

        all_results: list[dict[str, Any]] = []

        # Search each SIC code separately (API may not support multiple in
        # basic search) -- use advanced search which does support it
        for sic in target_sics:
            results = await self.advanced_search(
                incorporated_from=since_date.isoformat(),
                sic_codes=[sic],
                company_status="active",
                size=500,
            )
            # Deduplicate by company number
            seen = {r.get("company_number") for r in all_results}
            for r in results:
                if r.get("company_number") not in seen:
                    all_results.append(r)
                    seen.add(r.get("company_number"))

        logger.info(
            "new_incorporations_search_complete",
            since=since_date.isoformat(),
            sic_codes_checked=len(target_sics),
            total_found=len(all_results),
        )
        return all_results

    async def enrich_spv_details(
        self,
        company_number: str,
    ) -> dict[str, Any]:
        """Fetch full details for an SPV including officers and PSCs.

        Returns a combined dict with company profile, officers, PSCs,
        and recent filings.

        Parameters
        ----------
        company_number : str
            Companies House registration number.

        Returns
        -------
        dict
            Enriched company data with keys: profile, officers, pscs, filings.
        """
        profile = await self.get_company(company_number)
        if not profile:
            return {"company_number": company_number, "error": "not_found"}

        officers = await self.get_officers(company_number)
        pscs = await self.get_persons_with_significant_control(company_number)
        filings = await self.get_filing_history(company_number, items_per_page=10)

        return {
            "company_number": company_number,
            "profile": profile,
            "officers": officers,
            "pscs": pscs,
            "filings": filings,
        }

    @staticmethod
    def extract_parent_developer(
        pscs: list[dict[str, Any]],
        officers: list[dict[str, Any]],
    ) -> str | None:
        """Attempt to identify the parent developer from PSCs and officers.

        Looks for corporate PSCs (which indicate a parent company) and
        falls back to officer name patterns.

        Parameters
        ----------
        pscs : list[dict]
            PSC records from Companies House.
        officers : list[dict]
            Officer records from Companies House.

        Returns
        -------
        str | None
            Name of the likely parent developer, or None.
        """
        # Corporate PSCs are the strongest signal
        for psc in pscs:
            kind = psc.get("kind", "")
            if "corporate" in kind or "legal-person" in kind:
                name = psc.get("name", "")
                if name:
                    return name

        # Check for corporate officers (secretary or director that is a company)
        for officer in officers:
            officer_role = officer.get("officer_role", "")
            name = officer.get("name", "")
            # Corporate secretaries often indicate the parent group
            if officer_role == "corporate-secretary" and name:
                return name
            # Corporate directors
            if officer_role == "corporate-director" and name:
                return name

        return None

    @staticmethod
    def format_registered_address(address_data: dict[str, Any] | None) -> str:
        """Format a Companies House address dict into a single string."""
        if not address_data:
            return ""
        parts = []
        for key in [
            "premises",
            "address_line_1",
            "address_line_2",
            "locality",
            "region",
            "postal_code",
            "country",
        ]:
            val = address_data.get(key, "")
            if val:
                parts.append(val.strip())
        return ", ".join(parts)
