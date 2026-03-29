"""Companies House API client for company enrichment.

Provides async access to the Companies House REST API to look up company
details, officers, and filing history.  Implements rate-limiting (600 reqs
per 5-minute window) and maps API responses to internal Company model fields.

Typical usage::

    from app.enrichment.companies_house import CompaniesHouseEnricher

    enricher = CompaniesHouseEnricher(api_key="your-api-key")
    results = await enricher.search_company("Greystar")
    details = await enricher.get_company_details("12345678")
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

API_BASE = "https://api.company-information.service.gov.uk"

# Rate-limit: 600 requests per 5-minute window (300 seconds).
_RATE_LIMIT_MAX_REQUESTS = 600
_RATE_LIMIT_WINDOW_SECONDS = 300


@dataclass
class CompanySearchResult:
    """Lightweight representation of a Companies House search hit."""

    company_number: str
    title: str
    company_status: str | None = None
    company_type: str | None = None
    date_of_creation: str | None = None
    registered_office_address: dict[str, Any] | None = None
    snippet: str | None = None


@dataclass
class CompanyDetails:
    """Structured representation of full company information."""

    company_number: str
    company_name: str
    company_status: str | None = None
    company_type: str | None = None
    date_of_creation: str | None = None
    date_of_cessation: str | None = None
    registered_office_address: dict[str, Any] | None = None
    sic_codes: list[str] = field(default_factory=list)
    has_been_liquidated: bool = False
    has_charges: bool = False
    has_insolvency_history: bool = False
    jurisdiction: str | None = None


@dataclass
class Officer:
    """Representation of a company officer."""

    name: str
    officer_role: str
    appointed_on: str | None = None
    resigned_on: str | None = None
    nationality: str | None = None
    occupation: str | None = None
    address: dict[str, Any] | None = None


@dataclass
class FilingItem:
    """Representation of a filing history entry."""

    date: str
    category: str
    description: str
    type: str | None = None
    barcode: str | None = None


@dataclass
class PersonWithSignificantControl:
    """Representation of a Person with Significant Control (PSC) entry.

    For corporate PSCs (``kind == "corporate-entity-..."``) the
    ``identification`` dict will contain ``registration_number`` and
    ``country_registered`` which can be used to resolve the parent company
    in Companies House.
    """

    name: str
    kind: str  # individual-person-..., corporate-entity-..., legal-person-...
    natures_of_control: list[str]
    notified_on: str | None = None
    ceased_on: str | None = None
    identification: dict[str, Any] | None = None  # corporate PSCs only

    @property
    def is_corporate(self) -> bool:
        return "corporate-entity" in self.kind or "legal-person" in self.kind

    @property
    def corporate_registration_number(self) -> str | None:
        """Return the CH registration number for corporate PSCs, or None."""
        if self.identification:
            return self.identification.get("registration_number")
        return None


class RateLimiter:
    """Sliding-window rate limiter for API calls.

    Allows up to ``max_requests`` within a rolling ``window_seconds`` period.
    When the limit is reached, :meth:`acquire` will sleep until capacity is
    available.
    """

    def __init__(
        self,
        max_requests: int = _RATE_LIMIT_MAX_REQUESTS,
        window_seconds: int = _RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available, then record a timestamp."""
        async with self._lock:
            now = time.monotonic()
            # Prune timestamps outside the current window.
            cutoff = now - self._window_seconds
            self._timestamps = [t for t in self._timestamps if t > cutoff]

            if len(self._timestamps) >= self._max_requests:
                # Need to wait until the oldest relevant timestamp falls off.
                sleep_for = self._timestamps[0] - cutoff
                logger.warning(
                    "companies_house_rate_limit_hit",
                    sleep_seconds=round(sleep_for, 2),
                )
                await asyncio.sleep(sleep_for)
                # Re-prune after sleep.
                now = time.monotonic()
                cutoff = now - self._window_seconds
                self._timestamps = [t for t in self._timestamps if t > cutoff]

            self._timestamps.append(time.monotonic())


class CompaniesHouseEnricher:
    """Async client for the Companies House REST API.

    Parameters
    ----------
    api_key : str, optional
        API key used for HTTP Basic authentication (username = key, password
        empty).  Defaults to the value in application settings.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.COMPANIES_HOUSE_API_KEY
        self._rate_limiter = RateLimiter()
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=API_BASE,
                auth=(self._api_key, ""),
                timeout=httpx.Timeout(30.0),
                headers={"Accept": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any] | None:
        """Execute an API request with rate-limiting and error handling."""
        await self._rate_limiter.acquire()
        client = await self._get_client()

        log = logger.bind(method=method, path=path)
        try:
            response = await client.request(method, path, **kwargs)

            if response.status_code == 404:
                log.info("companies_house_not_found")
                return None

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "60"))
                log.warning("companies_house_429", retry_after=retry_after)
                await asyncio.sleep(retry_after)
                return await self._request(method, path, **kwargs)

            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as exc:
            log.error(
                "companies_house_http_error",
                status_code=exc.response.status_code,
                body=exc.response.text[:500],
            )
            raise
        except httpx.RequestError as exc:
            log.error("companies_house_request_error", error=str(exc))
            raise

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def search_company(
        self,
        name: str,
        *,
        items_per_page: int = 20,
        start_index: int = 0,
    ) -> list[CompanySearchResult]:
        """Search for companies by name.

        Parameters
        ----------
        name : str
            Company name query string.
        items_per_page : int
            Number of results per page (max 100).
        start_index : int
            Pagination offset.

        Returns
        -------
        list[CompanySearchResult]
            Matching companies.
        """
        data = await self._request(
            "GET",
            "/search/companies",
            params={
                "q": name,
                "items_per_page": items_per_page,
                "start_index": start_index,
            },
        )
        if not data or "items" not in data:
            return []

        results: list[CompanySearchResult] = []
        for item in data["items"]:
            results.append(
                CompanySearchResult(
                    company_number=item.get("company_number", ""),
                    title=item.get("title", ""),
                    company_status=item.get("company_status"),
                    company_type=item.get("company_type"),
                    date_of_creation=item.get("date_of_creation"),
                    registered_office_address=item.get("address"),
                    snippet=item.get("snippet"),
                )
            )

        logger.info(
            "companies_house_search",
            query=name,
            result_count=len(results),
        )
        return results

    async def get_company_details(self, company_number: str) -> CompanyDetails | None:
        """Retrieve full details for a company by its registration number.

        Handles dissolved companies, overseas companies, and LLPs by mapping
        ``company_type`` accordingly.
        """
        data = await self._request("GET", f"/company/{company_number}")
        if data is None:
            return None

        return CompanyDetails(
            company_number=data.get("company_number", company_number),
            company_name=data.get("company_name", ""),
            company_status=data.get("company_status"),
            company_type=data.get("type"),
            date_of_creation=data.get("date_of_creation"),
            date_of_cessation=data.get("date_of_cessation"),
            registered_office_address=data.get("registered_office_address"),
            sic_codes=data.get("sic_codes", []),
            has_been_liquidated=data.get("has_been_liquidated", False),
            has_charges=data.get("has_charges", False),
            has_insolvency_history=data.get("has_insolvency_history", False),
            jurisdiction=data.get("jurisdiction"),
        )

    async def get_officers(
        self,
        company_number: str,
        *,
        active_only: bool = True,
        items_per_page: int = 50,
    ) -> list[Officer]:
        """Retrieve the list of officers for a company.

        Parameters
        ----------
        active_only : bool
            If ``True``, exclude officers who have resigned.
        """
        data = await self._request(
            "GET",
            f"/company/{company_number}/officers",
            params={"items_per_page": items_per_page},
        )
        if not data or "items" not in data:
            return []

        officers: list[Officer] = []
        for item in data["items"]:
            if active_only and item.get("resigned_on"):
                continue
            officers.append(
                Officer(
                    name=item.get("name", ""),
                    officer_role=item.get("officer_role", ""),
                    appointed_on=item.get("appointed_on"),
                    resigned_on=item.get("resigned_on"),
                    nationality=item.get("nationality"),
                    occupation=item.get("occupation"),
                    address=item.get("address"),
                )
            )

        logger.info(
            "companies_house_officers",
            company_number=company_number,
            officer_count=len(officers),
        )
        return officers

    async def get_filing_history(
        self,
        company_number: str,
        *,
        category: str | None = None,
        items_per_page: int = 25,
    ) -> list[FilingItem]:
        """Retrieve filing history for a company.

        Parameters
        ----------
        category : str, optional
            Filter by filing category (e.g. ``"confirmation-statement"``).
        """
        params: dict[str, Any] = {"items_per_page": items_per_page}
        if category:
            params["category"] = category

        data = await self._request(
            "GET",
            f"/company/{company_number}/filing-history",
            params=params,
        )
        if not data or "items" not in data:
            return []

        filings: list[FilingItem] = []
        for item in data["items"]:
            filings.append(
                FilingItem(
                    date=item.get("date", ""),
                    category=item.get("category", ""),
                    description=item.get("description", ""),
                    type=item.get("type"),
                    barcode=item.get("barcode"),
                )
            )
        return filings

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------

    def map_to_company_fields(self, details: CompanyDetails) -> dict[str, Any]:
        """Map a :class:`CompanyDetails` instance to a dict compatible with
        our :class:`Company` ORM model.

        Handles edge cases:

        * **Dissolved companies** – ``is_active`` is set to ``False``.
        * **Overseas companies** – ``company_type`` maps to ``"Overseas"``.
        * **LLPs** – ``company_type`` maps to ``"LLP"``.
        """
        address_parts: list[str] = []
        addr = details.registered_office_address or {}
        for key in ("premises", "address_line_1", "address_line_2", "locality", "region", "postal_code", "country"):
            val = addr.get(key)
            if val:
                address_parts.append(val)

        # Map Companies House type codes to our domain types.
        ch_type = (details.company_type or "").lower()
        if "llp" in ch_type:
            company_type = "LLP"
        elif "oversea" in ch_type or ch_type.startswith("registered-overseas"):
            company_type = "Overseas"
        else:
            company_type = None  # Will be classified by other enrichment.

        status = (details.company_status or "").lower()
        is_active = status not in ("dissolved", "liquidation", "administration", "converted-closed")

        return {
            "name": details.company_name,
            "companies_house_number": details.company_number,
            "registered_address": ", ".join(address_parts) if address_parts else None,
            "sic_codes": details.sic_codes if details.sic_codes else None,
            "company_type": company_type,
            "is_active": is_active,
        }

    async def get_psc(
        self,
        company_number: str,
        *,
        active_only: bool = True,
        items_per_page: int = 25,
    ) -> list[PersonWithSignificantControl]:
        """Retrieve Persons with Significant Control for a company.

        Parameters
        ----------
        company_number : str
            Companies House registration number.
        active_only : bool
            If ``True``, exclude PSCs that have ceased to be significant.

        Returns
        -------
        list[PersonWithSignificantControl]
            PSC entries, which may be individuals or corporate entities.
        """
        data = await self._request(
            "GET",
            f"/company/{company_number}/persons-with-significant-control",
            params={"items_per_page": items_per_page},
        )
        if not data or "items" not in data:
            return []

        pscs: list[PersonWithSignificantControl] = []
        for item in data["items"]:
            if active_only and item.get("ceased_on"):
                continue
            pscs.append(
                PersonWithSignificantControl(
                    name=item.get("name", ""),
                    kind=item.get("kind", ""),
                    natures_of_control=item.get("natures_of_control", []),
                    notified_on=item.get("notified_on"),
                    ceased_on=item.get("ceased_on"),
                    identification=item.get("identification"),
                )
            )

        logger.info(
            "companies_house_psc",
            company_number=company_number,
            psc_count=len(pscs),
            corporate_count=sum(1 for p in pscs if p.is_corporate),
        )
        return pscs

    async def resolve_ultimate_owner(
        self,
        company_number: str,
        *,
        max_depth: int = 3,
        _visited: set[str] | None = None,
        _depth: int = 0,
    ) -> list[dict[str, Any]]:
        """Walk the PSC chain upward to resolve the ultimate corporate owner.

        Starting from ``company_number``, fetches PSCs and for each corporate
        PSC recursively resolves their own PSCs until:

        * An individual PSC is encountered (beneficial ownership found), or
        * A non-UK or unregistered entity is encountered, or
        * ``max_depth`` is reached, or
        * A cycle is detected (visited set).

        Returns
        -------
        list[dict]
            Ordered chain from immediate parent to ultimate owner.  Each entry
            has keys: ``company_number``, ``company_name``, ``company_status``,
            ``registered_address``, ``sic_codes``, ``depth``.
        """
        if _visited is None:
            _visited = set()

        if company_number in _visited or _depth >= max_depth:
            return []

        _visited.add(company_number)

        pscs = await self.get_psc(company_number, active_only=True)
        chain: list[dict[str, Any]] = []

        for psc in pscs:
            if not psc.is_corporate:
                continue

            parent_reg = psc.corporate_registration_number
            if not parent_reg or parent_reg in _visited:
                continue

            # Fetch the parent company's own details.
            details = await self.get_company_details(parent_reg)
            if details is None:
                continue

            chain.append({
                "company_number": details.company_number,
                "company_name": details.company_name,
                "company_status": details.company_status,
                "registered_address": self.map_to_company_fields(details).get("registered_address"),
                "sic_codes": details.sic_codes,
                "depth": _depth + 1,
            })

            # Recurse into this parent's PSC chain.
            sub_chain = await self.resolve_ultimate_owner(
                parent_reg,
                max_depth=max_depth,
                _visited=_visited,
                _depth=_depth + 1,
            )
            chain.extend(sub_chain)

        logger.info(
            "companies_house_psc_chain",
            root=company_number,
            depth=_depth,
            chain_length=len(chain),
        )
        return chain

    async def enrich_company(self, company_name: str) -> dict[str, Any] | None:
        """End-to-end enrichment: search by name, fetch best-match details,
        and return mapped company fields.

        Returns ``None`` if no match is found.
        """
        results = await self.search_company(company_name, items_per_page=5)
        if not results:
            logger.info("companies_house_enrich_no_results", company_name=company_name)
            return None

        # Prefer active companies; fall back to the first result.
        best = results[0]
        for r in results:
            if r.company_status and r.company_status.lower() == "active":
                best = r
                break

        details = await self.get_company_details(best.company_number)
        if details is None:
            return None

        mapped = self.map_to_company_fields(details)
        officers = await self.get_officers(best.company_number, active_only=True)
        mapped["_officers"] = [
            {
                "name": o.name,
                "role": o.officer_role,
                "appointed_on": o.appointed_on,
                "occupation": o.occupation,
            }
            for o in officers
        ]

        logger.info(
            "companies_house_enriched",
            company_name=company_name,
            company_number=best.company_number,
            officer_count=len(officers),
        )
        return mapped
