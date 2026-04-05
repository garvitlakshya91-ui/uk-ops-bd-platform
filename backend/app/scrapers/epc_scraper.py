"""
Scraper for the Energy Performance Certificate (EPC) register.

Uses the Open Data Communities API to retrieve EPC ratings for
properties by postcode, building reference, or address. This data
enriches existing scheme and planning application records with
energy performance information.

API base: https://epc.opendatacommunities.org/api/v1/
Auth: Basic auth using an API key (register at the above URL).
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import date, datetime
from typing import Any, Optional

import structlog

from app.config import settings
from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

EPC_API_BASE = "https://epc.opendatacommunities.org/api/v1"


class EPCScraper(BaseScraper):
    """
    Scraper for the EPC Open Data API.

    Provides methods to:
    - Search EPCs by postcode
    - Search EPCs by address
    - Get rating distribution for a postcode/address
    - Retrieve individual certificate details
    """

    def __init__(
        self,
        api_key: str | None = None,
        rate_limit: float | None = 1.5,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            council_name="EPC Register",
            council_id=0,
            portal_url=EPC_API_BASE,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        if api_key is None:
            from app.config import settings
            api_key = settings.EPC_API_KEY
        self.api_key = api_key

    def _auth_headers(self) -> dict[str, str]:
        """Build authentication headers for the EPC API."""
        headers: dict[str, str] = {
            "Accept": "application/json",
        }
        if self.api_key:
            import base64

            # EPC API requires Basic auth with base64(email:apikey).
            # If the key already contains ':', assume it's email:apikey format.
            if ":" in self.api_key:
                token = base64.b64encode(self.api_key.encode()).decode()
            else:
                token = base64.b64encode(f"{self.api_key}:".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        return headers

    # ------------------------------------------------------------------
    # Core API call
    # ------------------------------------------------------------------

    async def _api_get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue a GET request to the EPC API and return parsed JSON."""
        url = f"{EPC_API_BASE}/{endpoint.lstrip('/')}"
        response = await self.fetch(
            url,
            params=params,
            headers=self._auth_headers(),
            use_cache=True,
        )
        # EPC API returns empty body (200) when no results exist for a postcode.
        if not response.text:
            return {}
        return response.json()

    # ------------------------------------------------------------------
    # Search by postcode
    # ------------------------------------------------------------------

    async def search_by_postcode(
        self,
        postcode: str,
        page_size: int = 100,
        max_pages: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Search for EPC certificates by postcode.

        Returns list of certificate dicts with fields:
        lmk-key, address, postcode, current-energy-rating,
        current-energy-efficiency, potential-energy-rating,
        property-type, built-form, inspection-date, etc.
        """
        all_certs: list[dict[str, Any]] = []
        clean_postcode = postcode.strip().upper()

        for page in range(max_pages):
            params: dict[str, Any] = {
                "postcode": clean_postcode,
                "size": page_size,
                "from": page * page_size,
            }

            self.log.info(
                "epc_postcode_search",
                postcode=clean_postcode,
                page=page + 1,
            )

            try:
                data = await self._api_get("domestic/search", params=params)
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"epc_postcode:{clean_postcode}"
                )
                self.log.warning(
                    "epc_search_failed",
                    postcode=clean_postcode,
                    error=str(exc),
                )
                break

            rows = data.get("rows", [])
            if not rows:
                break

            all_certs.extend(rows)

            if len(rows) < page_size:
                break

        self.log.info(
            "epc_postcode_complete",
            postcode=clean_postcode,
            count=len(all_certs),
        )
        return all_certs

    # ------------------------------------------------------------------
    # Search by address
    # ------------------------------------------------------------------

    async def search_by_address(
        self,
        address: str,
        page_size: int = 50,
    ) -> list[dict[str, Any]]:
        """Search for EPC certificates by address text."""
        params: dict[str, Any] = {
            "address": address.strip(),
            "size": page_size,
        }

        self.log.info("epc_address_search", address=address)

        try:
            data = await self._api_get("domestic/search", params=params)
            return data.get("rows", [])
        except Exception as exc:
            self.metrics.record_error(exc, context=f"epc_address:{address}")
            self.log.warning(
                "epc_address_failed",
                address=address,
                error=str(exc),
            )
            return []

    # ------------------------------------------------------------------
    # Rating distribution
    # ------------------------------------------------------------------

    async def get_rating_distribution(
        self,
        postcode: str | None = None,
        address: str | None = None,
    ) -> dict[str, Any]:
        """
        Get EPC rating distribution for a given postcode or address.

        Returns a dict with:
        - ratings: Counter of {A: n, B: n, C: n, ...}
        - average_score: mean energy efficiency score
        - total: total certificates found
        - certificates: list of individual cert summaries
        """
        if postcode:
            certs = await self.search_by_postcode(postcode)
        elif address:
            certs = await self.search_by_address(address)
        else:
            return {"ratings": {}, "average_score": None, "total": 0, "certificates": []}

        ratings: Counter = Counter()
        scores: list[int] = []
        cert_summaries: list[dict[str, Any]] = []

        for cert in certs:
            rating = cert.get("current-energy-rating", "").upper()
            if rating in ("A", "B", "C", "D", "E", "F", "G"):
                ratings[rating] += 1

            score_raw = cert.get("current-energy-efficiency")
            if score_raw:
                try:
                    scores.append(int(score_raw))
                except (ValueError, TypeError):
                    pass

            cert_summaries.append(
                {
                    "lmk_key": cert.get("lmk-key", ""),
                    "address": cert.get("address", ""),
                    "postcode": cert.get("postcode", ""),
                    "current_rating": rating,
                    "current_score": score_raw,
                    "potential_rating": cert.get("potential-energy-rating", ""),
                    "potential_score": cert.get("potential-energy-efficiency"),
                    "property_type": cert.get("property-type", ""),
                    "built_form": cert.get("built-form", ""),
                    "inspection_date": cert.get("inspection-date"),
                    "total_floor_area": cert.get("total-floor-area"),
                }
            )

        avg_score = round(sum(scores) / len(scores), 1) if scores else None

        return {
            "ratings": dict(ratings),
            "average_score": avg_score,
            "total": len(certs),
            "certificates": cert_summaries,
        }

    # ------------------------------------------------------------------
    # Individual certificate
    # ------------------------------------------------------------------

    async def get_certificate(self, lmk_key: str) -> dict[str, Any] | None:
        """Fetch a single EPC certificate by its LMK key."""
        try:
            data = await self._api_get(f"domestic/certificate/{lmk_key}")
            rows = data.get("rows", [])
            return rows[0] if rows else None
        except Exception as exc:
            self.metrics.record_error(exc, context=f"epc_cert:{lmk_key}")
            return None

    # ------------------------------------------------------------------
    # Non-domestic certificates
    # ------------------------------------------------------------------

    async def search_non_domestic(
        self,
        postcode: str,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Search for non-domestic EPC certificates by postcode."""
        params: dict[str, Any] = {
            "postcode": postcode.strip().upper(),
            "size": page_size,
        }
        try:
            data = await self._api_get("non-domestic/search", params=params)
            return data.get("rows", [])
        except Exception as exc:
            self.metrics.record_error(
                exc, context=f"epc_nondom:{postcode}"
            )
            return []

    # ------------------------------------------------------------------
    # Abstract method stubs (EPC doesn't follow planning search pattern)
    # ------------------------------------------------------------------

    async def search_applications(self, **kwargs: Any) -> list[dict[str, Any]]:
        """EPC scraper uses search_by_postcode / search_by_address instead."""
        postcode = kwargs.get("postcode")
        address = kwargs.get("address")
        if postcode:
            return await self.search_by_postcode(postcode)
        elif address:
            return await self.search_by_address(address)
        return []

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Convert a raw EPC certificate dict to model-ready format."""
        return {
            "lmk_key": raw.get("lmk-key", ""),
            "address": raw.get("address", ""),
            "postcode": raw.get("postcode", ""),
            "current_rating": raw.get("current-energy-rating", ""),
            "current_score": self._safe_int(raw.get("current-energy-efficiency")),
            "potential_rating": raw.get("potential-energy-rating", ""),
            "potential_score": self._safe_int(raw.get("potential-energy-efficiency")),
            "property_type": raw.get("property-type", ""),
            "built_form": raw.get("built-form", ""),
            "inspection_date": self._parse_date(raw.get("inspection-date")),
            "lodgement_date": self._parse_date(raw.get("lodgement-date")),
            "transaction_type": raw.get("transaction-type", ""),
            "total_floor_area": self._safe_float(raw.get("total-floor-area")),
        }

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """Not applicable to EPC — use get_certificate instead."""
        return {}

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
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

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return datetime.fromisoformat(str(value)).date()
        except (ValueError, TypeError):
            pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"):
            try:
                return datetime.strptime(str(value).strip(), fmt).date()
            except ValueError:
                continue
        return None
