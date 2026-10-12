"""
Scraper for the Energy Performance Certificate (EPC) register.

Retrieves the certificates lodged at a postcode or address and summarises
their energy ratings. This data enriches existing scheme and planning
application records with energy performance information.

API and auth: see app.scrapers.epc_client.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import structlog

from app.scrapers.epc_client import EPCClient, EPCQueryError

logger = structlog.get_logger(__name__)


class EPCScraper:
    """
    Reads the EPC register by postcode or address.

    Provides methods to:
    - Search EPCs by postcode
    - Search EPCs by address
    - Get rating distribution for a postcode/address
    - Retrieve individual certificate details
    """

    def __init__(self, client: EPCClient | None = None) -> None:
        self.log = logger.bind(scraper="EPCScraper")
        self.client = client or EPCClient()

    async def __aenter__(self) -> "EPCScraper":
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.client.__aexit__(*args)

    # ------------------------------------------------------------------
    # Search by postcode
    # ------------------------------------------------------------------

    async def search_by_postcode(self, postcode: str) -> list[dict[str, Any]]:
        """
        Search for EPC certificates by postcode.

        Returns the register's summary rows: certificate-number, address,
        address1..3, postcode, current-energy-rating, lodgement-date,
        local-authority-label, schema-type. The score, property type and
        floor area are on the full certificate (get_certificate).

        A postcode the register refuses as malformed has no certificates.
        Any other failure raises.
        """
        clean_postcode = postcode.strip().upper()

        try:
            certs = await self.client.search_domestic(postcode=clean_postcode)
        except EPCQueryError as exc:
            self.log.warning(
                "epc_postcode_rejected",
                postcode=clean_postcode,
                error=str(exc),
            )
            return []

        self.log.info(
            "epc_postcode_complete",
            postcode=clean_postcode,
            count=len(certs),
        )
        return certs

    # ------------------------------------------------------------------
    # Search by address
    # ------------------------------------------------------------------

    async def search_by_address(
        self,
        address: str,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Search for EPC certificates by address text."""
        self.log.info("epc_address_search", address=address)
        return await self.client.search_domestic(
            address=address, max_results=max_results,
        )

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
        - total: total certificates found
        - certificates: list of individual cert summaries

        The register's search no longer returns the numeric score, so there
        is no average score here.
        """
        if postcode:
            certs = await self.search_by_postcode(postcode)
        elif address:
            certs = await self.search_by_address(address)
        else:
            return {"ratings": {}, "total": 0, "certificates": []}

        ratings: Counter = Counter()
        cert_summaries: list[dict[str, Any]] = []

        for cert in certs:
            rating = cert.get("current-energy-rating", "").upper()
            if rating in ("A", "B", "C", "D", "E", "F", "G"):
                ratings[rating] += 1

            cert_summaries.append(
                {
                    "certificate_number": cert.get("certificate-number", ""),
                    "address": cert.get("address", ""),
                    "postcode": cert.get("postcode", ""),
                    "current_rating": rating,
                    "lodgement_date": cert.get("lodgement-date"),
                }
            )

        return {
            "ratings": dict(ratings),
            "total": len(certs),
            "certificates": cert_summaries,
        }

    # ------------------------------------------------------------------
    # Individual certificate
    # ------------------------------------------------------------------

    async def get_certificate(self, certificate_number: str) -> dict[str, Any] | None:
        """Fetch a single EPC certificate in full by its certificate number."""
        return await self.client.get_certificate(certificate_number)
