"""Charity Commission API client for verifying registered charity status.

Cross-references Company records (especially Registered Providers) against
the Charity Commission register to verify charitable status and extract
trustee/leadership information.

API: https://api.charitycommission.gov.uk/register/api/
Alternative: https://charitybase.uk/api/graphql (free, no key needed for basic search)
"""

from __future__ import annotations
import httpx
import structlog
from typing import Any

logger = structlog.get_logger(__name__)

CHARITY_SEARCH_URL = "https://api.charitycommission.gov.uk/register/api/SearchCharities"
CHARITY_DETAIL_URL = "https://api.charitycommission.gov.uk/register/api/allcharitydetails"

# Alternative free API (no key needed)
CHARITY_BASE_URL = "https://charitybase.uk/api/graphql"


class CharityCommissionClient:
    """Client for the Charity Commission of England & Wales API."""

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = {"Accept": "application/json"}
            if self._api_key:
                headers["Ocp-Apim-Subscription-Key"] = self._api_key
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                headers=headers,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def search_charity(self, name: str) -> list[dict[str, Any]]:
        """Search for a charity by name using CharityBase GraphQL API."""
        client = await self._get_client()
        query = '''
        {
            CHC {
                getCharities(filters: {
                    search: "%s"
                    grants: {}
                }) {
                    count
                    list(limit: 5) {
                        id
                        names {
                            value
                            primary
                        }
                        activities
                        numPeople
                        income {
                            latest {
                                total
                            }
                        }
                        registrations {
                            registrationDate
                            removalDate
                        }
                        contact {
                            address
                            email
                            phone
                        }
                        trustees {
                            list(limit: 10) {
                                id
                                name
                            }
                        }
                    }
                }
            }
        }
        ''' % name.replace('"', '\\"')

        try:
            resp = await client.post(
                CHARITY_BASE_URL,
                json={"query": query},
            )
            if resp.status_code != 200:
                logger.warning("charity_search_error", status=resp.status_code, name=name)
                return []

            data = resp.json()
            charities = (
                data.get("data", {})
                .get("CHC", {})
                .get("getCharities", {})
                .get("list", [])
            )

            results = []
            for charity in charities:
                primary_name = ""
                for n in charity.get("names", []):
                    if n.get("primary"):
                        primary_name = n.get("value", "")
                        break

                results.append({
                    "charity_number": charity.get("id", ""),
                    "name": primary_name,
                    "activities": charity.get("activities", ""),
                    "income": (charity.get("income", {}).get("latest", {}) or {}).get("total"),
                    "num_trustees": charity.get("numPeople"),
                    "trustees": [
                        t.get("name", "") for t in
                        (charity.get("trustees", {}).get("list", []) or [])
                    ],
                    "contact": charity.get("contact", {}),
                    "registrations": charity.get("registrations", []),
                })

            logger.info("charity_search_complete", name=name, results=len(results))
            return results

        except Exception as exc:
            logger.warning("charity_search_failed", name=name, error=str(exc))
            return []

    async def is_registered_charity(self, name: str) -> dict[str, Any] | None:
        """Check if a company name matches a registered charity.

        Returns charity details if found, None otherwise.
        """
        results = await self.search_charity(name)
        if not results:
            return None

        # Check for name match
        name_lower = name.lower().strip()
        for charity in results:
            charity_name = charity.get("name", "").lower().strip()
            # Check if names are similar enough
            if name_lower in charity_name or charity_name in name_lower:
                # Verify it's still registered (no removal date)
                registrations = charity.get("registrations", [])
                is_active = any(
                    not reg.get("removalDate")
                    for reg in registrations
                ) if registrations else True

                if is_active:
                    return charity

        return None
