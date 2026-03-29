"""
Scraper for the Regulator of Social Housing (RSH) website.

Retrieves regulatory judgements (governance and viability ratings)
for Registered Providers (RPs). This data is used to assess the
financial health and governance quality of housing operators/owners.

Source: https://www.gov.uk/government/organisations/regulator-of-social-housing
Regulatory judgements: https://www.gov.uk/government/collections/regulatory-judgements
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

import structlog
from bs4 import BeautifulSoup, Tag

from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

RSH_BASE_URL = "https://www.gov.uk"
JUDGEMENTS_COLLECTION_URL = (
    "https://www.gov.uk/government/collections/regulatory-judgements"
)
RSH_SEARCH_URL = (
    "https://www.gov.uk/government/publications"
)

# Valid governance ratings: G1, G2, G3, G4
# Valid viability ratings: V1, V2, V3, V4
GOVERNANCE_RATINGS = {"G1", "G2", "G3", "G4"}
VIABILITY_RATINGS = {"V1", "V2", "V3", "V4"}


class RSHScraper(BaseScraper):
    """
    Scraper for RSH regulatory judgements.

    Scrapes the GOV.UK publications pages for regulatory judgement
    documents, extracting provider name, governance rating, viability
    rating, and judgement date.
    """

    def __init__(
        self,
        rate_limit: float | None = 3.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            council_name="Regulator of Social Housing",
            council_id=0,
            portal_url=RSH_BASE_URL,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        max_pages: int = 10,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Search GOV.UK publications for RSH regulatory judgements.

        Paginates through the publications listing to collect links to
        individual judgement pages.
        """
        all_results: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for page in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "departments[]": "regulator-of-social-housing",
                "publication_filter_option": "regulatory-judgements",  # GOV.UK filter
                "page": page,
            }

            self.log.info("rsh_search_page", page=page)

            try:
                resp = await self.fetch(
                    RSH_SEARCH_URL,
                    params=params,
                    use_cache=False,
                )
            except Exception as exc:
                self.metrics.record_error(exc, context=f"rsh_search_page:{page}")
                self.log.warning("rsh_search_failed", page=page, error=str(exc))
                break

            page_results = self._parse_publications_list(resp.text)

            if not page_results:
                break

            for r in page_results:
                url = r.get("detail_url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(r)

            self.log.info(
                "rsh_page_complete",
                page=page,
                count=len(page_results),
                cumulative=len(all_results),
            )

        self.metrics.applications_found = len(all_results)
        return all_results

    def _parse_publications_list(self, html: str) -> list[dict[str, Any]]:
        """
        Parse a GOV.UK publications listing page.

        Publications are listed as <li class="gem-c-document-list__item">
        or similar structures containing a title link and metadata.
        """
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        # GOV.UK document list items
        items = soup.select(
            "li.gem-c-document-list__item, "
            "li.document-row, "
            "div.document-list li, "
            "li.gem-c-document-list__item--with-subtext"
        )

        for item in items:
            link = item.find("a", href=True)
            if not link or not isinstance(link, Tag):
                continue

            title = link.get_text(strip=True)
            href = str(link.get("href", ""))

            # Only include judgement-related publications
            title_lower = title.lower()
            if not any(
                kw in title_lower
                for kw in [
                    "regulatory judgement",
                    "regulatory notice",
                    "governance and viability",
                    "grading under review",
                ]
            ):
                continue

            detail_url = urljoin(RSH_BASE_URL, href)

            # Try to extract date from metadata
            date_el = item.find(
                ["time", "span"],
                class_=re.compile(r"date|published|timestamp", re.IGNORECASE),
            )
            published_date = ""
            if date_el:
                datetime_attr = date_el.get("datetime") if isinstance(date_el, Tag) else None
                if datetime_attr:
                    published_date = str(datetime_attr)
                else:
                    published_date = date_el.get_text(strip=True)

            # Extract provider name from title
            provider_name = self._extract_provider_from_title(title)

            results.append(
                {
                    "title": title,
                    "detail_url": detail_url,
                    "provider_name": provider_name,
                    "published_date": published_date,
                }
            )

        # Fallback: <ol> based listings on GOV.UK
        if not results:
            for li in soup.select("ol.gem-c-document-list li, ul.document-list li"):
                link = li.find("a", href=True)
                if link and isinstance(link, Tag):
                    title = link.get_text(strip=True)
                    if "judgement" in title.lower() or "regulatory" in title.lower():
                        results.append(
                            {
                                "title": title,
                                "detail_url": urljoin(
                                    RSH_BASE_URL, str(link.get("href", ""))
                                ),
                                "provider_name": self._extract_provider_from_title(title),
                                "published_date": "",
                            }
                        )

        return results

    @staticmethod
    def _extract_provider_from_title(title: str) -> str:
        """
        Extract the registered provider name from a judgement title.

        Typical formats:
        - "Regulatory judgement: Acme Housing Association"
        - "Regulatory notice: Acme Housing Limited"
        - "Governance and viability ratings: Acme RP"
        """
        for separator in [":", " - ", " – "]:
            if separator in title:
                parts = title.split(separator, 1)
                if len(parts) == 2:
                    return parts[1].strip()
        return title.strip()

    # ------------------------------------------------------------------
    # Detail page
    # ------------------------------------------------------------------

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """Fetch and parse an RSH regulatory judgement detail page."""
        resp = await self.fetch(detail_url, use_cache=True)
        return self._parse_judgement_page(resp.text, detail_url)

    def _parse_judgement_page(self, html: str, detail_url: str) -> dict[str, Any]:
        """
        Parse an RSH judgement detail page from GOV.UK.

        The judgement page typically contains:
        - Provider name in the title
        - Governance rating (G1-G4) and viability rating (V1-V4) in the body
        - Date of judgement
        - Narrative text explaining the ratings
        """
        soup = BeautifulSoup(html, "html.parser")

        # Title
        title_el = soup.find("h1") or soup.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        provider_name = self._extract_provider_from_title(title)

        # Get main content body
        content = soup.find(
            "div", class_=re.compile(r"govspeak|body|content", re.IGNORECASE)
        )
        body_text = content.get_text(" ", strip=True) if content else soup.get_text(" ", strip=True)

        # Extract governance rating
        governance_rating = self._extract_rating(body_text, GOVERNANCE_RATINGS)
        viability_rating = self._extract_rating(body_text, VIABILITY_RATINGS)

        # Extract provider code (e.g., "L4321" or "4567")
        provider_code = ""
        code_match = re.search(
            r"(?:provider\s+(?:code|number|ref)[\s:]*)?([A-Z]?\d{3,5})",
            body_text,
            re.IGNORECASE,
        )
        if code_match:
            potential_code = code_match.group(1)
            # Only accept if it looks like an RP code (not a date or random number)
            if len(potential_code) <= 6:
                provider_code = potential_code

        # Extract judgement date
        judgement_date = self._extract_judgement_date(soup, body_text)

        # Judgement type
        judgement_type = "Regulatory Judgement"
        if "notice" in title.lower():
            judgement_type = "Regulatory Notice"
        elif "grading under review" in body_text.lower():
            judgement_type = "Grading Under Review"

        # Get narrative — first few paragraphs of the body
        narrative = ""
        if content:
            paragraphs = content.find_all("p")
            narrative_parts = []
            for p in paragraphs[:5]:
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    narrative_parts.append(text)
            narrative = " ".join(narrative_parts)

        return {
            "provider_name": provider_name,
            "provider_code": provider_code,
            "governance_rating": governance_rating,
            "viability_rating": viability_rating,
            "judgement_date": judgement_date,
            "judgement_type": judgement_type,
            "narrative": narrative,
            "detail_url": detail_url,
            "title": title,
        }

    @staticmethod
    def _extract_rating(
        text: str, valid_ratings: set[str]
    ) -> str | None:
        """Extract a G or V rating from text."""
        for rating in sorted(valid_ratings):
            # Look for patterns like "G1", "rated G2", "governance: G3"
            pattern = rf"\b{rating}\b"
            if re.search(pattern, text, re.IGNORECASE):
                return rating.upper()
        return None

    def _extract_judgement_date(
        self, soup: BeautifulSoup, body_text: str
    ) -> date | None:
        """Extract the judgement date from the page."""
        # Try metadata elements first
        for selector in [
            'time[class*="published"]',
            'time[datetime]',
            'dd.gem-c-metadata__definition time',
            'span.date',
        ]:
            el = soup.select_one(selector)
            if el:
                dt_attr = el.get("datetime") if isinstance(el, Tag) else None
                if dt_attr:
                    try:
                        return datetime.fromisoformat(str(dt_attr).replace("Z", "+00:00")).date()
                    except ValueError:
                        pass
                text = el.get_text(strip=True)
                parsed = self._try_parse_date(text)
                if parsed:
                    return parsed

        # Try to find a date in the body text near "published" or "judgement"
        date_patterns = [
            r"(\d{1,2}\s+\w+\s+\d{4})",
            r"(\d{1,2}/\d{1,2}/\d{4})",
        ]
        for pattern in date_patterns:
            match = re.search(pattern, body_text)
            if match:
                parsed = self._try_parse_date(match.group(1))
                if parsed:
                    return parsed

        return None

    @staticmethod
    def _try_parse_date(text: str) -> date | None:
        for fmt in (
            "%d %B %Y",
            "%d %b %Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
        ):
            try:
                return datetime.strptime(text.strip(), fmt).date()
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Fetch detail page for an RSH judgement and return a dict matching
        RSHJudgement-like fields.
        """
        detail_url = raw.get("detail_url", "")
        detail: dict[str, Any] = {}

        if detail_url:
            try:
                detail = await self.get_application_detail(detail_url)
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"rsh_detail:{raw.get('provider_name')}"
                )
                self.log.warning(
                    "rsh_detail_failed",
                    provider_name=raw.get("provider_name"),
                    error=str(exc),
                )

        merged = {**raw, **detail}

        return {
            "provider_name": merged.get("provider_name", ""),
            "provider_code": merged.get("provider_code", ""),
            "governance_rating": merged.get("governance_rating"),
            "viability_rating": merged.get("viability_rating"),
            "judgement_date": merged.get("judgement_date"),
            "judgement_type": merged.get("judgement_type", ""),
            "narrative": merged.get("narrative", ""),
            "detail_url": merged.get("detail_url", ""),
        }

    # ------------------------------------------------------------------
    # Convenience: look up provider by name
    # ------------------------------------------------------------------

    async def lookup_provider(self, provider_name: str) -> dict[str, Any] | None:
        """
        Search for the most recent regulatory judgement for a specific
        registered provider by name.
        """
        results = await self.search_applications(max_pages=5)

        for result in results:
            if provider_name.lower() in result.get("provider_name", "").lower():
                return await self.parse_application(result)

        return None
