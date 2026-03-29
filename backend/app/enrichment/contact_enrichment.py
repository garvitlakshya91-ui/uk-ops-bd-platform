"""Multi-source contact discovery and enrichment pipeline.

Discovers relevant BD contacts at a company by cascading through multiple
data sources: Apollo.io, Hunter.io, and direct website scraping.

Typical usage::

    from app.enrichment.contact_enrichment import ContactEnrichmentPipeline

    pipeline = ContactEnrichmentPipeline(db_session=session)
    contacts = await pipeline.enrich(company)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from sqlalchemy.orm import Session

from app.config import settings
from app.models.models import Company, Contact

logger = structlog.get_logger(__name__)

# Job titles relevant to BD outreach in the UK property/operations sector.
BD_RELEVANT_TITLES: list[str] = [
    "Development Director",
    "Asset Manager",
    "Operations Director",
    "Managing Director",
    "Head of Development",
    "Head of BTR",
    "Head of Build to Rent",
    "Head of Operations",
    "Head of Asset Management",
    "Director of Development",
    "Director of Operations",
    "Chief Operating Officer",
    "COO",
    "Chief Executive",
    "CEO",
    "Fund Manager",
    "Investment Director",
    "Head of Residential",
    "Head of Living",
    "Portfolio Manager",
    "Regional Director",
    "Property Director",
]

# Normalised set for fast matching.
_BD_TITLES_LOWER: set[str] = {t.lower() for t in BD_RELEVANT_TITLES}


def _is_relevant_title(title: str | None) -> bool:
    """Return ``True`` if the title looks BD-relevant."""
    if not title:
        return False
    title_lower = title.lower()
    # Exact match first.
    if title_lower in _BD_TITLES_LOWER:
        return True
    # Substring match for common keywords.
    keywords = (
        "director", "head of", "managing", "asset manag",
        "development", "operations", "ceo", "coo", "chief",
        "btr", "build to rent", "investment", "portfolio",
        "residential", "living",
    )
    return any(kw in title_lower for kw in keywords)


@dataclass
class DiscoveredContact:
    """Intermediate representation of a contact found from any source."""

    full_name: str
    job_title: str | None = None
    email: str | None = None
    phone: str | None = None
    linkedin_url: str | None = None
    source: str = ""
    confidence_score: float = 0.0

    @property
    def dedup_key(self) -> str:
        """Key used for deduplication across sources."""
        if self.email:
            return self.email.lower().strip()
        return self.full_name.lower().strip()


# ---------------------------------------------------------------------------
# Apollo.io enricher
# ---------------------------------------------------------------------------

class ApolloEnricher:
    """Find contacts via the Apollo.io People Search API.

    Uses ``POST /v1/mixed_people/search`` to find people associated with a
    company domain, filtering by BD-relevant job titles.
    """

    API_BASE = "https://api.apollo.io"
    CONFIDENCE = 0.9

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.APOLLO_API_KEY

    async def find_contacts(
        self,
        company_domain: str,
        company_name: str | None = None,
    ) -> list[DiscoveredContact]:
        """Search Apollo for contacts at the given company domain."""
        if not self._api_key:
            logger.warning("apollo_api_key_missing")
            return []

        title_keywords = [
            "Director",
            "Head of",
            "Managing Director",
            "CEO",
            "COO",
            "Asset Manager",
            "Operations",
            "Development",
        ]

        payload: dict[str, Any] = {
            "api_key": self._api_key,
            "q_organization_domains": company_domain,
            "page": 1,
            "per_page": 25,
            "person_titles": title_keywords,
        }

        if company_name:
            payload["q_organization_name"] = company_name

        contacts: list[DiscoveredContact] = []

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{self.API_BASE}/v1/mixed_people/search",
                    json=payload,
                )
                resp.raise_for_status()
                data = resp.json()

            for person in data.get("people", []):
                title = person.get("title")
                if not _is_relevant_title(title):
                    continue

                contacts.append(
                    DiscoveredContact(
                        full_name=person.get("name", ""),
                        job_title=title,
                        email=person.get("email"),
                        phone=_first_phone(person.get("phone_numbers")),
                        linkedin_url=person.get("linkedin_url"),
                        source="apollo",
                        confidence_score=self.CONFIDENCE,
                    )
                )

            logger.info(
                "apollo_contacts_found",
                domain=company_domain,
                total_people=len(data.get("people", [])),
                relevant_contacts=len(contacts),
            )

        except httpx.HTTPStatusError as exc:
            logger.error(
                "apollo_http_error",
                status=exc.response.status_code,
                body=exc.response.text[:300],
            )
        except httpx.RequestError as exc:
            logger.error("apollo_request_error", error=str(exc))

        return contacts


# ---------------------------------------------------------------------------
# Hunter.io enricher
# ---------------------------------------------------------------------------

class HunterEnricher:
    """Find and verify contacts via the Hunter.io API.

    Provides domain search, individual email lookup, and email verification.
    """

    API_BASE = "https://api.hunter.io"
    CONFIDENCE = 0.8

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.HUNTER_API_KEY

    async def domain_search(self, domain: str) -> list[DiscoveredContact]:
        """Search for all publicly-known emails at *domain*."""
        if not self._api_key:
            logger.warning("hunter_api_key_missing")
            return []

        contacts: list[DiscoveredContact] = []

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    f"{self.API_BASE}/v2/domain-search",
                    params={"domain": domain, "api_key": self._api_key, "limit": 50},
                )
                resp.raise_for_status()
                data = resp.json().get("data", {})

            for email_obj in data.get("emails", []):
                name_parts = [
                    email_obj.get("first_name", ""),
                    email_obj.get("last_name", ""),
                ]
                full_name = " ".join(p for p in name_parts if p).strip()
                if not full_name:
                    continue

                title = email_obj.get("position")
                if not _is_relevant_title(title):
                    continue

                # Hunter provides a per-email confidence (0-100).
                hunter_conf = email_obj.get("confidence", 80) / 100.0
                score = min(self.CONFIDENCE, hunter_conf)

                contacts.append(
                    DiscoveredContact(
                        full_name=full_name,
                        job_title=title,
                        email=email_obj.get("value"),
                        phone=email_obj.get("phone_number"),
                        linkedin_url=email_obj.get("linkedin"),
                        source="hunter",
                        confidence_score=score,
                    )
                )

            logger.info(
                "hunter_domain_search",
                domain=domain,
                contacts_found=len(contacts),
            )

        except httpx.HTTPStatusError as exc:
            logger.error("hunter_http_error", status=exc.response.status_code)
        except httpx.RequestError as exc:
            logger.error("hunter_request_error", error=str(exc))

        return contacts

    async def find_email(
        self,
        domain: str,
        first_name: str,
        last_name: str,
    ) -> str | None:
        """Attempt to find an individual's email at *domain*."""
        if not self._api_key:
            return None
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(
                    f"{self.API_BASE}/v2/email-finder",
                    params={
                        "domain": domain,
                        "first_name": first_name,
                        "last_name": last_name,
                        "api_key": self._api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json().get("data", {})
                return data.get("email")
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.error("hunter_email_finder_error", error=str(exc))
            return None

    async def verify_email(self, email: str) -> dict[str, Any]:
        """Verify deliverability of *email*.

        Returns a dict with keys ``status``, ``score``, and ``result``.
        """
        if not self._api_key:
            return {"status": "unknown", "score": 0, "result": "api_key_missing"}
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(
                    f"{self.API_BASE}/v2/email-verifier",
                    params={"email": email, "api_key": self._api_key},
                )
                resp.raise_for_status()
                data = resp.json().get("data", {})
                return {
                    "status": data.get("status", "unknown"),
                    "score": data.get("score", 0),
                    "result": data.get("result", "unknown"),
                }
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.error("hunter_verify_error", error=str(exc))
            return {"status": "error", "score": 0, "result": str(exc)}


# ---------------------------------------------------------------------------
# Website scraper
# ---------------------------------------------------------------------------

# Regex patterns for extracting contact info from web pages.
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)
_PHONE_RE = re.compile(
    r"(?:\+44|0)\s*(?:\d[\s\-]?){9,10}\d",
)
_PERSON_SCHEMA_RE = re.compile(
    r'"@type"\s*:\s*"Person".*?"name"\s*:\s*"([^"]+)"'
    r'(?:.*?"jobTitle"\s*:\s*"([^"]+)")?'
    r'(?:.*?"email"\s*:\s*"([^"]+)")?',
    re.DOTALL,
)

# Common team/about page paths to try.
_TEAM_PATHS = [
    "/team",
    "/our-team",
    "/about",
    "/about-us",
    "/people",
    "/leadership",
    "/contact",
    "/contact-us",
]


class WebsiteContactScraper:
    """Scrape company websites for contact information.

    Tries common team/about page URLs, extracts names, titles, emails, and
    phone numbers using regex and Schema.org Person markup.
    """

    CONFIDENCE = 0.6

    async def scrape(self, website_url: str) -> list[DiscoveredContact]:
        """Scrape the given *website_url* for contacts."""
        if not website_url:
            return []

        # Normalise base URL.
        parsed = urlparse(website_url)
        if not parsed.scheme:
            website_url = f"https://{website_url}"
        base = f"{urlparse(website_url).scheme}://{urlparse(website_url).netloc}"

        contacts: list[DiscoveredContact] = []
        visited: set[str] = set()

        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "UKOpsBDBot/1.0 (contact enrichment)"},
        ) as client:
            for path in _TEAM_PATHS:
                url = urljoin(base, path)
                if url in visited:
                    continue
                visited.add(url)

                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    html = resp.text
                    contacts.extend(self._extract_contacts(html))
                except httpx.RequestError:
                    continue

        # Deduplicate within scrape results.
        seen: set[str] = set()
        unique: list[DiscoveredContact] = []
        for c in contacts:
            if c.dedup_key not in seen:
                seen.add(c.dedup_key)
                unique.append(c)

        logger.info(
            "website_scrape_complete",
            website=website_url,
            contacts_found=len(unique),
        )
        return unique

    def _extract_contacts(self, html: str) -> list[DiscoveredContact]:
        """Extract contacts from raw HTML using regex and Schema.org markup."""
        contacts: list[DiscoveredContact] = []

        # 1) Schema.org Person markup.
        for match in _PERSON_SCHEMA_RE.finditer(html):
            name = match.group(1)
            title = match.group(2)
            email = match.group(3)
            if name and _is_relevant_title(title):
                contacts.append(
                    DiscoveredContact(
                        full_name=name,
                        job_title=title,
                        email=email,
                        source="website_scrape",
                        confidence_score=self.CONFIDENCE,
                    )
                )

        # 2) Collect all emails and phones found on the page as potential
        #    contact details (associated with Schema.org contacts above or
        #    standalone for later manual review).
        emails = _EMAIL_RE.findall(html)
        phones = _PHONE_RE.findall(html)

        # Filter out generic/noise emails.
        _noise = {"info@", "admin@", "support@", "noreply@", "no-reply@", "webmaster@", "privacy@"}
        emails = [e for e in emails if not any(e.lower().startswith(n) for n in _noise)]

        # If we found emails not yet associated with a person, add them as
        # unattributed contacts for the enrichment pipeline to attempt to
        # match later.
        existing_emails = {c.email for c in contacts if c.email}
        for email in emails:
            if email not in existing_emails:
                contacts.append(
                    DiscoveredContact(
                        full_name="(Unknown from website)",
                        email=email,
                        source="website_scrape",
                        confidence_score=self.CONFIDENCE * 0.5,
                    )
                )

        return contacts


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class ContactEnrichmentPipeline:
    """Orchestrates contact discovery across multiple sources.

    Tries sources in order of reliability:

    1. **Apollo.io** (confidence 0.9)
    2. **Hunter.io** (confidence 0.8)
    3. **Website scraping** (confidence 0.6)

    Deduplicates across sources, keeping the highest-confidence version of
    each contact, and persists results to the ``contacts`` table.

    Parameters
    ----------
    db_session : Session
        SQLAlchemy database session for persistence.
    """

    def __init__(self, db_session: Session) -> None:
        self._db = db_session
        self._apollo = ApolloEnricher()
        self._hunter = HunterEnricher()
        self._scraper = WebsiteContactScraper()

    async def enrich(self, company: Company) -> list[Contact]:
        """Run the full enrichment pipeline for *company*.

        Returns a list of new or updated :class:`Contact` ORM instances that
        have been flushed (but not committed) to the session.
        """
        log = logger.bind(company_id=company.id, company_name=company.name)
        all_discovered: list[DiscoveredContact] = []

        domain = self._extract_domain(company.website)

        # 1. Apollo
        if domain:
            try:
                apollo_contacts = await self._apollo.find_contacts(
                    company_domain=domain,
                    company_name=company.name,
                )
                all_discovered.extend(apollo_contacts)
                log.info("apollo_enrichment_done", count=len(apollo_contacts))
            except Exception:
                log.exception("apollo_enrichment_failed")

        # 2. Hunter
        if domain:
            try:
                hunter_contacts = await self._hunter.domain_search(domain)
                all_discovered.extend(hunter_contacts)
                log.info("hunter_enrichment_done", count=len(hunter_contacts))
            except Exception:
                log.exception("hunter_enrichment_failed")

        # 3. Website scrape
        if company.website:
            try:
                scraped = await self._scraper.scrape(company.website)
                all_discovered.extend(scraped)
                log.info("website_scrape_done", count=len(scraped))
            except Exception:
                log.exception("website_scrape_failed")

        # Deduplicate across all sources, keeping highest confidence.
        deduped = self._deduplicate(all_discovered)
        log.info("contacts_deduplicated", before=len(all_discovered), after=len(deduped))

        # Persist to database.
        saved: list[Contact] = []
        for dc in deduped:
            contact = self._upsert_contact(company.id, dc)
            saved.append(contact)

        self._db.flush()
        log.info("contacts_saved", count=len(saved))
        return saved

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_domain(website: str | None) -> str | None:
        """Extract the bare domain from a URL."""
        if not website:
            return None
        parsed = urlparse(website if "://" in website else f"https://{website}")
        host = parsed.netloc or parsed.path.split("/")[0]
        # Strip www.
        if host.startswith("www."):
            host = host[4:]
        return host or None

    @staticmethod
    def _deduplicate(
        contacts: list[DiscoveredContact],
    ) -> list[DiscoveredContact]:
        """Deduplicate contacts, keeping the version with highest confidence."""
        best: dict[str, DiscoveredContact] = {}
        for c in contacts:
            key = c.dedup_key
            existing = best.get(key)
            if existing is None or c.confidence_score > existing.confidence_score:
                best[key] = c
            else:
                # Merge any missing fields from lower-confidence duplicate.
                if not existing.email and c.email:
                    existing.email = c.email
                if not existing.phone and c.phone:
                    existing.phone = c.phone
                if not existing.linkedin_url and c.linkedin_url:
                    existing.linkedin_url = c.linkedin_url
                if not existing.job_title and c.job_title:
                    existing.job_title = c.job_title
        return list(best.values())

    def _upsert_contact(self, company_id: int, dc: DiscoveredContact) -> Contact:
        """Insert or update a contact in the database."""
        existing: Contact | None = None
        if dc.email:
            existing = (
                self._db.query(Contact)
                .filter(Contact.company_id == company_id, Contact.email == dc.email)
                .first()
            )
        if existing is None:
            existing = (
                self._db.query(Contact)
                .filter(
                    Contact.company_id == company_id,
                    Contact.full_name == dc.full_name,
                )
                .first()
            )

        if existing:
            # Update fields if the new source has higher confidence.
            if dc.confidence_score >= (existing.confidence_score or 0):
                if dc.job_title:
                    existing.job_title = dc.job_title
                if dc.email:
                    existing.email = dc.email
                if dc.phone:
                    existing.phone = dc.phone
                if dc.linkedin_url:
                    existing.linkedin_url = dc.linkedin_url
                existing.source = dc.source
                existing.confidence_score = dc.confidence_score
            return existing

        contact = Contact(
            company_id=company_id,
            full_name=dc.full_name,
            job_title=dc.job_title,
            email=dc.email,
            phone=dc.phone,
            linkedin_url=dc.linkedin_url,
            source=dc.source,
            confidence_score=dc.confidence_score,
        )
        self._db.add(contact)
        return contact


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _first_phone(phone_numbers: Any) -> str | None:
    """Extract the first sanitised phone number from an Apollo phone list."""
    if not phone_numbers or not isinstance(phone_numbers, list):
        return None
    for pn in phone_numbers:
        if isinstance(pn, dict):
            num = pn.get("sanitized_number") or pn.get("raw_number")
            if num:
                return num
        elif isinstance(pn, str):
            return pn
    return None
