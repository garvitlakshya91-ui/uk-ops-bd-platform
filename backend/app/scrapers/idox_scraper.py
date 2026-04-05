"""
Scraper for Idox Public Access planning portals.

Approximately 240 UK councils use the Idox platform. Portals share a common
URL structure and page layout, though individual councils may customise
field names and CSS classes. This scraper uses Playwright for JavaScript
rendering and handles session cookies, CSRF tokens, pagination, and
advanced keyword searches.

Portal URL pattern: {base_url}/online-applications/
Search page:        {base_url}/online-applications/search.do?action=advanced
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional
from urllib.parse import urljoin, urlencode

import structlog
from bs4 import BeautifulSoup, NavigableString, Tag

from app.scrapers.base import BaseScraper, ScraperMetrics

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Council configuration
# ---------------------------------------------------------------------------

@dataclass
class IdoxCouncilConfig:
    """Per-council overrides for portal URLs and search parameters."""
    name: str
    council_id: int
    base_url: str
    search_path: str = "/online-applications/search.do?action=advanced"
    results_path: str = "/online-applications/pagedSearchResults.do"
    detail_path_prefix: str = "/online-applications/applicationDetails.do"
    extra_search_params: dict[str, str] = field(default_factory=dict)


# At least 30 real Idox council portal URLs
IDOX_COUNCILS: list[IdoxCouncilConfig] = [
    # Birmingham moved to NEC — uses Northgate/Apex portal, not Idox.
    IdoxCouncilConfig(name="Manchester", council_id=2, base_url="https://pa.manchester.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Leeds", council_id=3, base_url="https://publicaccess.leeds.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Bristol", council_id=4, base_url="https://pa.bristol.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Sheffield", council_id=5, base_url="https://planningapps.sheffield.gov.uk/online-applications"),
    # Liverpool moved to NEC (Northgate). Newcastle migrated off Idox.
    IdoxCouncilConfig(name="Nottingham", council_id=8, base_url="https://publicaccess.nottinghamcity.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Leicester", council_id=9, base_url="https://planning.leicester.gov.uk/online-applications"),
    # Coventry migrated off Idox to a custom portal.
    IdoxCouncilConfig(name="Bradford", council_id=11, base_url="https://planning.bradford.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Wolverhampton", council_id=12, base_url="https://planning.wolverhampton.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Plymouth", council_id=13, base_url="https://planning.plymouth.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Derby", council_id=14, base_url="https://eplanning.derby.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Southampton", council_id=15, base_url="https://planningpublicaccess.southampton.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Portsmouth", council_id=16, base_url="https://publicaccess.portsmouth.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Brighton", council_id=17, base_url="https://planningapps.brighton-hove.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Bournemouth", council_id=18, base_url="https://planning.bournemouth.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Sunderland", council_id=19, base_url="https://online-applications.sunderland.gov.uk/online-applications"),
    # Reading uses FastWeb/NEC — moved to NEC scraper.
    # IdoxCouncilConfig(name="Reading", council_id=20, base_url="https://planning.reading.gov.uk/fastweb"),
    IdoxCouncilConfig(name="Luton", council_id=21, base_url="https://planning.luton.gov.uk/online-applications"),
    # Milton Keynes migrated to Salesforce-based system.
    IdoxCouncilConfig(name="Walsall", council_id=23, base_url="https://planning.walsall.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Peterborough", council_id=24, base_url="https://planpa.peterborough.gov.uk/online-applications"),
    # Slough migrated to IEG4/Agile Applications.
    IdoxCouncilConfig(name="Oxford", council_id=26, base_url="https://public.oxford.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Cambridge", council_id=27, base_url="https://applications.greatercambridgeplanning.org/online-applications"),
    IdoxCouncilConfig(name="Exeter", council_id=28, base_url="https://publicaccess.exeter.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Gloucester", council_id=29, base_url="https://publicaccess.gloucester.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Westminster", council_id=30, base_url="https://idoxpa.westminster.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Camden", council_id=31, base_url="https://publicaccess.camden.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Hackney", council_id=32, base_url="https://publicaccess.hackney.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Tower Hamlets", council_id=33, base_url="https://development.towerhamlets.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Southwark", council_id=34, base_url="https://planning.southwark.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Lambeth", council_id=35, base_url="https://planning.lambeth.gov.uk/online-applications"),
    # Reclassified from Civica — these are actually Idox portals
    IdoxCouncilConfig(name="Barnet", council_id=36, base_url="https://publicaccess.barnet.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Harrow", council_id=37, base_url="https://planningsearch.harrow.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Ealing", council_id=38, base_url="https://pam.ealing.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Bexley", council_id=41, base_url="https://pa.bexley.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Enfield", council_id=42, base_url="https://planningandbuildingcontrol.enfield.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Kingston upon Thames", council_id=46, base_url="https://publicaccess.kingston.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Sutton", council_id=47, base_url="https://planningregister.sutton.gov.uk/online-applications"),
    # Reclassified from NEC — these are actually Idox portals
    IdoxCouncilConfig(name="Croydon", council_id=200, base_url="https://publicaccess3.croydon.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Bromley", council_id=201, base_url="https://searchapplications.bromley.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Greenwich", council_id=205, base_url="https://planning.royalgreenwich.gov.uk/online-applications"),
    IdoxCouncilConfig(name="Lewisham", council_id=204, base_url="https://planning.lewisham.gov.uk/online-applications"),
]

# Keywords cycled in advanced searches to find relevant applications
SEARCH_KEYWORDS: list[str] = [
    "build to rent",
    "BTR",
    "student accommodation",
    "PBSA",
    "co-living",
    "later living",
    "retirement",
    "affordable housing",
    "residential",
    "dwellings",
    "flats",
    "apartments",
]


class IdoxScraper(BaseScraper):
    """
    Scraper for a single Idox Public Access planning portal.

    Uses Playwright to render pages (many Idox portals rely on JavaScript
    for search submission and pagination). Falls back to httpx + BeautifulSoup
    for portals that serve static HTML.
    """

    def __init__(
        self,
        config: IdoxCouncilConfig,
        rate_limit: float | None = 3.0,
        proxy_url: str | None = None,
        use_playwright: bool = True,
    ) -> None:
        super().__init__(
            council_name=config.name,
            council_id=config.council_id,
            portal_url=config.base_url,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        self.config = config
        self.use_playwright = use_playwright
        self._csrf_token: str | None = None
        self._cookies: dict[str, str] = {}
        self._playwright = None
        self._browser = None
        self._page = None

    # ------------------------------------------------------------------
    # Playwright lifecycle
    # ------------------------------------------------------------------

    async def _init_playwright(self) -> None:
        """Lazily initialise Playwright browser."""
        if self._page is not None:
            return

        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        launch_kwargs: dict[str, Any] = {
            "headless": True,
        }
        if self.proxy_url:
            launch_kwargs["proxy"] = {"server": self.proxy_url}

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        context = await self._browser.new_context(
            user_agent=self._next_user_agent(),
            viewport={"width": 1280, "height": 900},
        )
        self._page = await context.new_page()
        self.log.info("playwright_init", council=self.council_name)

    async def _close_playwright(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._page = None
        self._browser = None
        self._playwright = None

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._close_playwright()
        await super().__aexit__(exc_type, exc_val, exc_tb)

    # ------------------------------------------------------------------
    # Session / CSRF handling
    # ------------------------------------------------------------------

    async def _init_session_cookies(self) -> None:
        """
        Visit the search page to obtain session cookies and CSRF tokens
        needed for subsequent requests.
        """
        search_url = self._build_url(self.config.search_path)

        if self.use_playwright:
            await self._init_playwright()
            assert self._page is not None
            await self._page.goto(search_url, wait_until="networkidle", timeout=30_000)
            await asyncio.sleep(1)

            # Extract CSRF token from hidden input
            csrf_input = await self._page.query_selector(
                'input[name="org.apache.struts.taglib.html.TOKEN"]'
            )
            if csrf_input:
                self._csrf_token = await csrf_input.get_attribute("value")
                self.log.info("csrf_token_found", token=self._csrf_token[:8] + "...")

            # Capture cookies
            cookies = await self._page.context.cookies()
            self._cookies = {c["name"]: c["value"] for c in cookies}
        else:
            resp = await self.fetch(search_url, use_cache=False)
            soup = BeautifulSoup(resp.text, "html.parser")
            csrf_el = soup.find(
                "input", {"name": "org.apache.struts.taglib.html.TOKEN"}
            )
            if csrf_el and isinstance(csrf_el, Tag):
                self._csrf_token = csrf_el.get("value", "")

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _build_url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/")
        # Strip /online-applications if base_url already contains it
        if "/online-applications" in base and path.startswith("/online-applications"):
            path = path.replace("/online-applications", "", 1)
        return f"{base}{path}"

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        keywords: list[str] | None = None,
        max_pages: int = 20,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Search the Idox portal by cycling through keywords and date range.
        Returns a list of raw result dicts with at least reference and
        detail_url.
        """
        if date_from is None:
            date_from = date.today() - timedelta(days=90)
        if date_to is None:
            date_to = date.today()
        if keywords is None:
            keywords = SEARCH_KEYWORDS

        await self._init_session_cookies()

        all_results: list[dict[str, Any]] = []
        seen_refs: set[str] = set()

        for keyword in keywords:
            self.log.info(
                "keyword_search",
                keyword=keyword,
                council=self.council_name,
            )
            try:
                results = await self._search_keyword(
                    keyword=keyword,
                    date_from=date_from,
                    date_to=date_to,
                    max_pages=max_pages,
                )
                for r in results:
                    ref = r.get("reference", "")
                    if ref and ref not in seen_refs:
                        seen_refs.add(ref)
                        all_results.append(r)
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"keyword_search:{keyword}"
                )
                self.log.warning(
                    "keyword_search_failed",
                    keyword=keyword,
                    error=str(exc),
                )

        self.metrics.applications_found = len(all_results)
        return all_results

    async def _search_keyword(
        self,
        keyword: str,
        date_from: date,
        date_to: date,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        """Execute a single keyword search and paginate through results.
        Auto-falls back from Playwright to httpx on failure."""
        results: list[dict[str, Any]] = []

        if self.use_playwright:
            try:
                results = await self._search_keyword_playwright(
                    keyword, date_from, date_to, max_pages
                )
            except Exception as exc:
                self.log.warning(
                    "playwright_fallback_to_httpx",
                    keyword=keyword,
                    error=str(exc),
                )
                # Auto-fallback to httpx
                results = await self._search_keyword_httpx(
                    keyword, date_from, date_to, max_pages
                )
        else:
            results = await self._search_keyword_httpx(
                keyword, date_from, date_to, max_pages
            )

        return results

    # ------------------------------------------------------------------
    # Playwright search path
    # ------------------------------------------------------------------

    async def _search_keyword_playwright(
        self,
        keyword: str,
        date_from: date,
        date_to: date,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        assert self._page is not None
        results: list[dict[str, Any]] = []

        search_url = self._build_url(self.config.search_path)
        try:
            await self._page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        except Exception:
            await self._page.goto(search_url, timeout=30_000)
        await asyncio.sleep(2)

        # Fill in the search form — try multiple selector patterns
        desc_selectors = [
            'input[name="searchCriteria.description"]',
            '#description',
            'input[id="description"]',
            'textarea[name="searchCriteria.description"]',
        ]
        for sel in desc_selectors:
            desc_el = await self._page.query_selector(sel)
            if desc_el:
                await desc_el.fill(keyword)
                break

        # Date fields (DD/MM/YYYY format)
        date_from_sels = [
            'input[name="date(applicationReceivedStart)"]',
            '#applicationReceivedStart',
            'input[name="searchCriteria.caseAddedDate"]',
        ]
        date_to_sels = [
            'input[name="date(applicationReceivedEnd)"]',
            '#applicationReceivedEnd',
            'input[name="searchCriteria.caseAddedDateTo"]',
        ]

        for sel in date_from_sels:
            from_el = await self._page.query_selector(sel)
            if from_el:
                await from_el.fill(date_from.strftime("%d/%m/%Y"))
                break

        for sel in date_to_sels:
            to_el = await self._page.query_selector(sel)
            if to_el:
                await to_el.fill(date_to.strftime("%d/%m/%Y"))
                break

        # Submit — try click then fallback to form submit / Enter
        submit_sels = [
            'input[type="submit"][value="Search"]',
            'button[type="submit"]',
            '#searchSubmit',
            'input.button.primary[type="submit"]',
            'button:has-text("Search")',
        ]
        submitted = False
        for sel in submit_sels:
            submit_btn = await self._page.query_selector(sel)
            if submit_btn:
                try:
                    async with self._page.expect_navigation(timeout=15_000):
                        await submit_btn.click()
                    submitted = True
                    break
                except Exception:
                    # Navigation didn't happen — try next selector
                    continue

        if not submitted:
            # Fallback: press Enter or submit form via JS
            try:
                async with self._page.expect_navigation(timeout=15_000):
                    await self._page.keyboard.press("Enter")
            except Exception:
                # Last resort: submit the form via JavaScript
                await self._page.evaluate("document.querySelector('form')?.submit()")
                await asyncio.sleep(3)

        await asyncio.sleep(2)

        # Paginate through results
        for page_num in range(1, max_pages + 1):
            await self._respect_rate_limit()
            page_html = await self._page.content()
            page_results = self._parse_search_results_html(page_html)

            if not page_results:
                break

            results.extend(page_results)
            self.log.info(
                "results_page",
                page=page_num,
                count=len(page_results),
                cumulative=len(results),
            )

            # Check for next page link
            next_link = await self._page.query_selector(
                'a.next, a[title="Next page"], a:has-text("Next"), '
                'a.page-next'
            )
            if not next_link:
                break

            try:
                async with self._page.expect_navigation(timeout=15_000):
                    await next_link.click()
            except Exception:
                await next_link.click()
                await asyncio.sleep(3)
            await asyncio.sleep(1)

        return results

    # ------------------------------------------------------------------
    # httpx fallback search path
    # ------------------------------------------------------------------

    async def _search_keyword_httpx(
        self,
        keyword: str,
        date_from: date,
        date_to: date,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        search_url = self._build_url(self.config.search_path)

        form_data: dict[str, str] = {
            "searchCriteria.description": keyword,
            "searchCriteria.caseAddedDate": date_from.strftime("%d/%m/%Y"),
            "searchCriteria.caseAddedDateTo": date_to.strftime("%d/%m/%Y"),
            "searchType": "Application",
            "action": "firstPage",
        }
        if self._csrf_token:
            form_data["org.apache.struts.taglib.html.TOKEN"] = self._csrf_token
        form_data.update(self.config.extra_search_params)

        # Idox portals require browser-like headers on POST (WAF protection)
        base_origin = self.config.base_url.split("/online-applications")[0]
        post_headers: dict[str, str] = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": search_url,
            "Origin": base_origin,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        }

        resp = await self.fetch(
            search_url,
            method="POST",
            data=form_data,
            headers=post_headers,
            use_cache=False,
        )

        for page_num in range(1, max_pages + 1):
            page_results = self._parse_search_results_html(resp.text)
            if not page_results:
                break

            results.extend(page_results)
            self.log.info(
                "results_page",
                page=page_num,
                count=len(page_results),
            )

            # Look for next page URL
            soup = BeautifulSoup(resp.text, "html.parser")
            next_link = (
                soup.find("a", class_="next")
                or soup.find("a", title="Next page")
                or soup.find("a", string=re.compile(r"Next", re.IGNORECASE))
            )
            if not next_link or not isinstance(next_link, Tag):
                break

            next_href = next_link.get("href", "")
            if not next_href:
                break

            next_url = urljoin(self.config.base_url, str(next_href))
            await self._respect_rate_limit()
            resp = await self.fetch(next_url, use_cache=False)

        return results

    # ------------------------------------------------------------------
    # Parse search results page HTML
    # ------------------------------------------------------------------

    def _parse_search_results_html(self, html: str) -> list[dict[str, Any]]:
        """
        Parse an Idox search results page and return a list of dicts
        containing reference, address, description, and detail_url.

        Idox results pages have two common layouts:
        1. <li class="searchresult"> containing <a> with the reference
        2. <table id="searchresults"> with <tr> rows

        We try both.
        """
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        # ----- Layout 1: <li class="searchresult"> -----
        result_items = soup.select("li.searchresult, li.search-result")
        for item in result_items:
            parsed = self._parse_result_li(item)
            if parsed:
                results.append(parsed)

        if results:
            return results

        # ----- Layout 2: <table> rows -----
        table = soup.find("table", id="searchresults") or soup.find(
            "table", class_="searchresults"
        )
        if table and isinstance(table, Tag):
            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header
                parsed = self._parse_result_tr(row)
                if parsed:
                    results.append(parsed)

        if results:
            return results

        # ----- Layout 3: generic <div class="result"> or similar -----
        divs = soup.select(
            "div.result, div.search-result-item, div.applicationResult"
        )
        for div in divs:
            link = div.find("a", href=True)
            if link and isinstance(link, Tag):
                href = str(link.get("href", ""))
                text_parts = [t.strip() for t in div.stripped_strings]
                results.append(
                    {
                        "reference": link.get_text(strip=True),
                        "detail_url": urljoin(self.config.base_url, href),
                        "address": text_parts[1] if len(text_parts) > 1 else "",
                        "description": " ".join(text_parts[2:]) if len(text_parts) > 2 else "",
                    }
                )

        return results

    def _parse_result_li(self, li: Tag) -> dict[str, Any] | None:
        """Parse a single <li class="searchresult"> item."""
        link = li.find("a", href=True)
        if not link or not isinstance(link, Tag):
            return None

        href = str(link.get("href", ""))
        detail_url = urljoin(self.config.base_url, href)

        # The <a> link text is the description (inside summaryLinkTextClamp),
        # NOT the reference.  Reference lives in <p class="metaInfo">.
        description = link.get_text(strip=True)
        reference = ""

        # Extract reference from metaInfo: "Ref. No: 26/9/00058/MOD"
        meta_el = li.find("p", class_="metaInfo")
        if meta_el:
            meta_text = meta_el.get_text(" ", strip=True)
            ref_match = re.search(r"Ref\.?\s*No[.:]?\s*([A-Z0-9/]+(?:\s*/\s*[A-Z0-9/]+)*)", meta_text, re.IGNORECASE)
            if ref_match:
                reference = ref_match.group(1).strip()
            # Extract status from metaInfo too
            status_match = re.search(r"Status:\s*(\w[\w\s]*?)(?:\||$)", meta_text)
            status = status_match.group(1).strip() if status_match else ""
        else:
            status = ""

        # If no metaInfo reference, fall back to extracting from keyVal param
        if not reference:
            key_match = re.search(r"keyVal=(\w+)", href)
            reference = key_match.group(1) if key_match else link.get_text(strip=True)

        address = ""

        # Address is often in the first <p> or span after the link
        addr_el = li.find("p", class_="address") or li.find("span", class_="address")
        if addr_el:
            address = addr_el.get_text(strip=True)

        desc_el = li.find("p", class_="description") or li.find(
            "span", class_="description"
        )
        if desc_el:
            description = desc_el.get_text(strip=True)

        # Fallback: use all text after the link
        if not address and not description:
            text_parts = [t.strip() for t in li.stripped_strings]
            if len(text_parts) > 1:
                address = text_parts[1]
            if len(text_parts) > 2:
                description = " ".join(text_parts[2:])

        status_el = li.find("span", class_="status") or li.find(
            "p", class_="status"
        )
        if status_el:
            status = status_el.get_text(strip=True) or status

        return {
            "reference": reference,
            "detail_url": detail_url,
            "address": address,
            "description": description,
            "status": status,
        }

    def _parse_result_tr(self, tr: Tag) -> dict[str, Any] | None:
        """Parse a single <tr> from a results table."""
        cells = tr.find_all("td")
        if len(cells) < 2:
            return None

        link = tr.find("a", href=True)
        if not link or not isinstance(link, Tag):
            return None

        href = str(link.get("href", ""))
        reference = link.get_text(strip=True)
        detail_url = urljoin(self.config.base_url, href)

        cell_texts = [c.get_text(strip=True) for c in cells]
        return {
            "reference": reference,
            "detail_url": detail_url,
            "address": cell_texts[1] if len(cell_texts) > 1 else "",
            "description": cell_texts[2] if len(cell_texts) > 2 else "",
            "status": cell_texts[3] if len(cell_texts) > 3 else "",
        }

    # ------------------------------------------------------------------
    # Application detail page
    # ------------------------------------------------------------------

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """
        Fetch and parse the detail page for a single Idox application.

        Fetches the 'details' tab (not summary) — that's where applicant
        and agent names are. Uses httpx (much faster than Playwright).
        """
        # Switch to the details tab URL — applicant/agent are on this tab
        details_url = detail_url.replace("activeTab=summary", "activeTab=details")
        if "activeTab=" not in details_url:
            details_url += "&activeTab=details"
        return await self._get_detail_httpx(details_url)

    async def _get_detail_playwright(self, detail_url: str) -> dict[str, Any]:
        assert self._page is not None
        await self._respect_rate_limit()
        try:
            await self._page.goto(detail_url, wait_until="domcontentloaded", timeout=15_000)
        except Exception:
            # Fallback to httpx if Playwright fails on detail page
            return await self._get_detail_httpx(detail_url)
        await asyncio.sleep(1)
        html = await self._page.content()
        return self._parse_detail_html(html, detail_url)

    async def _get_detail_httpx(self, detail_url: str) -> dict[str, Any]:
        resp = await self.fetch(detail_url, use_cache=True)
        return self._parse_detail_html(resp.text, detail_url)

    def _parse_detail_html(self, html: str, detail_url: str) -> dict[str, Any]:
        """
        Extract structured fields from an Idox application detail page.

        The detail page typically contains several <table> elements with
        header-value rows.  We scan all of them and build a key-value map,
        then pick out known fields.
        """
        soup = BeautifulSoup(html, "html.parser")
        kv: dict[str, str] = {}

        # Approach 1: <th>Label</th><td>Value</td> rows
        for row in soup.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True).lower().rstrip(":")
                value = td.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Approach 2a: <span class="label">…</span> paired with nearest <span class="value">
        for label in soup.select("span.label"):
            value_span = label.find_next_sibling("span", class_="value")
            if not value_span:
                # Try next span.value in the same parent container
                parent = label.parent
                if parent:
                    value_span = parent.find("span", class_="value")
            if value_span:
                key = label.get_text(strip=True).lower().rstrip(":")
                value = value_span.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Approach 2b: <dt>…</dt> paired with adjacent <dd>
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                key = dt.get_text(strip=True).lower().rstrip(":")
                value = dd.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Approach 3: <span>Label</span> + bare text sibling in parent div
        # Handles Northgate/PlanningExplorer portals (e.g., Birmingham)
        for li in soup.find_all("li"):
            div = li.find("div")
            container = div or li
            span = container.find("span", recursive=False)
            if not span or span.get("class"):
                continue
            key = span.get_text(strip=True).lower().rstrip(":")
            if not key or key in kv:
                continue
            value = " ".join(
                child.strip() for child in container.children
                if isinstance(child, NavigableString) and child.strip()
            )
            if value:
                kv[key] = value

        # Map known keys
        reference = (
            kv.get("reference")
            or kv.get("application reference")
            or kv.get("ref")
            or kv.get("case reference")
            or ""
        )
        address = (
            kv.get("address")
            or kv.get("site address")
            or kv.get("location")
            or ""
        )
        description = (
            kv.get("proposal")
            or kv.get("description")
            or kv.get("development description")
            or ""
        )
        applicant_name = (
            kv.get("applicant")
            or kv.get("applicant name")
            or ""
        )
        # Skip values that look like planning references
        # (e.g. "2024/12345/PA", "PA/2024/0123", "DC/24/00123")
        if applicant_name and re.search(r"\d+/|/\d+", applicant_name):
            applicant_name = ""
        agent_name = (
            kv.get("agent")
            or kv.get("agent name")
            or kv.get("agent details")
            or ""
        )
        app_type = (
            kv.get("application type")
            or kv.get("type")
            or kv.get("case type")
            or ""
        )
        status = (
            kv.get("status")
            or kv.get("application status")
            or kv.get("decision")
            or ""
        )
        decision = kv.get("decision", "")

        # Date parsing helpers
        def _parse_date_str(s: str) -> date | None:
            for fmt in ("%d/%m/%Y", "%d %b %Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s.strip(), fmt).date()
                except ValueError:
                    continue
            return None

        date_received_raw = (
            kv.get("received")
            or kv.get("date received")
            or kv.get("application received")
            or kv.get("registered")
            or kv.get("date registered")
            or ""
        )
        date_validated_raw = (
            kv.get("validated")
            or kv.get("date validated")
            or ""
        )
        decision_date_raw = (
            kv.get("decision date")
            or kv.get("decision issued")
            or kv.get("decided")
            or ""
        )

        # Documents link
        docs_link = soup.find("a", string=re.compile(r"document", re.IGNORECASE))
        documents_url = ""
        if docs_link and isinstance(docs_link, Tag):
            documents_url = urljoin(detail_url, str(docs_link.get("href", "")))

        return {
            "reference": reference,
            "address": address,
            "description": description,
            "applicant_name": applicant_name,
            "agent_name": agent_name,
            "application_type": app_type,
            "status": status,
            "decision": decision,
            "submission_date": _parse_date_str(date_received_raw),
            "validated_date": _parse_date_str(date_validated_raw),
            "decision_date": _parse_date_str(decision_date_raw),
            "documents_url": documents_url,
            "detail_url": detail_url,
            "raw_kv": kv,
        }

    # ------------------------------------------------------------------
    # Parse (combine search result + detail into model-ready dict)
    # ------------------------------------------------------------------

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Given a dict (search result + merged detail fields from run()),
        convert into a model-ready dict.

        Note: detail page fetching is done in BaseScraper.run() which
        merges detail fields into raw before calling this method.
        """
        merged = raw

        description = merged.get("description", "")
        address = merged.get("address", "")
        scheme_type = self.classify_scheme_type(description)
        num_units = self.extract_unit_count(description)
        postcode = self.extract_postcode(address)
        status = self.normalise_status(merged.get("status", ""))

        return {
            "reference": merged.get("reference", ""),
            "council_id": self.council_id,
            "address": address,
            "postcode": postcode,
            "description": description,
            "applicant_name": merged.get("applicant_name", ""),
            "agent_name": merged.get("agent_name", ""),
            "application_type": merged.get("application_type", ""),
            "status": status,
            "scheme_type": scheme_type,
            "num_units": num_units,
            "submission_date": merged.get("submission_date"),
            "decision_date": merged.get("decision_date"),
            "documents_url": merged.get("documents_url", ""),
            "raw_html": None,
        }
