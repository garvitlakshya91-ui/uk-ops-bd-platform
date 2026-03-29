"""
Scraper for Civica (Acolaid) planning portals.

Civica portals use a different HTML structure from Idox, typically with
ASP.NET-style controls, ViewState tokens, and a distinct page layout.
Common URL pattern: {base_url}/planning/ or {base_url}/swift/apas/run/
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import structlog
from bs4 import BeautifulSoup, NavigableString, Tag

from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)


@dataclass
class CivicaCouncilConfig:
    """Per-council configuration for Civica portals."""
    name: str
    council_id: int
    base_url: str
    search_path: str = "/swift/apas/run/WPHAPPLIST.displayResultList"
    detail_path: str = "/swift/apas/run/WPHAPPDETAIL.displayResultList"
    extra_params: dict[str, str] = field(default_factory=dict)


CIVICA_COUNCILS: list[CivicaCouncilConfig] = [
    CivicaCouncilConfig(
        name="Barnet",
        council_id=100,
        base_url="https://publicaccess.barnet.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    CivicaCouncilConfig(
        name="Harrow",
        council_id=101,
        base_url="https://planningsearch.harrow.gov.uk",
        search_path="/planning/search-applications",
    ),
    CivicaCouncilConfig(
        name="Ealing",
        council_id=102,
        base_url="https://pam.ealing.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    CivicaCouncilConfig(
        name="Hounslow",
        council_id=103,
        base_url="https://planning.hounslow.gov.uk",
        search_path="/planning_search.aspx",
    ),
    CivicaCouncilConfig(
        name="Hillingdon",
        council_id=104,
        base_url="https://planning.hillingdon.gov.uk",
        search_path="/OcellaWeb/planningSearch",
    ),
    CivicaCouncilConfig(
        name="Bexley",
        council_id=105,
        base_url="https://pa.bexley.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    CivicaCouncilConfig(
        name="Enfield",
        council_id=106,
        base_url="https://planningandbuildingcontrol.enfield.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    CivicaCouncilConfig(
        name="Havering",
        council_id=107,
        base_url="https://development.havering.gov.uk",
        search_path="/OcellaWeb/planningSearch",
    ),
    CivicaCouncilConfig(
        name="Redbridge",
        council_id=108,
        base_url="https://planning.redbridge.gov.uk",
        search_path="/OcellaWeb/planningSearch",
    ),
    CivicaCouncilConfig(
        name="Merton",
        council_id=109,
        base_url="https://planning.merton.gov.uk",
        search_path="/Northgate/PlanningExplorer/GeneralSearch.aspx",
    ),
    CivicaCouncilConfig(
        name="Kingston upon Thames",
        council_id=110,
        base_url="https://publicaccess.kingston.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    CivicaCouncilConfig(
        name="Sutton",
        council_id=111,
        base_url="https://publicaccess.sutton.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
]

SEARCH_KEYWORDS: list[str] = [
    "build to rent",
    "BTR",
    "student accommodation",
    "PBSA",
    "co-living",
    "retirement",
    "affordable housing",
    "residential",
    "flats",
    "apartments",
]


class CivicaScraper(BaseScraper):
    """
    Scraper for Civica / Acolaid planning portals.

    Civica portals typically use ASP.NET WebForms with ViewState-based
    postbacks. The HTML structure features <table>-based layouts with
    specific CSS classes for results grids.
    """

    def __init__(
        self,
        config: CivicaCouncilConfig,
        rate_limit: float | None = 4.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            council_name=config.name,
            council_id=config.council_id,
            portal_url=config.base_url,
            rate_limit=rate_limit,
            proxy_url=proxy_url,
        )
        self.config = config
        self._viewstate: str = ""
        self._viewstate_generator: str = ""
        self._event_validation: str = ""

    # ------------------------------------------------------------------
    # ASP.NET state extraction
    # ------------------------------------------------------------------

    def _extract_aspnet_state(self, html: str) -> None:
        """Extract ASP.NET hidden form fields from page HTML."""
        soup = BeautifulSoup(html, "html.parser")

        vs = soup.find("input", {"name": "__VIEWSTATE"})
        if vs and isinstance(vs, Tag):
            self._viewstate = str(vs.get("value", ""))

        vsg = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        if vsg and isinstance(vsg, Tag):
            self._viewstate_generator = str(vsg.get("value", ""))

        ev = soup.find("input", {"name": "__EVENTVALIDATION"})
        if ev and isinstance(ev, Tag):
            self._event_validation = str(ev.get("value", ""))

    def _build_url(self, path: str) -> str:
        return f"{self.config.base_url.rstrip('/')}{path}"

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_applications(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        keywords: list[str] | None = None,
        max_pages: int = 15,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if date_from is None:
            date_from = date.today() - timedelta(days=90)
        if date_to is None:
            date_to = date.today()
        if keywords is None:
            keywords = SEARCH_KEYWORDS

        all_results: list[dict[str, Any]] = []
        seen_refs: set[str] = set()

        # Load search page to get initial state
        search_url = self._build_url(self.config.search_path)
        init_resp = await self.fetch(search_url, use_cache=False)
        self._extract_aspnet_state(init_resp.text)

        for keyword in keywords:
            self.log.info(
                "civica_keyword_search",
                keyword=keyword,
                council=self.council_name,
            )
            try:
                results = await self._search_keyword(
                    keyword, date_from, date_to, max_pages
                )
                for r in results:
                    ref = r.get("reference", "")
                    if ref and ref not in seen_refs:
                        seen_refs.add(ref)
                        all_results.append(r)
            except Exception as exc:
                self.metrics.record_error(exc, context=f"keyword:{keyword}")
                self.log.warning(
                    "civica_search_failed",
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
        results: list[dict[str, Any]] = []
        search_url = self._build_url(self.config.search_path)

        form_data: dict[str, str] = {
            "__VIEWSTATE": self._viewstate,
            "__VIEWSTATEGENERATOR": self._viewstate_generator,
            "__EVENTVALIDATION": self._event_validation,
            "ctl00$MainContent$txtSearch": keyword,
            "ctl00$MainContent$txtDateFrom": date_from.strftime("%d/%m/%Y"),
            "ctl00$MainContent$txtDateTo": date_to.strftime("%d/%m/%Y"),
            "ctl00$MainContent$btnSearch": "Search",
        }
        form_data.update(self.config.extra_params)

        resp = await self.fetch(
            search_url,
            method="POST",
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            use_cache=False,
        )

        for page_num in range(1, max_pages + 1):
            page_results = self._parse_results_page(resp.text)
            if not page_results:
                break

            results.extend(page_results)
            self.log.info(
                "civica_results_page",
                page=page_num,
                count=len(page_results),
            )

            # Check for next-page link or postback
            soup = BeautifulSoup(resp.text, "html.parser")
            self._extract_aspnet_state(resp.text)

            next_link = (
                soup.find("a", id=re.compile(r"lnkNext", re.IGNORECASE))
                or soup.find("a", string=re.compile(r"Next", re.IGNORECASE))
                or soup.find("a", class_="next")
            )
            if not next_link or not isinstance(next_link, Tag):
                break

            # ASP.NET postback style
            href = str(next_link.get("href", ""))
            postback_match = re.search(
                r"__doPostBack\('([^']+)','([^']*)'\)", href
            )
            if postback_match:
                event_target = postback_match.group(1)
                event_arg = postback_match.group(2)
                postback_data = {
                    "__VIEWSTATE": self._viewstate,
                    "__VIEWSTATEGENERATOR": self._viewstate_generator,
                    "__EVENTVALIDATION": self._event_validation,
                    "__EVENTTARGET": event_target,
                    "__EVENTARGUMENT": event_arg,
                }
                resp = await self.fetch(
                    search_url,
                    method="POST",
                    data=postback_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    use_cache=False,
                )
            else:
                next_url = urljoin(self.config.base_url, href)
                resp = await self.fetch(next_url, use_cache=False)

        return results

    # ------------------------------------------------------------------
    # Parse results page
    # ------------------------------------------------------------------

    def _parse_results_page(self, html: str) -> list[dict[str, Any]]:
        """
        Parse a Civica results page.

        Civica portals typically use one of these layouts:
        1. <table class="display-data"> with rows per application
        2. <div class="result"> blocks
        3. ASP.NET GridView table (id containing "GridView" or "grdResults")
        """
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        # Layout 1: GridView table
        grid = (
            soup.find("table", id=re.compile(r"GridView|grdResults", re.IGNORECASE))
            or soup.find("table", class_="display-data")
            or soup.find("table", class_="searchresults")
        )
        if grid and isinstance(grid, Tag):
            rows = grid.find_all("tr")
            # Determine column indices from header
            header_row = rows[0] if rows else None
            col_map = self._build_column_map(header_row)

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                link = row.find("a", href=True)
                ref_text = ""
                detail_url = ""
                if link and isinstance(link, Tag):
                    ref_text = link.get_text(strip=True)
                    detail_url = urljoin(
                        self.config.base_url, str(link.get("href", ""))
                    )

                cell_texts = [c.get_text(strip=True) for c in cells]

                results.append(
                    {
                        "reference": ref_text or cell_texts[col_map.get("reference", 0)],
                        "detail_url": detail_url,
                        "address": cell_texts[col_map.get("address", 1)]
                        if len(cell_texts) > col_map.get("address", 1)
                        else "",
                        "description": cell_texts[col_map.get("description", 2)]
                        if len(cell_texts) > col_map.get("description", 2)
                        else "",
                        "status": cell_texts[col_map.get("status", 3)]
                        if len(cell_texts) > col_map.get("status", 3)
                        else "",
                    }
                )

            if results:
                return results

        # Layout 2: div-based results
        result_divs = soup.select(
            "div.result, div.searchResult, div.planning-result, "
            "div.application-row"
        )
        for div in result_divs:
            link = div.find("a", href=True)
            if not link or not isinstance(link, Tag):
                continue

            ref = link.get_text(strip=True)
            href = str(link.get("href", ""))

            address_el = div.find(
                ["p", "span", "div"],
                class_=re.compile(r"address|location", re.IGNORECASE),
            )
            desc_el = div.find(
                ["p", "span", "div"],
                class_=re.compile(r"desc|proposal", re.IGNORECASE),
            )
            status_el = div.find(
                ["p", "span", "div"],
                class_=re.compile(r"status|decision", re.IGNORECASE),
            )

            results.append(
                {
                    "reference": ref,
                    "detail_url": urljoin(self.config.base_url, href),
                    "address": address_el.get_text(strip=True) if address_el else "",
                    "description": desc_el.get_text(strip=True) if desc_el else "",
                    "status": status_el.get_text(strip=True) if status_el else "",
                }
            )

        return results

    @staticmethod
    def _build_column_map(header_row: Tag | None) -> dict[str, int]:
        """Build a mapping from field name to column index by inspecting header text."""
        default = {"reference": 0, "address": 1, "description": 2, "status": 3}
        if not header_row:
            return default

        headers = header_row.find_all(["th", "td"])
        mapping: dict[str, int] = {}
        for idx, th in enumerate(headers):
            text = th.get_text(strip=True).lower()
            if "ref" in text or "number" in text or "case" in text:
                mapping["reference"] = idx
            elif "address" in text or "location" in text or "site" in text:
                mapping["address"] = idx
            elif "desc" in text or "proposal" in text:
                mapping["description"] = idx
            elif "status" in text or "decision" in text:
                mapping["status"] = idx
            elif "date" in text and "received" in text:
                mapping["date_received"] = idx

        return {**default, **mapping}

    # ------------------------------------------------------------------
    # Application detail page
    # ------------------------------------------------------------------

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        """Fetch and parse a Civica application detail page."""
        resp = await self.fetch(detail_url, use_cache=True)
        return self._parse_detail_html(resp.text, detail_url)

    def _parse_detail_html(self, html: str, detail_url: str) -> dict[str, Any]:
        """
        Parse a Civica detail page.

        Civica detail pages often use <table> layouts or
        definition-list (<dl>/<dt>/<dd>) structures.
        """
        soup = BeautifulSoup(html, "html.parser")
        kv: dict[str, str] = {}

        # Strategy 1: <th>/<td> pairs
        for row in soup.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True).lower().rstrip(":")
                value = td.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Strategy 2: <dt>/<dd> pairs (sibling-based, not global zip)
        for dt_el in soup.find_all("dt"):
            dd_el = dt_el.find_next_sibling("dd")
            if dd_el:
                key = dt_el.get_text(strip=True).lower().rstrip(":")
                value = dd_el.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Strategy 3: <label>/<span> pairs
        for label in soup.find_all("label"):
            span = label.find_next_sibling("span")
            if span:
                key = label.get_text(strip=True).lower().rstrip(":")
                value = span.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Strategy 4: <span>Label</span> + bare text sibling in parent div
        # Handles Northgate/PlanningExplorer-style portals
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

        def _parse_dt(s: str) -> date | None:
            for fmt in ("%d/%m/%Y", "%d %b %Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s.strip(), fmt).date()
                except ValueError:
                    continue
            return None

        reference = kv.get("reference") or kv.get("application reference") or kv.get("case reference") or ""
        address = kv.get("address") or kv.get("site address") or kv.get("location") or ""
        description = kv.get("proposal") or kv.get("description") or kv.get("development description") or ""
        applicant = kv.get("applicant") or kv.get("applicant name") or ""
        # Skip values that look like planning references
        # (e.g. "2024/12345/PA", "PA/2024/0123", "DC/24/00123")
        if applicant and re.search(r"\d+/|/\d+", applicant):
            applicant = ""
        agent = kv.get("agent") or kv.get("agent name") or ""
        status = kv.get("status") or kv.get("application status") or ""
        decision = kv.get("decision") or ""
        app_type = kv.get("application type") or kv.get("type") or ""

        date_received = _parse_dt(
            kv.get("date received") or kv.get("received") or kv.get("registered") or ""
        )
        decision_date = _parse_dt(
            kv.get("decision date") or kv.get("decided") or ""
        )

        docs_link = soup.find("a", string=re.compile(r"document", re.IGNORECASE))
        documents_url = ""
        if docs_link and isinstance(docs_link, Tag):
            documents_url = urljoin(detail_url, str(docs_link.get("href", "")))

        return {
            "reference": reference,
            "address": address,
            "description": description,
            "applicant_name": applicant,
            "agent_name": agent,
            "application_type": app_type,
            "status": status,
            "decision": decision,
            "submission_date": date_received,
            "decision_date": decision_date,
            "documents_url": documents_url,
            "detail_url": detail_url,
            "raw_kv": kv,
        }

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Fetch detail page and merge with search result into model-ready dict."""
        detail_url = raw.get("detail_url", "")
        detail: dict[str, Any] = {}

        if detail_url:
            try:
                detail = await self.get_application_detail(detail_url)
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"civica_detail:{raw.get('reference')}"
                )
                self.log.warning(
                    "civica_detail_failed",
                    reference=raw.get("reference"),
                    error=str(exc),
                )

        merged = {**raw, **detail}
        description = merged.get("description", "")
        address = merged.get("address", "")

        return {
            "reference": merged.get("reference", ""),
            "council_id": self.council_id,
            "address": address,
            "postcode": self.extract_postcode(address),
            "description": description,
            "applicant_name": merged.get("applicant_name", ""),
            "agent_name": merged.get("agent_name", ""),
            "application_type": merged.get("application_type", ""),
            "status": self.normalise_status(merged.get("status", "")),
            "scheme_type": self.classify_scheme_type(description),
            "num_units": self.extract_unit_count(description),
            "submission_date": merged.get("submission_date"),
            "decision_date": merged.get("decision_date"),
            "documents_url": merged.get("documents_url", ""),
            "raw_html": None,
        }
