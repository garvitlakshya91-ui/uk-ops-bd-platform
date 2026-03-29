"""
Scraper for NEC Planning Explorer portals.

NEC (formerly Northgate) Planning Explorer is an older portal system used
by several UK councils. It uses a distinct URL structure with ASPX pages,
ViewState-based form submission, and table-based result layouts.

Typical URL: {base_url}/Northgate/PlanningExplorer/GeneralSearch.aspx
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
class NECCouncilConfig:
    """Per-council configuration for NEC Planning Explorer portals."""
    name: str
    council_id: int
    base_url: str
    search_path: str = "/Northgate/PlanningExplorer/GeneralSearch.aspx"
    results_path: str = "/Northgate/PlanningExplorer/PLEResultList.aspx"
    detail_path: str = "/Northgate/PlanningExplorer/Generic/StdDetails.aspx"
    extra_params: dict[str, str] = field(default_factory=dict)


NEC_COUNCILS: list[NECCouncilConfig] = [
    NECCouncilConfig(
        name="Croydon",
        council_id=200,
        base_url="https://publicaccess3.croydon.gov.uk",
    ),
    NECCouncilConfig(
        name="Bromley",
        council_id=201,
        base_url="https://searchapplications.bromley.gov.uk",
    ),
    NECCouncilConfig(
        name="Wandsworth",
        council_id=202,
        base_url="https://planning.wandsworth.gov.uk",
        search_path="/Northgate/PlanningExplorer/GeneralSearch.aspx",
    ),
    NECCouncilConfig(
        name="Richmond upon Thames",
        council_id=203,
        base_url="https://www2.richmond.gov.uk",
        search_path="/Northgate/PlanningExplorer/GeneralSearch.aspx",
    ),
    NECCouncilConfig(
        name="Lewisham",
        council_id=204,
        base_url="https://planning.lewisham.gov.uk",
        search_path="/online-applications/search.do?action=advanced",
    ),
    NECCouncilConfig(
        name="Greenwich",
        council_id=205,
        base_url="https://planning.royalgreenwich.gov.uk",
    ),
    NECCouncilConfig(
        name="Waltham Forest",
        council_id=206,
        base_url="https://planning.walthamforest.gov.uk",
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


class NECScraper(BaseScraper):
    """
    Scraper for NEC Planning Explorer portals.

    These portals use ASP.NET WebForms with ViewState and postback-based
    navigation. Results are displayed in table grids with ASPX-style
    paging controls.
    """

    def __init__(
        self,
        config: NECCouncilConfig,
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
    # ASP.NET state
    # ------------------------------------------------------------------

    def _extract_aspnet_state(self, html: str) -> None:
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
        max_pages: int = 10,
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

        # Load search page to get initial ASP.NET state
        search_url = self._build_url(self.config.search_path)
        init_resp = await self.fetch(search_url, use_cache=False)
        self._extract_aspnet_state(init_resp.text)

        for keyword in keywords:
            self.log.info(
                "nec_keyword_search",
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
                self.metrics.record_error(exc, context=f"nec_keyword:{keyword}")
                self.log.warning(
                    "nec_search_failed",
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

        # NEC forms typically use these control names
        form_data: dict[str, str] = {
            "__VIEWSTATE": self._viewstate,
            "__VIEWSTATEGENERATOR": self._viewstate_generator,
            "__EVENTVALIDATION": self._event_validation,
            "ctl00$MainBodyContent$txtProposal": keyword,
            "ctl00$MainBodyContent$txtDateReceivedFrom": date_from.strftime("%d/%m/%Y"),
            "ctl00$MainBodyContent$txtDateReceivedTo": date_to.strftime("%d/%m/%Y"),
            "ctl00$MainBodyContent$btnSearch": "Search",
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
                "nec_results_page",
                page=page_num,
                count=len(page_results),
            )

            # Handle ASP.NET GridView paging
            soup = BeautifulSoup(resp.text, "html.parser")
            self._extract_aspnet_state(resp.text)

            # Look for pager row with next page link
            pager = soup.find("tr", class_="pager") or soup.find(
                "tr", class_="PagerStyle"
            )
            if not pager:
                break

            next_link = pager.find("a", string=str(page_num + 1))
            if not next_link or not isinstance(next_link, Tag):
                # Try ">" or "Next" link
                next_link = (
                    pager.find("a", string=">")
                    or pager.find("a", string="Next")
                    or pager.find("a", title="Next Page")
                )
            if not next_link or not isinstance(next_link, Tag):
                break

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
                break

        return results

    # ------------------------------------------------------------------
    # Parse results page
    # ------------------------------------------------------------------

    def _parse_results_page(self, html: str) -> list[dict[str, Any]]:
        """
        Parse NEC Planning Explorer results page.

        NEC portals typically render results in a <table> with:
        - Header row: Reference, Location, Proposal, Status, etc.
        - Data rows with links to detail pages.
        """
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        # Find the results table by common IDs/classes
        table = (
            soup.find("table", id=re.compile(r"ResultList|dgResults|GridView", re.IGNORECASE))
            or soup.find("table", class_=re.compile(r"results|searchResults", re.IGNORECASE))
        )

        if not table or not isinstance(table, Tag):
            # Fallback: look for any table containing application links
            for t in soup.find_all("table"):
                if t.find("a", href=re.compile(r"StdDetails|applicationDetails", re.IGNORECASE)):
                    table = t
                    break

        if not table or not isinstance(table, Tag):
            return results

        rows = table.find_all("tr")
        if not rows:
            return results

        # Build column map from header
        col_map = self._build_column_map(rows[0])

        for row in rows[1:]:
            # Skip pager rows
            if row.get("class") and any(
                c in str(row.get("class", "")).lower()
                for c in ["pager", "pagerstyle", "footer"]
            ):
                continue

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
                    "reference": ref_text or self._safe_index(cell_texts, col_map.get("reference", 0)),
                    "detail_url": detail_url,
                    "address": self._safe_index(cell_texts, col_map.get("address", 1)),
                    "description": self._safe_index(cell_texts, col_map.get("description", 2)),
                    "status": self._safe_index(cell_texts, col_map.get("status", 3)),
                }
            )

        return results

    @staticmethod
    def _safe_index(lst: list[str], idx: int) -> str:
        if 0 <= idx < len(lst):
            return lst[idx]
        return ""

    @staticmethod
    def _build_column_map(header_row: Tag | None) -> dict[str, int]:
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

        return {**default, **mapping}

    # ------------------------------------------------------------------
    # Detail page
    # ------------------------------------------------------------------

    async def get_application_detail(self, detail_url: str) -> dict[str, Any]:
        resp = await self.fetch(detail_url, use_cache=True)
        return self._parse_detail_html(resp.text, detail_url)

    def _parse_detail_html(self, html: str, detail_url: str) -> dict[str, Any]:
        """
        Parse an NEC Planning Explorer detail page.

        NEC detail pages use a table-based layout with label/value cells,
        or <span> elements with IDs like ctl00_MainBodyContent_lbl*.
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

        # Strategy 2: Labelled spans (ctl00_MainBodyContent_lbl*)
        for span in soup.find_all("span", id=re.compile(r"lbl", re.IGNORECASE)):
            span_id = str(span.get("id", ""))
            # Extract label text from preceding element
            prev = span.find_previous(["th", "td", "label", "span"])
            if prev and prev != span:
                key = prev.get_text(strip=True).lower().rstrip(":")
                value = span.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Strategy 3: <dt>/<dd> pairs (sibling-based, not global zip)
        for dt_el in soup.find_all("dt"):
            dd_el = dt_el.find_next_sibling("dd")
            if dd_el:
                key = dt_el.get_text(strip=True).lower().rstrip(":")
                value = dd_el.get_text(strip=True)
                if key and value:
                    kv[key] = value

        # Strategy 4: <span>Label</span> + bare text sibling in parent div
        # Handles Northgate/PlanningExplorer-style pages
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

        reference = kv.get("reference") or kv.get("application number") or kv.get("case reference") or ""
        address = kv.get("address") or kv.get("site address") or kv.get("location") or ""
        description = kv.get("proposal") or kv.get("description") or ""
        applicant = kv.get("applicant") or kv.get("applicant name") or ""
        # Skip values that look like planning references
        # (e.g. "2024/12345/PA", "PA/2024/0123", "DC/24/00123")
        if applicant and re.search(r"\d+/|/\d+", applicant):
            applicant = ""
        agent = kv.get("agent") or kv.get("agent name") or ""
        app_type = kv.get("application type") or kv.get("type") or ""
        status = kv.get("status") or kv.get("decision") or ""

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
        detail_url = raw.get("detail_url", "")
        detail: dict[str, Any] = {}

        if detail_url:
            try:
                detail = await self.get_application_detail(detail_url)
            except Exception as exc:
                self.metrics.record_error(
                    exc, context=f"nec_detail:{raw.get('reference')}"
                )
                self.log.warning(
                    "nec_detail_failed",
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
