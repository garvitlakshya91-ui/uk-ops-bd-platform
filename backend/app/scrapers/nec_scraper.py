"""
Scraper for NEC Planning Explorer portals.

NEC (formerly Northgate) Planning Explorer is an older portal system used
by several UK councils. It uses a distinct URL structure with ASPX pages,
ViewState-based form submission, and table-based result layouts.

Typical URL: {base_url}/Northgate/PlanningExplorer/GeneralSearch.aspx

This scraper supports two search strategies:
1. **Date-range search** (primary) — fetches ALL applications received in a
   date window, optionally filtered by application type.  This is the main
   mode used for regular scraping runs and ensures no applications are missed.
2. **Keyword search** (supplementary) — searches by BD-relevant keywords to
   find applications whose descriptions mention specific scheme types.

Both strategies are combined in ``search_applications`` with deduplication
by reference number.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import structlog
from bs4 import BeautifulSoup, NavigableString, Tag

from app.scrapers.base import BaseScraper, ScraperMetrics

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
    # Application type codes to search when doing date-range searches.
    # Empty list means search all types (no filter).  Codes are portal-
    # specific and detected at runtime from the <select> options.
    application_type_codes: list[str] = field(default_factory=list)
    extra_params: dict[str, str] = field(default_factory=dict)


NEC_COUNCILS: list[NECCouncilConfig] = [
    # --- Genuine Northgate / PlanningExplorer portals ---
    NECCouncilConfig(
        name="Birmingham",
        council_id=1,
        base_url="https://eplanning.birmingham.gov.uk",
        search_path="/Northgate/PlanningExplorer/GeneralSearch.aspx",
    ),
    NECCouncilConfig(
        name="Wandsworth",
        council_id=202,
        base_url="https://planning.wandsworth.gov.uk",
        search_path="/Northgate/PlanningExplorer/GeneralSearch.aspx",
    ),
    NECCouncilConfig(
        name="Merton",
        council_id=45,
        base_url="https://planning.merton.gov.uk",
        search_path="/Northgate/PlanningExplorerAA/GeneralSearch.aspx",
    ),
    NECCouncilConfig(
        name="Liverpool",
        council_id=7,
        base_url="http://northgate.liverpool.gov.uk",
        search_path="/PlanningExplorer/generalsearch.aspx",
    ),
    # Reading uses FastWeb (NEC variant) — path is /fastweb_PL/
    NECCouncilConfig(
        name="Reading",
        council_id=20,
        base_url="https://planning.reading.gov.uk",
        search_path="/fastweb_PL/search.asp",
    ),
    # Reclassified out of NEC:
    # Croydon → Idox (publicaccess3.croydon.gov.uk/online-applications)
    # Bromley → Idox (searchapplications.bromley.gov.uk/online-applications)
    # Greenwich → Idox (planning.royalgreenwich.gov.uk/online-applications)
    # Lewisham → Idox (planning.lewisham.gov.uk/online-applications)
    # Richmond upon Thames → API (custom ASP.NET portal, not NEC)
    # Waltham Forest → API (old NEC portal is dead)
]

# Keywords used for supplementary BD-focused keyword searches.
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

# Application types that are relevant for BD purposes.  These are matched
# (case-insensitive substring) against the <option> text in the application
# type dropdown to auto-detect the correct codes per portal.
BD_RELEVANT_APP_TYPES: list[str] = [
    "full planning",
    "outline",
    "reserved matters",
    "major",
    "full application",
    "full permission",
    "prior notification",
    "permission in principle",
    "hybrid",
    "environmental impact",
    "screening",
    "scoping",
]

# Application types to skip during residential filtering — these are very
# unlikely to be BD-relevant and produce noise.
SKIP_APP_TYPES: list[str] = [
    "householder",
    "tree",
    "hedge",
    "advertisement",
    "telecoms",
    "listed building",
    "conservation area",
    "lawful development",
    "discharge of condition",
    "non-material amendment",
    "certificate of lawfulness",
]

# Minimum number of units (from description) for an application to be
# considered BD-relevant when doing broad date-range searches.
MIN_UNITS_BD_RELEVANT = 5


class NECScraper(BaseScraper):
    """
    Scraper for NEC Planning Explorer portals.

    These portals use ASP.NET WebForms with ViewState and postback-based
    navigation. Results are displayed in table grids with ASPX-style
    paging controls.

    The scraper uses two complementary search strategies:

    1. **Date-range search** — fetches all applications received within a
       configurable window (default: last 28 days).  For portals that
       support application-type filtering, it focuses on full/outline/major
       types.  This ensures we capture every new planning application.

    2. **Keyword search** — supplements the date-range search by searching
       for BD-specific terms (BTR, PBSA, etc.) over a longer lookback
       period (default: 180 days).  This catches older applications that
       may have been missed or recently updated.

    Results from both strategies are deduplicated by reference number.
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
        # Detected form field names (populated on first search page load)
        self._form_fields: dict[str, str] = {}
        # Detected application type options: {code: display_text}
        self._app_type_options: dict[str, str] = {}

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

    def _detect_form_fields(self, html: str) -> dict[str, str]:
        """Auto-detect form field names from the search page HTML.

        NEC/Northgate portals use two naming conventions:
        1. ASP.NET style: ``ctl00$MainBodyContent$txtProposal``
        2. Simple style: ``txtProposal`` (used by older Northgate/PlanningExplorer)

        This method inspects the actual form to find the correct names.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Map of logical field -> list of possible names (ordered by preference)
        candidates = {
            "proposal": [
                "ctl00$MainBodyContent$txtProposal",
                "txtProposal",
            ],
            "date_from": [
                "ctl00$MainBodyContent$txtDateReceivedFrom",
                "dateStart",
                "ctl00$MainBodyContent$dateStart",
            ],
            "date_to": [
                "ctl00$MainBodyContent$txtDateReceivedTo",
                "dateEnd",
                "ctl00$MainBodyContent$dateEnd",
            ],
            "search_btn": [
                "ctl00$MainBodyContent$btnSearch",
                "csbtnSearch",
                "ctl00$MainBodyContent$csbtnSearch",
            ],
            "app_type": [
                "cboApplicationTypeCode",
                "ctl00$MainBodyContent$cboApplicationTypeCode",
                "ctl00$MainBodyContent$ddlApplicationType",
            ],
            "status_code": [
                "cboStatusCode",
                "ctl00$MainBodyContent$cboStatusCode",
                "ctl00$MainBodyContent$ddlStatus",
            ],
            "date_type": [
                "cboSelectDateValue",
                "ctl00$MainBodyContent$cboSelectDateValue",
                "ctl00$MainBodyContent$ddlDateType",
            ],
            "site_address": [
                "txtSiteAddress",
                "ctl00$MainBodyContent$txtSiteAddress",
                "ctl00$MainBodyContent$txtAddress",
            ],
            "applicant_name": [
                "txtApplicantName",
                "ctl00$MainBodyContent$txtApplicantName",
            ],
            "agent_name": [
                "txtAgentName",
                "ctl00$MainBodyContent$txtAgentName",
            ],
            "app_number": [
                "txtAppNumber",
                "ctl00$MainBodyContent$txtAppNumber",
                "ctl00$MainBodyContent$txtApplicationNumber",
            ],
            "ward": [
                "cboWardCode",
                "ctl00$MainBodyContent$cboWardCode",
            ],
            "constituency": [
                "cboConstituencyCode",
                "ctl00$MainBodyContent$cboConstituencyCode",
            ],
            "dev_type": [
                "cboDevelopmentTypeCode",
                "ctl00$MainBodyContent$cboDevelopmentTypeCode",
            ],
        }

        detected: dict[str, str] = {}
        for logical_name, possible_names in candidates.items():
            for name in possible_names:
                el = soup.find("input", {"name": name})
                if not el:
                    el = soup.find("select", {"name": name})
                if el:
                    detected[logical_name] = name
                    break
            else:
                # Use the first candidate as fallback
                detected[logical_name] = possible_names[0]

        self.log.debug(
            "nec_form_fields_detected",
            fields=detected,
            council=self.council_name,
        )
        return detected

    def _detect_app_type_options(self, html: str) -> dict[str, str]:
        """Extract available application type options from the search form.

        Returns a dict mapping option value (code) to display text, e.g.:
        {"FUL": "Full Planning", "OUT": "Outline Permission", ...}
        """
        soup = BeautifulSoup(html, "html.parser")
        options: dict[str, str] = {}

        # Find the application type <select> element
        field_name = self._form_fields.get("app_type", "cboApplicationTypeCode")
        select_el = soup.find("select", {"name": field_name})
        if not select_el or not isinstance(select_el, Tag):
            return options

        for opt in select_el.find_all("option"):
            if not isinstance(opt, Tag):
                continue
            value = str(opt.get("value", "")).strip()
            text = opt.get_text(strip=True)
            if value and text:
                options[value] = text

        self.log.debug(
            "nec_app_type_options",
            count=len(options),
            council=self.council_name,
        )
        return options

    def _get_bd_relevant_type_codes(self) -> list[str]:
        """Return application type codes that are relevant for BD.

        Matches detected portal options against BD_RELEVANT_APP_TYPES
        keywords.  If the config specifies explicit codes, use those instead.
        """
        if self.config.application_type_codes:
            return self.config.application_type_codes

        if not self._app_type_options:
            return []

        relevant_codes: list[str] = []
        for code, text in self._app_type_options.items():
            text_lower = text.lower()
            # Skip types we know are irrelevant
            if any(skip in text_lower for skip in SKIP_APP_TYPES):
                continue
            # Include types that match BD-relevant patterns
            if any(kw in text_lower for kw in BD_RELEVANT_APP_TYPES):
                relevant_codes.append(code)

        self.log.info(
            "nec_bd_relevant_types",
            codes=relevant_codes,
            council=self.council_name,
        )
        return relevant_codes

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
        max_pages: int = 20,
        keyword_lookback_days: int = 180,
        date_range_lookback_days: int = 28,
        skip_keyword_search: bool = False,
        skip_date_range_search: bool = False,
        application_type_code: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Search for planning applications using combined strategies.

        Parameters
        ----------
        date_from : date, optional
            Start of date range for the primary date-range search.
            Defaults to ``date_range_lookback_days`` ago.
        date_to : date, optional
            End of date range.  Defaults to today.
        keywords : list[str], optional
            Keywords for supplementary keyword search.
            Defaults to ``SEARCH_KEYWORDS``.
        max_pages : int
            Maximum result pages to paginate through per search.
        keyword_lookback_days : int
            How far back to look for keyword searches (default 180 days).
        date_range_lookback_days : int
            How far back to look for date-range searches (default 28 days).
        skip_keyword_search : bool
            If True, skip the keyword search strategy entirely.
        skip_date_range_search : bool
            If True, skip the date-range search strategy entirely.
        application_type_code : str, optional
            If set, restrict date-range search to this specific app type code.
        """
        if date_to is None:
            date_to = date.today()
        if keywords is None:
            keywords = SEARCH_KEYWORDS

        all_results: list[dict[str, Any]] = []
        seen_refs: set[str] = set()

        # Load search page to get initial ASP.NET state and detect fields
        search_url = self._build_url(self.config.search_path)
        init_resp = await self.fetch(search_url, use_cache=False)
        self._extract_aspnet_state(init_resp.text)
        self._form_fields = self._detect_form_fields(init_resp.text)
        self._app_type_options = self._detect_app_type_options(init_resp.text)

        # ------------------------------------------------------------------
        # Strategy 1: Date-range search (primary)
        # Fetches ALL applications received within the date window.
        # If application-type filtering is available, restricts to
        # BD-relevant types to reduce noise.
        # ------------------------------------------------------------------
        if not skip_date_range_search:
            dr_from = date_from or (date_to - timedelta(days=date_range_lookback_days))

            if application_type_code:
                # Single specific type requested
                type_codes = [application_type_code]
            else:
                type_codes = self._get_bd_relevant_type_codes()

            if type_codes:
                # Search each relevant application type separately
                for type_code in type_codes:
                    self.log.info(
                        "nec_date_range_search",
                        app_type=type_code,
                        date_from=str(dr_from),
                        date_to=str(date_to),
                        council=self.council_name,
                    )
                    try:
                        # Re-load search page to get fresh ViewState
                        init_resp = await self.fetch(search_url, use_cache=False)
                        self._extract_aspnet_state(init_resp.text)

                        results = await self._search_date_range(
                            dr_from, date_to, max_pages,
                            application_type_code=type_code,
                        )
                        for r in results:
                            ref = r.get("reference", "")
                            if ref and ref not in seen_refs:
                                seen_refs.add(ref)
                                all_results.append(r)
                    except Exception as exc:
                        self.metrics.record_error(
                            exc, context=f"nec_date_range:{type_code}"
                        )
                        self.log.warning(
                            "nec_date_range_search_failed",
                            app_type=type_code,
                            error=str(exc),
                        )
            else:
                # No type codes detected — search without type filter
                self.log.info(
                    "nec_date_range_search_all_types",
                    date_from=str(dr_from),
                    date_to=str(date_to),
                    council=self.council_name,
                )
                try:
                    init_resp = await self.fetch(search_url, use_cache=False)
                    self._extract_aspnet_state(init_resp.text)

                    results = await self._search_date_range(
                        dr_from, date_to, max_pages,
                    )
                    for r in results:
                        ref = r.get("reference", "")
                        if ref and ref not in seen_refs:
                            seen_refs.add(ref)
                            all_results.append(r)
                except Exception as exc:
                    self.metrics.record_error(exc, context="nec_date_range:all")
                    self.log.warning(
                        "nec_date_range_search_failed",
                        error=str(exc),
                    )

        # ------------------------------------------------------------------
        # Strategy 2: Keyword search (supplementary)
        # Searches for BD-relevant keywords over a longer lookback period.
        # ------------------------------------------------------------------
        if not skip_keyword_search:
            kw_from = date_from or (date_to - timedelta(days=keyword_lookback_days))

            for keyword in keywords:
                self.log.info(
                    "nec_keyword_search",
                    keyword=keyword,
                    council=self.council_name,
                )
                try:
                    # Re-load search page for fresh ViewState
                    init_resp = await self.fetch(search_url, use_cache=False)
                    self._extract_aspnet_state(init_resp.text)

                    results = await self._search_keyword(
                        keyword, kw_from, date_to, max_pages
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

        self.log.info(
            "nec_search_complete",
            total_unique=len(all_results),
            council=self.council_name,
        )
        self.metrics.applications_found = len(all_results)
        return all_results

    # ------------------------------------------------------------------
    # Date-range search (no keyword, optional type filter)
    # ------------------------------------------------------------------

    async def _search_date_range(
        self,
        date_from: date,
        date_to: date,
        max_pages: int,
        application_type_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search by date range, optionally filtered by application type.

        This is the primary search strategy that catches all planning
        applications received in the given window.  Unlike keyword search,
        it does not require a proposal text — it uses the date fields and
        optionally the application type dropdown.
        """
        results: list[dict[str, Any]] = []
        search_url = self._build_url(self.config.search_path)

        fields = self._form_fields
        is_northgate = fields.get("proposal") == "txtProposal"

        form_data: dict[str, str] = {
            "__VIEWSTATE": self._viewstate,
            "__VIEWSTATEGENERATOR": self._viewstate_generator,
            "__EVENTVALIDATION": self._event_validation,
            # Leave proposal/keyword empty — date range is the filter
            fields.get("proposal", "ctl00$MainBodyContent$txtProposal"): "",
            fields.get("date_from", "ctl00$MainBodyContent$txtDateReceivedFrom"): date_from.strftime("%d/%m/%Y"),
            fields.get("date_to", "ctl00$MainBodyContent$txtDateReceivedTo"): date_to.strftime("%d/%m/%Y"),
            fields.get("search_btn", "ctl00$MainBodyContent$btnSearch"): "Search",
        }

        # Set application type filter if specified
        if application_type_code:
            form_data[fields.get("app_type", "cboApplicationTypeCode")] = application_type_code

        # For Northgate-style portals, add required form defaults
        if is_northgate:
            form_data.update({
                "rbGroup": "rbRange",
                "edrDateSelection": "",
                "cboSelectDateValue": "DATE_RECEIVED",
                "cboStatusCode": "",
                "cboConstituencyCode": "",
                "cboWardCode": "",
                "cboDevelopmentTypeCode": "",
            })
            # Only set app type if not already set above
            if not application_type_code:
                form_data.setdefault("cboApplicationTypeCode", "")

        form_data.update(self.config.extra_params)

        resp = await self._submit_search_form(search_url, form_data, is_northgate)
        current_results_url = str(resp.url) if hasattr(resp, "url") else search_url

        results = await self._paginate_results(
            resp, search_url, current_results_url, max_pages
        )

        self.log.info(
            "nec_date_range_results",
            count=len(results),
            app_type=application_type_code or "all",
            council=self.council_name,
        )
        return results

    # ------------------------------------------------------------------
    # Keyword search
    # ------------------------------------------------------------------

    async def _search_keyword(
        self,
        keyword: str,
        date_from: date,
        date_to: date,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        search_url = self._build_url(self.config.search_path)

        fields = self._form_fields
        is_northgate = fields.get("proposal") == "txtProposal"

        form_data: dict[str, str] = {
            "__VIEWSTATE": self._viewstate,
            "__VIEWSTATEGENERATOR": self._viewstate_generator,
            "__EVENTVALIDATION": self._event_validation,
            fields.get("proposal", "ctl00$MainBodyContent$txtProposal"): keyword,
            fields.get("date_from", "ctl00$MainBodyContent$txtDateReceivedFrom"): date_from.strftime("%d/%m/%Y"),
            fields.get("date_to", "ctl00$MainBodyContent$txtDateReceivedTo"): date_to.strftime("%d/%m/%Y"),
            fields.get("search_btn", "ctl00$MainBodyContent$btnSearch"): "Search",
        }

        if is_northgate:
            form_data.update({
                "rbGroup": "rbRange",
                "edrDateSelection": "",
                "cboSelectDateValue": "DATE_RECEIVED",
                "cboApplicationTypeCode": "",
                "cboStatusCode": "",
                "cboConstituencyCode": "",
                "cboWardCode": "",
                "cboDevelopmentTypeCode": "",
            })

        form_data.update(self.config.extra_params)

        resp = await self._submit_search_form(search_url, form_data, is_northgate)
        current_results_url = str(resp.url) if hasattr(resp, "url") else search_url

        results = await self._paginate_results(
            resp, search_url, current_results_url, max_pages
        )

        return results

    # ------------------------------------------------------------------
    # Form submission helper
    # ------------------------------------------------------------------

    async def _submit_search_form(
        self,
        search_url: str,
        form_data: dict[str, str],
        is_northgate: bool,
    ) -> Any:
        """Submit the search form, handling Northgate redirect behaviour."""
        if is_northgate and self.session:
            # Northgate portals redirect POST -> 302 -> results page.
            # We disable auto-redirect and follow manually with proper headers.
            raw_resp = await self.session.request(
                "POST",
                search_url,
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": search_url,
                },
                follow_redirects=False,
            )
            self.metrics.requests_made += 1
            self.metrics.requests_successful += 1

            if raw_resp.status_code in (301, 302, 303, 307, 308):
                redirect_url = raw_resp.headers.get("location", "")
                if redirect_url:
                    redirect_url = urljoin(self.config.base_url, redirect_url)
                    return await self.fetch(
                        redirect_url,
                        headers={"Referer": search_url},
                        use_cache=False,
                    )
                return raw_resp
            return raw_resp
        else:
            return await self.fetch(
                search_url,
                method="POST",
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": search_url,
                },
                use_cache=False,
            )

    # ------------------------------------------------------------------
    # Pagination helper
    # ------------------------------------------------------------------

    async def _paginate_results(
        self,
        initial_resp: Any,
        search_url: str,
        results_url: str,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        """Parse results from the initial response and paginate through."""
        results: list[dict[str, Any]] = []
        resp = initial_resp

        for page_num in range(1, max_pages + 1):
            page_results = self._parse_results_page(
                resp.text, results_url=results_url
            )
            if not page_results:
                break

            results.extend(page_results)
            self.log.info(
                "nec_results_page",
                page=page_num,
                count=len(page_results),
                total_so_far=len(results),
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

    def _parse_results_page(self, html: str, results_url: str | None = None) -> list[dict[str, Any]]:
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
                base_for_join = results_url or self.config.base_url
                detail_url = urljoin(
                    base_for_join, str(link.get("href", ""))
                )

            cell_texts = [c.get_text(strip=True) for c in cells]

            results.append(
                {
                    "reference": ref_text or self._safe_index(cell_texts, col_map.get("reference", 0)),
                    "detail_url": detail_url,
                    "address": self._safe_index(cell_texts, col_map.get("address", 1)),
                    "description": self._safe_index(cell_texts, col_map.get("description", 2)),
                    "status": self._safe_index(cell_texts, col_map.get("status", 3)),
                    "application_type": self._safe_index(cell_texts, col_map.get("app_type", -1)),
                    "date_received": self._safe_index(cell_texts, col_map.get("date_received", -1)),
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
            elif "type" in text and "app" in text:
                mapping["app_type"] = idx
            elif "received" in text or "registered" in text:
                mapping["date_received"] = idx

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
        if applicant and re.search(r"\d+/|/\d+", applicant):
            applicant = ""
        agent = kv.get("agent") or kv.get("agent name") or ""
        app_type = kv.get("application type") or kv.get("type") or ""
        status = kv.get("status") or kv.get("decision") or ""
        ward = kv.get("ward") or kv.get("ward name") or ""
        decision = kv.get("decision") or kv.get("decision type") or ""

        date_received = _parse_dt(
            kv.get("date received") or kv.get("received") or kv.get("registered") or ""
        )
        date_validated = _parse_dt(
            kv.get("date validated") or kv.get("validated") or ""
        )
        decision_date = _parse_dt(
            kv.get("decision date") or kv.get("decided") or ""
        )
        consultation_end = _parse_dt(
            kv.get("consultation expiry") or kv.get("neighbour expiry") or
            kv.get("consultation end") or ""
        )
        committee_date = _parse_dt(
            kv.get("committee date") or kv.get("committee") or ""
        )
        target_date = _parse_dt(
            kv.get("target date") or kv.get("target decision date") or ""
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
            "ward": ward,
            "decision": decision,
            "submission_date": date_received,
            "validated_date": date_validated,
            "decision_date": decision_date,
            "consultation_end_date": consultation_end,
            "committee_date": committee_date,
            "target_date": target_date,
            "documents_url": documents_url,
            "detail_url": detail_url,
            "raw_kv": kv,
        }

    # ------------------------------------------------------------------
    # Residential relevance check
    # ------------------------------------------------------------------

    @staticmethod
    def _is_residential_relevant(app_data: dict[str, Any]) -> bool:
        """Check whether an application is likely residential/BD-relevant.

        Used to filter results from broad date-range searches.  Applications
        found via keyword search are assumed relevant and bypass this check.
        """
        description = (app_data.get("description") or "").lower()
        app_type = (app_data.get("application_type") or "").lower()

        # Skip clearly non-residential types
        for skip_kw in SKIP_APP_TYPES:
            if skip_kw in app_type:
                return False

        # Positive signals: residential keywords in description
        residential_keywords = [
            "residential", "dwelling", "flat", "apartment", "house",
            "housing", "home", "bungalow", "maisonette", "bedroom",
            "bed ", "unit", "storey", "mixed use", "mixed-use",
            "build to rent", "btr", "student", "pbsa", "co-living",
            "retirement", "extra care", "assisted living", "senior",
            "affordable", "social rent", "shared ownership",
        ]
        for kw in residential_keywords:
            if kw in description:
                return True

        # Applications with extracted unit counts are relevant
        unit_count = BaseScraper.extract_unit_count(app_data.get("description"))
        if unit_count and unit_count >= MIN_UNITS_BD_RELEVANT:
            return True

        # Major/large-scale applications are worth reviewing
        if any(kw in app_type for kw in ["major", "full planning", "outline", "hybrid"]):
            return True

        return False

    # ------------------------------------------------------------------
    # Parse application
    # ------------------------------------------------------------------

    async def parse_application(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Convert raw result + detail data into a PlanningApplication-compatible dict.

        Note: The base class ``run()`` method already fetches detail pages
        and merges them into ``raw`` before calling this method.  We do NOT
        re-fetch the detail page here to avoid duplicate requests.
        """
        description = raw.get("description", "")
        address = raw.get("address", "")

        return {
            "reference": raw.get("reference", ""),
            "council_id": self.council_id,
            "address": address,
            "postcode": self.extract_postcode(address),
            "ward": raw.get("ward", ""),
            "description": description,
            "applicant_name": raw.get("applicant_name", ""),
            "agent_name": raw.get("agent_name", ""),
            "application_type": raw.get("application_type", ""),
            "status": self.normalise_status(raw.get("status", "")),
            "decision": raw.get("decision", ""),
            "scheme_type": self.classify_scheme_type(description),
            "total_units": self.extract_unit_count(description),
            "submitted_date": raw.get("submission_date"),
            "validated_date": raw.get("validated_date"),
            "decision_date": raw.get("decision_date"),
            "consultation_end_date": raw.get("consultation_end_date"),
            "committee_date": raw.get("committee_date"),
            "documents_url": raw.get("documents_url", ""),
            "portal_url": raw.get("detail_url", ""),
            "source": "nec_scraper",
            "is_btr": self.classify_scheme_type(description) == "BTR",
            "is_pbsa": self.classify_scheme_type(description) == "PBSA",
            "is_affordable": self.classify_scheme_type(description) == "Affordable",
            "raw_data": raw.get("raw_kv"),
        }

    # ------------------------------------------------------------------
    # Override run() to add residential filtering for date-range results
    # ------------------------------------------------------------------

    async def run(self, **search_kwargs: Any) -> list[dict[str, Any]]:
        """Execute a complete scrape with residential relevance filtering.

        Extends the base class ``run()`` to filter out non-residential
        applications from broad date-range searches.  Applications found
        via keyword search are kept regardless (they matched a BD keyword).
        """
        self.log.info("scrape_start", council=self.council_name)
        self.metrics = ScraperMetrics()

        results: list[dict[str, Any]] = []
        try:
            raw_results = await self.search_applications(**search_kwargs)
            self.metrics.applications_found = len(raw_results)
            self.log.info(
                "search_complete",
                results_count=len(raw_results),
            )

            for raw in raw_results:
                try:
                    # Fetch detail page if available
                    detail_url = raw.get("detail_url")
                    if detail_url:
                        try:
                            detail = await self.get_application_detail(detail_url)
                            if detail:
                                raw = {**raw, **{k: v for k, v in detail.items() if v}}
                        except Exception as detail_exc:
                            self.log.warning(
                                "detail_fetch_failed",
                                reference=raw.get("reference", "")[:80],
                                error=str(detail_exc)[:120],
                            )

                    # Filter non-residential applications
                    if not self._is_residential_relevant(raw):
                        self.log.debug(
                            "skipped_non_residential",
                            reference=raw.get("reference", ""),
                            app_type=raw.get("application_type", ""),
                        )
                        continue

                    parsed = await self.parse_application(raw)
                    results.append(parsed)
                except Exception as exc:
                    self.metrics.record_error(
                        exc,
                        context=f"parse: {raw.get('reference', 'unknown')}",
                    )
                    self.log.warning(
                        "parse_error",
                        reference=raw.get("reference"),
                        error=str(exc),
                    )
        except Exception as exc:
            self.metrics.record_error(exc, context="search_applications")
            self.log.error("scrape_failed", error=str(exc))
            raise

        self.log.info(
            "scrape_complete",
            total_found=self.metrics.applications_found,
            residential_relevant=len(results),
            **self.metrics.to_dict(),
        )
        return results

