"""University halls-of-residence discovery scraper.

Scrapes each university's OWN accommodation pages for 10 secondary
university cities (+ Middlesbrough) to list halls of residence and any
partner / nominated PBSA schemes. Partner schemes (private blocks let
under nomination agreements, e.g. Student Roost at Chester, Student
Castle at York) are high-value BD intelligence: they show which private
operators already hold university relationships in each market.

Approach (generic crawl + parse engine, config per university):
  1. Fetch robots.txt per host; honour ``User-agent: *`` Disallow rules
     and Crawl-delay (Kent asks for 10s, Lancaster 5s).
  2. Crawl seed/listing pages (BFS, bounded by ``max_pages``); discover
     hall-page links from anchors AND from raw HTML/JSON (several sites
     - CCCU, Kent, Lancaster - embed links in JSON islands with
     ``\\u002F`` escapes).
  3. Fetch each hall page; parse name (h1/og:title/slug), postcode and
     address (footer/nav stripped first so campus footer addresses do
     not leak in), JSON-LD, "managed/operated by X" statements and
     known PBSA operator brands.
  4. Pages that list halls without dedicated sub-pages (Kent's
     find-my-room app, Worcester's accommodation guide) are handled by
     inline heading extraction and flagged for manual follow-up.

Output: ``UniversityHall`` dataclasses; the runner groups them per city
and writes JSONL to ``data/university_halls/<city>.jsonl``.

Polite by design: >=1 req/sec per host (more where robots asks), custom
browser UA, robots-allowed paths only, hard page budget per university.
"""
from __future__ import annotations

import json
import re
import time
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

POSTCODE_RE = re.compile(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2}\b")

# "managed by X" style statements. NB: "provided by" is deliberately
# excluded - on university pages it is almost always the internet
# provider ("internet provided by Glide").
MANAGED_BY_RE = re.compile(
    r"\b(?:managed|operated|owned|run|delivered)\s+(?:and\s+\w+\s+)?by\s+"
    r"([A-Z][\w&'\.\- ]{2,50})"
)

# Names the managed-by regex may capture that are NOT operators.
NOT_OPERATORS = re.compile(
    r"glide|ask4|studentcom|wifinity|the\s+university|university\s+of|"
    r"our\s+|trained|professional|dedicated|student\s+services",
    re.I,
)

# Private PBSA operator brands. A match on a hall page is a strong
# signal the block is a partner/nominated scheme, not university stock.
KNOWN_PBSA_OPERATORS = [
    "unite students", "iq student", "vita student", "yugo",
    "homes for students", "crm students", "prestige student living",
    "collegiate ac", "student roost", "hello student", "mansion student",
    "abodus", "true student", "novel student", "downing students",
    "study inn", "nido student", "dwell student", "student castle",
    "fresh student living", "fresh property",
    "sanctuary students", "derwent students", "cityblock", "kexgill",
    "city block", "liberty living", "campus living villages",
    "uliving", "upp ", "unipol",
]

PARTNER_WORDS_RE = re.compile(
    r"nomination agreement|nominated accommodation|partner provider|"
    r"partnership accommodation|partner accommodation|managed by a partner|"
    r"private provider|third[- ]party provider",
    re.I,
)

GENERIC_NAMES = {
    "accommodation", "home", "index", "student accommodation", "halls",
    "our halls", "welcome", "find your room", "explore accommodation",
}


# ---------------------------------------------------------------------------
@dataclass
class UniversityHall:
    city_slug: str
    university: str
    hall_name: str
    address: str = ""
    postcode: str = ""
    ownership: str = "unknown"      # "university" | "partner" | "unknown"
    operator: str = ""              # stated operator, if any
    source_url: str = ""
    notes: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InlinePage:
    """A page whose halls are listed as headings, with no hall sub-pages."""
    url: str
    include_re: str
    exclude_re: str = r"faq|contact|apply|price|cost|timeline|guarantee|insurance|reasons?\b"
    note: str = ""


@dataclass
class TablePage:
    """A page whose halls appear in the first column of price tables."""
    url: str
    note: str = ""


@dataclass
class UniversityConfig:
    key: str
    city_slug: str
    university: str
    base: str                                  # scheme + host
    seeds: list[str] = field(default_factory=list)
    hall_patterns: list[str] = field(default_factory=list)
    follow_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    partner_patterns: list[str] = field(default_factory=list)
    inline_pages: list[InlinePage] = field(default_factory=list)
    table_pages: list[TablePage] = field(default_factory=list)
    extra_hall_urls: list[str] = field(default_factory=list)
    extra_hosts: list[str] = field(default_factory=list)
    crawl_delay: float = 1.0
    max_pages: int = 60
    default_ownership: str = "university"
    follow_depth: int = 3
    config_notes: str = ""                     # surfaced in run summary


# ---------------------------------------------------------------------------
# Per-university configs (probed 2026-06: all index pages verified live).
# ---------------------------------------------------------------------------
CONFIGS: dict[str, UniversityConfig] = {}


def _add(cfg: UniversityConfig) -> None:
    CONFIGS[cfg.key] = cfg


_add(UniversityConfig(
    key="kent",
    city_slug="canterbury",
    university="University of Kent",
    base="https://www.kent.ac.uk",
    seeds=[
        "https://www.kent.ac.uk/accommodation",
        "https://www.kent.ac.uk/accommodation/canterbury",
    ],
    # Kent has no per-hall pages: halls live inside the find-my-room app
    # and campus pages. Pier Quays (Medway) and KMMS pages do exist.
    hall_patterns=[
        r"/accommodation/medway/pier-quays$",
        r"/accommodation/canterbury/kmms$",
    ],
    follow_patterns=[
        r"/accommodation/canterbury/(undergraduate|postgraduate)-accommodation",
    ],
    table_pages=[
        TablePage(
            url="https://www.kent.ac.uk/accommodation/canterbury/prices",
            note="residence name from Kent price tables (find-my-room tool "
                 "is JS-only)",
        ),
    ],
    crawl_delay=10.0,   # robots.txt User-agent:* Crawl-delay: 10
    max_pages=12,
    config_notes="kent.ac.uk publishes no per-hall pages; Canterbury "
                 "residence names extracted from the prices page tables. "
                 "Room-level detail sits in a JS-only find-my-room tool.",
))

_add(UniversityConfig(
    key="cccu",
    city_slug="canterbury",
    university="Canterbury Christ Church University",
    base="https://www.canterbury.ac.uk",
    seeds=[
        "https://www.canterbury.ac.uk/study-here/student-life/accommodation",
        "https://www.canterbury.ac.uk/study-here/student-life/accommodation/canterbury-accommodation",
    ],
    hall_patterns=[
        r"/study-here/student-life/accommodation/canterbury-accommodation/[a-z0-9\-]+$",
        r"/student-accommodation/medway/pier-quays$",
    ],
    follow_patterns=[
        r"/study-here/student-life/accommodation/canterbury-accommodation$",
    ],
    exclude_patterns=[
        r"prices$", r"homestay$", r"short-courses$", r"how-to-apply",
        r"reasons", r"accommodation-guarantee", r"deposit-guarantor",
        r"become-a-homestay-host",
    ],
    max_pages=25,
    config_notes="SPA-style pages; hall links recovered from JSON islands "
                 "(\\u002F-escaped).",
))

_add(UniversityConfig(
    key="lincoln",
    city_slug="lincoln",
    university="University of Lincoln",
    base="https://www.lincoln.ac.uk",
    seeds=["https://www.lincoln.ac.uk/studentlife/accommodation/"],
    hall_patterns=[
        r"/studentlife/accommodation/[a-z0-9]+/?$",
    ],
    exclude_patterns=[
        r"howtoapply", r"residencelifeteam", r"commuters", r"faq",
        r"internationalstudents", r"accommodation/?$",
    ],
    max_pages=20,
))

_add(UniversityConfig(
    key="bgu",
    city_slug="lincoln",
    university="Bishop Grosseteste University (Lincoln Bishop University)",
    base="https://www.lincolnbishop.ac.uk",
    seeds=["https://www.lincolnbishop.ac.uk/student/accommodation"],
    hall_patterns=[
        r"/student/accommodation/(student-village|wickham-hall|constance-stewart-hall)$",
    ],
    exclude_patterns=[r"/apply$", r"off-campus"],
    max_pages=15,
    config_notes="bgu.ac.uk now redirects to lincolnbishop.ac.uk (2026 "
                 "rebrand to Lincoln Bishop University).",
))

_add(UniversityConfig(
    key="chester",
    city_slug="chester",
    university="University of Chester",
    base="https://www.chester.ac.uk",
    seeds=[
        "https://www.chester.ac.uk/student-life/accommodation/",
        "https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/",
        "https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/?page=2",
        "https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/?page=3",
        "https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/?page=4",
    ],
    hall_patterns=[
        r"/student-life/accommodation/explore-accommodation/[a-z0-9\-]+/$",
    ],
    follow_patterns=[
        r"/explore-accommodation/\?page=\d+$",
    ],
    exclude_patterns=[r"private-landlords"],
    max_pages=45,
    config_notes="explore-accommodation cards include partner blocks "
                 "(e.g. Student Roost) alongside university halls.",
))

_add(UniversityConfig(
    key="worcester",
    city_slug="worcester",
    university="University of Worcester",
    base="https://www.worcester.ac.uk",
    seeds=["https://www.worcester.ac.uk/campaigns/accommodation-guide"],
    inline_pages=[
        InlinePage(
            url="https://www.worcester.ac.uk/campaigns/accommodation-guide",
            include_re=r"\bhalls\b",
            exclude_re=r"reasons|worry|contract|access|bedroom|live on campus",
            note="room-grade category, not a named hall (Worcester publishes "
                 "hall names only in its PDF campus map)",
        ),
        InlinePage(
            url="https://www.worcester.ac.uk/about/community-collaboration/venue-hire/accommodation-for-hire.aspx",
            include_re=r"\b(hall|court|house|village|lodge|mews)\b",
            exclude_re=r"hire|conference|contact|enquir",
            note="hall name sourced from venue-hire pages",
        ),
    ],
    extra_hosts=["www.worc.ac.uk"],
    max_pages=10,
    config_notes="All /life/accommodation/* pages redirect to a marketing "
                 "funnel; named halls only in PDF map "
                 "(worcester.ac.uk/documents/map-of-halls-of-residence.pdf) "
                 "- manual follow-up needed.",
))

_add(UniversityConfig(
    key="winchester",
    city_slug="winchester",
    university="University of Winchester",
    base="https://www.winchester.ac.uk",
    seeds=["https://www.winchester.ac.uk/student-life/accommodation/"],
    hall_patterns=[
        r"(?i)/student-life/accommodation/[^/?]*(?:village|catered)[^/?]*/$",
    ],
    max_pages=15,
    config_notes="Winchester groups halls into three student villages plus "
                 "catered halls; village pages carry the postcodes.",
))

_add(UniversityConfig(
    key="lancaster",
    city_slug="lancaster",
    university="Lancaster University",
    base="https://www.lancaster.ac.uk",
    seeds=[
        "https://www.lancaster.ac.uk/accommodation/",
        "https://www.lancaster.ac.uk/accommodation/city-accommodation/",
    ],
    hall_patterns=[
        r"/accommodation/city-accommodation/[a-z0-9\-]+/$",
    ],
    exclude_patterns=[r"homes-standard"],
    extra_hall_urls=[
        # the nine college microsites (verified live); accommodation at
        # Lancaster is college-based, each college = one residence record
        "https://www.lancaster.ac.uk/bowland/",
        "https://www.lancaster.ac.uk/cartmel/",
        "https://www.lancaster.ac.uk/county/",
        "https://www.lancaster.ac.uk/furness/",
        "https://www.lancaster.ac.uk/fylde/",
        "https://www.lancaster.ac.uk/grizedale/",
        "https://www.lancaster.ac.uk/lonsdale/",
        "https://www.lancaster.ac.uk/pendle/",
        "https://www.lancaster.ac.uk/graduate-college/",
    ],
    crawl_delay=5.0,    # robots.txt User-agent:* Crawl-delay: 5
    max_pages=18,
    config_notes="Accommodation is college-based (9 colleges, all "
                 "university-owned, on campus) plus Chancellor's Wharf in "
                 "the city; the colleges listing page is JS-rendered so "
                 "college microsites are seeded directly.",
))

_add(UniversityConfig(
    key="durham",
    city_slug="durham",
    university="Durham University",
    base="https://www.durham.ac.uk",
    seeds=["https://www.durham.ac.uk/colleges-and-student-experience/colleges/"],
    hall_patterns=[
        r"/colleges-and-student-experience/colleges/[a-z0-9\-]+/$",
    ],
    exclude_patterns=[
        r"membership", r"accommodation", r"faq", r"compare", r"pledge",
        r"welcome", r"office", r"your-college", r"frequently-asked",
        r"colleges/$",
    ],
    max_pages=30,
    config_notes="Durham accommodation is collegiate: each college page is "
                 "one residence record.",
))

_add(UniversityConfig(
    key="bangor",
    city_slug="bangor",
    university="Bangor University",
    base="https://www.bangor.ac.uk",
    seeds=[
        "https://www.bangor.ac.uk/accommodation",
        "https://www.bangor.ac.uk/ffriddoedd-village",
        "https://www.bangor.ac.uk/accommodation/st-marys-village",
        "https://www.bangor.ac.uk/accommodation/includes/halltabs",
    ],
    hall_patterns=[
        r"/accommodation/halls/[a-z0-9\-]+/[a-z0-9\-]+$",
    ],
    exclude_patterns=[
        r"bedroom|kitchen|/tv$|parking|paying|mentors|important_info|why$|"
        r"lounge|reception|fitness|studio$|accessible|flat-",
    ],
    extra_hall_urls=[
        "https://www.bangor.ac.uk/ffriddoedd-village",
        "https://www.bangor.ac.uk/accommodation/st-marys-village",
    ],
    max_pages=40,
    config_notes="Per-hall names inside each village (~20 halls: Adda, "
                 "Alaw, Braint etc.) are rendered by a JS tab component, so "
                 "only halls with direct HTML links are captured. Manual "
                 "follow-up: village pages' hall tabs / room-finder app.",
))

_add(UniversityConfig(
    key="aber",
    city_slug="aberystwyth",
    university="Aberystwyth University",
    base="https://www.aber.ac.uk",
    seeds=[
        "https://www.aber.ac.uk/en/accommodation/",
        "https://www.aber.ac.uk/en/study-with-us/accommodation/accommodation-options/",
    ],
    hall_patterns=[
        r"/en/study-with-us/accommodation/accommodation-options/[a-z0-9\-]+/$",
    ],
    exclude_patterns=[
        r"compare", r"designated-areas", r"welsh-medium",
        r"summer-accommodation", r"private-sector", r"accessible",
        r"booking-communal-room", r"accommodation-options/$",
    ],
    max_pages=25,
))

_add(UniversityConfig(
    key="york",
    city_slug="york",
    university="University of York",
    base="https://www.york.ac.uk",
    seeds=[
        "https://www.york.ac.uk/study/accommodation/",
        "https://www.york.ac.uk/study/accommodation/rooms-prices/",
    ],
    hall_patterns=[
        r"/study/accommodation/rooms-prices/[a-z0-9\-]+/$",
    ],
    exclude_patterns=[r"rents-at-york", r"rooms-prices/$"],
    max_pages=30,
    config_notes="rooms-prices list includes Student Castle - a private "
                 "operator block offered through the university.",
))

_add(UniversityConfig(
    key="yorksj",
    city_slug="york",
    university="York St John University",
    base="https://www.yorksj.ac.uk",
    seeds=["https://www.yorksj.ac.uk/study/accommodation/"],
    hall_patterns=[
        # index page itself excluded; its child house pages are halls
        r"/study/accommodation/(?!university-managed-housing/$)[a-z0-9\-]+/$",
        r"/study/accommodation/university-managed-housing/[a-z0-9\-]+/$",
    ],
    follow_patterns=[
        r"/study/accommodation/university-managed-housing/$",
        r"/study/accommodation/apply-for-accommodation/partner-providers/$",
    ],
    exclude_patterns=[
        r"apply-for-accommodation", r"private-accommodation",
        r"students-with-additional-requirements", r"paying-for",
        r"policies", r"accommodation/$",
    ],
    partner_patterns=[
        r"/study/accommodation/apply-for-accommodation/partner-providers/$",
    ],
    max_pages=45,
    config_notes="YSJ openly lists partner providers; several blocks "
                 "(Abode, The Brickworks, The Coal Yard, The Grange) are "
                 "operated by private partners.",
))

_add(UniversityConfig(
    key="tees",
    city_slug="middlesbrough",
    university="Teesside University",
    base="https://www.tees.ac.uk",
    seeds=["https://www.tees.ac.uk/sections/accommodation/"],
    hall_patterns=[
        r"/sections/accommodation/buildings/[a-z0-9_\-]+\.cfm$",
    ],
    max_pages=15,
))


CITY_UNIVERSITIES: dict[str, list[str]] = {
    "canterbury": ["kent", "cccu"],
    "lincoln": ["lincoln", "bgu"],
    "chester": ["chester"],
    "worcester": ["worcester"],
    "winchester": ["winchester"],
    "lancaster": ["lancaster"],
    "durham": ["durham"],
    "bangor": ["bangor"],
    "aberystwyth": ["aber"],
    "york": ["york", "yorksj"],
    "middlesbrough": ["tees"],
}


# ---------------------------------------------------------------------------
class _RobotsRules:
    """Minimal robots.txt: User-agent:* Disallow prefixes + crawl-delay."""

    def __init__(self, text: str = ""):
        self.disallow: list[re.Pattern] = []
        self.crawl_delay: float = 0.0
        if not text:
            return
        active = False
        for line in text.splitlines():
            ls = line.split("#", 1)[0].strip()
            if not ls:
                continue
            low = ls.lower()
            if low.startswith("user-agent:"):
                active = low.split(":", 1)[1].strip() == "*"
            elif active and low.startswith("disallow:"):
                path = ls.split(":", 1)[1].strip()
                if not path:
                    continue
                pat = re.escape(path).replace(r"\*", ".*").replace(r"\$", "$")
                try:
                    self.disallow.append(re.compile("^" + pat))
                except re.error:
                    continue
            elif active and low.startswith("crawl-delay:"):
                try:
                    self.crawl_delay = float(ls.split(":", 1)[1].strip())
                except ValueError:
                    pass

    def allowed(self, path: str) -> bool:
        return not any(p.match(path) for p in self.disallow)


class UniversityHallsScraper:
    """Generic crawl + parse engine driven by ``UniversityConfig``."""

    def __init__(self, timeout: float = 30.0):
        self.client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._last_req: dict[str, float] = {}     # per-host throttle
        self._robots: dict[str, _RobotsRules] = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.client.close()

    # -- politeness ----------------------------------------------------
    def _throttle(self, host: str, delay: float):
        last = self._last_req.get(host, 0.0)
        wait = delay - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)
        self._last_req[host] = time.monotonic()

    def _robots_for(self, host: str) -> _RobotsRules:
        if host not in self._robots:
            self._throttle(host, 1.0)
            try:
                r = self.client.get(f"https://{host}/robots.txt")
                text = r.text if r.status_code == 200 else ""
            except Exception as e:
                logger.warning("robots_fetch_error", host=host, error=str(e)[:100])
                text = ""
            self._robots[host] = _RobotsRules(text)
        return self._robots[host]

    def get(self, url: str, delay: float) -> Optional[httpx.Response]:
        parsed = urlparse(url)
        host = parsed.netloc
        rules = self._robots_for(host)
        if not rules.allowed(parsed.path):
            logger.info("robots_disallowed", url=url)
            return None
        self._throttle(host, max(delay, rules.crawl_delay))
        try:
            r = self.client.get(url)
        except Exception as e:
            logger.warning("fetch_error", url=url, error=str(e)[:120])
            return None
        if r.status_code != 200:
            logger.warning("fetch_http", url=url, status=r.status_code)
            return None
        return r

    # -- link discovery -------------------------------------------------
    @staticmethod
    def _candidate_urls(html: str, page_url: str, hosts: set[str]) -> set[str]:
        """All same-site URL candidates from anchors AND raw HTML/JSON."""
        out: set[str] = set()
        unescaped = html.replace("\\u002F", "/").replace("\\/", "/")
        soup = BeautifulSoup(unescaped, "html.parser")
        for a in soup.find_all("a", href=True):
            out.add(urljoin(page_url, a["href"].split("#")[0]))
        # raw scan catches links inside JSON islands / JS-rendered cards
        for m in re.finditer(
            r'["\'](https?://[^"\'\s<>]+|/[a-zA-Z0-9\-_/\.%]+(?:\?page=\d+)?)["\']',
            unescaped,
        ):
            out.add(urljoin(page_url, m.group(1).split("#")[0]))
        keep = set()
        for u in out:
            p = urlparse(u)
            if p.scheme not in ("http", "https") or p.netloc not in hosts:
                continue
            if p.query and "page=" not in p.query:
                u = u.split("?")[0]
            keep.add(u)
        return keep

    @staticmethod
    def _match_any(url: str, patterns: list[str]) -> bool:
        return any(re.search(p, url) for p in patterns)

    # -- page parsing ----------------------------------------------------
    @staticmethod
    def _strip_furniture(soup: BeautifulSoup) -> BeautifulSoup:
        for sel in ("header", "footer", "nav", "script", "style", "noscript"):
            for el in soup.find_all(sel):
                el.decompose()
        for el in soup.select(
            '[class*="footer"], [id*="footer"], [class*="cookie"], '
            '[class*="breadcrumb"], [class*="nav-"], [class*="menu"]'
        ):
            el.decompose()
        return soup

    @staticmethod
    def _name_from_url(url: str) -> str:
        slug = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"\.(cfm|aspx|html?)$", "", slug)
        return slug.replace("-", " ").replace("_", " ").strip().title()

    # JSON-LD node types whose address describes the page's own building
    # (an Organization/CollegeOrUniversity address is the campus HQ and
    # appears on every page - it must not be attributed to a hall).
    _LD_PLACE_TYPES = {
        "place", "residence", "apartmentcomplex", "accommodation",
        "apartment", "house", "lodgingbusiness", "hotel", "localbusiness",
    }

    @classmethod
    def _json_ld_address(cls, html: str) -> tuple[str, str]:
        """(address, postcode) from place-typed JSON-LD, if present."""
        for m in re.finditer(
            r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', html, re.S
        ):
            try:
                data = json.loads(m.group(1))
            except (json.JSONDecodeError, ValueError):
                continue
            stack = [data]
            while stack:
                node = stack.pop()
                if isinstance(node, list):
                    stack.extend(node)
                    continue
                if not isinstance(node, dict):
                    continue
                ntype = node.get("@type") or ""
                if isinstance(ntype, list):
                    types = {str(t).lower() for t in ntype}
                else:
                    types = {str(ntype).lower()}
                addr = node.get("address")
                if isinstance(addr, dict) and types & cls._LD_PLACE_TYPES:
                    street = (addr.get("streetAddress") or "").strip()
                    town = (addr.get("addressLocality") or "").strip()
                    pc = (addr.get("postalCode") or "").strip()
                    full = ", ".join(x for x in (street, town) if x)
                    if full or pc:
                        return full, pc
                stack.extend(v for v in node.values()
                             if isinstance(v, (dict, list)))
        return "", ""

    @staticmethod
    def _is_self_reference(candidate: str, university: str) -> bool:
        """True when 'managed by X' is the university referring to itself."""
        skip = {"university", "the", "of", "and"}
        uni_words = set(re.sub(r"[^a-z ]", "", university.lower()).split())
        cand_words = [
            w for w in re.sub(r"[^a-z ]", "", candidate.lower()).split()
            if w not in skip
        ]
        return not cand_words or all(w in uni_words for w in cand_words)

    def _extract_operator(self, text: str, university: str) -> str:
        m = MANAGED_BY_RE.search(text)
        if m:
            name = m.group(1).strip()
            # cut at common run-ons
            name = re.split(r"\s+(?:who|which|and\s+is|on\s+behalf|in\s+)", name)[0]
            name = name.strip(" .,-")
            if (name and len(name) > 2
                    and not NOT_OPERATORS.search(name)
                    and not self._is_self_reference(name, university)):
                return name
        low = text.lower()
        for brand in KNOWN_PBSA_OPERATORS:
            if brand in low:
                return brand.strip().title()
        return ""

    def parse_hall_page(
        self, cfg: UniversityConfig, url: str, html: str,
        partner_context: bool,
    ) -> Optional[UniversityHall]:
        ld_addr, ld_pc = self._json_ld_address(html)
        soup = BeautifulSoup(html, "html.parser")

        name = ""
        for h1 in soup.find_all("h1"):
            t = h1.get_text(" ", strip=True)
            if t and not re.search(r"cookie|privacy|search|menu|sign in", t, re.I):
                name = t
                break
        if not name:
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                name = og["content"].strip()
        if not name and soup.title:
            name = soup.title.get_text(strip=True)
        name = re.split(r"\s*[|–—]\s*", name)[0].strip()
        name = re.sub(r"\s*-\s*(University|Lancaster|Teesside|York|Durham).*$",
                      "", name).strip()
        if not name or name.lower() in GENERIC_NAMES or len(name) > 90:
            name = self._name_from_url(url)
        if not name or name.lower() in GENERIC_NAMES:
            return None

        self._strip_furniture(soup)
        main = soup.find("main") or soup.body or soup
        text = main.get_text(" ", strip=True)

        postcode = ld_pc
        address = ld_addr
        pcs = POSTCODE_RE.findall(text)
        if not postcode and pcs:
            postcode = pcs[0]
        if not address and postcode:
            # smallest element containing the postcode = address line
            best = ""
            for el in main.find_all(["address", "p", "li", "span", "div"]):
                t = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
                if postcode in t and 8 <= len(t) <= 160:
                    if not best or len(t) < len(best):
                        best = t
            address = best

        operator = self._extract_operator(text, cfg.university)
        partner_text = bool(PARTNER_WORDS_RE.search(text))

        # strip operator branding baked into the page h1
        # ("Student Roost The Towpath", "Granary Studios Collegiate AC")
        brands = set(KNOWN_PBSA_OPERATORS)
        if operator:
            brands.add(operator.lower())
        for b in brands:
            if b and b in name.lower():
                cleaned = re.sub(
                    re.escape(b) + r"(\s+ac\b)?", "", name, flags=re.I
                )
                cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -–—|,")
                if len(cleaned) >= 3 and cleaned.lower() not in GENERIC_NAMES:
                    name = cleaned
        # "Alcuin College accommodation" -> "Alcuin College"
        # (but keep single-word remainders: "Catered accommodation")
        stripped = re.sub(r"\s+accommodation$", "", name, flags=re.I).strip()
        if len(stripped.split()) >= 2:
            name = stripped
        if not name or name.lower() in GENERIC_NAMES:
            name = self._name_from_url(url)
        if not name or name.lower() in GENERIC_NAMES:
            return None

        notes: list[str] = []
        if partner_context:
            notes.append("listed via partner/nominated accommodation page")
        if partner_text:
            notes.append("partner/nomination wording on page")
        if operator:
            notes.append(f"operator stated: {operator}")
        if len(set(pcs)) > 1:
            notes.append(f"multiple postcodes on page: {sorted(set(pcs))[:4]}")
        if "medway" in url:
            notes.append("Medway campus scheme (Gillingham), not city centre")

        if partner_context or partner_text or operator:
            ownership = "partner"
        else:
            ownership = cfg.default_ownership

        return UniversityHall(
            city_slug=cfg.city_slug,
            university=cfg.university,
            hall_name=name,
            address=address,
            postcode=postcode,
            ownership=ownership,
            operator=operator,
            source_url=url,
            notes="; ".join(notes),
        )

    def parse_inline_page(
        self, cfg: UniversityConfig, page: InlinePage, html: str,
    ) -> list[UniversityHall]:
        soup = self._strip_furniture(BeautifulSoup(html, "html.parser"))
        inc = re.compile(page.include_re, re.I)
        exc = re.compile(page.exclude_re, re.I)
        halls: list[UniversityHall] = []
        seen: set[str] = set()
        for tag in soup.find_all(["h2", "h3", "h4"]):
            t = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            if not t or len(t) > 60 or not inc.search(t) or exc.search(t):
                continue
            key = t.lower()
            if key in seen or key in GENERIC_NAMES:
                continue
            seen.add(key)
            # postcode from the heading's enclosing section, if any
            postcode = ""
            parent = tag.parent
            for _ in range(3):
                if parent is None:
                    break
                pcs = POSTCODE_RE.findall(parent.get_text(" ", strip=True))
                if pcs:
                    postcode = pcs[0]
                    break
                parent = parent.parent
            halls.append(UniversityHall(
                city_slug=cfg.city_slug,
                university=cfg.university,
                hall_name=t,
                postcode=postcode,
                ownership=cfg.default_ownership,
                source_url=page.url,
                notes=page.note or "extracted inline from listing page",
            ))
        return halls

    # room-type descriptors trimmed off table "Location" cells:
    # "Park Wood 5 bed houses" -> "Park Wood",
    # "Giles Court - standard en-suite" -> "Giles Court"
    _ROOM_DESC_RE = re.compile(
        r"(?i)\s+(?:\d+\s*bed\b.*|twin\b.*|room in\b.*|studio\b.*|"
        r"standard\b.*|large\b.*|extra\b.*|en-?suite\b.*|with\b.*|"
        r"catered\b.*|self.?catered\b.*)$"
    )
    _BLOCK_LETTER_RE = re.compile(r"\s+[A-Z](?:\s*/\s*[A-Z])*$")

    def parse_table_page(
        self, cfg: UniversityConfig, page: TablePage, html: str,
    ) -> list[UniversityHall]:
        soup = self._strip_furniture(BeautifulSoup(html, "html.parser"))
        halls: list[UniversityHall] = []
        seen: set[str] = set()
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                raw = re.sub(r"\s+", " ", cells[0].get_text(" ", strip=True))
                if (not raw or "£" in raw or len(raw) > 60
                        or raw.lower() in ("location", "accommodation")
                        or re.search(r"\d{2}/\d{2}/\d{2}", raw)):
                    continue
                name = raw.split("(")[0]
                name = name.split(" - ")[0]
                name = self._ROOM_DESC_RE.sub("", name).strip(" .,- ")
                name = self._BLOCK_LETTER_RE.sub("", name).strip()
                if (len(name) < 4 or name.lower() in GENERIC_NAMES
                        or name.lower() in seen
                        or re.search(r"(?i)\b(weeks?|contract|total|fees?)\b",
                                     name)):
                    continue
                seen.add(name.lower())
                halls.append(UniversityHall(
                    city_slug=cfg.city_slug,
                    university=cfg.university,
                    hall_name=name,
                    ownership=cfg.default_ownership,
                    source_url=page.url,
                    notes=page.note or "residence name from price table",
                ))
        # drop bare prefixes of longer names ("Keynes" vs "Keynes College")
        names = [h.hall_name for h in halls]
        halls = [
            h for h in halls
            if not any(
                o.lower().startswith(h.hall_name.lower() + " ")
                for o in names if o != h.hall_name
            )
        ]
        return halls

    # -- main entry -------------------------------------------------------
    def scrape_university(self, key: str) -> tuple[list[UniversityHall], dict]:
        cfg = CONFIGS[key]
        hosts = {urlparse(cfg.base).netloc, *cfg.extra_hosts}
        stats: dict[str, Any] = {
            "university": cfg.university,
            "pages_fetched": 0,
            "errors": 0,
            "config_notes": cfg.config_notes,
            "follow_up": [],
        }
        fetched = 0
        visited: set[str] = set()
        hall_urls: dict[str, bool] = {}     # url -> partner_context

        def norm(u: str) -> str:
            return u.rstrip("/")

        queue: deque[tuple[str, int, bool]] = deque(
            (s, 0, False) for s in cfg.seeds
        )
        # ---- BFS over listing pages ----
        # cap the BFS so hall/inline pages always keep part of the budget
        bfs_cap = max(len(cfg.seeds) + 2, cfg.max_pages // 2)
        while queue and fetched < bfs_cap:
            url, depth, partner_ctx = queue.popleft()
            if norm(url) in visited:
                continue
            visited.add(norm(url))
            r = self.get(url, cfg.crawl_delay)
            fetched += 1
            if r is None:
                stats["errors"] += 1
                continue
            final = str(r.url)
            visited.add(norm(final))
            host_final = urlparse(final).netloc
            if host_final not in hosts:
                hosts.add(host_final)   # follow cross-host redirect target
            page_is_partner = partner_ctx or self._match_any(
                final, cfg.partner_patterns
            ) or self._match_any(url, cfg.partner_patterns)
            for cand in self._candidate_urls(r.text, final, hosts):
                if self._match_any(cand, cfg.exclude_patterns):
                    continue
                if self._match_any(cand, cfg.hall_patterns):
                    k = norm(cand)
                    hall_urls[k] = hall_urls.get(k, False) or page_is_partner
                elif (depth < cfg.follow_depth
                      and self._match_any(cand, cfg.follow_patterns)
                      and norm(cand) not in visited):
                    queue.append((cand, depth + 1, page_is_partner))

        for extra in cfg.extra_hall_urls:
            hall_urls.setdefault(norm(extra), False)

        # ---- fetch hall pages ----
        halls: list[UniversityHall] = []
        seen_names: set[str] = set()
        for hurl, partner_ctx in sorted(hall_urls.items()):
            if fetched >= cfg.max_pages:
                stats["follow_up"].append(
                    f"page budget hit before fetching {hurl}"
                )
                continue
            r = self.get(hurl, cfg.crawl_delay)
            fetched += 1
            if r is None:
                stats["errors"] += 1
                continue
            final = str(r.url)
            extra_norm = {norm(u) for u in cfg.extra_hall_urls}
            if (norm(final) != hurl
                    and not self._match_any(final, cfg.hall_patterns)
                    and hurl not in extra_norm):
                # redirected away to an index/landing page - not a hall
                continue
            hall = self.parse_hall_page(cfg, final, r.text, partner_ctx)
            if hall and hall.hall_name.lower() not in seen_names:
                seen_names.add(hall.hall_name.lower())
                halls.append(hall)

        # ---- price-table pages ----
        for tpage in cfg.table_pages:
            if fetched >= cfg.max_pages:
                stats["follow_up"].append(
                    f"page budget hit before table page {tpage.url}"
                )
                continue
            r = self.get(tpage.url, cfg.crawl_delay)
            fetched += 1
            if r is None:
                stats["errors"] += 1
                stats["follow_up"].append(f"table page failed: {tpage.url}")
                continue
            for hall in self.parse_table_page(cfg, tpage, r.text):
                if hall.hall_name.lower() not in seen_names:
                    seen_names.add(hall.hall_name.lower())
                    halls.append(hall)

        # ---- inline listing pages ----
        for page in cfg.inline_pages:
            if fetched >= cfg.max_pages:
                stats["follow_up"].append(
                    f"page budget hit before inline page {page.url}"
                )
                continue
            r = self.get(page.url, cfg.crawl_delay)
            fetched += 1
            if r is None:
                stats["errors"] += 1
                stats["follow_up"].append(f"inline page failed: {page.url}")
                continue
            for hall in self.parse_inline_page(cfg, page, r.text):
                if hall.hall_name.lower() not in seen_names:
                    seen_names.add(hall.hall_name.lower())
                    halls.append(hall)

        # flag campus-wide postcodes (same code on nearly every hall page is
        # usually the campus/accommodation-office address, not the hall's)
        coded = [h.postcode for h in halls if h.postcode]
        if len(coded) >= 4:
            top, n = Counter(coded).most_common(1)[0]
            if n >= 4 and n / len(coded) >= 0.8:
                for h in halls:
                    if h.postcode == top:
                        tag = "campus-level postcode (shared across hall pages)"
                        h.notes = f"{h.notes}; {tag}" if h.notes else tag

        if not halls:
            stats["follow_up"].append(
                "no halls extracted - pages may be JS-only; manual review "
                f"needed: {cfg.seeds[0] if cfg.seeds else cfg.base}"
            )
        stats["pages_fetched"] = fetched
        stats["halls"] = len(halls)
        stats["partner"] = sum(1 for h in halls if h.ownership == "partner")
        stats["operators"] = sorted(
            {h.operator for h in halls if h.operator}
        )
        logger.info(
            "university_scraped", key=key, halls=len(halls),
            partner=stats["partner"], pages=fetched,
        )
        return halls, stats

    def scrape_city(self, city_slug: str) -> tuple[list[UniversityHall], list[dict]]:
        halls: list[UniversityHall] = []
        stats: list[dict] = []
        for key in CITY_UNIVERSITIES[city_slug]:
            h, s = self.scrape_university(key)
            halls.extend(h)
            stats.append(s)
        return halls, stats
