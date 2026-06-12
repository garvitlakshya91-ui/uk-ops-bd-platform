"""
PBSA (Purpose-Built Student Accommodation) operator scraper.

Scrapes public portfolio pages of the major UK PBSA operators to collect scheme
names, addresses, postcodes, and bed counts. Creates/updates ExistingScheme
records with operator_company_id linked. Owner data is left null and is expected
to be filled in later by a CCOD re-enrichment pass (matched by postcode).

Covered operators (13):
  1. Unite Students         — unitestudents.com
  2. iQ Student Accommodation — iqstudentaccommodation.com
  3. Fresh (Fresh Student Living) — freshstudentliving.com
  4. Vita Student           — vitastudent.com
  5. Prime Student Living   — primestudentliving.com
  6. Scape                  — scape.com
  7. Chapter                — chapter-living.com
  8. Host Students          — host-students.com
  9. Downing Students       — downingstudents.com
  10. Collegiate AC         — collegiate-ac.com
  11. CRM Students          — crm-students.com
  12. Sanctuary Students    — sanctuary-students.com
  13. The Student Housing Company — tshc.eu

Strategy:
  * For each operator, fetch a listing/sitemap page (configurable per-operator)
  * Extract a list of detail URLs, one per scheme
  * Fetch each detail page, extract name / address / postcode / bed count
  * Each operator has a bespoke parser, but they share common helpers
    (postcode extraction, bed-count regexes, JSON-LD parsing).
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse

import httpx
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)
_BED_COUNT_PATTERNS: list[re.Pattern] = [
    # Very specific: "500 beds", "500 bed spaces", "500 bedrooms", "500 student rooms"
    re.compile(r"\b(\d{2,4})\s*(?:bed[\s-]?spaces|beds|bedrooms|student\s+rooms|student\s+beds)\b", re.IGNORECASE),
    # "500 studios" / "500 en-suite rooms"
    re.compile(r"\b(\d{2,4})\s*(?:studios|en[\s-]?suite\s+rooms?|cluster\s+rooms?)\b", re.IGNORECASE),
    # "home to 500" / "accommodates 500"
    re.compile(r"(?:home\s+to|accommodates?|houses?|hosts?)\s+(\d{2,4})\s+students?", re.IGNORECASE),
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def extract_postcode(text: str | None) -> str | None:
    """Extract the first valid UK postcode from text."""
    if not text:
        return None
    m = _POSTCODE_RE.search(text)
    if not m:
        return None
    pc = re.sub(r"\s+", "", m.group(1)).upper()
    if len(pc) >= 5:
        return f"{pc[:-3]} {pc[-3:]}"
    return pc


def extract_bed_count(text: str | None) -> int | None:
    """Best-effort extraction of bed/room count from a string."""
    if not text:
        return None
    for pat in _BED_COUNT_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                n = int(m.group(1).replace(",", ""))
                if 20 <= n <= 5000:
                    return n
            except ValueError:
                continue
    return None


def parse_json_ld(html: str) -> list[dict[str, Any]]:
    """Extract all JSON-LD blobs from an HTML document (no bs4 required)."""
    blobs: list[dict[str, Any]] = []
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            blobs.extend(d for d in data if isinstance(d, dict))
        elif isinstance(data, dict):
            blobs.append(data)
    return blobs


def json_ld_address(blob: dict[str, Any]) -> Optional[str]:
    """Try to pull a postal address out of a JSON-LD blob."""
    addr = blob.get("address")
    if not addr:
        return None
    if isinstance(addr, str):
        return addr
    if isinstance(addr, dict):
        parts = [
            addr.get("streetAddress"),
            addr.get("addressLocality"),
            addr.get("addressRegion"),
            addr.get("postalCode"),
        ]
        return ", ".join(p for p in parts if p)
    return None


# ---------------------------------------------------------------------------
# Operator configs and bespoke parsers
# ---------------------------------------------------------------------------

@dataclass
class PBSAScheme:
    operator_name: str
    scheme_name: str
    detail_url: str
    address: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    num_units: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_name": self.operator_name,
            "scheme_name": self.scheme_name,
            "detail_url": self.detail_url,
            "address": self.address,
            "postcode": self.postcode,
            "city": self.city,
            "num_units": self.num_units,
        }


@dataclass
class OperatorConfig:
    operator_name: str
    base_url: str
    # Either: sitemap URL(s) to pull detail links from, OR listing page URL(s)
    sitemap_urls: list[str] = field(default_factory=list)
    listing_urls: list[str] = field(default_factory=list)
    # Regex for detail URLs (must match to be collected)
    detail_url_pattern: Optional[re.Pattern] = None
    # Anchor-text-scrape CSS selector for listing pages (approximated with regex)
    listing_link_pattern: Optional[re.Pattern] = None
    # Regex to exclude URLs that match the pattern but aren't scheme pages
    exclude_patterns: list[re.Pattern] = field(default_factory=list)
    # How to extract the scheme name from <title>: "first" (default) or "last" segment
    title_segment: str = "first"
    # Extra hardcoded detail URLs (for operators with no sitemap / small portfolio)
    extra_detail_urls: list[str] = field(default_factory=list)


OPERATOR_CONFIGS: dict[str, OperatorConfig] = {
    "unite_students": OperatorConfig(
        operator_name="Unite Students",
        base_url="https://www.unitestudents.com",
        sitemap_urls=["https://www.unitestudents.com/sitemap.xml"],
        # Unite: /student-accommodation/<city>/<property>
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?unitestudents\.com/student-accommodation/[a-z0-9-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "iq_student": OperatorConfig(
        operator_name="iQ Student Accommodation",
        base_url="https://www.iqstudentaccommodation.com",
        sitemap_urls=["https://www.iqstudentaccommodation.com/sitemap.xml"],
        # iQ: /{city}/{property-slug}/welcome for each property (English locale only;
        # skip /zh-hans/ Chinese duplicates)
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?iqstudentaccommodation\.com/[a-z][a-z0-9-]*/[a-z][a-z0-9-]*/welcome/?$",
            re.IGNORECASE,
        ),
        exclude_patterns=[
            re.compile(r"/zh-hans/", re.I),  # Chinese locale duplicates
            re.compile(r"/(news|blog|help|contact|privacy|boke|gonglue)", re.I),
        ],
    ),
    "fresh_student_living": OperatorConfig(
        operator_name="Fresh",
        base_url="https://www.freshstudentliving.com",
        sitemap_urls=["https://www.freshstudentliving.com/sitemap.xml"],
        listing_urls=["https://www.freshstudentliving.com/our-locations"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?freshstudentliving\.com/locations/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "vita_student": OperatorConfig(
        operator_name="Vita Student",
        base_url="https://www.vitastudent.com",
        sitemap_urls=["https://www.vitastudent.com/sitemap.xml"],
        listing_urls=["https://www.vitastudent.com/en/cities"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?vitastudent\.com/en/cities/[a-z0-9-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "prime_student_living": OperatorConfig(
        operator_name="Prime Student Living",
        base_url="https://primestudentliving.com",
        listing_urls=["https://primestudentliving.com/locations/"],
        # Prime: each city page is a single scheme
        extra_detail_urls=[
            "https://primestudentliving.com/birmingham/",
        ],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?primestudentliving\.com/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
        exclude_patterns=[
            re.compile(r"/(blog|faqs|rebook|locations|booking-promise|about_us|terms|privacy|referral|get_in_touch|group-bookings|environmental|FAQs|home-v2|feed|wp-|xmlrpc|2020|2021|2022|2023|2024|2025|2026)", re.I),
        ],
        title_segment="last",
    ),
    "scape": OperatorConfig(
        operator_name="Scape",
        base_url="https://scape.com",
        sitemap_urls=["https://scape.com/sitemap.xml"],
        listing_urls=["https://scape.com/en/locations"],
        detail_url_pattern=re.compile(
            r"^https?://scape\.com/en/locations/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "chapter": OperatorConfig(
        operator_name="Chapter",
        base_url="https://www.chapter-living.com",
        sitemap_urls=["https://www.chapter-living.com/sitemap.xml"],
        listing_urls=["https://www.chapter-living.com/locations"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?chapter-living\.com/locations/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "host_students": OperatorConfig(
        operator_name="Host Students",
        base_url="https://host-students.com",
        sitemap_urls=["https://host-students.com/sitemap.xml"],
        listing_urls=["https://host-students.com/locations/"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?host-students\.com/locations/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "downing_students": OperatorConfig(
        operator_name="Downing Students",
        base_url="https://www.downingstudents.com",
        sitemap_urls=["https://www.downingstudents.com/sitemap.xml"],
        listing_urls=["https://www.downingstudents.com/our-properties/"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?downingstudents\.com/[a-z-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
        exclude_patterns=[
            re.compile(r"/(about|contact|privacy|terms|help|blog|news|careers)/", re.I),
        ],
    ),
    "collegiate_ac": OperatorConfig(
        operator_name="Collegiate AC",
        base_url="https://www.collegiate-ac.com",
        sitemap_urls=["https://www.collegiate-ac.com/sitemap.xml"],
        listing_urls=["https://www.collegiate-ac.com/uk-student-accommodation/"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?collegiate-ac\.com/uk-student-accommodation/[a-z0-9-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "crm_students": OperatorConfig(
        operator_name="CRM Students",
        base_url="https://www.crm-students.com",
        sitemap_urls=["https://www.crm-students.com/sitemap.xml"],
        listing_urls=["https://www.crm-students.com/properties/"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?crm-students\.com/properties/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "sanctuary_students": OperatorConfig(
        operator_name="Sanctuary Students",
        base_url="https://www.sanctuary-students.com",
        sitemap_urls=["https://www.sanctuary-students.com/sitemap.xml"],
        listing_urls=["https://www.sanctuary-students.com/find-accommodation"],
        detail_url_pattern=re.compile(
            r"^https?://(?:www\.)?sanctuary-students\.com/find-accommodation/[a-z0-9-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
    "tshc": OperatorConfig(
        operator_name="The Student Housing Company",
        base_url="https://tshc.eu",
        sitemap_urls=["https://tshc.eu/sitemap.xml"],
        listing_urls=["https://tshc.eu/en-gb/our-locations/"],
        detail_url_pattern=re.compile(
            r"^https?://tshc\.eu/en-gb/our-locations/[a-z0-9-]+/[a-z0-9-]+/?$",
            re.IGNORECASE,
        ),
    ),
}


# ---------------------------------------------------------------------------
# Core scraper
# ---------------------------------------------------------------------------

class PBSAScraper:
    """Scraper for public PBSA operator portfolio pages."""

    def __init__(
        self,
        rate_limit_seconds: float = 1.5,
        timeout: float = 30.0,
        proxy_url: str | None = None,
    ) -> None:
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout = timeout
        self.proxy_url = proxy_url
        self.client: httpx.AsyncClient | None = None
        self.log = logger.bind(scraper="PBSAScraper")

    async def __aenter__(self) -> "PBSAScraper":
        kwargs: dict[str, Any] = {}
        if self.proxy_url:
            kwargs["proxy"] = self.proxy_url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout, connect=10.0),
            follow_redirects=True,
            verify=False,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            **kwargs,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    async def _get(self, url: str) -> Optional[str]:
        """Fetch a URL, return text on 2xx or None on any failure."""
        assert self.client is not None
        try:
            resp = await self.client.get(url)
            if resp.status_code >= 400:
                self.log.debug("http_non_2xx", url=url, status=resp.status_code)
                return None
            return resp.text
        except Exception as exc:
            self.log.debug("http_error", url=url, error=str(exc)[:120])
            return None

    # ------------------------------------------------------------------
    # URL discovery: sitemap + listing page fallback
    # ------------------------------------------------------------------

    async def _collect_urls_from_sitemap(
        self, sitemap_url: str, visited: set[str] | None = None
    ) -> list[str]:
        """Recursively walk a sitemap (or sitemap index) and return every <loc>."""
        visited = visited if visited is not None else set()
        if sitemap_url in visited:
            return []
        visited.add(sitemap_url)

        text = await self._get(sitemap_url)
        if not text:
            return []

        urls: list[str] = []
        # Detect sitemap index (has <sitemap><loc> entries)
        is_index = "<sitemapindex" in text.lower()

        for m in re.finditer(r"<loc>\s*([^<]+?)\s*</loc>", text, re.IGNORECASE):
            loc = m.group(1).strip()
            if is_index:
                # Recurse into sub-sitemaps
                urls.extend(await self._collect_urls_from_sitemap(loc, visited))
                await asyncio.sleep(self.rate_limit_seconds)
            else:
                urls.append(loc)
        return urls

    def _filter_detail_urls(
        self, urls: list[str], config: OperatorConfig
    ) -> list[str]:
        """Keep only URLs matching the operator's detail URL pattern."""
        out: list[str] = []
        seen: set[str] = set()
        for u in urls:
            u_clean = u.split("#")[0].split("?")[0].rstrip("/")
            # Normalise www/non-www for dedup
            dedup_key = re.sub(r"^https?://(?:www\.)?", "https://", u_clean, flags=re.I).lower()
            if dedup_key in seen:
                continue
            # Skip if excluded
            if any(p.search(u_clean) for p in config.exclude_patterns):
                continue
            # Apply detail pattern if provided
            if config.detail_url_pattern:
                # Try with and without trailing slash
                if not (
                    config.detail_url_pattern.match(u_clean)
                    or config.detail_url_pattern.match(u_clean + "/")
                ):
                    continue
            seen.add(dedup_key)
            out.append(u_clean)
        return out

    async def _collect_urls_from_listing(
        self, listing_url: str, config: OperatorConfig
    ) -> list[str]:
        """Extract anchor hrefs from a listing page, filtered to detail URLs."""
        text = await self._get(listing_url)
        if not text:
            return []

        urls: list[str] = []
        for m in re.finditer(r'href=["\']([^"\']+)["\']', text, re.IGNORECASE):
            href = m.group(1).strip()
            if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            full = urljoin(listing_url, href)
            urls.append(full)
        return urls

    async def discover_scheme_urls(self, config: OperatorConfig) -> list[str]:
        """Find all scheme detail URLs for a given operator."""
        all_urls: list[str] = []

        # 1. Try sitemaps
        for sm in config.sitemap_urls:
            sitemap_urls = await self._collect_urls_from_sitemap(sm)
            all_urls.extend(sitemap_urls)
            await asyncio.sleep(self.rate_limit_seconds)

        # 2. Fallback or supplement: scrape listing pages
        for listing in config.listing_urls:
            listing_urls = await self._collect_urls_from_listing(listing, config)
            all_urls.extend(listing_urls)
            await asyncio.sleep(self.rate_limit_seconds)

        detail_urls = self._filter_detail_urls(all_urls, config)

        # 3. Always include hardcoded extras (they bypass filtering)
        for extra in config.extra_detail_urls:
            cleaned = extra.split("#")[0].split("?")[0].rstrip("/")
            if cleaned not in detail_urls:
                detail_urls.append(cleaned)

        self.log.info(
            "discover_complete",
            operator=config.operator_name,
            raw=len(all_urls),
            detail=len(detail_urls),
        )
        return detail_urls

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_title(html: str, segment: str = "first") -> Optional[str]:
        m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if not m:
            return None
        title = m.group(1).strip()
        parts = [p.strip() for p in re.split(r"\s*[\|\-–—]\s*", title) if p.strip()]
        if not parts:
            return None
        if segment == "last":
            return parts[-1]
        return parts[0]

    @staticmethod
    def _extract_h1(html: str) -> Optional[str]:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        # Replace tags with space (not empty) to avoid concatenating text
        text = re.sub(r"<[^>]+>", " ", m.group(1))
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

    @staticmethod
    def _extract_meta_desc(html: str) -> Optional[str]:
        m = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_og_desc(html: str) -> Optional[str]:
        m = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        return m.group(1).strip() if m else None

    @staticmethod
    def _visible_text(html: str) -> str:
        """Stripped-down text for regex scanning (not perfect but fine for our uses)."""
        text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    async def parse_scheme(
        self, detail_url: str, config: OperatorConfig
    ) -> Optional[PBSAScheme]:
        """Fetch a scheme detail page and extract structured data."""
        html = await self._get(detail_url)
        if not html:
            return None

        scheme_name: Optional[str] = None
        address: Optional[str] = None
        postcode: Optional[str] = None
        num_units: Optional[int] = None

        # 1. Prefer JSON-LD (most structured)
        for blob in parse_json_ld(html):
            t = blob.get("@type")
            types = t if isinstance(t, list) else [t]
            if any(
                tt in ("LodgingBusiness", "Residence", "ApartmentComplex", "Hotel",
                       "Place", "LocalBusiness", "Product", "Accommodation")
                for tt in types if isinstance(tt, str)
            ):
                scheme_name = scheme_name or blob.get("name")
                addr = json_ld_address(blob)
                if addr and not address:
                    address = addr

        # 2. Fallbacks: H1, title, meta description
        # For operators like Prime whose <title> format is "Brand | City | Scheme",
        # use title_segment="last" to pick the scheme name.
        scheme_name = (
            scheme_name
            or self._extract_h1(html)
            or self._extract_title(html, segment=config.title_segment)
        )

        visible = self._visible_text(html)
        meta = " ".join(
            filter(None, [self._extract_meta_desc(html), self._extract_og_desc(html)])
        )
        search_text = " ".join(filter(None, [address, visible[:3000], meta]))

        postcode = extract_postcode(address) or extract_postcode(search_text)
        num_units = extract_bed_count(visible) or extract_bed_count(meta)

        if not scheme_name:
            return None

        # Pull city hint from URL (e.g. /cities/birmingham/kensington-house)
        city = None
        path_parts = [p for p in urlparse(detail_url).path.split("/") if p]
        if len(path_parts) >= 2:
            city = path_parts[-2].replace("-", " ").title()

        return PBSAScheme(
            operator_name=config.operator_name,
            scheme_name=scheme_name.strip()[:500],
            detail_url=detail_url,
            address=address.strip()[:1000] if address else None,
            postcode=postcode,
            city=city,
            num_units=num_units,
        )

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    async def scrape_operator(
        self, operator_key: str, max_schemes: int | None = None
    ) -> list[PBSAScheme]:
        config = OPERATOR_CONFIGS[operator_key]
        self.log.info("operator_start", operator=config.operator_name)

        try:
            detail_urls = await self.discover_scheme_urls(config)
        except Exception:
            self.log.exception("operator_discover_failed", operator=config.operator_name)
            return []

        if max_schemes:
            detail_urls = detail_urls[:max_schemes]

        schemes: list[PBSAScheme] = []
        for i, url in enumerate(detail_urls):
            try:
                scheme = await self.parse_scheme(url, config)
                if scheme:
                    schemes.append(scheme)
            except Exception:
                self.log.warning("parse_scheme_failed", url=url)
            if i < len(detail_urls) - 1:
                await asyncio.sleep(self.rate_limit_seconds)

        self.log.info(
            "operator_complete",
            operator=config.operator_name,
            detail_urls=len(detail_urls),
            parsed=len(schemes),
        )
        return schemes

    async def scrape_all(
        self,
        operator_keys: list[str] | None = None,
        max_schemes_per_operator: int | None = None,
    ) -> dict[str, list[PBSAScheme]]:
        keys = operator_keys or list(OPERATOR_CONFIGS.keys())
        results: dict[str, list[PBSAScheme]] = {}
        for key in keys:
            try:
                results[key] = await self.scrape_operator(key, max_schemes_per_operator)
            except Exception:
                self.log.exception("scrape_operator_crashed", operator=key)
                results[key] = []
        return results


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _normalise_company_name(name: str) -> str:
    n = name.lower().strip()
    for suffix in (" ltd", " limited", " plc", " llp", " inc", " uk"):
        n = n.replace(suffix, "")
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return re.sub(r"\s+", " ", n).strip()


def save_pbsa_schemes(
    scraped: dict[str, list[PBSAScheme]],
    db,  # SQLAlchemy Session
) -> dict[str, int]:
    """
    Persist scraped PBSA schemes to existing_schemes.

    - Creates/links a Company for each operator
    - Upserts schemes by (source="pbsa_operator", source_reference=detail_url)
    - Sets scheme_type="PBSA", status="operational"
    - Leaves owner_company_id null (to be filled by CCOD re-enrichment)

    Returns counters: found/new/updated/errors.
    """
    from app.models.models import Company, ExistingScheme

    found = 0
    new = 0
    updated = 0
    errors = 0

    # Cache existing companies by normalised name
    company_cache: dict[str, int] = {}
    for cid, cname in db.query(Company.id, Company.normalized_name).all():
        if cname:
            company_cache[cname] = cid

    def _get_or_create_operator(name: str) -> Optional[int]:
        if not name:
            return None
        norm = _normalise_company_name(name)
        if norm in company_cache:
            cid = company_cache[norm]
            co = db.query(Company).get(cid)
            if co and co.company_type != "Operator":
                co.company_type = "Operator"
            return cid

        co = Company(
            name=name.strip()[:255],
            normalized_name=norm[:255],
            company_type="Operator",
            is_active=True,
        )
        db.add(co)
        db.flush()
        company_cache[norm] = co.id
        return co.id

    for operator_key, schemes in scraped.items():
        for s in schemes:
            found += 1
            try:
                operator_id = _get_or_create_operator(s.operator_name)

                existing = (
                    db.query(ExistingScheme)
                    .filter(
                        ExistingScheme.source == "pbsa_operator",
                        ExistingScheme.source_reference == s.detail_url,
                    )
                    .first()
                )

                from app.scrapers.field_protection import set_field, FieldValidationError

                if existing:
                    changed = False
                    field_map = {
                        "name": s.scheme_name,
                        "address": s.address,
                        "postcode": s.postcode,
                        "num_units": s.num_units,
                        "operator_company_id": operator_id,
                        "scheme_type": "PBSA",
                    }
                    for field_name, value in field_map.items():
                        if value is None:
                            continue
                        try:
                            applied = set_field(
                                existing, field_name, value,
                                source="operator_scraper", db=db,
                                changed_by="system:pbsa_scraper",
                            )
                        except FieldValidationError:
                            continue
                        if applied:
                            changed = True
                    if changed:
                        existing.last_verified_at = datetime.utcnow()
                        updated += 1
                else:
                    # New scheme — no existing locks to worry about. Build normally
                    # and record locks directly since we know the source.
                    initial_locks = {
                        "num_units": "operator_scraper" if s.num_units else None,
                        "operator_company_id": "operator_scraper" if operator_id else None,
                    }
                    initial_locks = {k: v for k, v in initial_locks.items() if v}
                    scheme = ExistingScheme(
                        name=s.scheme_name,
                        address=s.address,
                        postcode=s.postcode,
                        operator_company_id=operator_id,
                        scheme_type="PBSA",
                        status="operational",
                        num_units=s.num_units,
                        source="pbsa_operator",
                        source_reference=s.detail_url,
                        data_confidence_score=0.75,
                        last_verified_at=datetime.utcnow(),
                        locked_fields=initial_locks,
                    )
                    db.add(scheme)
                    new += 1

                db.commit()
            except Exception:
                logger.exception("save_pbsa_scheme_failed", url=s.detail_url)
                errors += 1
                db.rollback()

    logger.info(
        "save_pbsa_schemes_complete",
        found=found, new=new, updated=updated, errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}


# ---------------------------------------------------------------------------
# Convenience runner
# ---------------------------------------------------------------------------

async def run_pbsa_scrape(
    operator_keys: list[str] | None = None,
    max_schemes_per_operator: int | None = None,
    rate_limit_seconds: float = 1.5,
) -> dict[str, list[dict[str, Any]]]:
    """Async entry point: scrape and return dicts (does NOT persist)."""
    async with PBSAScraper(rate_limit_seconds=rate_limit_seconds) as scraper:
        result = await scraper.scrape_all(operator_keys, max_schemes_per_operator)
    return {k: [s.to_dict() for s in v] for k, v in result.items()}
