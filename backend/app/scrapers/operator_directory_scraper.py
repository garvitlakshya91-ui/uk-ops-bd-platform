"""PBSA operator-website directory scraper.

Dev step 3: scrape each PBSA operator brand's own website to get their
full UK property directory (name, address, postcode, city, rent range).
This validates / extends operator-scheme links and adds rent benchmarks.

Design: one generic engine + a per-brand config dict (``BRAND_CONFIGS``).
Each config describes:

  * discovery  — sitemap URL(s) (gz-aware, index-aware) or index pages +
                 an include/exclude URL regex, optional "derive parent"
                 (room URL -> property URL) and "require child" (property
                 URL must have room sub-pages in the sitemap) rules
  * parsing    — "jsonld" (schema.org LodgingBusiness / LocalBusiness /
                 Accommodation blocks) or "text" (h1/title + postcode-
                 context address line), plus brand-specific extractors
                 (e.g. Abodus PHP-serialised location pins, Host footer
                 address blocks)

Politeness (mandatory):
  * robots.txt checked per brand before anything else; disallowed paths
    are never fetched. If robots blocks everything -> "blocked_robots".
  * >= 1 second between requests to the same host.
  * No Cloudflare / bot-protection bypass: a 403 wall -> "blocked".
  * Browser-rendered-only sites -> "needs_browser".
  * Hard cap of ~400 pages per brand.

Probe findings (2026-06-11):
  * Fresh rebranded to thisisfresh.com; robots ok but all pages 403
    (Cloudflare) -> blocked.
  * homesforstudents.co.uk is an HMO lettings marketplace, NOT the PBSA
    operator site; the operator runs wearehomesforstudents.com which is
    403-walled -> blocked.
  * Yugo + CRM Students share one CMS: gzipped service sitemaps with
    /<city>/<property> pages and /<city>/<property>/<room>-<id> rooms.
  * Vita / Collegiate / Prestige / iQ expose schema.org JSON-LD with
    full postal addresses.
  * Study Inn property pages carry no postcode (name/city/rent only).
"""
from __future__ import annotations

import gzip
import html as html_mod
import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Iterable, Optional
from urllib import robotparser
from urllib.parse import urlparse

import httpx
import structlog

logger = structlog.get_logger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# UK postcode with the space (avoids hex-colour false positives like C6C9ED)
POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}[0-9][0-9A-Z]?\s+[0-9][A-Z]{2})\b")

# £ amounts with an explicit per-week context
PPW_RE = re.compile(
    r"£\s?(\d{2,4}(?:\.\d{1,2})?)\s*(?:(?:/|per\s*)\s*week|p\.?p\.?p\.?w\.?|ppw\b|pw\b|p/w)",
    re.I,
)
BETWEEN_RE = re.compile(
    r"between\s+£\s?(\d{2,4}(?:\.\d{1,2})?)\s+and\s+£\s?(\d{2,4}(?:\.\d{1,2})?)", re.I
)
FROM_RE = re.compile(r"from\s*£\s?(\d{2,4}(?:\.\d{1,2})?)", re.I)
BARE_PRICE_RE = re.compile(r"£\s?(\d{2,3}(?:\.\d{2})?)\b")

LOC_RE = re.compile(r"<loc>\s*([^<\s]+?)\s*</loc>")
JSONLD_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)

LODGING_TYPES = {
    "LodgingBusiness", "LocalBusiness", "Accommodation", "ApartmentComplex",
    "Residence", "Hostel", "Apartment", "House", "Place",
}


@dataclass
class OperatorProperty:
    """One property in an operator's own directory."""

    operator: str
    name: str = ""
    address: str = ""
    postcode: str = ""
    city: str = ""
    url: str = ""
    rent_ppw_min: Optional[float] = None
    rent_ppw_max: Optional[float] = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BrandResult:
    brand: str
    operator: str
    status: str                      # ok | blocked | blocked_robots | needs_browser | error
    properties: list[OperatorProperty] = field(default_factory=list)
    notes: str = ""
    pages_fetched: int = 0

    @property
    def postcode_fill(self) -> float:
        if not self.properties:
            return 0.0
        return 100.0 * sum(1 for p in self.properties if p.postcode) / len(self.properties)

    @property
    def rent_fill(self) -> float:
        if not self.properties:
            return 0.0
        return 100.0 * sum(1 for p in self.properties if p.rent_ppw_min) / len(self.properties)


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------

def _clean_text(s: str) -> str:
    s = html_mod.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_tags(s: str) -> str:
    s = s.replace("\\u003c", "<").replace("\\u003e", ">").replace("\\/", "/")
    s = re.sub(r"<[^<>]*>", "|", s)
    return s


_NAME_SUFFIX_RE = re.compile(
    r"\s*[|\-–—]\s*(yugo|hello student.*|host.*|study inn.*|abodus.*|iq student.*|"
    r"student roost.*|mezzino.*|downing students.*|collegiate.*|"
    r"prestige student living.*|vita student.*|crm students.*|book direct.*|"
    r"student accommodation.*)\s*$",
    re.I,
)


def _clean_name(name: str) -> str:
    name = _clean_text(name)
    prev = None
    while prev != name:
        prev = name
        name = _NAME_SUFFIX_RE.sub("", name).strip()
    return name.strip(" ,|-–")


def _strip_city_and_operator(name: str, city: str, operator: str) -> str:
    """'Vita Student First Street' -> 'First Street';
    '33 Parkside, Coventry' -> '33 Parkside'; 'The Octagon Liverpool' -> 'The Octagon'."""
    if name.lower().startswith(operator.lower() + " "):
        name = name[len(operator) + 1:]
    if city:
        low = name.lower()
        if low.endswith(", " + city.lower()):
            name = name[: -(len(city) + 2)]
        elif low.endswith(" " + city.lower()) and len(name) > len(city) + 3:
            name = name[: -(len(city) + 1)]
    return name.strip(" ,|-–")


def _title_name(page_html: str) -> str:
    m = TITLE_RE.search(page_html)
    return _clean_name(m.group(1)) if m else ""


def _h1_name(page_html: str) -> str:
    m = H1_RE.search(page_html)
    if not m:
        return ""
    return _clean_name(re.sub(r"<[^>]+>", " ", m.group(1)))


def _walk_jsonld(node: Any) -> Iterable[dict]:
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk_jsonld(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk_jsonld(v)


def extract_jsonld_lodging(page_html: str) -> Optional[dict]:
    """First schema.org node that looks like a lodging/place entity."""
    for m in JSONLD_RE.finditer(page_html):
        blob = m.group(1).strip()
        try:
            data = json.loads(blob)
        except (json.JSONDecodeError, ValueError):
            continue
        for node in _walk_jsonld(data):
            t = node.get("@type")
            types = {t} if isinstance(t, str) else set(t or [])
            if types & LODGING_TYPES and (node.get("name") or node.get("address")):
                return node
    return None


def extract_rent_range(
    page_html: str, *, heuristic: bool = False,
) -> tuple[Optional[float], Optional[float], str]:
    """(min, max, method) weekly rent from page text."""
    vals: list[float] = []
    for v in PPW_RE.findall(page_html):
        vals.append(float(v))
    for a, b in BETWEEN_RE.findall(page_html):
        vals += [float(a), float(b)]
    method = "ppw_context"
    if not vals:
        vals = [float(v) for v in FROM_RE.findall(page_html)]
        method = "from_price"
    if not vals and heuristic:
        # Repeated bare £ values in a plausible PBSA weekly band.
        counts: dict[float, int] = {}
        for v in BARE_PRICE_RE.findall(page_html):
            f = float(v)
            if 100 <= f <= 500:
                counts[f] = counts.get(f, 0) + 1
        vals = [v for v, c in counts.items() if c >= 2]
        method = "heuristic_repeated"
    vals = [v for v in vals if 60 <= v <= 1200]
    if not vals:
        return None, None, ""
    return min(vals), max(vals), method


def address_near_postcode(
    page_html: str, exclude_postcodes: Iterable[str] = (),
) -> tuple[str, str]:
    """Find the first 'street..., city..., POSTCODE' style line in the page.

    Returns (address_without_postcode, postcode) or ("", "").
    """
    excl = {pc.upper().replace("  ", " ") for pc in exclude_postcodes}
    for m in POSTCODE_RE.finditer(page_html):
        pc = re.sub(r"\s+", " ", m.group(1)).upper()
        if pc in excl:
            continue
        ctx = page_html[max(0, m.start() - 220):m.start()]
        ctx = _strip_tags(ctx)
        ctx = html_mod.unescape(ctx)
        # last text chunk before the postcode
        chunk = ctx.split("|")[-1]
        chunk = re.sub(r"\s+", " ", chunk).strip(" ,;:-")
        # drop obvious serialised/JSON junk
        chunk = re.sub(r'^[^A-Za-z0-9]*', "", chunk)
        if len(chunk) < 5 or "{" in chunk or '":' in chunk:
            chunk = ""
        return chunk, pc
    return "", ""


def city_from_slug(url: str, seg: int) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    try:
        slug = parts[seg]
    except IndexError:
        return ""
    return slug.replace("-", " ").title()


# ---------------------------------------------------------------------------
# Brand-specific parser callables
# ---------------------------------------------------------------------------

def parse_jsonld_page(page_html: str, url: str, cfg: dict) -> Optional[OperatorProperty]:
    node = extract_jsonld_lodging(page_html)
    name = address = postcode = city = ""
    raw: dict[str, Any] = {}
    if node:
        name = _clean_name(str(node.get("name") or ""))
        addr = node.get("address")
        if isinstance(addr, dict):
            street = _clean_text(str(addr.get("streetAddress") or ""))
            locality = _clean_text(str(addr.get("addressLocality") or ""))
            postcode = _clean_text(str(addr.get("postalCode") or "")).upper()
            city = locality
            address = ", ".join(x for x in (street, locality) if x)
        elif isinstance(addr, str):
            address = _clean_text(addr)
            pm = POSTCODE_RE.search(address.upper())
            if pm:
                postcode = re.sub(r"\s+", " ", pm.group(1))
                address = address[: pm.start()].rstrip(" ,") if pm.start() else address
        geo = node.get("geo") or {}
        if isinstance(geo, dict) and geo.get("latitude"):
            raw["lat"] = geo.get("latitude")
            raw["lng"] = geo.get("longitude")
        raw["jsonld_type"] = node.get("@type")
        if node.get("priceRange"):
            raw["price_range"] = str(node["priceRange"])
    if not name:
        name = _h1_name(page_html) or _title_name(page_html)
    if not postcode:
        a2, p2 = address_near_postcode(page_html, cfg.get("exclude_postcodes", ()))
        if p2:
            postcode = p2
            address = address or a2
    if cfg.get("city_seg") is not None:
        # the operator's own URL taxonomy is more reliable than locality
        slug_city = city_from_slug(url, cfg["city_seg"])
        if slug_city:
            if city and city.lower() != slug_city.lower():
                raw["jsonld_locality"] = city
            city = slug_city
    name = _strip_city_and_operator(name, city, cfg["operator"])
    if not name:
        return None
    rmin, rmax, method = extract_rent_range(
        page_html, heuristic=cfg.get("rent_heuristic", False)
    )
    if rmin is None and raw.get("price_range"):
        nums = [float(v) for v in re.findall(r"(\d{2,4}(?:\.\d{1,2})?)", raw["price_range"])]
        nums = [v for v in nums if 60 <= v <= 1200]
        if nums:
            rmin, rmax, method = min(nums), max(nums), "jsonld_price_range"
    if method:
        raw["rent_method"] = method
    return OperatorProperty(
        operator=cfg["operator"], name=name, address=address, postcode=postcode,
        city=city, url=url, rent_ppw_min=rmin, rent_ppw_max=rmax, raw=raw,
    )


def parse_text_page(page_html: str, url: str, cfg: dict) -> Optional[OperatorProperty]:
    """h1/title name + postcode-context address line."""
    if cfg.get("name_from_slug"):
        slug = urlparse(url).path.strip("/").split("/")[-1]
        name = slug.replace("-", " ").title()
    else:
        name = _h1_name(page_html) or _title_name(page_html)
    if not name:
        return None
    address, postcode = address_near_postcode(
        page_html, cfg.get("exclude_postcodes", ())
    )
    city = ""
    if cfg.get("city_seg") is not None:
        city = city_from_slug(url, cfg["city_seg"])
    if not city and address:
        parts = [p.strip() for p in re.split(r"[.,]", address) if p.strip()]
        if len(parts) >= 2:
            city = parts[-1]
    name = _strip_city_and_operator(name, city, cfg["operator"])
    rmin, rmax, method = extract_rent_range(
        page_html, heuristic=cfg.get("rent_heuristic", False)
    )
    raw: dict[str, Any] = {}
    if method:
        raw["rent_method"] = method
    return OperatorProperty(
        operator=cfg["operator"], name=name, address=address, postcode=postcode,
        city=city, url=url, rent_ppw_min=rmin, rent_ppw_max=rmax, raw=raw,
    )


def parse_host_page(page_html: str, url: str, cfg: dict) -> Optional[OperatorProperty]:
    """Host: name from the URL slug (titles are unreliable), address from the
    per-property footer-address block."""
    slug = urlparse(url).path.strip("/").split("/")[-1]
    name = slug.replace("-", " ").title()
    address = postcode = city = ""
    m = re.search(
        r'class="footer-address".*?</h2>(.*?)</div>', page_html, re.S
    )
    if m:
        items = [
            _clean_text(re.sub(r"<[^>]+>", "", p))
            for p in re.findall(r"<p[^>]*>(.*?)</p>", m.group(1), re.S)
        ]
        items = [i for i in items if i and i.lower() != "united kingdom"]
        pc_idx = None
        for i, it in enumerate(items):
            pm = POSTCODE_RE.search(it.upper())
            if pm:
                postcode = re.sub(r"\s+", " ", pm.group(1))
                pc_idx = i
                break
        if pc_idx is not None:
            address = ", ".join(items[:pc_idx])
            if pc_idx >= 1:
                city = items[pc_idx - 1]
    if not postcode:
        address, postcode = address_near_postcode(page_html)
    if not city and address:
        parts = [p.strip() for p in re.split(r"[.,]", address) if p.strip()]
        if len(parts) >= 2:
            city = parts[-1]
    rmin, rmax, method = extract_rent_range(page_html)
    raw: dict[str, Any] = {"rent_method": method} if method else {}
    if not name:
        return None
    return OperatorProperty(
        operator=cfg["operator"], name=name, address=address, postcode=postcode,
        city=city, url=url, rent_ppw_min=rmin, rent_ppw_max=rmax, raw=raw,
    )


def parse_abodus_page(page_html: str, url: str, cfg: dict) -> Optional[OperatorProperty]:
    """Abodus: PHP-serialised location_pin blob carries the full address."""
    name = _h1_name(page_html) or _title_name(page_html)
    if not name:
        return None

    def _ser(key: str) -> str:
        m = re.search(
            r'\\?"' + key + r'\\?";s:\d+:\\?"(.*?)\\?";', page_html
        )
        return _clean_text(m.group(1)) if m else ""

    street_no = _ser("street_number")
    street = _ser("street_name")
    city = _ser("city")
    postcode = _ser("post_code").upper()
    full = _ser("address")
    address = ", ".join(x for x in (f"{street_no} {street}".strip(), city) if x) or full
    if not postcode:
        address, postcode = address_near_postcode(page_html)
    raw: dict[str, Any] = {}
    if full:
        raw["pin_address"] = full
    rmin, rmax, method = (None, None, "")
    m = re.search(r"Prices from\s*<b>£\s?([\d,.]+)\s*P/W", page_html, re.I)
    if m:
        rmin = float(m.group(1).replace(",", ""))
        method = "prices_from_banner"
    else:
        rmin, rmax, method = extract_rent_range(page_html)
    if method:
        raw["rent_method"] = method
    return OperatorProperty(
        operator=cfg["operator"], name=name, address=address, postcode=postcode,
        city=city, url=url, rent_ppw_min=rmin, rent_ppw_max=rmax, raw=raw,
    )


# ---------------------------------------------------------------------------
# Brand configs
# ---------------------------------------------------------------------------
# Discovery keys:
#   sitemaps      list of sitemap URLs (indexes + .gz handled automatically)
#   index_pages   list of HTML pages to harvest property links from
#   link_re       regex for index_pages link harvesting
#   include       regex a property URL must match
#   exclude       regex that rejects a URL
#   derive_parent regex with 1 group = property URL derived from deeper URLs
#   require_child property URL kept only if the sitemap holds sub-pages of it
#   max_pages     per-brand fetch cap (default 400)
# Parse keys:
#   parse         callable(html, url, cfg) -> OperatorProperty | None
#   city_seg      path segment index for the city slug
#   exclude_postcodes / rent_heuristic / timeout

BRAND_CONFIGS: dict[str, dict[str, Any]] = {
    "fresh": {
        "operator": "Fresh",
        "robots": "https://www.thisisfresh.com/robots.txt",
        "sitemaps": ["https://www.thisisfresh.com/sitemap.xml"],
        "include": r"thisisfresh\.com/.+",
        "parse": parse_jsonld_page,
        "notes": "freshstudentliving.co.uk redirects to thisisfresh.com; "
                 "robots.txt is open but all content paths return 403 (Cloudflare).",
    },
    "vita_student": {
        "operator": "Vita Student",
        "robots": "https://www.vitastudent.com/robots.txt",
        "sitemaps": ["https://www.vitastudent.com/developments-sitemap.xml"],
        "include": r"vitastudent\.com/en/cities/[a-z0-9\-]+/[a-z0-9\-]+/$",
        "exclude": r"/cities/(barcelona|madrid|valencia|sevilla|seville|milan|florence)/",
        "parse": parse_jsonld_page,
        "city_seg": -2,
        "notes": "JSON-LD LodgingBusiness with full PostalAddress. UK + Spain "
                 "sites; Spanish cities excluded.",
    },
    "yugo": {
        "operator": "Yugo",
        "robots": "https://yugo.com/robots.txt",
        "sitemaps": ["https://yugo.com/service-sitemap-en-gb-sitemap_index.xml"],
        "include": r"^https://yugo\.com/en-gb/global/united-kingdom/[a-z0-9\-]+/[a-z0-9\-]+$",
        "require_child": True,
        "parse": parse_text_page,
        "city_seg": -2,
        "rent_heuristic": True,
        "notes": "UK properties = /en-gb/global/united-kingdom/<city>/<prop> "
                 "pages that have room sub-pages; university landing pages "
                 "have no children and are skipped.",
    },
    "homes_for_students": {
        "operator": "Homes for Students",
        "robots": "https://wearehomesforstudents.com/robots.txt",
        "sitemaps": [
            "https://wearehomesforstudents.com/sitemap_index.xml",
            "https://wearehomesforstudents.com/sitemap.xml",
        ],
        "include": r"wearehomesforstudents\.com/.+",
        "parse": parse_jsonld_page,
        "notes": "homesforstudents.co.uk is an HMO lettings marketplace, not "
                 "the PBSA operator directory. The operator site "
                 "wearehomesforstudents.com sits behind a hard Cloudflare 403.",
    },
    "crm_students": {
        "operator": "CRM Students",
        "robots": "https://www.crm-students.com/robots.txt",
        "sitemaps": ["https://www.crm-students.com/service-sitemap-crm-en-gb-sitemap_index.xml"],
        "include": r"^https://www\.crm-students\.com/[a-z0-9\-]+/[a-z0-9\-]+$",
        "exclude": r"/(resource|favorites|search)/",
        "require_child": True,
        "parse": parse_text_page,
        "city_seg": -2,
        "rent_heuristic": True,
        "notes": "Same CMS as Yugo (GSA). Properties = /<city>/<prop> pages "
                 "with room sub-pages.",
    },
    "prestige_student_living": {
        "operator": "Prestige Student Living",
        "robots": "https://www.prestigestudentliving.com/robots.txt",
        "sitemaps": ["https://www.prestigestudentliving.com/sitemap.xml"],
        "include": r"prestigestudentliving\.com/student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+$",
        "parse": parse_jsonld_page,
        "city_seg": -2,
        "notes": "JSON-LD LodgingBusiness with PostalAddress + GeoCoordinates. "
                 "Part of the Homes for Students group.",
    },
    "collegiate": {
        "operator": "Collegiate",
        "robots": "https://www.collegiate-ac.com/robots.txt",
        "sitemaps": ["https://www.collegiate-ac.com/locations-sitemap.xml"],
        "include": r"collegiate-ac\.com/uk-student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+/$",
        "parse": parse_jsonld_page,
        "city_seg": -2,
        "notes": "JSON-LD LodgingBusiness; address is a single comma string "
                 "with the postcode at the end.",
    },
    "student_roost": {
        "operator": "Student Roost",
        "robots": "https://www.studentroost.co.uk/robots.txt",
        "sitemaps": ["https://www.studentroost.co.uk/sitemaps-1-section-properties-1-sitemap.xml"],
        "include": r"studentroost\.co\.uk/locations/[a-z0-9\-]+/[a-z0-9\-]+$",
        "parse": parse_text_page,
        "city_seg": -2,
        "notes": "robots.txt 404s (allow-all). Address appears as "
                 "'street, city, POSTCODE, UK' text in the page header.",
    },
    "iq_student_accommodation": {
        "operator": "iQ Student Accommodation",
        "robots": "https://www.iqstudentaccommodation.com/robots.txt",
        "sitemaps": ["https://www.iqstudentaccommodation.com/sitemap.xml"],
        "derive_parent": r"^(https://www\.iqstudentaccommodation\.com/[a-z0-9\-]+/[a-z0-9\-]+)/welcome$",
        "include": r"^https://www\.iqstudentaccommodation\.com/[a-z0-9\-]+/[a-z0-9\-]+/$",
        "exclude": r"/(zh-hans|articles|events|blog|offers|guides|city-guides|"
                   r"summer|guarantor|about-us|contact-us|careers|faqs|sitemap|"
                   r"privacy|terms|corporate|investors|media|modern-slavery|"
                   r"cookie|legal|university|landing|search|book|payments|"
                   r"support|node|form|index\.php|student-life|wellbeing)(/|$)",
        "parse": parse_jsonld_page,
        "city_seg": -2,
        "max_pages": 400,
        "notes": "Drupal; property pages = /<city>/<property> with JSON-LD "
                 "LocalBusiness. Pages without a lodging JSON-LD node are "
                 "dropped (university/city landing pages).",
        "require_jsonld": True,
    },
    "hello_student": {
        "operator": "Hello Student",
        "robots": "https://www.hellostudent.co.uk/robots.txt",
        "sitemaps": ["https://www.hellostudent.co.uk/properties-sitemap.xml"],
        "include": r"hellostudent\.co\.uk/student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+$",
        "parse": parse_text_page,
        "city_seg": -2,
        "notes": "Next.js RSC payload; address appears as a "
                 "'<p>Name, Street, City, POSTCODE</p>' paragraph.",
    },
    "mezzino": {
        "operator": "Mezzino",
        "robots": "https://www.mezzino.com/robots.txt",
        "sitemaps": ["https://www.mezzino.com/property-sitemap.xml"],
        "include": r"mezzino\.com/property/[a-z0-9\-]+/$",
        "exclude": r"/property/$",
        "parse": parse_text_page,
        "exclude_postcodes": ["NG22 8LS"],  # head-office footer address
        "notes": "mezzino.co.uk robots points at mezzino.com sitemap. Includes "
                 "some Irish properties (no UK postcode). City derived from "
                 "the address line.",
    },
    "host": {
        "operator": "Host",
        "robots": "https://host-students.com/robots.txt",
        "sitemaps": ["https://host-students.com/building-sitemap.xml"],
        "include": r"^https://host-students\.com/property/[a-z0-9\-]+/$",
        "parse": parse_host_page,
        "notes": "Per-property footer carries the property address. Rent in "
                 "'costs between £X and £Y per week' prose.",
    },
    "study_inn": {
        "operator": "Study Inn",
        "robots": "https://studyinn.com/robots.txt",
        "sitemaps": [
            "https://studyinn.com/location-sitemap.xml",
            "https://studyinn.com/page-sitemap.xml",
        ],
        "sitemap_union": True,
        "derive_parent": r"^(https://studyinn\.com/student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+)/",
        "include": r"^https://studyinn\.com/student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+/?$",
        "exclude": r"/student-accommodation/(uk|england)/",
        "parse": parse_text_page,
        "city_seg": -2,
        "name_from_slug": True,
        "notes": "Property pages carry no postcode; name/city/rent only.",
    },
    "downing_students": {
        "operator": "Downing Students",
        "robots": "https://www.downingstudents.com/robots.txt",
        "sitemaps": ["https://www.downingstudents.com/property-sitemap.xml"],
        "include": r"downingstudents\.com/student-accommodation/[a-z0-9\-]+/[a-z0-9\-]+/$",
        "parse": parse_text_page,
        "city_seg": -2,
        "name_from_slug": True,
        "skip_address_scan": True,
        "notes": "Pages list nearby-university addresses, so generic postcode "
                 "scanning is unsafe; postcode left blank unless a dedicated "
                 "address block is found.",
    },
    "abodus": {
        "operator": "Abodus",
        "robots": "https://www.abodusstudents.com/robots.txt",
        "index_pages": [
            "https://abodusstudents.com/",
            "https://abodusstudents.com/our-locations",
        ],
        "link_re": r"https://abodusstudents\.com/accommodation/[a-z0-9\-]+",
        "include": r"abodusstudents\.com/accommodation/[a-z0-9\-]+$",
        "parse": parse_abodus_page,
        "timeout": 60.0,
        "notes": "WordPress; address in a PHP-serialised location_pin blob. "
                 "Slow origin - generous timeout.",
    },
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class OperatorDirectoryScraper:
    """Generic, polite, config-driven operator directory crawler."""

    request_interval_sec = 1.1
    max_pages_default = 400

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self.client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "en-GB,en;q=0.9"},
            follow_redirects=True,
        )
        self._last_req: dict[str, float] = {}
        self._robots: dict[str, robotparser.RobotFileParser] = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.client.close()

    # -- politeness --------------------------------------------------------

    def _throttle(self, url: str):
        host = urlparse(url).netloc
        now = time.monotonic()
        last = self._last_req.get(host, 0.0)
        wait = self.request_interval_sec - (now - last)
        if wait > 0:
            time.sleep(wait)
        self._last_req[host] = time.monotonic()

    def _robots_for(self, url: str) -> robotparser.RobotFileParser:
        host = urlparse(url).netloc
        if host in self._robots:
            return self._robots[host]
        rp = robotparser.RobotFileParser()
        robots_url = f"https://{host}/robots.txt"
        status, text = self._fetch(robots_url, check_robots=False)
        if status == 200 and text:
            rp.parse(text.splitlines())
        else:
            # 404/403/unreachable robots -> treat as allow-all (convention)
            rp.parse([])
        self._robots[host] = rp
        return rp

    def allowed(self, url: str) -> bool:
        return self._robots_for(url).can_fetch(USER_AGENT, url)

    # -- fetching -----------------------------------------------------------

    def _fetch(
        self, url: str, *, check_robots: bool = True, timeout: Optional[float] = None,
    ) -> tuple[Optional[int], Optional[str]]:
        if check_robots and not self.allowed(url):
            logger.info("robots_disallowed", url=url)
            return -1, None
        self._throttle(url)
        try:
            r = self.client.get(url, timeout=timeout or self.timeout)
        except Exception as e:
            logger.warning("fetch_error", url=url, error=str(e)[:120])
            return None, None
        content = r.content
        if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
            try:
                content = gzip.decompress(content)
            except OSError:
                pass
        return r.status_code, content.decode("utf-8", "replace")

    # -- discovery ----------------------------------------------------------

    def _sitemap_urls(self, sitemap_url: str, cfg: dict, depth: int = 0) -> tuple[list[str], Optional[int]]:
        """All page URLs from a sitemap (recursing one level into indexes)."""
        status, xml = self._fetch(sitemap_url, timeout=cfg.get("timeout"))
        if status != 200 or not xml:
            return [], status
        locs = LOC_RE.findall(xml)
        urls: list[str] = []
        children = [u for u in locs if re.search(r"\.xml(\.gz)?(\?|$)", u)]
        if children and ("<sitemapindex" in xml or len(children) == len(locs)):
            if depth >= 2:
                return [], status
            for child in children[:30]:
                child_urls, _ = self._sitemap_urls(child, cfg, depth + 1)
                urls.extend(child_urls)
        else:
            urls = locs
        return urls, status

    def discover(self, cfg: dict) -> tuple[list[str], str]:
        """Return (property_urls, status_hint)."""
        all_urls: list[str] = []
        statuses: list[Optional[int]] = []
        if cfg.get("sitemaps"):
            for sm in cfg["sitemaps"]:
                urls, status = self._sitemap_urls(sm, cfg)
                statuses.append(status)
                if urls:
                    all_urls.extend(urls)
                    if not cfg.get("sitemap_union"):
                        break  # first working sitemap wins
        elif cfg.get("index_pages"):
            for page in cfg["index_pages"]:
                status, page_html = self._fetch(page, timeout=cfg.get("timeout"))
                statuses.append(status)
                if status == 200 and page_html:
                    all_urls.extend(re.findall(cfg["link_re"], page_html))

        if not all_urls:
            if any(s == 403 for s in statuses):
                return [], "blocked"
            if any(s == -1 for s in statuses):
                return [], "blocked_robots"
            return [], "error"

        include = re.compile(cfg["include"]) if cfg.get("include") else None
        exclude = re.compile(cfg["exclude"]) if cfg.get("exclude") else None
        derive = re.compile(cfg["derive_parent"]) if cfg.get("derive_parent") else None

        url_set = list(dict.fromkeys(u.strip() for u in all_urls))
        candidates: list[str] = []
        for u in url_set:
            if derive:
                dm = derive.match(u)
                if dm:
                    candidates.append(dm.group(1) + "/")
                    continue
            candidates.append(u)
        candidates = list(dict.fromkeys(candidates))

        props = []
        for u in candidates:
            if include and not include.search(u):
                continue
            if exclude and exclude.search(u):
                continue
            props.append(u)

        if cfg.get("require_child"):
            full = set(url_set)
            props = [
                u for u in props
                if any(o.startswith(u + "/") for o in full)
            ]
        return list(dict.fromkeys(props)), "ok"

    # -- main entry ----------------------------------------------------------

    def scrape_brand(
        self, brand: str, limit: Optional[int] = None,
    ) -> BrandResult:
        cfg = BRAND_CONFIGS[brand]
        result = BrandResult(brand=brand, operator=cfg["operator"], status="ok",
                             notes=cfg.get("notes", ""))

        # robots first
        probe_url = (cfg.get("sitemaps") or cfg.get("index_pages"))[0]
        rp = self._robots_for(probe_url)
        if not rp.can_fetch(USER_AGENT, probe_url):
            result.status = "blocked_robots"
            return result

        urls, hint = self.discover(cfg)
        logger.info("brand_discovery", brand=brand, urls=len(urls), hint=hint)
        if not urls:
            result.status = hint if hint != "ok" else "error"
            return result

        cap = min(cfg.get("max_pages", self.max_pages_default), self.max_pages_default)
        if limit:
            cap = min(cap, limit)
        urls = urls[:cap]

        parse: Callable = cfg["parse"]
        n403 = 0
        seen_names: set[tuple[str, str]] = set()
        for url in urls:
            status, page_html = self._fetch(url, timeout=cfg.get("timeout"))
            if status == -1:
                continue
            result.pages_fetched += 1
            if status == 403:
                n403 += 1
                if n403 >= 5 and not result.properties:
                    result.status = "blocked"
                    return result
                continue
            if status != 200 or not page_html:
                continue
            if cfg.get("require_jsonld") and not extract_jsonld_lodging(page_html):
                continue
            if cfg.get("skip_address_scan"):
                # neutralise generic postcode scanning for unsafe pages
                page_cfg = {**cfg, "exclude_postcodes": ()}
                prop = parse(_suppress_postcodes(page_html), url, page_cfg)
            else:
                prop = parse(page_html, url, cfg)
            if not prop:
                continue
            key = (prop.name.lower(), prop.city.lower())
            if key in seen_names:
                continue
            seen_names.add(key)
            result.properties.append(prop)

        if not result.properties and result.pages_fetched:
            # pages fetched fine but nothing parseable -> JS-only site
            result.status = "needs_browser"
        return result


def _suppress_postcodes(page_html: str) -> str:
    """Used for brands where in-page postcodes belong to nearby POIs."""
    return POSTCODE_RE.sub("", page_html)
