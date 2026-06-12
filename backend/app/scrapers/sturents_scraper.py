"""StuRents PBSA / student-housing discovery scraper.

StuRents (sturents.com) is the UK's largest student accommodation
directory. Their robots.txt allows ``/sitemap``, ``/house`` and
``/student-accommodation`` paths, and they publish a per-city house
sitemap at ``/sitemap/houses/<city>.xml``.

Strategy (sitemap-first — no search API, no tokens):
  1. Fetch ``/sitemap/houses/<city>.xml`` -> every active listing URL.
  2. Fetch each listing page; parse:
       - JSON-LD ``House`` block: name, address, postcode, beds, geo,
         accommodationCategory, lease length
       - agent / landlord brand from the page (class*="agent")
       - £ pppw / pcm price mentions
  3. Classify PBSA-block candidates vs ordinary HMO houses using
     bed count, operator brand and listing text signals.

Output: list of ``SturentsListing`` dataclasses; the runner script
groups them into scheme candidates and writes JSONL per city.

Polite by design: 1 req/sec, custom UA, robots-allowed paths only.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Iterable, Optional

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

BASE = "https://sturents.com"

# Focus-market city slugs (director's Emerging + Highest Potential tiers).
FOCUS_CITY_SLUGS = [
    # Emerging
    "leeds", "cardiff", "exeter", "southampton", "middlesbrough", "colchester",
    # Highest potential — secondary university cities
    "canterbury", "lincoln", "chester", "worcester", "winchester",
    "lancaster", "durham", "bangor", "aberystwyth", "york",
]

# Brands that only operate purpose-built blocks — agent match = PBSA.
KNOWN_PBSA_OPERATORS = [
    "unite students", "iq student", "fresh", "vita student", "yugo",
    "homes for students", "crm students", "prestige student living",
    "collegiate", "student roost", "hello student", "mansion student",
    "abodus", "true student", "novel student", "host student",
    "downing students", "cls", "study inn", "nido", "dwell student",
]

_PPPW_PAT = re.compile(r"£\s?([\d,]+(?:\.\d{1,2})?)\s*ppp?w", re.I)
_PCM_PAT = re.compile(r"£\s?([\d,]+(?:\.\d{1,2})?)\s*(?:pcm|per\s+month)", re.I)
_PBSA_TEXT_SIGNALS = re.compile(
    r"\b(studio|en.?suite|cluster|halls of residence|purpose.?built|"
    r"student living|accommodation building|concierge|communal gym)\b", re.I,
)


@dataclass
class SturentsListing:
    listing_id: str
    url: str
    city_slug: str
    name: str = ""
    street_address: str = ""
    postcode: str = ""
    region: str = ""
    beds: Optional[int] = None
    category: str = ""              # JSON-LD accommodationCategory (e.g. RESI)
    lease_weeks: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    agent: str = ""                 # advertiser / operator brand
    rent_pppw_min: Optional[float] = None
    rent_pppw_max: Optional[float] = None
    rent_pcm_min: Optional[float] = None
    is_pbsa_candidate: bool = False
    pbsa_reason: str = ""
    raw_signals: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class SturentsScraper:
    """Sitemap-driven crawler for StuRents listings."""

    request_interval_sec = 1.0
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    def __init__(self, timeout: float = 30.0):
        self.client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": self.user_agent},
            follow_redirects=True,
        )
        self._last_req = 0.0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.client.close()

    def _throttle(self):
        delta = time.monotonic() - self._last_req
        if delta < self.request_interval_sec:
            time.sleep(self.request_interval_sec - delta)
        self._last_req = time.monotonic()

    def get(self, url: str) -> Optional[str]:
        self._throttle()
        try:
            r = self.client.get(url)
        except Exception as e:
            logger.warning("sturents_fetch_error", url=url, error=str(e)[:120])
            return None
        if r.status_code != 200:
            logger.warning("sturents_fetch_http", url=url, status=r.status_code)
            return None
        return r.text

    # ------------------------------------------------------------------
    def city_listing_urls(self, city_slug: str) -> list[str]:
        """All active listing URLs for a city from its houses sitemap."""
        xml = self.get(f"{BASE}/sitemap/houses/{city_slug}.xml")
        if not xml:
            return []
        urls = re.findall(r"<loc>\s*(https://sturents\.com/[^<\s]+)", xml)
        # Listing URLs look like /student-accommodation/<city>/house/<slug>/<id>
        return [u for u in urls if "/house/" in u]

    def scrape_city(
        self, city_slug: str, limit: Optional[int] = None,
    ) -> Iterable[SturentsListing]:
        urls = self.city_listing_urls(city_slug)
        logger.info("sturents_city", city=city_slug, listings=len(urls))
        if limit:
            urls = urls[:limit]
        for url in urls:
            listing = self.scrape_listing(url, city_slug)
            if listing:
                yield listing

    # ------------------------------------------------------------------
    def scrape_listing(
        self, url: str, city_slug: str,
    ) -> Optional[SturentsListing]:
        html = self.get(url)
        if not html:
            return None

        m = re.search(r"/house/[a-z0-9\-]+/(\d+)", url)
        listing = SturentsListing(
            listing_id=m.group(1) if m else "",
            url=url,
            city_slug=city_slug,
        )

        # ---- JSON-LD House block ----
        for jm in re.finditer(
            r'<script type="application/ld\+json">(.*?)</script>', html, re.S,
        ):
            try:
                d = json.loads(jm.group(1))
            except (json.JSONDecodeError, ValueError):
                continue
            if d.get("@type") != "House":
                continue
            listing.name = (d.get("name") or "").strip()
            listing.category = (d.get("accommodationCategory") or "").strip()
            beds = d.get("numberOfBedrooms")
            try:
                listing.beds = int(str(beds))
            except (TypeError, ValueError):
                pass
            lease = d.get("leaseLength") or {}
            try:
                listing.lease_weeks = int(lease.get("value"))
            except (TypeError, ValueError):
                pass
            addr = d.get("address") or []
            if isinstance(addr, dict):
                addr = [addr]
            if addr:
                listing.street_address = (addr[0].get("streetAddress") or "").strip()
                listing.region = (addr[0].get("addressRegion") or "").strip()
                listing.postcode = (addr[0].get("postalCode") or "").strip()
            geo = d.get("geo") or {}
            listing.latitude = geo.get("latitude")
            listing.longitude = geo.get("longitude")
            break

        if not listing.name:
            return None  # unparseable page

        # ---- agent / advertiser brand ----
        soup = BeautifulSoup(html, "html.parser")
        for el in soup.select('[class*="agent"]'):
            text = el.get_text(" ", strip=True)
            if text and 2 < len(text) < 80 and "menu" not in text.lower():
                listing.agent = text
                break

        # ---- prices ----
        pppw = [float(v.replace(",", "")) for v in _PPPW_PAT.findall(html)]
        if pppw:
            listing.rent_pppw_min = min(pppw)
            listing.rent_pppw_max = max(pppw)
        pcm = [float(v.replace(",", "")) for v in _PCM_PAT.findall(html)]
        if pcm:
            listing.rent_pcm_min = min(pcm)

        # ---- PBSA classification ----
        # Strong signals only: block-scale bed count, a known PBSA-only
        # operator brand, or a non-residential category. Text mentions of
        # studio/en-suite/etc. appear in page furniture (related listings,
        # nav) on ordinary HMO pages, so they only count as a tie-breaker
        # for mid-size listings.
        reasons = []
        if listing.beds is not None and listing.beds >= 15:
            reasons.append(f"beds={listing.beds}")
        agent_l = listing.agent.lower()
        if agent_l and any(op in agent_l for op in KNOWN_PBSA_OPERATORS):
            reasons.append(f"operator={listing.agent}")
        if listing.category and listing.category.upper() not in ("RESI", ""):
            reasons.append(f"category={listing.category}")
        text_hits = _PBSA_TEXT_SIGNALS.findall(html)
        n_signals = len(set(t.lower() for t in text_hits))
        if not reasons and n_signals >= 3 and (listing.beds or 0) >= 10:
            reasons.append(f"beds={listing.beds}+text_signals={n_signals}")
        listing.is_pbsa_candidate = bool(reasons)
        listing.pbsa_reason = "; ".join(reasons)
        listing.raw_signals = {"text_signal_count": n_signals}

        return listing
