"""AccommodationForStudents (AFS) PBSA discovery scraper.

AFS city landing pages (``/<city>/student-halls``) are Next.js
server-rendered and embed the full search result set as JSON in
``__NEXT_DATA__`` — including the field we care most about:

    property.accommodationProvider.name   -> the OPERATOR

plus explicit ``propertyAdvertType: "Pbsa"``, postcode, rent ppw,
room-option counts, coordinates and tenancy contract dates.

robots.txt allows city/landing pages (only ``/search-results*`` and
``/book-room*`` are disallowed). One fetch per city page — extremely
light touch.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Iterable, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

BASE = "https://www.accommodationforstudents.com"

FOCUS_CITY_SLUGS = [
    "leeds", "cardiff", "exeter", "southampton", "middlesbrough", "colchester",
    "canterbury", "lincoln", "chester", "worcester", "winchester",
    "lancaster", "durham", "bangor", "aberystwyth", "york",
]

# Landing pages that surface purpose-built stock. ``student-halls`` is
# the canonical PBSA category; ``studios`` catches studio-led blocks
# that sit closer to co-living. ``student-flats`` is the fallback for
# small markets (e.g. Middlesbrough) where AFS files block stock under
# flats — the Pbsa advert_type field still identifies the real blocks.
CATEGORY_PATHS = ["student-halls", "studios", "student-flats"]

_NEXT_DATA_PAT = re.compile(
    r'__NEXT_DATA__" type="application/json">(.*?)</script>', re.S,
)


@dataclass
class AfsProperty:
    afs_id: str
    url: str
    city_slug: str
    category: str                       # which landing page it came from
    name: str = ""
    address: str = ""
    area: str = ""
    postcode: str = ""
    operator: str = ""
    advert_type: str = ""               # "Pbsa" | "Standard" ...
    property_type: str = ""             # "halls" | "flat" | ...
    rent_ppw: Optional[float] = None
    room_options: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    contracts: list[dict[str, Any]] = field(default_factory=list)
    bills_included: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class AfsScraper:
    request_interval_sec = 1.5
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
            logger.warning("afs_fetch_error", url=url, error=str(e)[:120])
            return None
        if r.status_code != 200:
            logger.warning("afs_fetch_http", url=url, status=r.status_code)
            return None
        return r.text

    # ------------------------------------------------------------------
    def _page_view_model(self, html: str) -> Optional[dict]:
        m = _NEXT_DATA_PAT.search(html)
        if not m:
            return None
        try:
            d = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            return None
        return (d.get("props") or {}).get("pageProps", {}).get("viewModel")

    def _extract_properties(self, vm: dict) -> list[dict]:
        out = []
        groups = ((vm.get("properties") or {}).get("groups")) or []
        for g in groups:
            for res in g.get("results") or []:
                p = res.get("property")
                if p:
                    out.append(p)
        return out

    def scrape_city_category(
        self, city_slug: str, category: str,
        seen_ids: Optional[set[str]] = None,
    ) -> Iterable[AfsProperty]:
        """One city × one landing category, following pagination."""
        page = 1
        if seen_ids is None:
            seen_ids = set()
        while True:
            url = f"{BASE}/{city_slug}/{category}"
            if page > 1:
                url += f"?page={page}"
            html = self.get(url)
            if not html:
                return
            vm = self._page_view_model(html)
            if not vm:
                logger.warning("afs_no_viewmodel", url=url)
                return
            props = self._extract_properties(vm)
            page_count = vm.get("pageCount") or 1
            logger.info(
                "afs_page", city=city_slug, category=category,
                page=page, of=page_count, props=len(props),
                total=vm.get("numberOfProperties"),
            )
            for p in props:
                pid = str(p.get("id") or "")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                yield self._to_record(p, city_slug, category)
            if page >= page_count:
                return
            page += 1

    def scrape_city(self, city_slug: str) -> Iterable[AfsProperty]:
        seen_ids: set[str] = set()
        for category in CATEGORY_PATHS:
            yield from self.scrape_city_category(
                city_slug, category, seen_ids=seen_ids,
            )

    # ------------------------------------------------------------------
    def _to_record(
        self, p: dict, city_slug: str, category: str,
    ) -> AfsProperty:
        addr = p.get("address") or {}
        terms = p.get("terms") or {}
        rent = (terms.get("rentPpw") or {}).get("value")
        provider = (p.get("accommodationProvider") or {}).get("name") or ""
        coords = p.get("coordinates") or {}
        return AfsProperty(
            afs_id=str(p.get("id") or ""),
            url=BASE + (p.get("url") or ""),
            city_slug=city_slug,
            category=category,
            name=(addr.get("address1") or "").strip(),
            address=", ".join(
                s for s in [addr.get("address2"), addr.get("city")] if s
            ),
            area=(addr.get("area") or "").strip(),
            postcode=(addr.get("postcode") or "").strip(),
            operator=provider.strip(),
            advert_type=(p.get("propertyAdvertType") or "").strip(),
            property_type=(p.get("propertyType") or "").strip(),
            rent_ppw=rent,
            room_options=p.get("numberOfRoomOptionsAvailable"),
            latitude=coords.get("lat"),
            longitude=coords.get("lon"),
            contracts=p.get("contracts") or [],
            bills_included=(terms.get("billsIncluded") or ""),
            raw={
                "isPbsaProperty": p.get("isPbsaProperty"),
                "isSoldOut": p.get("isSoldOut"),
                "academicYearLabel": p.get("academicYearLabel"),
            },
        )
