"""Unite Students rent scraper.

Unite is the largest UK PBSA operator with ~180 properties across ~50 cities.
Their site (unitestudents.com) embeds room data as JSON inside
``<script>self.__next_f.push([...])</script>`` SSR payloads, plus a
``priceRange`` at the property level.

Strategy:
1. Crawl ``/student-accommodation/<city>`` index pages for property URLs.
2. For each property URL, fetch HTML + extract:
   - property name, address, postcode
   - priceRange "228 - 369"  -> min/max £/week for the property
   - per-room availabilityBands (lengthWeeks, price, contract dates)
3. Emit RentRecord per room type, with the longest contract band as
   "academic_year" rate.
"""
from __future__ import annotations

import json
import re
from typing import Iterable, Optional

import structlog
from bs4 import BeautifulSoup

from .base_operator import BaseOperatorScraper, RentRecord

logger = structlog.get_logger(__name__)


# Unite has a "find your city" pattern. These are the cities they cover.
UNITE_CITIES = [
    "aberdeen", "bath", "birmingham", "bournemouth", "brighton", "bristol",
    "cambridge", "canterbury", "cardiff", "chester", "coventry", "dundee",
    "durham", "edinburgh", "exeter", "glasgow", "hatfield", "huddersfield",
    "kingston", "lancaster", "leeds", "leicester", "lincoln", "liverpool",
    "loughborough", "london", "manchester", "newcastle", "northampton",
    "norwich", "nottingham", "oxford", "plymouth", "portsmouth", "reading",
    "salford", "sheffield", "southampton", "stirling", "stoke", "swansea",
    "uxbridge", "wolverhampton", "york",
]

PROPERTY_URL_PAT = re.compile(
    r'/student-accommodation/([a-z]+)/([a-z0-9\-]+)'
)


class UniteScraper(BaseOperatorScraper):
    operator_name = "Unite Students"
    base_url = "https://www.unitestudents.com"

    def fetch_all(self) -> Iterable[RentRecord]:
        seen_urls: set[str] = set()
        for city in UNITE_CITIES:
            city_url = f"{self.base_url}/student-accommodation/{city}"
            html = self.get(city_url)
            if not html:
                continue
            # Extract property URLs from the city page
            for m in PROPERTY_URL_PAT.finditer(html):
                slug = m.group(2)
                prop_url = f"{self.base_url}/student-accommodation/{m.group(1)}/{slug}"
                if prop_url in seen_urls:
                    continue
                seen_urls.add(prop_url)
                yield from self._scrape_property(prop_url, city)

    def _scrape_property(
        self, prop_url: str, city: str,
    ) -> Iterable[RentRecord]:
        html = self.get(prop_url)
        if not html:
            return
        soup = BeautifulSoup(html, "html.parser")

        # Property name from JSON-LD or <h1>
        name = self._extract_property_name(html, soup)
        if not name:
            return

        addr_line, postcode = self._extract_address(html)

        # Pull priceRange ("228 - 369") at property level
        m = re.search(r'"priceRange":"(\d+)\s*-\s*(\d+)"', html)
        price_min = float(m.group(1)) if m else None
        price_max = float(m.group(2)) if m else None

        # Per-room data: room name precedes the availabilityBands JSON array.
        # We parse all availabilityBands blocks and try to pull the nearest
        # preceding "name" or "title" string. v1: emit a single record per
        # property using the priceRange range; v2 can add per-room granularity.
        if price_min is not None or price_max is not None:
            yield RentRecord(
                scheme_name=name,
                address=addr_line,
                postcode=postcode,
                council_hint=city.title(),
                room_type="Range",
                rent_min_per_week=price_min,
                rent_max_per_week=price_max,
                rent_per_week=(price_min + price_max) / 2 if (price_min and price_max) else (price_min or price_max),
                academic_year=self._infer_academic_year(html),
                operator_name=self.operator_name,
                source_url=prop_url,
                raw={"price_range": f"{price_min}-{price_max}"},
            )

        # Per-room extraction — best effort
        for room in self._extract_room_bands(html):
            yield RentRecord(
                scheme_name=name,
                address=addr_line,
                postcode=postcode,
                council_hint=city.title(),
                room_type=room["room_type"],
                rent_per_week=room["price"],
                contract_length_weeks=room["weeks"],
                academic_year=room["academic_year"],
                operator_name=self.operator_name,
                source_url=prop_url,
                raw=room,
            )

    @staticmethod
    def _extract_property_name(html: str, soup: BeautifulSoup) -> str:
        # Try JSON-LD first
        for blk in soup.select('script[type="application/ld+json"]'):
            try:
                d = json.loads(blk.text)
            except Exception:
                continue
            nodes = d.get("@graph", [d]) if isinstance(d, dict) else []
            for n in nodes:
                if isinstance(n, dict) and "Apartment" in str(n.get("@type", "")):
                    return n.get("name", "") or ""
        # Fallback: <h1>
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
        # Last resort: parse from URL
        return ""

    @staticmethod
    def _extract_address(html: str) -> tuple[str, str]:
        addr_match = re.search(
            r'"streetAddress":"([^"]+)"[^}]*"postalCode":"([^"]+)"', html
        )
        if addr_match:
            return addr_match.group(1), addr_match.group(2)
        # Try reverse order
        pc_match = re.search(r'"postalCode":"([A-Z0-9 ]+)"', html)
        sa_match = re.search(r'"streetAddress":"([^"]+)"', html)
        return (sa_match.group(1) if sa_match else "",
                pc_match.group(1) if pc_match else "")

    @staticmethod
    def _infer_academic_year(html: str) -> Optional[str]:
        # Look for contractStartDate patterns
        years = sorted(set(re.findall(r'"contractStartDate":"(\d{4})-', html)))
        if not years:
            return None
        y = int(years[0])  # earliest start year is the academic year start
        return f"{y}/{str(y+1)[-2:]}"

    @staticmethod
    def _extract_room_bands(html: str) -> Iterable[dict]:
        """Extract per-room availability bands.

        Bands look like:
        "availabilityBands":[{"contractLengthWeeks":51,"contractStartDate":...,"price":272,...}]
        """
        # Find availabilityBands arrays
        for m in re.finditer(
            r'"availabilityBands":\[((?:[^][]|\[[^]]*\])*)\]', html,
        ):
            try:
                bands_text = "[" + m.group(1) + "]"
                bands = json.loads(bands_text)
            except Exception:
                continue
            if not bands:
                continue

            # Pick the longest contract = academic-year rate
            longest = max(bands, key=lambda b: b.get("contractLengthWeeks", 0))
            weeks = longest.get("contractLengthWeeks")
            price = longest.get("price")
            if not price:
                continue

            # Find a nearby room name — look ~600 chars backwards for "name":
            window = html[max(0, m.start() - 600):m.start()]
            name_match = re.findall(r'"name":"([^"]{3,80})"', window)
            room_type = name_match[-1] if name_match else "Unknown"
            # Avoid picking up generic property name — heuristics
            if room_type in ("Parkway Gate", ""):
                room_type = "Unknown"

            start = longest.get("contractStartDate", "")
            ay = None
            if start:
                yr = start[:4]
                try:
                    ay = f"{int(yr)}/{str(int(yr)+1)[-2:]}"
                except ValueError:
                    pass

            yield {
                "room_type": room_type,
                "price": float(price),
                "weeks": weeks,
                "academic_year": ay,
                "contract_start": longest.get("contractStartDate"),
                "contract_end": longest.get("contractEndDate"),
                "contract_name": longest.get("contractName"),
            }
