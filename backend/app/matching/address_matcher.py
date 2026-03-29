"""Address matching and geocoding utilities for UK addresses.

Provides normalisation, postcode extraction, fuzzy matching, and geocoding
to link planning applications to existing schemes by location.

Typical usage::

    from app.matching.address_matcher import (
        normalize_address,
        extract_postcode,
        addresses_match,
        geocode_address,
    )

    match = addresses_match(
        "123 High Street, London SW1A 1AA",
        "123 High St, London, SW1A 1AA",
    )
    assert match.is_match  # True
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import httpx
import structlog
from fuzzywuzzy import fuzz  # type: ignore[import-untyped]

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# UK postcode regex
# ---------------------------------------------------------------------------

_POSTCODE_RE = re.compile(
    r"\b([Gg][Ii][Rr] 0[Aa]{2})|"
    r"(([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])"
    r"|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s*[0-9][A-Za-z]{2}\b",
)

# ---------------------------------------------------------------------------
# Address abbreviation mappings (expand abbreviated forms to standard)
# ---------------------------------------------------------------------------

_ABBREVIATIONS: dict[str, str] = {
    "road": "rd",
    "street": "st",
    "avenue": "ave",
    "lane": "ln",
    "drive": "dr",
    "crescent": "cres",
    "terrace": "tce",
    "close": "cl",
    "court": "ct",
    "place": "pl",
    "square": "sq",
    "gardens": "gdns",
    "grove": "gr",
    "park": "pk",
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
    "mount": "mt",
    "saint": "st",
    "floor": "fl",
    "building": "bldg",
    "apartment": "apt",
    "flat": "flat",
}

# Build a reverse map for normalisation (both directions map to abbreviated form).
_NORM_MAP: dict[str, str] = {}
for full, abbr in _ABBREVIATIONS.items():
    _NORM_MAP[full] = abbr
    _NORM_MAP[abbr] = abbr  # Abbreviated form maps to itself.

_WORD_BOUNDARY_RE = re.compile(r"\b(\w+)\b")
_MULTI_SPACE_RE = re.compile(r"\s+")
_PUNCTUATION_RE = re.compile(r"[^\w\s]")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_address(address: str) -> str:
    """Normalise a UK address for comparison.

    Steps:

    1. Lowercase.
    2. Remove punctuation.
    3. Standardise road/street abbreviations.
    4. Remove postcode (handled separately).
    5. Collapse whitespace.

    Parameters
    ----------
    address : str
        Raw address string.

    Returns
    -------
    str
        Normalised address.

    Examples
    --------
    >>> normalize_address("123 High Street, London SW1A 1AA")
    '123 high st london'
    >>> normalize_address("123 High St., London, SW1A 1AA")
    '123 high st london'
    """
    result = address.lower().strip()

    # Remove postcode.
    result = _POSTCODE_RE.sub("", result)

    # Remove punctuation.
    result = _PUNCTUATION_RE.sub(" ", result)

    # Standardise abbreviations.
    def _replace(m: re.Match[str]) -> str:
        word = m.group(1)
        return _NORM_MAP.get(word, word)

    result = _WORD_BOUNDARY_RE.sub(_replace, result)

    # Collapse whitespace.
    result = _MULTI_SPACE_RE.sub(" ", result).strip()
    return result


def extract_postcode(address: str) -> str | None:
    """Extract a UK postcode from an address string.

    Returns the postcode in uppercase with a single space between outcode
    and incode, or ``None`` if no postcode is found.

    Examples
    --------
    >>> extract_postcode("123 High Street, London SW1A 1AA")
    'SW1A 1AA'
    >>> extract_postcode("no postcode here")
    """
    match = _POSTCODE_RE.search(address)
    if not match:
        return None
    raw = match.group(0).strip().upper()
    # Ensure standard formatting: outcode + space + incode (last 3 chars).
    raw_no_space = raw.replace(" ", "")
    if len(raw_no_space) >= 5:
        return f"{raw_no_space[:-3]} {raw_no_space[-3:]}"
    return raw


@dataclass
class AddressMatchResult:
    """Result of an address comparison."""

    is_match: bool
    overall_score: float
    postcode_match: bool
    address_similarity: float
    distance_metres: float | None = None

    @property
    def confidence(self) -> str:
        """Human-readable confidence label."""
        if self.overall_score >= 0.95:
            return "high"
        if self.overall_score >= 0.80:
            return "medium"
        if self.overall_score >= 0.65:
            return "low"
        return "none"


def addresses_match(
    addr1: str,
    addr2: str,
    *,
    threshold: float = 0.80,
) -> AddressMatchResult:
    """Fuzzy-match two UK addresses.

    The comparison works in two parts:

    1. **Postcode match** — if both addresses contain a postcode, an exact
       postcode match carries significant weight.
    2. **Fuzzy address similarity** — the normalised address text is compared
       using ``fuzzywuzzy.fuzz.token_sort_ratio``.

    Parameters
    ----------
    addr1, addr2 : str
        Raw address strings.
    threshold : float
        Minimum overall score to consider a match.

    Returns
    -------
    AddressMatchResult
    """
    pc1 = extract_postcode(addr1)
    pc2 = extract_postcode(addr2)

    postcode_match = False
    postcode_score = 0.0
    if pc1 and pc2:
        postcode_match = pc1 == pc2
        postcode_score = 1.0 if postcode_match else 0.0

    norm1 = normalize_address(addr1)
    norm2 = normalize_address(addr2)
    address_similarity = fuzz.token_sort_ratio(norm1, norm2) / 100.0

    # Weighted combination: postcode match is a strong signal.
    if pc1 and pc2:
        overall = 0.4 * postcode_score + 0.6 * address_similarity
    else:
        overall = address_similarity

    return AddressMatchResult(
        is_match=overall >= threshold,
        overall_score=round(overall, 3),
        postcode_match=postcode_match,
        address_similarity=round(address_similarity, 3),
    )


async def geocode_address(address: str) -> dict[str, float] | None:
    """Geocode a UK address to latitude/longitude using the Nominatim API.

    Returns ``{"lat": ..., "lng": ...}`` or ``None`` if geocoding fails.

    Note: For production use, consider a commercial geocoding service with
    higher rate limits.  Nominatim's usage policy requires at most 1 req/s.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": address,
                    "format": "json",
                    "countrycodes": "gb",
                    "limit": 1,
                    "addressdetails": 0,
                },
                headers={
                    "User-Agent": "UKOpsBDPlatform/1.0 (geocoding)",
                },
            )
            resp.raise_for_status()
            results = resp.json()

        if not results:
            logger.info("geocode_no_results", address=address)
            return None

        best = results[0]
        coords = {
            "lat": float(best["lat"]),
            "lng": float(best["lon"]),
        }
        logger.info("geocode_success", address=address, **coords)
        return coords

    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, ValueError) as exc:
        logger.error("geocode_failed", address=address, error=str(exc))
        return None
