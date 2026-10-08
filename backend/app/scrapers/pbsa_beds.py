"""Parse PBSA bed counts and expected delivery from planning text.

Council planning descriptions state student beds in free text ("erection
of purpose built student accommodation comprising 540 bedspaces"). The
reports product needs beds, not units, so this module extracts them.
"""
from __future__ import annotations

import datetime
import re
from typing import Optional

# "540 bed", "540-bed", "540 beds", "540 bedspaces", "540 bed spaces",
# "540 student beds", "540 student bedrooms", "540no. bedspaces",
# "1,544 beds". Studios are counted separately and used as a floor only.
_BED_RE = re.compile(
    r"(\d{1,2}(?:,\d{3})|\d{1,4})\s*(?:no\.?\s*)?[-\s]?"
    r"(?:student\s+)?(bed(?:\s?space|room)?s?)\b",
    re.I,
)
# "523 units of purpose built student accommodation", "732 purpose built
# student accommodation apartments", "950 student accommodation apartments",
# "180 student rooms" — unit-style counts that are bed spaces in PBSA.
_UNIT_RE = re.compile(
    r"(\d{1,2}(?:,\d{3})|\d{1,4})\s*(?:no\.?\s*)?[-\s]?"
    r"(?:(?:units?|rooms?|apartments?|cluster\s+(?:flats?|rooms?))\s+(?:of\s+)?"
    r"(?:purpose[-\s]built\s+)?student|"
    r"(?:purpose[-\s]built\s+)?student\s+(?:accommodation\s+)?"
    r"(?:apartments?|rooms?|units?))\b",
    re.I,
)
_STUDIO_RE = re.compile(
    r"(\d{1,2}(?:,\d{3})|\d{1,4})\s*(?:no\.?\s*)?[-\s]?studio(?:s|\s+(?:flat|apartment)s?)?\b",
    re.I,
)
# "up to 1,544 student bedrooms", "up to 950 student accommodation
# apartments": an envelope on an outline or hybrid permission, not a
# committed count.
_UP_TO_RE = re.compile(
    r"up\s+to\s+(?:a\s+maximum\s+of\s+)?(\d{1,2}(?:,\d{3})|\d{1,4})\s*"
    r"(?:no\.?\s*)?[-\s]?(?:purpose[-\s]built\s+)?student[^.;,]{0,60}?"
    r"(?:bed(?:\s?space|room)?s?|apartments?|units?|rooms?|studios?)\b",
    re.I,
)


def parse_beds_max(text: str | None) -> Optional[int]:
    """The 'up to N' student envelope on an outline/hybrid description."""
    if not text:
        return None
    vals = [int(m.group(1).replace(",", "")) for m in _UP_TO_RE.finditer(text)]
    vals = [v for v in vals if 10 <= v <= MAX_PLAUSIBLE_BEDS]
    return max(vals) if vals else None
# Phrases that mean the number is NOT a bed count ("care home of 60 beds"
# is handled by callers already filtering to PBSA applications).
_NEGATIVE_NEAR = re.compile(r"hotel|care\s+home|nursing|hospital|hmo\b", re.I)

MAX_PLAUSIBLE_BEDS = 4000


def parse_beds(text: str | None) -> Optional[int]:
    """Extract the PBSA bed count from a planning description.

    Returns the largest plausible bed figure (descriptions often repeat
    smaller per-block numbers); falls back to a studio count, which in an
    all-studio scheme equals beds. None when nothing plausible is found.
    """
    if not text:
        return None
    candidates = []
    for m in _BED_RE.finditer(text):
        window = text[max(0, m.start() - 40):m.end() + 20]
        if _NEGATIVE_NEAR.search(window):
            continue
        n = int(m.group(1).replace(",", ""))
        if 10 <= n <= MAX_PLAUSIBLE_BEDS:
            candidates.append(n)
    if candidates:
        return max(candidates)
    for m in _UNIT_RE.finditer(text):
        window = text[max(0, m.start() - 40):m.end() + 20]
        if _NEGATIVE_NEAR.search(window):
            continue
        n = int(m.group(1).replace(",", ""))
        if 10 <= n <= MAX_PLAUSIBLE_BEDS:
            candidates.append(n)
    if candidates:
        return max(candidates)
    studios = [
        int(m.group(1).replace(",", ""))
        for m in _STUDIO_RE.finditer(text)
        if 10 <= int(m.group(1).replace(",", "")) <= MAX_PLAUSIBLE_BEDS
    ]
    return max(studios) if studios else None


# Build time assumed from consent to doors opening. C&W's published
# convention: a 10-20% slippage haircut on top is applied at report
# time, not stored.
_BUILD_YEARS_BY_STATUS = {
    "under_construction": 1,
    "approved": 2,
    "permitted": 2,
}


def expected_delivery_year(
    status: str | None,
    decision_date: datetime.date | datetime.datetime | None,
) -> Optional[int]:
    """Expected opening year: decision year + typical build time.

    Only approved/under-construction applications get a year — submitted
    or refused schemes have no credible delivery date.
    """
    if not status or decision_date is None:
        return None
    key = status.lower().replace(" ", "_")
    for needle, years in _BUILD_YEARS_BY_STATUS.items():
        if needle in key:
            return decision_date.year + years
    return None
