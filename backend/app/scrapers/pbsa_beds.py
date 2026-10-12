"""Parse PBSA bed counts and expected delivery from planning text.

Thin wrappers over app.extraction.units, kept for existing callers. New
code should call extract_units directly: it returns the confidence, the
quoted phrase and whether a number is an "up to" envelope or sits inside
an either/or permission, none of which these wrappers can express.
"""
from __future__ import annotations

import datetime
from typing import Optional

from app.extraction.units import PLAUSIBLE, extract_units

MAX_PLAUSIBLE_BEDS = PLAUSIBLE["beds"][1]


def parse_beds(text: str | None) -> Optional[int]:
    """The committed student bed count, else the "up to" envelope.

    Falling back to the envelope keeps the behaviour older callers
    relied on; classify_pipeline uses the extractor directly so it can
    label an envelope as such.
    """
    ex = extract_units(text)
    fig = ex.best("beds") or ex.maximum("beds")
    return fig.value if fig else None


def parse_beds_max(text: str | None) -> Optional[int]:
    """The 'up to N' student envelope on an outline/hybrid description."""
    fig = extract_units(text).maximum("beds")
    return fig.value if fig else None


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
