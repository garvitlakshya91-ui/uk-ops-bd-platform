"""Extract contract dates from free-text tender/contract descriptions.

Handles patterns like:
- "contract period: 1 April 2024 to 31 March 2029"
- "for a period of 5 years commencing 1st January 2025"
- "start date: 01/04/2024, end date: 31/03/2029"
- "from April 2024 for 5 years"
- "the contract will run from 2024-04-01 to 2029-03-31"
- "initial term of 3 years with option to extend"
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

# ---------------------------------------------------------------------------
# Month name lookup
# ---------------------------------------------------------------------------

_MONTH_NAMES: dict[str, int] = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# ---------------------------------------------------------------------------
# Individual date patterns
# ---------------------------------------------------------------------------

# "1 April 2024", "1st January 2025", "31st March 2029", "02 Dec 2024"
_RE_UK_LONG = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(" + "|".join(_MONTH_NAMES.keys()) + r")\s+"
    r"(\d{4})\b",
    re.IGNORECASE,
)

# "April 2024" (no day — assume 1st)
_RE_MONTH_YEAR = re.compile(
    r"\b(" + "|".join(_MONTH_NAMES.keys()) + r")\s+(\d{4})\b",
    re.IGNORECASE,
)

# "01/04/2024" or "01-04-2024" (DD/MM/YYYY — UK format)
_RE_UK_NUMERIC = re.compile(
    r"\b(\d{2})[/\-](\d{2})[/\-](\d{4})\b",
)

# "2024-04-01" (ISO format)
_RE_ISO = re.compile(
    r"\b(\d{4})-(\d{2})-(\d{2})\b",
)


def _parse_single_date(text: str) -> date | None:
    """Try to parse a single date from a short text fragment."""
    text = text.strip()

    # ISO: 2024-04-01
    m = _RE_ISO.search(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # UK long: 1 April 2024 / 1st January 2025
    m = _RE_UK_LONG.search(text)
    if m:
        try:
            day = int(m.group(1))
            month = _MONTH_NAMES[m.group(2).lower()]
            year = int(m.group(3))
            return date(year, month, day)
        except (ValueError, KeyError):
            pass

    # UK numeric: 01/04/2024
    m = _RE_UK_NUMERIC.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # Month year: April 2024 (assume 1st)
    m = _RE_MONTH_YEAR.search(text)
    if m:
        try:
            month = _MONTH_NAMES[m.group(1).lower()]
            year = int(m.group(2))
            return date(year, month, 1)
        except (ValueError, KeyError):
            pass

    return None


def _find_all_dates(text: str) -> list[tuple[int, date]]:
    """Find all dates in text and return them with their character position."""
    results: list[tuple[int, date]] = []
    seen_positions: set[int] = set()

    # ISO dates
    for m in _RE_ISO.finditer(text):
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if m.start() not in seen_positions:
                results.append((m.start(), d))
                seen_positions.add(m.start())
        except ValueError:
            pass

    # UK long dates
    for m in _RE_UK_LONG.finditer(text):
        try:
            day = int(m.group(1))
            month = _MONTH_NAMES[m.group(2).lower()]
            year = int(m.group(3))
            d = date(year, month, day)
            if m.start() not in seen_positions:
                results.append((m.start(), d))
                seen_positions.add(m.start())
        except (ValueError, KeyError):
            pass

    # UK numeric dates
    for m in _RE_UK_NUMERIC.finditer(text):
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            if m.start() not in seen_positions:
                results.append((m.start(), d))
                seen_positions.add(m.start())
        except ValueError:
            pass

    # Month-year only (no day)
    for m in _RE_MONTH_YEAR.finditer(text):
        # Skip if this position was already captured by a UK long date
        if m.start() in seen_positions:
            continue
        # Also skip if this month-year is part of a longer "1 April 2024" pattern
        prefix = text[max(0, m.start() - 10):m.start()]
        if re.search(r"\d{1,2}(?:st|nd|rd|th)?\s*$", prefix, re.IGNORECASE):
            continue
        try:
            month = _MONTH_NAMES[m.group(1).lower()]
            year = int(m.group(2))
            results.append((m.start(), date(year, month, 1)))
            seen_positions.add(m.start())
        except (ValueError, KeyError):
            pass

    results.sort(key=lambda x: x[0])
    return results


# ---------------------------------------------------------------------------
# Range patterns (from X to Y, start date: X end date: Y, etc.)
# ---------------------------------------------------------------------------

# "from X to Y", "from X - Y", "X to Y", "X - Y" (with date-like substrings)
_RE_RANGE_CONNECTORS = re.compile(
    r"(?:from\s+)?(.+?)\s+(?:to|until|through|[-\u2013\u2014])\s+(.+)",
    re.IGNORECASE,
)

# Explicit labelled dates
_RE_START_DATE = re.compile(
    r"(?:start\s*date|commencement\s*date|commencing|start(?:ing)?)\s*[:;]?\s*(.+?)(?:[,;.]|\s+(?:end|to|until|$))",
    re.IGNORECASE,
)
_RE_END_DATE = re.compile(
    r"(?:end\s*date|expiry\s*date|termination\s*date|ending|expires?)\s*[:;]?\s*(.+?)(?:[,;.]|$)",
    re.IGNORECASE,
)

# "contract period: X to Y"
_RE_CONTRACT_PERIOD = re.compile(
    r"(?:contract\s+period|period\s+of\s+contract|term)\s*[:;]?\s*(.+?)(?:\.|$)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Duration patterns
# ---------------------------------------------------------------------------

# "for N years", "N year contract", "initial term of N years",
# "for a period of N years"
_RE_DURATION_YEARS = re.compile(
    r"(?:for\s+(?:a\s+)?(?:period\s+of\s+)?|initial\s+term\s+of\s+|"
    r"(?:contract|agreement)\s+(?:term\s+(?:of\s+)?|for\s+)?)(\d+)\s*[\-\u2013]?\s*years?",
    re.IGNORECASE,
)

# "for N months"
_RE_DURATION_MONTHS = re.compile(
    r"(?:for\s+(?:a\s+)?(?:period\s+of\s+)?|initial\s+term\s+of\s+|"
    r"(?:contract|agreement)\s+(?:term\s+(?:of\s+)?|for\s+)?)(\d+)\s*months?",
    re.IGNORECASE,
)

# "N year" as a standalone pattern (e.g. "5 year contract")
_RE_N_YEAR = re.compile(
    r"\b(\d+)\s*[\-\u2013]?\s*year\b",
    re.IGNORECASE,
)

# "commencing X", "from X for N years"
_RE_START_WITH_DURATION = re.compile(
    r"(?:commencing|starting|from|beginning)\s+(.+?)\s+for\s+(\d+)\s*[\-\u2013]?\s*(years?|months?)",
    re.IGNORECASE,
)


def _add_duration(start: date, amount: int, unit: str) -> date:
    """Add a duration (years or months) to a start date."""
    unit_lower = unit.lower().rstrip("s")
    if unit_lower == "year":
        try:
            return start.replace(year=start.year + amount)
        except ValueError:
            # Handle leap day edge case: Feb 29 + N years
            return start.replace(year=start.year + amount, day=28)
    elif unit_lower == "month":
        new_month = start.month + amount
        new_year = start.year + (new_month - 1) // 12
        new_month = ((new_month - 1) % 12) + 1
        try:
            return start.replace(year=new_year, month=new_month)
        except ValueError:
            # Day doesn't exist in target month
            import calendar
            max_day = calendar.monthrange(new_year, new_month)[1]
            return start.replace(year=new_year, month=new_month, day=min(start.day, max_day))
    return start


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_contract_dates(text: str) -> dict[str, date | None]:
    """Extract contract start and end dates from a free-text description.

    Tries multiple strategies in order of reliability:
    1. Explicitly labelled start/end dates
    2. Contract period range patterns ("from X to Y")
    3. Start date + duration ("from X for N years")
    4. Heuristic: use the first two dates found in the text

    Parameters
    ----------
    text : str
        Free-text description, e.g. a tender or contract description.

    Returns
    -------
    dict with keys ``start_date`` and ``end_date``, each ``date | None``.
    """
    if not text or not text.strip():
        return {"start_date": None, "end_date": None}

    start_date: date | None = None
    end_date: date | None = None

    # ------------------------------------------------------------------
    # Strategy 1: Explicitly labelled start/end dates
    # ------------------------------------------------------------------
    m_start = _RE_START_DATE.search(text)
    if m_start:
        start_date = _parse_single_date(m_start.group(1))

    m_end = _RE_END_DATE.search(text)
    if m_end:
        end_date = _parse_single_date(m_end.group(1))

    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}

    # ------------------------------------------------------------------
    # Strategy 2: Contract period range ("contract period: X to Y")
    # ------------------------------------------------------------------
    m_period = _RE_CONTRACT_PERIOD.search(text)
    if m_period:
        period_text = m_period.group(1)
        m_range = _RE_RANGE_CONNECTORS.match(period_text.strip())
        if m_range:
            s = _parse_single_date(m_range.group(1))
            e = _parse_single_date(m_range.group(2))
            if s:
                start_date = start_date or s
            if e:
                end_date = end_date or e

    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}

    # ------------------------------------------------------------------
    # Strategy 3: "from X to Y" / "X to Y" / "X - Y"
    # ------------------------------------------------------------------
    if not (start_date and end_date):
        # Look for range patterns containing dates
        range_patterns = [
            # "from 1 April 2024 to 31 March 2029"
            re.compile(
                r"(?:from|running\s+from|run\s+from)\s+(.+?)\s+(?:to|until|through|[-\u2013\u2014])\s+(.+?)(?:[.,;]|\s+(?:with|plus|and\s+option)|$)",
                re.IGNORECASE,
            ),
            # "1 April 2024 to 31 March 2029" (without "from")
            re.compile(
                r"(\d{1,2}(?:st|nd|rd|th)?\s+(?:" + "|".join(_MONTH_NAMES.keys()) + r")\s+\d{4})\s+"
                r"(?:to|until|through|[-\u2013\u2014])\s+"
                r"(\d{1,2}(?:st|nd|rd|th)?\s+(?:" + "|".join(_MONTH_NAMES.keys()) + r")\s+\d{4})",
                re.IGNORECASE,
            ),
            # ISO range: "2024-04-01 to 2029-03-31"
            re.compile(
                r"(\d{4}-\d{2}-\d{2})\s+(?:to|until|through|[-\u2013\u2014])\s+(\d{4}-\d{2}-\d{2})",
            ),
            # UK numeric range: "01/04/2024 to 31/03/2029"
            re.compile(
                r"(\d{2}/\d{2}/\d{4})\s+(?:to|until|through|[-\u2013\u2014])\s+(\d{2}/\d{2}/\d{4})",
            ),
        ]

        for pat in range_patterns:
            m_range = pat.search(text)
            if m_range:
                s = _parse_single_date(m_range.group(1))
                e = _parse_single_date(m_range.group(2))
                if s:
                    start_date = start_date or s
                if e:
                    end_date = end_date or e
                if start_date and end_date:
                    break

    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}

    # ------------------------------------------------------------------
    # Strategy 4: Start date + duration
    # ------------------------------------------------------------------
    m_sd = _RE_START_WITH_DURATION.search(text)
    if m_sd:
        s = _parse_single_date(m_sd.group(1))
        if s:
            start_date = start_date or s
            amount = int(m_sd.group(2))
            end_date = end_date or _add_duration(s, amount, m_sd.group(3))

    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}

    # Duration with separately found start date
    if start_date and not end_date:
        duration_months = extract_contract_duration(text)
        if duration_months:
            end_date = _add_duration(start_date, duration_months, "month")

    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}

    # ------------------------------------------------------------------
    # Strategy 5: Heuristic — use the first two dates found
    # ------------------------------------------------------------------
    all_dates = _find_all_dates(text)
    if len(all_dates) >= 2:
        d1 = all_dates[0][1]
        d2 = all_dates[1][1]
        if d1 <= d2:
            start_date = start_date or d1
            end_date = end_date or d2
        else:
            start_date = start_date or d2
            end_date = end_date or d1
    elif len(all_dates) == 1 and not start_date:
        start_date = all_dates[0][1]
        # Try to derive end from duration
        duration_months = extract_contract_duration(text)
        if duration_months:
            end_date = _add_duration(start_date, duration_months, "month")

    return {"start_date": start_date, "end_date": end_date}


def extract_contract_duration(text: str) -> int | None:
    """Extract contract duration in months from a free-text description.

    Looks for patterns like "for 5 years", "3 year contract",
    "initial term of 36 months", etc.

    Parameters
    ----------
    text : str
        Free-text description.

    Returns
    -------
    int | None
        Duration in months, or ``None`` if no duration is found.
    """
    if not text or not text.strip():
        return None

    # Check explicit month durations first (more precise)
    m = _RE_DURATION_MONTHS.search(text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass

    # Then year durations
    m = _RE_DURATION_YEARS.search(text)
    if m:
        try:
            return int(m.group(1)) * 12
        except ValueError:
            pass

    # Fallback: "N year" anywhere
    m = _RE_N_YEAR.search(text)
    if m:
        try:
            years = int(m.group(1))
            if 1 <= years <= 30:  # Sanity check
                return years * 12
        except ValueError:
            pass

    return None
