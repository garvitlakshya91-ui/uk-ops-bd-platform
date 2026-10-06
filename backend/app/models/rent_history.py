"""Append-only writes to scheme_rents.

Rent observations are the one dataset that cannot be backfilled: once an
advertised price leaves an operator's website it is gone. Nothing may
DELETE from or UPDATE prices in scheme_rents. A new observation
supersedes the old row (``is_current = false``) and is inserted as a new
row, so year-over-year growth can be computed from the superseded rows.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.models.models import SchemeRent

SUPERSEDE_SQL = """
    UPDATE scheme_rents
       SET is_current = FALSE, superseded_at = NOW()
     WHERE is_current
       AND source = ANY(:sources)
       {scheme_filter}
"""


def supersede_rents(conn, sources: Sequence[str],
                    scheme_ids: Optional[Iterable[int]] = None) -> int:
    """Mark a source's current rows superseded ahead of a reload.

    ``conn`` is a SQLAlchemy connection in a transaction. Restricting to
    ``scheme_ids`` lets partial re-scrapes keep untouched schemes current.
    Returns the number of rows superseded.
    """
    scheme_filter = "AND scheme_id = ANY(:sids)" if scheme_ids is not None else ""
    params = {"sources": list(sources)}
    if scheme_ids is not None:
        params["sids"] = list(scheme_ids)
    result = conn.execute(
        text(SUPERSEDE_SQL.format(scheme_filter=scheme_filter)), params
    )
    return result.rowcount or 0


def record_rent(
    db: Session,
    *,
    scheme_id: int,
    source: str,
    room_type: Optional[str] = None,
    academic_year: Optional[str] = None,
    sub_classification: Optional[str] = None,
    rent_per_week: Optional[float] = None,
    rent_per_month: Optional[float] = None,
    currency: str = "GBP",
    contract_length_weeks: Optional[int] = None,
    source_reference: Optional[str] = None,
) -> str:
    """ORM upsert that keeps history.

    Matches the current row on (scheme_id, source, room_type,
    sub_classification, academic_year). Identical values are a no-op;
    changed values supersede the old row and insert a new one. Returns
    "unchanged", "updated" or "created".
    """
    existing = (
        db.query(SchemeRent)
        .filter(
            SchemeRent.scheme_id == scheme_id,
            SchemeRent.source == source,
            SchemeRent.room_type == room_type,
            SchemeRent.sub_classification == sub_classification,
            SchemeRent.academic_year == academic_year,
            SchemeRent.is_current.is_(True),
        )
        .first()
    )
    if existing is not None:
        same = (
            (rent_per_week is None or existing.rent_per_week == rent_per_week)
            and (rent_per_month is None or existing.rent_per_month == rent_per_month)
            and (contract_length_weeks is None
                 or existing.contract_length_weeks == contract_length_weeks)
        )
        if same:
            return "unchanged"
        existing.is_current = False
        existing.superseded_at = func.now()

    db.add(SchemeRent(
        scheme_id=scheme_id,
        source=source,
        room_type=room_type,
        sub_classification=sub_classification,
        academic_year=academic_year,
        rent_per_week=rent_per_week if rent_per_week is not None
        else (existing.rent_per_week if existing else None),
        rent_per_month=rent_per_month if rent_per_month is not None
        else (existing.rent_per_month if existing else None),
        currency=currency,
        contract_length_weeks=contract_length_weeks
        if contract_length_weeks is not None
        else (existing.contract_length_weeks if existing else None),
        source_reference=source_reference,
    ))
    return "updated" if existing is not None else "created"
