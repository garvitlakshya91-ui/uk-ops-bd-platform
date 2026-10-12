"""Append-only writes to the four rent tables.

Rent observations cannot be backfilled: once a price leaves a page it is
gone. Nothing here deletes or edits a price. A new capture supersedes
the prior row for the same key (``is_current = false``) and inserts a
new one, so year-over-year growth is a query over superseded rows.

    record_room_rent(db, scheme_id=1, source="operator_page", room_type="Studio",
                     sub_classification="Gold", academic_year="2026/27", rent_ppw=215)
    record_advertised_rent(db, scheme_id=1, source="afs_directory", kind="from", rent_ppw=189)
    record_incentive(db, scheme_id=1, source="operator_page", text="£300 cashback")
    record_area_benchmark(db, council_id=18, source="ons_pipr_area",
                          segment="Area avg 1-bed (LA)", rent_pcm=850, period="2026-06")
"""
from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.models import AreaRentBenchmark, SchemeAdvertisedRent, SchemeIncentive, SchemeRoomRent

ADVERTISED_KINDS = ("from", "to", "advertised")

_CASH = re.compile(r"£\s?([\d,]{2,6})(?!\s*(?:/|per|a)\s*(?:week|wk|pw))", re.I)
_WEEKLY = re.compile(r"£\s?(\d{1,3})\s*(?:off\s+)?(?:a|per|/)\s*(?:week|wk)|£\s?(\d{1,3})\s*(?:pw|p/w)\s*off", re.I)
_PERCENT = re.compile(r"(\d{1,2})\s?%\s*(?:off|discount|saving)?", re.I)
_FREE_WEEKS = re.compile(r"(\d{1,2}|one|two|three|four)\s+(?:weeks?|months?)\s+(?:rent\s+)?free|free\s+(?:weeks?|month)", re.I)


def norm_academic_year(value: Optional[str]) -> Optional[str]:
    """One spelling for the letting year: "2026-27", "2026/2027", "26/27" -> "2026/27"."""
    if not value:
        return None
    m = re.match(r"^\s*(?:20)?(\d{2})\s*[/–-]\s*(?:20)?(\d{2})\s*$", str(value))
    return f"20{m.group(1)}/{m.group(2)}" if m else str(value).strip()


def supersede(db: Session, model, sources: Sequence[str],
              scheme_ids: Optional[Iterable[int]] = None) -> int:
    """Mark a source's current rows superseded ahead of a reload; returns the count."""
    q = db.query(model).filter(model.is_current.is_(True), model.source.in_(list(sources)))
    if scheme_ids is not None:
        q = q.filter(model.scheme_id.in_(list(scheme_ids)))
    return q.update({"is_current": False, "superseded_at": func.now()}, synchronize_session=False)


def _upsert(db: Session, model, key: dict, values: dict) -> str:
    """Keep history: identical values are a no-op, changed values supersede and insert."""
    existing = db.query(model).filter_by(is_current=True, **key).first()
    if existing is not None:
        same = all(v is None or getattr(existing, k) == v for k, v in values.items())
        if same:
            return "unchanged"
        existing.is_current = False
        existing.superseded_at = func.now()
        for k, v in values.items():
            if v is None:
                values[k] = getattr(existing, k)
    db.add(model(**key, **values))
    return "updated" if existing is not None else "created"


def record_room_rent(db: Session, *, scheme_id: int, source: str, room_type: str,
                     sub_classification: Optional[str] = None, academic_year: Optional[str] = None,
                     rent_ppw: Optional[float] = None, rent_pcm: Optional[float] = None,
                     currency: str = "GBP", contract_length_weeks: Optional[int] = None,
                     bills_included: Optional[bool] = None, source_reference: Optional[str] = None) -> str:
    key = {"scheme_id": scheme_id, "source": source, "room_type": (room_type or "Unknown")[:100],
           "sub_classification": sub_classification[:150] if sub_classification else None,
           "academic_year": norm_academic_year(academic_year)}
    values = {"rent_ppw": rent_ppw, "rent_pcm": rent_pcm, "currency": currency,
              "contract_length_weeks": contract_length_weeks, "bills_included": bills_included,
              "source_reference": source_reference[:500] if source_reference else None}
    return _upsert(db, SchemeRoomRent, key, values)


def record_advertised_rent(db: Session, *, scheme_id: int, source: str, kind: str,
                           rent_ppw: Optional[float] = None, rent_pcm: Optional[float] = None,
                           academic_year: Optional[str] = None, contract_length_weeks: Optional[int] = None,
                           source_reference: Optional[str] = None) -> str:
    if kind not in ADVERTISED_KINDS:
        raise ValueError(f"kind must be one of {ADVERTISED_KINDS}")
    key = {"scheme_id": scheme_id, "source": source, "kind": kind, "academic_year": norm_academic_year(academic_year)}
    values = {"rent_ppw": rent_ppw, "rent_pcm": rent_pcm, "contract_length_weeks": contract_length_weeks,
              "source_reference": source_reference[:500] if source_reference else None}
    return _upsert(db, SchemeAdvertisedRent, key, values)


def classify_incentive(text: str) -> tuple[str, Optional[float]]:
    """('cash', 300.0) for '£300 cashback'; ('weekly_off', 20.0); ('percent', 20.0); ('free_weeks', None)."""
    t = text or ""
    m = _WEEKLY.search(t)
    if m:
        return "weekly_off", float(m.group(1) or m.group(2))
    if re.search(r"cash|voucher|rebate|credit", t, re.I):
        m = _CASH.search(t)
        if m:
            return "cash", float(m.group(1).replace(",", ""))
    m = _PERCENT.search(t)
    if m:
        return "percent", float(m.group(1))
    if _FREE_WEEKS.search(t):
        return "free_weeks", None
    m = _CASH.search(t)
    if m and re.search(r"off|save|discount", t, re.I):
        return "cash", float(m.group(1).replace(",", ""))
    return "other", None


def record_incentive(db: Session, *, scheme_id: int, source: str, text: str,
                     academic_year: Optional[str] = None, source_reference: Optional[str] = None) -> str:
    """Record an offer seen on a capture. Call supersede(...) for the scheme
    first on a fresh capture so offers no longer advertised drop out of
    the current set while staying in history."""
    raw = (text or "").strip()[:300]
    if not raw:
        return "unchanged"
    existing = (db.query(SchemeIncentive)
                .filter(SchemeIncentive.scheme_id == scheme_id, SchemeIncentive.source == source,
                        SchemeIncentive.is_current.is_(True),
                        func.lower(SchemeIncentive.raw_text) == raw.lower()).first())
    if existing is not None:
        return "unchanged"
    kind, amount = classify_incentive(raw)
    db.add(SchemeIncentive(scheme_id=scheme_id, source=source, raw_text=raw, kind=kind, amount=amount,
                           academic_year=norm_academic_year(academic_year),
                           source_reference=source_reference[:500] if source_reference else None))
    return "created"


def record_area_benchmark(db: Session, *, council_id: Optional[int], source: str, segment: str,
                          rent_pcm: Optional[float] = None, rent_ppw: Optional[float] = None,
                          period: Optional[str] = None, area_code: Optional[str] = None,
                          area_name: Optional[str] = None, source_reference: Optional[str] = None) -> str:
    key = {"council_id": council_id, "source": source, "segment": segment[:80], "period": period}
    values = {"rent_pcm": rent_pcm, "rent_ppw": rent_ppw, "area_code": area_code, "area_name": area_name,
              "source_reference": source_reference[:500] if source_reference else None}
    return _upsert(db, AreaRentBenchmark, key, values)
