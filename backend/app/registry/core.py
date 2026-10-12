"""Registry operations on existing_schemes.

A scheme's row id is its permanent identifier. Everything a feed calls
the building is an alias; every feed record that describes it is a
source record linked to that id. Names on verified schemes are locked
(field_protection) so a monthly ingest cannot rename a building; a
scraper that disagrees contributes an alias instead.

    propose_name(db, scheme, "THE PENWORKS", "epc_new_dwelling")
        -> "alias"      (name locked; spelling kept as an alias)
    match_record(db, council_id, "Oak Brook Park", "B29 6BL")
        -> (scheme, "alias", 1.5)
"""
from __future__ import annotations

import datetime
import re
from typing import Iterable, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.models import ExistingScheme, SchemeAlias, SchemeSourceRecord
from app.scrapers.field_protection import can_write, set_field
from app.scrapers.scheme_matching import best_match, build_index, norm_name, norm_pc

VERIFICATION_STATUSES = ("unverified", "probable", "verified", "disputed", "retired")
# Rows that are not schemes or clusters at all: never listed, mapped or counted by default.
NON_SCHEME_CLASSES = ("ingest_artefact", "procurement_notice", "listing_fragment")
LISTABLE_SQL = ("es.merged_into_id IS NULL AND es.record_class NOT IN "
                "('ingest_artefact', 'procurement_notice', 'listing_fragment')")


def listable_filters() -> tuple:
    """Conditions for a default scheme list: live rows of a listable class."""
    return (ExistingScheme.merged_into_id.is_(None),
            ExistingScheme.record_class.notin_(NON_SCHEME_CLASSES))


# Counts, coverage percentages and alerts are about real schemes only. EPC
# development clusters are listable but outnumber schemes about 17 to 1 and
# mostly have no operator, so they would swamp any statistic they are in.
REPORTABLE_CLASS = "scheme"
REPORTABLE_SQL = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"


def reportable_filters() -> tuple:
    """Conditions for scheme statistics: live rows that are real schemes."""
    return (ExistingScheme.merged_into_id.is_(None),
            ExistingScheme.record_class == REPORTABLE_CLASS)


# A source record's link is only replaced while it is still machine-made.
_REPLACEABLE = ("unmatched", "proposed")


def alias_norm(name: str | None) -> str:
    """The alias match key: significant tokens, sorted; else the bare lower-case string."""
    tokens = norm_name(name or "")
    if tokens:
        return " ".join(sorted(tokens))
    return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


# ------------------------------------------------------------------ aliases
def add_alias(db: Session, scheme_id: int, alias: str | None, source: str) -> bool:
    """Record a name the scheme is known by. Returns True when new."""
    key = alias_norm(alias)
    if not alias or not key:
        return False
    # Pending rows from this session are not visible to the query until
    # flushed; check them too or a run can insert the same alias twice.
    pending = any(isinstance(o, SchemeAlias) and o.scheme_id == scheme_id and o.alias_norm == key
                  for o in db.new)
    exists = pending or (db.query(SchemeAlias.id)
                         .filter(SchemeAlias.scheme_id == scheme_id, SchemeAlias.alias_norm == key)
                         .first())
    if exists:
        return False
    db.add(SchemeAlias(scheme_id=scheme_id, alias=alias.strip()[:500], alias_norm=key[:500],
                       source=source[:100]))
    return True


def find_by_alias(db: Session, name: str | None, council_id: Optional[int] = None,
                  scheme_type: Optional[str] = None) -> list[ExistingScheme]:
    key = alias_norm(name)
    if not key:
        return []
    q = (db.query(ExistingScheme)
         .join(SchemeAlias, SchemeAlias.scheme_id == ExistingScheme.id)
         .filter(SchemeAlias.alias_norm == key, ExistingScheme.merged_into_id.is_(None)))
    if council_id is not None:
        q = q.filter(ExistingScheme.council_id == council_id)
    if scheme_type:
        q = q.filter(ExistingScheme.scheme_type == scheme_type)
    return q.all()


def lock_field(scheme: ExistingScheme, field: str, source: str) -> None:
    """Lock a field under a source without changing its value."""
    locks = dict(scheme.locked_fields or {})
    locks[field] = source
    scheme.locked_fields = locks
    flag_modified(scheme, "locked_fields")


def propose_name(db: Session, scheme: ExistingScheme, new_name: str | None, source: str,
                 *, changed_by: str = "system") -> str:
    """Offer a feed's name for a scheme.

    Renames when precedence allows (the old name becomes an alias);
    otherwise keeps the offered spelling as an alias. Returns
    "renamed", "alias" or "unchanged".
    """
    if not new_name or not new_name.strip():
        return "unchanged"
    new_name = new_name.strip()
    if alias_norm(new_name) == alias_norm(scheme.name):
        if new_name != scheme.name:
            add_alias(db, scheme.id, new_name, source)
        return "unchanged"
    if can_write(scheme, "name", source):
        old = scheme.name
        if set_field(scheme, "name", new_name, source, db, changed_by=changed_by):
            add_alias(db, scheme.id, old, (scheme.locked_fields or {}).get("name", "former_name"))
            add_alias(db, scheme.id, new_name, source)
            return "renamed"
        return "unchanged"
    add_alias(db, scheme.id, new_name, source)
    return "alias"


# ------------------------------------------------------------------ matching
def match_record(db: Session, council_id: int, name: str | None, postcode: str | None,
                 *, scheme_type: str = "PBSA", candidates: Optional[Iterable[ExistingScheme]] = None,
                 ) -> tuple[Optional[ExistingScheme], Optional[str], float]:
    """Find the registry scheme a feed record describes.

    Aliases first (exact key, postcode must not contradict), then the
    shared postcode-plus-name-token match. Returns (scheme, method, score).
    """
    pc = norm_pc(postcode)
    for s in find_by_alias(db, name, council_id, scheme_type):
        spc = norm_pc(s.postcode)
        if not pc or not spc or pc == spc or pc[:-3] == spc[:-3]:
            return s, "alias", 1.5 if pc and pc == spc else 1.0
    if candidates is None:
        candidates = (db.query(ExistingScheme)
                      .filter(ExistingScheme.council_id == council_id,
                              ExistingScheme.scheme_type == scheme_type,
                              ExistingScheme.merged_into_id.is_(None)).all())
    scheme, score = best_match(build_index(list(candidates)), name or "", postcode)
    if scheme is not None:
        return scheme, "postcode_name", score
    return None, None, 0.0


# ------------------------------------------------------------------ source records
def link_source_record(db: Session, source: str, reference: str, *,
                       scheme_id: Optional[int] = None, raw_name: Optional[str] = None,
                       raw_postcode: Optional[str] = None, raw_operator: Optional[str] = None,
                       raw_address: Optional[str] = None, payload: Optional[dict] = None,
                       match_method: Optional[str] = None, match_score: Optional[float] = None,
                       status: Optional[str] = None) -> SchemeSourceRecord:
    """Upsert one feed record and its link to a scheme.

    A link confirmed by a person is never replaced by a machine match;
    an unmatched or proposed record takes whatever the caller found.
    """
    reference = reference[:500]
    status = status or ("matched" if scheme_id else "unmatched")
    rec = next((o for o in db.new if isinstance(o, SchemeSourceRecord)
                and o.source == source and o.source_reference == reference), None)
    if rec is None:
        rec = (db.query(SchemeSourceRecord)
               .filter(SchemeSourceRecord.source == source, SchemeSourceRecord.source_reference == reference)
               .first())
    if rec is None:
        rec = SchemeSourceRecord(source=source[:100], source_reference=reference)
        db.add(rec)
    rec.last_seen = _now()
    for col, val in (("raw_name", raw_name), ("raw_postcode", raw_postcode),
                     ("raw_operator", raw_operator), ("raw_address", raw_address)):
        if val:
            setattr(rec, col, str(val)[: 500 if col != "raw_postcode" else 12])
    if payload is not None:
        rec.payload = payload
    if rec.status in _REPLACEABLE or rec.status is None or rec.confirmed_by is None and rec.status == "matched" \
            and rec.match_method != "manual":
        rec.scheme_id = scheme_id
        rec.match_method = match_method
        rec.match_score = match_score
        rec.status = status
    return rec


# ------------------------------------------------------------------ verification
def mark_verified(db: Session, scheme: ExistingScheme, status: str, by: str,
                  note: Optional[str] = None) -> None:
    if status not in VERIFICATION_STATUSES:
        raise ValueError(f"status must be one of {VERIFICATION_STATUSES}")
    scheme.verification_status = status
    scheme.verified_at = _now()
    scheme.verified_by = by[:100]
    if note is not None:
        scheme.verification_note = note
    if status == "verified":
        lock_field(scheme, "name", "registry")
        scheme.last_verified_at = scheme.verified_at


def retire_duplicate(db: Session, duplicate: ExistingScheme, survivor: ExistingScheme,
                     by: str, note: Optional[str] = None) -> None:
    """Point a duplicate row at the building it duplicates; nothing is deleted."""
    if duplicate.id == survivor.id:
        raise ValueError("a scheme cannot be merged into itself")
    duplicate.merged_into_id = survivor.id
    mark_verified(db, duplicate, "retired", by, note or f"duplicate of #{survivor.id} {survivor.name}")
    add_alias(db, survivor.id, duplicate.name, "merged_duplicate")
    for a in db.query(SchemeAlias).filter(SchemeAlias.scheme_id == duplicate.id):
        add_alias(db, survivor.id, a.alias, a.source)
    (db.query(SchemeSourceRecord)
     .filter(SchemeSourceRecord.scheme_id == duplicate.id)
     .update({"scheme_id": survivor.id}, synchronize_session=False))


def queue_counts(db: Session, council_id: int) -> dict:
    """What the registry still needs a person for, in one council."""
    schemes = dict(db.query(ExistingScheme.verification_status, func.count())
                   .filter(ExistingScheme.council_id == council_id, ExistingScheme.scheme_type == "PBSA")
                   .group_by(ExistingScheme.verification_status).all())
    records = dict(db.query(SchemeSourceRecord.status, func.count())
                   .outerjoin(ExistingScheme, ExistingScheme.id == SchemeSourceRecord.scheme_id)
                   .filter((ExistingScheme.council_id == council_id) | (SchemeSourceRecord.scheme_id.is_(None)))
                   .group_by(SchemeSourceRecord.status).all())
    return {"schemes": schemes, "source_records": records}
