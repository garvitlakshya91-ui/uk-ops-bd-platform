"""Retract observations the audit showed to be wrong in kind.

1. build_year derived from a planning decision date (0 of 7 correct in the
   Birmingham audit): a build year is read from an operator or completion
   record, never derived from a consent.
2. beds_total taken from a planning consent on a scheme that already
   existed when the consent was decided (3 of 6 wrong): a consent's bed
   count describes the consented building, not the operating scheme.
   Consent beds stay valid for a scheme created from that consent.
3. StuRents HMO "house" listings attached to PBSA schemes: superseded.

Observations are append-only, so wrong derivations are deleted outright
(they were never published) and the provenance cache is rebuilt from the
remaining publishable observations for the touched fields.

Usage: .venv/bin/python retract_derived_observations.py --city Birmingham [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime

from sqlalchemy import text
from sqlalchemy.orm.attributes import flag_modified

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme, SchemeObservation, SchemeRent

MIN_RENT_PPW = 60.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    scheme_ids = [s.id for s in db.query(ExistingScheme.id)
                  .filter(ExistingScheme.council_id == council.id)]

    # 1. derived build years
    q1 = (db.query(SchemeObservation)
          .filter(SchemeObservation.scheme_id.in_(scheme_ids),
                  SchemeObservation.field == "build_year",
                  SchemeObservation.basis == "derived"))
    n1 = q1.count()

    # 2. consent beds on schemes that pre-date the consent: the scheme's
    # own source is not that consent (source != planning_consent)
    q2 = (db.query(SchemeObservation)
          .join(ExistingScheme, ExistingScheme.id == SchemeObservation.scheme_id)
          .filter(SchemeObservation.scheme_id.in_(scheme_ids),
                  SchemeObservation.field == "beds_total",
                  SchemeObservation.source == "planning_consent",
                  ExistingScheme.source != "planning_consent"))
    n2 = q2.count()
    touched = {o.scheme_id for o in q1} | {o.scheme_id for o in q2}

    # 3. StuRents house listings and implausible rents on PBSA schemes
    bad_rents = (db.query(SchemeRent)
                 .join(ExistingScheme, ExistingScheme.id == SchemeRent.scheme_id)
                 .filter(ExistingScheme.council_id == council.id,
                         ExistingScheme.scheme_type == "PBSA",
                         SchemeRent.is_current.is_(True),
                         text("scheme_rents.source_reference LIKE '%/house/%' "
                              "OR scheme_rents.rent_per_week < :floor")).params(floor=MIN_RENT_PPW)
                 .all())
    print(f"{args.city}: retracting {n1} derived build years, {n2} consent bed counts on "
          f"pre-existing schemes; superseding {len(bad_rents)} rent rows "
          f"({len({r.scheme_id for r in bad_rents})} schemes)")
    for r in bad_rents[:12]:
        print(f"   rent  scheme {r.scheme_id} {r.source} £{r.rent_per_week} {r.source_reference}")
    if args.dry_run:
        print("(dry run)")
        return

    q1.delete(synchronize_session=False)
    for o in q2.all():
        db.delete(o)
    now = datetime.datetime.now(datetime.timezone.utc)
    for r in bad_rents:
        r.is_current, r.superseded_at = False, now

    # rebuild the cache entries for the touched fields from what remains
    for sid in touched:
        s = db.get(ExistingScheme, sid)
        prov = dict(s.field_provenance or {})
        for field in ("build_year", "beds_total"):
            entry = prov.get(field)
            if entry and entry.get("source") == "planning_consent":
                prov.pop(field, None)
        s.field_provenance = prov
        flag_modified(s, "field_provenance")
        if s.source != "planning_consent" and s.source != "benchmark_report":
            pass
    db.commit()
    print("done")


if __name__ == "__main__":
    main()
