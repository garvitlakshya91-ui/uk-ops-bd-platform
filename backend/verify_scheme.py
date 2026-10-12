"""Analyst actions on the scheme registry.

    python verify_scheme.py --city Birmingham --queue
    python verify_scheme.py --id 85624 --status verified --by LG --note "operator page 556 beds, matches benchmark"
    python verify_scheme.py --id 85624 --shared-postcode-ok --by LG
    python verify_scheme.py --id 45011 --duplicate-of 85624 --by LG
    python verify_scheme.py --record 123 --link 85624 --by LG       # confirm a source record's scheme
    python verify_scheme.py --record 123 --reject --by LG
    python verify_scheme.py --city Birmingham --app 2023/02925/PA --beds 759 --by LG \
        --note "benchmark pack and committee report both state 759"   # analyst-set pipeline count
"""
from __future__ import annotations

import argparse
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme, PlanningApplication, SchemeSourceRecord
from app.registry.core import VERIFICATION_STATUSES, add_alias, mark_verified, queue_counts, retire_duplicate


def show_queue(db, council: Council) -> None:
    counts = queue_counts(db, council.id)
    print(f"{council.name}: schemes {counts['schemes']}; source records {counts['source_records']}")
    print("\n-- schemes needing a decision (disputed, then probable by beds)")
    rows = (db.query(ExistingScheme)
            .filter(ExistingScheme.council_id == council.id, ExistingScheme.scheme_type == "PBSA",
                    ExistingScheme.operating_status.isnot(None),
                    ExistingScheme.verification_status.in_(("disputed", "probable", "unverified")))
            .all())
    rows.sort(key=lambda s: (s.verification_status != "disputed", -(s.beds_total or 0)))
    for s in rows[:80]:
        print(f"  #{s.id:<6} {s.verification_status:10s} {s.name[:34]:34s} {s.postcode or '':9s} "
              f"{s.beds_total or 0:5d}  {(s.verification_note or '')[:70]}")
    print("\n-- unmatched or proposed source records")
    recs = (db.query(SchemeSourceRecord)
            .filter(SchemeSourceRecord.status.in_(("unmatched", "proposed")))
            .order_by(SchemeSourceRecord.source, SchemeSourceRecord.raw_name).all())
    for r in recs[:80]:
        prop = f" -> proposed #{r.proposed_scheme_id} ({r.proposal_confidence})" if r.proposed_scheme_id else ""
        print(f"  rec {r.id:<6} {r.source:20s} {str(r.raw_name)[:34]:34s} {r.raw_postcode or '':9s} "
              f"{str(r.raw_operator or '')[:24]}{prop}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city")
    ap.add_argument("--queue", action="store_true")
    ap.add_argument("--id", type=int)
    ap.add_argument("--status", choices=VERIFICATION_STATUSES)
    ap.add_argument("--note")
    ap.add_argument("--by", default=os.environ.get("USERNAME") or os.environ.get("USER") or "analyst")
    ap.add_argument("--shared-postcode-ok", action="store_true")
    ap.add_argument("--duplicate-of", type=int)
    ap.add_argument("--alias", help="add a name the scheme is known by")
    ap.add_argument("--record", type=int, help="a scheme_source_records id")
    ap.add_argument("--link", type=int, help="scheme id to link the record to")
    ap.add_argument("--reject", action="store_true", help="the record describes no scheme we track")
    ap.add_argument("--app", help="a planning reference (with --city) whose bed count an analyst sets")
    ap.add_argument("--beds", type=int, help="the bed count for --app")
    ap.add_argument("--admit", action="store_true",
                    help="bring --id into the census as a live scheme (status probable unless --status given)")
    ap.add_argument("--new-scheme", action="store_true",
                    help="with --record and --city: create a scheme from the record, outside the census, for review")
    args = ap.parse_args()

    db = SessionLocal()
    council = None
    if args.queue or args.app or args.new_scheme:
        council = db.query(Council).filter(Council.name.ilike(args.city or "")).first()
        if council is None:
            raise SystemExit("--queue, --app and --new-scheme need --city")
    if args.queue:
        show_queue(db, council)
        db.close()
        return
    if args.app:
        if args.beds is None:
            raise SystemExit("--app needs --beds")
        a = (db.query(PlanningApplication)
             .filter(PlanningApplication.council_id == council.id, PlanningApplication.reference == args.app).first())
        if a is None:
            raise SystemExit(f"{args.app} not found in {council.name}")
        # An analyst's count outranks every machine read and is never re-derived.
        a.pbsa_beds, a.beds_basis, a.beds_extractor, a.beds_confidence = args.beds, "stated", "manual", 1.0
        a.beds_quote = f"set by {args.by} {datetime.date.today()}: {args.note or 'no note'}"[:2000]
        db.commit()
        print(f"{a.reference}: beds set to {args.beds} by {args.by}")
        db.close()
        return

    if args.record:
        rec = db.get(SchemeSourceRecord, args.record)
        if rec is None:
            raise SystemExit(f"record {args.record} not found")
        rec.confirmed_by, rec.confirmed_at = args.by, datetime.datetime.now(datetime.timezone.utc)
        if args.new_scheme:
            if not args.city:
                raise SystemExit("--new-scheme needs --city")
            from app.models.models import Company
            from app.registry.core import alias_norm
            operator = None
            if rec.raw_operator:
                key = re.sub(r"[^a-z0-9]+", "", rec.raw_operator.lower())
                operator = (db.query(Company)
                            .filter((Company.normalized_name == key) | (Company.name.ilike(rec.raw_operator.strip())))
                            .first())
                if operator is None:
                    operator = Company(name=rec.raw_operator.strip(), normalized_name=key,
                                       company_type="Operator", is_active=True)
                    db.add(operator)
                    db.flush()
            s = ExistingScheme(
                name=rec.raw_name or "Unnamed scheme", postcode=rec.raw_postcode, address=rec.raw_address,
                council_id=council.id, scheme_type="PBSA", status="operational",
                operator_company_id=operator.id if operator else None,
                source=rec.source, source_reference=rec.source_reference,
                verification_note=(f"created {datetime.date.today()} by {args.by} from {rec.source} record "
                                   f"{rec.id}; not in the census until admitted: {args.note or ''}")[:1000],
            )
            db.add(s)
            db.flush()
            add_alias(db, s.id, s.name, "registry")
            rec.scheme_id, rec.status, rec.match_method, rec.match_score = s.id, "matched", "manual", 1.0
            print(f"scheme #{s.id} {s.name!r} created from record {rec.id} (outside the census; "
                  f"admit with --id {s.id} --admit)")
        elif args.link:
            rec.scheme_id, rec.status, rec.match_method, rec.match_score = args.link, "matched", "manual", 1.0
            add_alias(db, args.link, rec.raw_name, rec.source)
            print(f"record {rec.id} linked to #{args.link}")
        elif args.reject:
            rec.scheme_id, rec.status = None, "rejected"
            print(f"record {rec.id} rejected")
        else:
            raise SystemExit("--record needs --link <scheme id> or --reject")
        db.commit()
        db.close()
        return

    if not args.id:
        raise SystemExit("give --id, --record or --city --queue")
    s = db.get(ExistingScheme, args.id)
    if s is None:
        raise SystemExit(f"scheme {args.id} not found")
    if args.duplicate_of:
        survivor = db.get(ExistingScheme, args.duplicate_of)
        if survivor is None:
            raise SystemExit(f"scheme {args.duplicate_of} not found")
        retire_duplicate(db, s, survivor, args.by, args.note)
        print(f"#{s.id} {s.name} retired into #{survivor.id} {survivor.name}")
    if args.alias:
        print(f"alias {'added' if add_alias(db, s.id, args.alias, 'manual') else 'already present'}")
    if args.shared_postcode_ok:
        s.shared_postcode_ok = True
        if args.note and not args.status:
            s.verification_note = f"{args.note} ({args.by}, {datetime.date.today()})"[:1000]
        print(f"#{s.id} marked distinct at shared postcode {s.postcode}")
    if args.admit:
        s.operating_status = s.operating_status or "live"
        s.status = "operational"
        if not args.status:
            mark_verified(db, s, "probable", args.by, args.note)
        print(f"#{s.id} {s.name}: admitted to the census as {s.operating_status}")
    if args.status:
        mark_verified(db, s, args.status, args.by, args.note)
        print(f"#{s.id} {s.name}: {args.status} by {args.by}")
    db.commit()
    db.close()


if __name__ == "__main__":
    main()
