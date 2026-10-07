"""Derive PBSA transactions and owner changes from Land Registry corporate
titles (CCOD/OCOD, table title_ownership loaded by hmlr_ingest.py).

For every PBSA scheme, titles at the scheme's postcode held by a company
give:
  - a TRANSACTION where the title carries a price paid: date = the date
    the proprietor was registered, price = price paid, buyer = the
    proprietor. Titles bought by the same proprietor on the same date are
    one deal (a scheme often spans several titles), prices summed.
  - an OWNER_CHANGE event where the registered proprietor differs between
    monthly files (or from the scheme's recorded owner).

Basis is "derived": CCOD price paid is the price at registration of the
current proprietor, which may be a site purchase before construction
rather than a trade of the finished asset — the report says so. £/bed
uses the scheme's published bed count.

Usage:
    python derive_transactions_from_titles.py [--council Birmingham] [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text

from app.census_sources import published_value
from app.database import SessionLocal
from app.models.models import Council, ExistingScheme, SchemeEvent, Transaction

SOURCE = "hmlr_ccod"
# A PBSA title sale below this is a flat or a strip of land, not the scheme.
MIN_SCHEME_PRICE = 1_000_000


def pc_key(postcode: str | None) -> str:
    return re.sub(r"\s+", "", (postcode or "").upper())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    if db.execute(text("SELECT to_regclass('title_ownership')")).scalar() is None:
        raise SystemExit("title_ownership not present — run hmlr_ingest.py "
                         "(production database) first")

    q = db.query(ExistingScheme).filter(ExistingScheme.scheme_type == "PBSA",
                                        ExistingScheme.postcode.isnot(None))
    if args.council:
        council = db.query(Council).filter(Council.name.ilike(args.council)).first()
        if council is None:
            raise SystemExit(f"Council {args.council!r} not found")
        q = q.filter(ExistingScheme.council_id == council.id)
    schemes = q.all()
    by_pc: dict[str, list[ExistingScheme]] = defaultdict(list)
    for s in schemes:
        by_pc[pc_key(s.postcode)].append(s)
    print(f"{len(schemes)} PBSA schemes at {len(by_pc)} postcodes")

    titles = db.execute(text("""
        SELECT title_number, pc_key, price_paid, proprietor_name_1, ch_number_1,
               country_1, date_added, file_month, source
        FROM title_ownership
        WHERE pc_key = ANY(:keys)
        ORDER BY title_number, file_month
    """), {"keys": list(by_pc)}).fetchall()
    print(f"{len(titles)} corporate titles at those postcodes")

    # Latest file per title = current proprietor; earlier files give history.
    latest: dict[str, tuple] = {}
    history: dict[str, list[tuple]] = defaultdict(list)
    for t in titles:
        history[t[0]].append(t)
        latest[t[0]] = t

    deals: dict[tuple, dict] = {}
    events = 0
    for title_number, t in latest.items():
        _, pc, price, proprietor, ch_no, country, date_added, file_month, src = t
        for s in by_pc.get(pc, []):
            # Owner change: proprietor differs across monthly files
            prev = [h for h in history[title_number] if h[7] != file_month]
            if prev and (prev[-1][3] or "").strip().lower() != (proprietor or "").strip().lower():
                events += 1
                if not args.dry_run:
                    exists = db.query(SchemeEvent).filter_by(
                        scheme_id=s.id, event_type="owner_change",
                        source_reference=title_number, event_date=date_added).first()
                    if not exists:
                        db.add(SchemeEvent(
                            scheme_id=s.id, event_type="owner_change", event_date=date_added,
                            detail={"from": prev[-1][3], "to": proprietor, "ch_number": ch_no,
                                    "country": country, "title": title_number},
                            source=SOURCE, source_reference=title_number))
            if price and price >= MIN_SCHEME_PRICE and date_added:
                key = (s.id, (proprietor or "").strip().lower(), date_added)
                d = deals.setdefault(key, {"scheme": s, "buyer": proprietor, "ch": ch_no,
                                           "country": country, "date": date_added,
                                           "price": 0, "titles": []})
                d["price"] += int(price)
                d["titles"].append(title_number)

    created = 0
    for d in deals.values():
        s = d["scheme"]
        ref = ",".join(sorted(d["titles"]))[:480]
        exists = db.query(Transaction).filter_by(
            scheme_id=s.id, transaction_date=d["date"], source=SOURCE).first()
        if exists:
            continue
        beds = published_value(s, "beds_total") or s.beds_total
        created += 1
        print(f"  {s.name[:34]:34} {d['date']} £{d['price']:>12,}  {d['buyer'][:40]}"
              + (f"  £{d['price'] // beds:,}/bed" if beds else ""))
        if not args.dry_run:
            db.add(Transaction(
                scheme_id=s.id, council_id=s.council_id, asset_name=s.name,
                transaction_date=d["date"], price_gbp=d["price"], beds=beds,
                price_per_bed_gbp=round(d["price"] / beds) if beds else None,
                buyer=d["buyer"], seller=None, deal_type="title_registration",
                basis="derived", source=SOURCE, source_reference=ref))
    if not args.dry_run:
        db.commit()
    db.close()
    print(f"\n{'would create' if args.dry_run else 'created'} {created} transactions, "
          f"{events} owner-change events")


if __name__ == "__main__":
    main()
