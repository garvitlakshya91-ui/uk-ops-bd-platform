"""Load PBSA transactions and published yield benchmarks.

Transactions come from a CSV we maintain (deal announcements, Land
Registry corporate-title price paid, agent round-ups), one row per
deal: asset_name, date, price_gbp, beds, buyer, seller, yield_pct,
built_year, deal_type, city, source, source_reference. Each row is
matched to a scheme by name/postcode when it can be.

Yield benchmarks are published headline figures and are stored with
their source so the report cites them, never claims them.

Usage:
    python load_transactions.py --csv data/transactions.csv
    python load_transactions.py --seed-yields
"""
from __future__ import annotations

import argparse
import csv
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal
from app.models.models import Council, ExistingScheme, Transaction, YieldBenchmark
from app.scrapers.scheme_matching import best_match, build_index

# Published UK PBSA prime yields, Q4 2025, from Cushman & Wakefield's
# UK Student Accommodation Report 2025 (public). Cited as context only.
PUBLISHED_YIELDS = [
    ("prime_london", 4.25), ("super_prime_regional", 5.25),
    ("prime_regional", 5.50), ("secondary", 6.75),
]
YIELD_SOURCE = "Cushman & Wakefield, UK Student Accommodation Report 2025 (published)"
YIELD_AS_OF = datetime.date(2025, 12, 31)


def to_int(v):
    try:
        return int(str(v).replace(",", "").replace("£", "").strip())
    except (TypeError, ValueError):
        return None


def to_float(v):
    try:
        return float(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def to_date(v):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%b %Y", "%B %Y", "%Y"):
        try:
            return datetime.datetime.strptime(str(v).strip(), fmt).date()
        except (TypeError, ValueError):
            continue
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv")
    ap.add_argument("--seed-yields", action="store_true")
    args = ap.parse_args()
    db = SessionLocal()

    if args.seed_yields:
        n = 0
        for segment, pct in PUBLISHED_YIELDS:
            if not db.query(YieldBenchmark).filter_by(
                    segment=segment, as_of=YIELD_AS_OF, source=YIELD_SOURCE).first():
                db.add(YieldBenchmark(segment=segment, yield_pct=pct,
                                      as_of=YIELD_AS_OF, source=YIELD_SOURCE))
                n += 1
        db.commit()
        print(f"yield benchmarks seeded: {n}")

    if args.csv:
        councils = {c.name.lower(): c for c in db.query(Council).all()}
        indexes: dict[int, list] = {}
        n = 0
        with open(args.csv, newline="", encoding="utf-8-sig") as fh:
            for rec in csv.DictReader(fh):
                council = councils.get((rec.get("city") or "").strip().lower())
                scheme = None
                if council:
                    if council.id not in indexes:
                        indexes[council.id] = build_index(
                            db.query(ExistingScheme)
                            .filter(ExistingScheme.council_id == council.id).all())
                    scheme, _ = best_match(indexes[council.id], rec.get("asset_name", ""),
                                           rec.get("postcode"))
                price, beds = to_int(rec.get("price_gbp")), to_int(rec.get("beds"))
                exists = db.query(Transaction).filter_by(
                    asset_name=rec["asset_name"].strip(),
                    transaction_date=to_date(rec.get("date"))).first()
                if exists:
                    continue
                db.add(Transaction(
                    scheme_id=scheme.id if scheme else None,
                    council_id=council.id if council else None,
                    asset_name=rec["asset_name"].strip(),
                    transaction_date=to_date(rec.get("date")),
                    price_gbp=price, beds=beds,
                    price_per_bed_gbp=round(price / beds) if price and beds else None,
                    buyer=rec.get("buyer") or None, seller=rec.get("seller") or None,
                    yield_pct=to_float(rec.get("yield_pct")),
                    asset_built_year=to_int(rec.get("built_year")),
                    deal_type=rec.get("deal_type") or None,
                    source=rec.get("source") or "deal_announcement",
                    source_reference=rec.get("source_reference") or None,
                ))
                n += 1
        db.commit()
        print(f"transactions loaded: {n}")
    db.close()


if __name__ == "__main__":
    main()
