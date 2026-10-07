"""Investment context: transactions, published yields, valuation sensitivity.

Transactions and yields are observed/cited. The sensitivity is a
forecast built from our own rent captures and stated assumptions, so
an acquisitions team can see what the market's rents imply per bed at
a range of yields — and change the assumptions.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.models import Transaction, YieldBenchmark

# Underwriting assumptions (forecast inputs, stated in the report)
TENANCY_WEEKS = 51
OCCUPANCY = 0.97          # BONARD 2024 sample average, cited as assumption
OPEX_RATIO = 0.35         # gross-to-net, typical direct-let PBSA
YIELD_GRID = [5.0, 5.5, 6.0, 6.5, 7.0]


def gather_finance_context(db: Session, council_id: int, median_rent_ppw: float | None,
                           private_beds: int) -> dict:
    deals = (
        db.query(Transaction)
        .filter(Transaction.council_id == council_id)
        .order_by(Transaction.transaction_date.desc().nullslast())
        .limit(12).all()
    )
    transactions = [{
        "asset": d.asset_name,
        "date": d.transaction_date.isoformat() if d.transaction_date else None,
        "price_gbp": float(d.price_gbp) if d.price_gbp is not None else None,
        "beds": d.beds,
        "price_per_bed": float(d.price_per_bed_gbp) if d.price_per_bed_gbp is not None else None,
        "buyer": d.buyer, "seller": d.seller, "yield_pct": d.yield_pct,
        "built": d.asset_built_year, "deal_type": d.deal_type,
        "source": d.source, "basis": d.basis,
    } for d in deals]
    priced = [t for t in transactions if t["price_per_bed"]]
    avg_ppb = round(sum(t["price_per_bed"] for t in priced) / len(priced)) if priced else None

    latest_as_of = db.query(text("MAX(as_of)")).select_from(YieldBenchmark).scalar()
    yields = []
    if latest_as_of:
        yields = [{"segment": y.segment.replace("_", " "), "yield_pct": y.yield_pct,
                   "as_of": y.as_of.isoformat(), "source": y.source}
                  for y in db.query(YieldBenchmark).filter(YieldBenchmark.as_of == latest_as_of)
                  .order_by(YieldBenchmark.yield_pct).all()]

    sensitivity = None
    if median_rent_ppw:
        gross_per_bed = median_rent_ppw * TENANCY_WEEKS * OCCUPANCY
        noi_per_bed = gross_per_bed * (1 - OPEX_RATIO)
        sensitivity = {
            "median_rent_ppw": median_rent_ppw,
            "gross_per_bed": round(gross_per_bed),
            "noi_per_bed": round(noi_per_bed),
            "grid": [{"yield_pct": y, "value_per_bed": round(noi_per_bed / (y / 100)),
                      "market_value_m": round(noi_per_bed / (y / 100) * private_beds / 1e6)}
                     for y in YIELD_GRID],
            "assumptions": {"tenancy_weeks": TENANCY_WEEKS, "occupancy": OCCUPANCY,
                            "opex_ratio": OPEX_RATIO, "private_beds": private_beds},
        }
    return {"transactions": transactions, "avg_price_per_bed": avg_ppb,
            "yields": yields, "sensitivity": sensitivity}
