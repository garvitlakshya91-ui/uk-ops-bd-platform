"""Score the unit extractor against the Birmingham gold set.

Two truths are available for a city with a licensed benchmark pack:

  pipeline   the pack's pipeline rows carry a planning reference and the
             approved bed count; the extractor reads our copy of that
             application's description and is scored against the pack.
  census     schemes with both an own-source page reading (operator_page
             observation) and a benchmark bed count; agreement is reported
             by confidence band, which is what justifies the cache bar.

Nothing is written to the database. A JSON summary goes to
data/validation_<slug>_<date>.json for the record.

Usage:
    python validate_extraction.py --city Birmingham
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text

from app.database import SessionLocal
from app.extraction.units import extract_units
from app.models.models import Council, PlanningApplication, SchemeSourceRecord
from app.models.observations import CACHE_MIN_CONFIDENCE
from report.evidence import load_benchmark_packs

TOL = 0.05


def close(a, b) -> bool:
    return a is not None and b is not None and abs(a - b) <= max(5, TOL * b)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--slug", default=None)
    args = ap.parse_args()
    slug = args.slug or args.city.lower().replace(" ", "-")
    db = SessionLocal()
    db.execute(text("set transaction read only"))
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    packs = load_benchmark_packs(slug)
    if not packs:
        raise SystemExit(f"no benchmark pack for {slug} under data/benchmarks")
    pack = packs[-1]
    print(f"{council.name} against {pack.get('producer')} {pack.get('report_date')}")

    # ---------------------------------------------------------------- pipeline
    print("\n== pipeline: extractor vs the pack's approved bed counts")
    rows, tally = [], Counter()
    for r in pack.get("pipeline") or []:
        ref, truth = r.get("planning_ref"), r.get("beds_approved") or r.get("beds")
        if not ref or not truth:
            continue
        a = (db.query(PlanningApplication)
             .filter(PlanningApplication.council_id == council.id,
                     PlanningApplication.reference.ilike(ref.strip())).first())
        if a is None:
            tally["not_in_register"] += 1
            rows.append({"ref": ref, "truth": truth, "outcome": "not in our register"})
            continue
        ex = extract_units(a.description or "")
        fig = ex.best("beds") or ex.maximum("beds")
        pred = fig.value if fig else None
        if pred is None:
            outcome = "no figure"
        elif close(pred, truth):
            outcome = "agree"
        elif fig.is_additional:
            outcome = "increment"     # the text states what an extension adds, the pack the total
        else:
            outcome = "wrong"
        tally[outcome] += 1
        if fig and fig.is_maximum:
            tally["envelope"] += 1
        # The stored count is what the report uses: descriptions, documents,
        # the model and analysts all feed it.
        if a.pbsa_beds:
            tally["stored_agree" if close(a.pbsa_beds, truth) else "stored_wrong"] += 1
            if not close(a.pbsa_beds, truth):
                print(f"      stored {a.pbsa_beds} ({a.beds_extractor or 'unknown'}) disagrees with pack {truth}")
        else:
            tally["stored_missing"] += 1
        rows.append({"ref": ref, "site": r.get("site"), "truth": truth, "extracted": pred,
                     "confidence": fig.confidence if fig else None, "stored": a.pbsa_beds,
                     "stored_basis": a.beds_basis, "outcome": outcome,
                     "quote": (fig.quote if fig else None), "either_or": ex.either_or})
        flag = "" if outcome == "agree" else "  <--"
        print(f"  {ref:16s} pack {truth:5d}  extractor {str(pred or '-'):>5s} "
              f"({fig.confidence if fig else '-'})  stored {str(a.pbsa_beds or '-'):>5s} {a.beds_basis or '':9s} "
              f"{outcome}{flag}  {str(fig.quote if fig else '')[:40]}")
    scored = tally["agree"] + tally["wrong"]
    with_truth = scored + tally["no figure"] + tally["increment"]
    print(f"\n  {with_truth} applications with a pack count in our register; "
          f"extractor read {scored}: {tally['agree']} agree, {tally['wrong']} wrong "
          f"({tally['envelope']} read as envelopes); {tally['increment']} extension increments; "
          f"{tally['no figure']} no figure; {tally['not_in_register']} not in our register")
    if scored:
        print(f"  description read: precision {100 * tally['agree'] / scored:.0f}%  "
              f"recall {100 * tally['agree'] / with_truth:.0f}%")
    stored = tally["stored_agree"] + tally["stored_wrong"]
    if stored:
        print(f"  stored count (description + documents + model + analyst): "
              f"{tally['stored_agree']}/{stored} agree ({100 * tally['stored_agree'] / stored:.0f}%), "
              f"{tally['stored_missing']} still missing")

    # ------------------------------------------------------------------ census
    print("\n== census: own-source page readings vs the pack, by confidence")
    bench = {}
    for rec in (db.query(SchemeSourceRecord)
                .filter(SchemeSourceRecord.source == "benchmark_report",
                        SchemeSourceRecord.status == "matched",
                        SchemeSourceRecord.scheme_id.isnot(None))):
        beds = (rec.payload or {}).get("beds")
        if beds and "private_stock" in rec.source_reference:
            bench[rec.scheme_id] = int(beds)
    obs = db.execute(text("""
        SELECT DISTINCT ON (o.scheme_id) o.scheme_id, es.name, o.value_num, o.confidence, o.quote, o.extractor
        FROM scheme_observations o JOIN existing_schemes es ON es.id = o.scheme_id
        WHERE es.council_id = :cid AND o.field = 'beds_total' AND o.source = 'operator_page'
        ORDER BY o.scheme_id, o.observed_at DESC
    """), {"cid": council.id}).fetchall()
    bands = {"firm (>= bar)": Counter(), "below bar": Counter(), "unscored": Counter()}
    census_rows = []
    for sid, name, val, conf, quote, extractor in obs:
        if sid not in bench:
            continue
        val = int(val)
        band = "unscored" if conf is None else ("firm (>= bar)" if conf >= CACHE_MIN_CONFIDENCE else "below bar")
        ok = close(val, bench[sid])
        bands[band]["agree" if ok else "disagree"] += 1
        census_rows.append({"scheme_id": sid, "name": name, "ours": val, "pack": bench[sid],
                            "confidence": conf, "quote": quote, "agree": ok})
        print(f"  {name[:30]:30s} ours {val:5d} pack {bench[sid]:5d} conf {str(conf or '-'):>5s} "
              f"{'agree' if ok else 'DISAGREE'}  {str(quote or '')[:50]}")
    for band, c in bands.items():
        n = c["agree"] + c["disagree"]
        if n:
            print(f"  {band:14s} {c['agree']}/{n} agree ({100 * c['agree'] / n:.0f}%)")
    if not census_rows:
        print("  no scheme has both a page reading and a pack count yet")

    out = Path(__file__).parent / "data" / f"validation_{slug}_{datetime.date.today():%Y%m%d}.json"
    out.write_text(json.dumps({"city": council.name, "pack": f"{pack.get('producer')} {pack.get('report_date')}",
                               "pipeline": rows, "pipeline_tally": dict(tally),
                               "census": census_rows, "cache_bar": CACHE_MIN_CONFIDENCE}, indent=1),
                   encoding="utf-8")
    print(f"\nwritten {out.name}")
    db.close()


if __name__ == "__main__":
    main()
