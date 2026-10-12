"""Evidence annex: every census figure with its source, date and standing.

Answers "your figures are wrong" line by line. For each census scheme the
annex shows the bed count, where it came from and when, the registry's
verification state and anything that keeps it out of the verified total.
Each licensed benchmark pack on disk is reconciled against the census:
matched schemes, schemes the pack has that we lack, ours it lacks, and
bed counts that disagree. Totals are split verified / probable /
unresolved so a reader can see how much of the headline rests on what.

Benchmark packs are QA references: in a publication build their
figures appear only as a relative delta, never as a number.
"""
from __future__ import annotations

import datetime
import glob
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from app.census_sources import PUBLISHABLE_SOURCES
from app.extraction.units import extract_units
from app.models.models import ExistingScheme, PlanningApplication, SchemeSourceRecord
from app.scrapers.scheme_matching import best_match, build_index, norm_pc

DATA = Path(__file__).resolve().parent.parent / "data"
BENCHMARK_TOLERANCE = 0.05      # scheme-level agreement
DISPUTE_TOLERANCE = 0.10        # beyond this a scheme is unresolved
LIVE_STATUSES = ("under_construction", "pre_letting", "active_consent", "consented_full", "consented_outline")


def _date(entry: Optional[dict], scheme: ExistingScheme) -> Optional[str]:
    at = (entry or {}).get("at")
    if at:
        return str(at)[:10]
    for ts in (scheme.last_verified_at, scheme.updated_at):
        if ts:
            return ts.date().isoformat()
    return None


def load_benchmark_packs(slug: str) -> list[dict]:
    packs = []
    for f in sorted(glob.glob(str(DATA / "benchmarks" / f"{slug}_*.json"))):
        pack = json.loads(Path(f).read_text(encoding="utf-8"))
        pack["_file"] = Path(f).name
        packs.append(pack)
    return packs


def _benchmark_links(db: Session, scheme_ids: list[int]) -> dict[str, dict]:
    """benchmark row key -> {scheme_id, payload} from the registry's source records."""
    out = {}
    for r in (db.query(SchemeSourceRecord)
              .filter(SchemeSourceRecord.source == "benchmark_report",
                      SchemeSourceRecord.status == "matched",
                      SchemeSourceRecord.scheme_id.in_(scheme_ids or [0]))):
        out[r.source_reference] = {"scheme_id": r.scheme_id, "payload": r.payload or {}}
    return out


def reconcile_pack(db: Session, pack: dict, schemes: list[ExistingScheme], beds_of,
                   publication: bool) -> dict:
    """One benchmark pack against the private census."""
    label = f"{pack.get('producer')} {pack.get('report_date')}"
    links = _benchmark_links(db, [s.id for s in schemes])
    by_id = {s.id: s for s in schemes}
    index = build_index(schemes)
    matched, missing, mismatches = [], [], []
    used: set[int] = set()
    for row in pack.get("private_stock") or []:
        key = f"{label}|private_stock|{row.get('property')}|{row.get('postcode') or ''}"
        scheme = by_id.get((links.get(key) or {}).get("scheme_id"))
        if scheme is None:
            scheme, _ = best_match(index, row.get("property") or "", row.get("postcode"), used=used)
        if scheme is None:
            missing.append({"property": row.get("property"), "postcode": row.get("postcode"),
                            "operator": row.get("operator"), "beds": None if publication else row.get("beds")})
            continue
        used.add(scheme.id)
        ours, theirs = beds_of(scheme), row.get("beds")
        entry = {"scheme_id": scheme.id, "name": scheme.name, "property": row.get("property"),
                 "ours": ours, "theirs": None if publication else theirs, "delta_pct": None, "agree": None}
        if ours and theirs:
            entry["delta_pct"] = round(100 * (ours - theirs) / theirs, 1)
            entry["agree"] = abs(ours - theirs) <= max(5, BENCHMARK_TOLERANCE * theirs)
            if not entry["agree"]:
                mismatches.append(entry)
        matched.append(entry)
    ours_only = [{"scheme_id": s.id, "name": s.name, "postcode": s.postcode, "beds": beds_of(s)}
                 for s in schemes if s.id not in used]
    our_total = sum(e["ours"] or 0 for e in matched)
    their_total = sum((r.get("beds") or 0) for r in pack.get("private_stock") or [])
    our_full_total = sum(beds_of(s) or 0 for s in schemes)
    delta = round(100 * (our_full_total - their_total) / their_total, 1) if their_total else None
    return {
        "label": label, "file": pack["_file"], "license_note": pack.get("license_note"),
        "pack_schemes": len(pack.get("private_stock") or []),
        "pack_beds": None if publication else their_total,
        "matched": len(matched), "missing_from_ours": missing, "ours_not_in_pack": ours_only,
        "mismatches": sorted(mismatches, key=lambda e: -abs(e["delta_pct"] or 0)),
        "agreeing": sum(1 for e in matched if e["agree"]),
        "our_total": our_full_total, "our_matched_total": our_total,
        "total_delta_pct": delta,
        "within_tolerance": delta is not None and abs(delta) <= 100 * BENCHMARK_TOLERANCE,
        "per_scheme": {e["scheme_id"]: e for e in matched},
    }


def gather_evidence(db: Session, council, census: list[ExistingScheme], apps: list[PlanningApplication],
                    operators: dict, publication: bool, slug: str,
                    today: Optional[datetime.date] = None) -> dict:
    """The annex context: per-scheme evidence, tiers, reconciliations, flags."""
    today = today or datetime.date.today()

    def beds_of(s: ExistingScheme) -> Optional[int]:
        entry = (s.field_provenance or {}).get("beds_total") or {}
        if publication:
            return entry.get("value") if entry.get("source") in PUBLISHABLE_SOURCES else None
        return s.beds_total or s.num_units or s.total_units

    packs = [reconcile_pack(db, p, census, beds_of, publication) for p in load_benchmark_packs(slug)]

    # Shared postcodes inside the census
    by_pc: dict[str, list[ExistingScheme]] = defaultdict(list)
    for s in census:
        if s.postcode:
            by_pc[norm_pc(s.postcode)].append(s)
    shared = [{"postcode": grp[0].postcode, "schemes": [g.name for g in grp],
               "confirmed": all(g.shared_postcode_ok for g in grp)}
              for pc, grp in by_pc.items() if len(grp) > 1]
    unconfirmed_pc = {g.id for grp in by_pc.values() if len(grp) > 1
                      for g in grp if not g.shared_postcode_ok}

    rows, totals = [], {t: {"schemes": 0, "beds": 0} for t in ("verified", "probable", "unresolved")}
    for s in census:
        prov = s.field_provenance or {}
        entry = prov.get("beds_total") or {}
        beds = beds_of(s)
        source = entry.get("source") or s.source or "unknown"
        issues = []
        if beds is None:
            issues.append("no bed count from a source we may publish" if publication else "no bed count")
        elif source not in PUBLISHABLE_SOURCES:
            issues.append(f"bed count from {source} only")
        if s.verification_status == "disputed":
            issues.append("sources disagree: " + (s.verification_note or "")[:80])
        elif s.verification_status in ("unverified", "retired"):
            issues.append(f"registry: {s.verification_status}")
        if s.id in unconfirmed_pc:
            issues.append(f"shares postcode {s.postcode}, not confirmed distinct")
        for p in packs:
            e = p["per_scheme"].get(s.id)
            if e and e["agree"] is False and abs(e["delta_pct"] or 0) > 100 * DISPUTE_TOLERANCE:
                issues.append(f"{p['label']}: {e['delta_pct']:+.0f}% vs benchmark")
        quote = entry.get("quote")
        if quote and extract_units(quote).beds not in (None, entry.get("value")):
            issues.append("stored count differs from its quoted phrase")
        age = None
        ref_date = s.verified_at or s.last_verified_at
        if ref_date:
            age = (today - ref_date.date()).days
        if s.verification_status == "verified" and not issues:
            tier = "verified"
        elif issues and any(k in i for i in issues for k in ("no bed count", "disagree", "unverified", "retired",
                                                             "not confirmed", "vs benchmark", "quoted", "only")):
            tier = "unresolved"
        else:
            tier = "probable"
        totals[tier]["schemes"] += 1
        totals[tier]["beds"] += beds or 0
        bench = {p["label"]: p["per_scheme"].get(s.id) for p in packs}
        rows.append({
            "id": s.id, "name": s.name, "postcode": s.postcode,
            "operator": (prov.get("operator") or {}).get("value") or operators.get(s.operator_company_id),
            "beds": beds, "beds_source": source, "beds_date": _date(entry, s),
            "beds_confidence": entry.get("confidence"), "beds_ref": entry.get("ref"),
            "build_year": s.build_year, "build_year_source": (prov.get("build_year") or {}).get("source"),
            "status": s.operating_status, "verification": s.verification_status,
            "verified_by": s.verified_by, "verified_at": s.verified_at.date().isoformat() if s.verified_at else None,
            "age_days": age, "tier": tier, "issues": issues,
            "benchmark": {k: (v and {"delta_pct": v["delta_pct"], "agree": v["agree"], "theirs": v["theirs"]})
                          for k, v in bench.items()},
        })
    order = {"unresolved": 0, "probable": 1, "verified": 2}
    rows.sort(key=lambda r: (order[r["tier"]], -(r["beds"] or 0)))
    all_beds = sum(t["beds"] for t in totals.values())
    for t in totals.values():
        t["share"] = round(100 * t["beds"] / all_beds, 1) if all_beds else 0

    # Pipeline: stored counts the description does not support
    contradictions = []
    for a in apps:
        if a.delivery_status not in LIVE_STATUSES + ("submitted",) or not a.pbsa_beds:
            continue
        if a.beds_extractor in ("manual", "ai"):
            continue
        ex = extract_units(a.description or "")
        fig = (ex.maximum("beds") or ex.best("beds")) if a.permission_type in ("outline", "hybrid") \
            else (ex.best("beds") or ex.maximum("beds"))
        if a.beds_extractor == "unknown" or (fig and fig.value != a.pbsa_beds):
            contradictions.append({"reference": a.reference, "address": (a.address or "")[:50],
                                   "stored": a.pbsa_beds, "basis": a.beds_basis,
                                   "extracted": fig.value if fig else None,
                                   "quote": (fig.quote if fig else None) or "no figure in the description"})
    stale_pipeline = [{"reference": a.reference, "status": a.delivery_status,
                       "status_at": a.delivery_status_at.isoformat() if a.delivery_status_at else None,
                       "beds": a.pbsa_beds}
                      for a in apps if a.delivery_status in LIVE_STATUSES
                      and (a.delivery_status_at is None or (today - a.delivery_status_at).days > 365)]

    return {
        "rows": rows, "totals": totals, "total_beds": all_beds,
        "packs": [{k: v for k, v in p.items() if k != "per_scheme"} for p in packs],
        "shared_postcodes": shared, "pipeline_contradictions": contradictions,
        "stale_pipeline": stale_pipeline, "publication": publication,
        "generated": today.isoformat(),
    }
