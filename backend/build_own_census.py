"""Rebuild a city's census from our own sources, with per-field provenance.

For every PBSA scheme in the city this records, in
existing_schemes.field_provenance, only what OUR sources show:

  observed   which of our scrapers saw the scheme (rents, listings,
             operator directories, availability checks)
  operator   corroborated when one of our listings names the same operator
  beds_total from the operator/listing page text, else from a matching
             approved planning consent's parsed bed count
  build_year estimated from a matching consent (decision year + typical
             build), labelled basis="estimated_from_consent"

Values that exist only because a licensed benchmark pack supplied them
are recorded as source="benchmark_report" (QA only, never published).
A QA table compares our bed counts with the benchmark's.

Usage:
    python build_own_census.py --city Birmingham \
        --benchmark data/benchmarks/birmingham_student_source_jan26.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import text
from sqlalchemy.orm.attributes import flag_modified

from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme, PlanningApplication
from app.models.observations import record_observation
from app.scrapers.pbsa_scraper import extract_bed_count
from app.scrapers.scheme_matching import best_match, build_index, norm_name, norm_pc
from ai_room_rents import page_text

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
NOW = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def entry(value, source, ref=None, basis=None) -> dict:
    e = {"value": value, "source": source, "at": NOW}
    if ref:
        e["ref"] = ref
    if basis:
        e["basis"] = basis
    return e


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]


def fetch_text(url: str, browser) -> str | None:
    try:
        r = httpx.get(url, headers=UA, timeout=25.0, follow_redirects=True)
        if r.status_code == 200 and len(r.text) > 1500:
            return page_text(r.text, cap=60000)
    except Exception:
        pass
    if browser is not None:
        html = browser.get(url)
        if html:
            return page_text(html, cap=60000)
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--slug", default=None)
    ap.add_argument("--benchmark", default=None)
    ap.add_argument("--no-fetch", action="store_true",
                    help="skip operator-page fetching (planning only)")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--dry-run-obs", action="store_true",
                    help="do not append scheme_observations rows")
    args = ap.parse_args()
    slug = args.slug or args.city.lower()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    schemes = (db.query(ExistingScheme)
               .filter(ExistingScheme.council_id == council.id,
                       ExistingScheme.scheme_type == "PBSA").all())
    index = build_index(schemes)
    operators = {c.id: c.name for c in db.query(Company).filter(
        Company.id.in_([s.operator_company_id for s in schemes
                        if s.operator_company_id] or [0]))}

    # ---- evidence 1: our rent captures and availability checks
    observed: dict[int, set[str]] = {s.id: set() for s in schemes}
    urls: dict[int, list[str]] = {s.id: [] for s in schemes}
    for sid, source, ref in db.execute(text("""
        SELECT scheme_id, source, source_reference FROM scheme_rents
        WHERE scheme_id = ANY(:ids)
        UNION ALL
        SELECT scheme_id, source, source_reference FROM scheme_availability
        WHERE scheme_id = ANY(:ids)
    """), {"ids": list(observed)}):
        observed[sid].add(source)
        if ref and ref.startswith("http") and ref not in urls[sid]:
            urls[sid].append(ref)

    # ---- evidence 2: our listing files (existence + operator name)
    data = Path(__file__).parent / "data"
    listing_ops: dict[int, list[tuple[str, str]]] = {s.id: [] for s in schemes}
    feeds = [("afs_directory", read_jsonl(data / "afs" / f"{slug}.jsonl"), "operator"),
             ("sturents", [r for r in read_jsonl(data / "sturents" / f"{slug}.jsonl")
                           if r.get("is_pbsa_candidate")], "agent")]
    opdir = []
    for f in sorted((data / "operator_directories").glob("*.jsonl")):
        opdir += [r for r in read_jsonl(f)
                  if args.city.lower() in (r.get("city") or "").lower()]
    feeds.append(("operator_directory", opdir, "operator"))
    for source, rows, op_key in feeds:
        for rec in rows:
            s, _ = best_match(index, rec.get("name") or "", rec.get("postcode"))
            if s is None:
                continue
            observed[s.id].add(source)
            if rec.get(op_key):
                listing_ops[s.id].append((source, rec[op_key]))
            if rec.get("url") and rec["url"] not in urls[s.id]:
                urls[s.id].append(rec["url"])

    # ---- evidence 3: approved planning consents with parsed beds
    # A whole-scheme consent, not an extension or change of use within one:
    # small bed counts at the same postcode are almost always later
    # alterations, so they never stand in for the scheme's size.
    MIN_SCHEME_BEDS = 50
    consents = (db.query(PlanningApplication)
                .filter(PlanningApplication.council_id == council.id,
                        PlanningApplication.pbsa_beds >= MIN_SCHEME_BEDS).all())

    def consent_for(s: ExistingScheme):
        pc, tokens = norm_pc(s.postcode), norm_name(s.name)
        best = None
        for a in consents:
            blob = f"{a.address or ''} {a.description or ''}"
            same_pc = pc and pc in norm_pc(blob)
            name_hit = tokens and len(tokens & norm_name(blob)) >= max(1, len(tokens) - 1)
            if (same_pc and name_hit) or (same_pc and len(tokens) == 0):
                approved = "approv" in (a.status or "").lower() or \
                    (a.decision or "").lower() in ("permitted", "conditions")
                if approved and (best is None or (a.decision_date or datetime.date.min)
                                 > (best.decision_date or datetime.date.min)):
                    best = a
        return best

    browser = None
    if not args.no_fetch and not args.no_browser:
        from app.scrapers.browser_fetch import BrowserFetcher
        browser = BrowserFetcher()

    bench = {}
    if args.benchmark:
        b = json.loads(Path(args.benchmark).read_text())
        for r in b.get("private_stock", []) + b.get("university_stock", []):
            bench[(norm_pc(r.get("postcode")), frozenset(norm_name(r["property"])))] = r

    stats = {"schemes": len(schemes), "observed": 0, "operator": 0,
             "beds_page": 0, "beds_planning": 0, "build_year": 0}
    qa = []
    try:
        for s in schemes:
            prov = dict(s.field_provenance or {})
            seen = sorted(observed[s.id])
            if seen:
                prov["observed"] = entry(seen, seen[0])
                stats["observed"] += 1
            else:
                prov.pop("observed", None)

            op_name = operators.get(s.operator_company_id)
            corroborated = None
            if op_name:
                op_tokens = norm_name(op_name)
                for source, listed in listing_ops[s.id]:
                    if op_tokens & norm_name(listed):
                        corroborated = entry(op_name, source)
                        break
            prov["operator"] = corroborated or (
                entry(op_name, s.source or "benchmark_report") if op_name else None)
            if corroborated:
                stats["operator"] += 1

            beds_entry = None
            kept = (s.field_provenance or {}).get("beds_total")
            if args.no_fetch and kept and kept.get("source") == "operator_page":
                beds_entry = kept
                stats["beds_page"] += 1
            if not args.no_fetch:
                for url in urls[s.id][:3]:
                    txt = fetch_text(url, browser)
                    n = extract_bed_count(txt)
                    if n:
                        beds_entry = entry(n, "operator_page", ref=url)
                        stats["beds_page"] += 1
                        break
            consent = consent_for(s)
            if beds_entry is None and consent is not None:
                beds_entry = entry(consent.pbsa_beds, "planning_consent",
                                   ref=consent.reference, basis="consented_beds")
                stats["beds_planning"] += 1
            if beds_entry is None and s.beds_total:
                beds_entry = entry(s.beds_total, s.source or "benchmark_report")
            prov["beds_total"] = beds_entry
            if beds_entry and beds_entry["source"] != "benchmark_report" and not args.dry_run_obs:
                record_observation(db, s, "beds_total", beds_entry["value"],
                                   beds_entry["source"], reference=beds_entry.get("ref"),
                                   basis="observed", refresh_cache=False)
            if corroborated and not args.dry_run_obs:
                record_observation(db, s, "operator", corroborated["value"],
                                   corroborated["source"], refresh_cache=False)

            if consent is not None and consent.expected_delivery_year:
                est = min(consent.expected_delivery_year, datetime.date.today().year)
                prov["build_year"] = entry(est, "planning_consent", ref=consent.reference,
                                           basis="estimated_from_consent")
                if not args.dry_run_obs:
                    record_observation(db, s, "build_year", est, "planning_consent",
                                       reference=consent.reference, basis="derived",
                                       refresh_cache=False)
                stats["build_year"] += 1
            elif s.build_year:
                prov["build_year"] = entry(s.build_year, s.source or "benchmark_report")

            s.field_provenance = prov
            flag_modified(s, "field_provenance")

            b = bench.get((norm_pc(s.postcode), frozenset(norm_name(s.name))))
            ours = beds_entry["value"] if beds_entry and beds_entry["source"] != "benchmark_report" else None
            if b and ours:
                qa.append((s.name, ours, b["beds"], beds_entry["source"]))
            print(f"  {s.name[:34]:34} seen={len(seen)} "
                  f"op={'Y' if corroborated else '-'} "
                  f"beds={ours or '-'} ({beds_entry['source'] if beds_entry else 'none'})")
        db.commit()
    finally:
        if browser:
            browser.close()
        db.close()

    print("\n=== own-source coverage ===")
    for k, v in stats.items():
        print(f"  {k:14} {v}")
    if qa:
        agree = sum(1 for _, o, b, _ in qa if abs(o - b) <= max(5, 0.1 * b))
        print(f"\n=== QA vs benchmark: {agree}/{len(qa)} bed counts within 10% ===")
        for name, o, b, src in sorted(qa, key=lambda r: -abs(r[1] - r[2]))[:12]:
            flag = "" if abs(o - b) <= max(5, 0.1 * b) else "  <-- check"
            print(f"  {name[:34]:34} ours {o:>5} ({src})  bench {b:>5}{flag}")


if __name__ == "__main__":
    main()
