"""Seed the scheme registry for one city from what the database already holds.

Four passes, each idempotent:

  names     Census schemes matched during the city-work merge kept the old
            record's name ("Birmingham", "THE PENWORKS"). The census name
            is restored from the merge source database and the old name
            becomes an alias. Every census scheme's name is then locked
            under source "census" so a monthly feed cannot rename it.
  aliases   The current name, every earlier name in the change log and
            every matched feed spelling, as aliases.
  sources   One source record per feed record that describes a scheme:
            rent/availability URLs, listing files (AFS, StuRents, operator
            directories) and licensed benchmark rows. Records that match
            nothing wait in the queue with status "unmatched".
  verify    Census schemes become "probable"; "verified" where our own
            bed count agrees with the benchmark within 5% and a listing
            corroborates the operator; "disputed" where our count and the
            benchmark's differ by more than 10%. Statuses a person set
            are never touched.

Usage:
    python registry_backfill.py --city Birmingham            # dry run
    python registry_backfill.py --city Birmingham --apply
"""
from __future__ import annotations

import argparse
import datetime
import glob
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from sqlalchemy.orm.attributes import flag_modified

from app.census_sources import PUBLISHABLE_SOURCES
from app.config import settings
from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme, SchemeChangeLog
from app.registry.core import (add_alias, alias_norm, link_source_record, lock_field,
                               mark_verified, match_record, propose_name, queue_counts)
from app.scrapers.field_protection import set_field
from app.scrapers.scheme_matching import norm_name, norm_pc

AUTO = "auto:registry_backfill"
DATA = Path(__file__).parent / "data"
OPERATOR_FEEDS = {"pbsa_operator", "operator_page", "operator_directory", "operator_scraper"}
# AFS lists HMO flats beside halls; only purpose-built records belong in the queue.
AFS_PBSA = {"halls", "pbsa", "studios"}


def noisy_name(name: str, city: str) -> bool:
    """A record name that is not a scheme name: the city, a page title, EPC caps."""
    n = (name or "").strip()
    return (not n or n.lower() == city.lower() or n.lower().startswith("welcome to")
            or (n.isupper() and len(n) > 3) or not norm_name(n))


def clean_operator_name(name: str, city: str) -> str:
    """'Oak Brook Park Birmingham' -> 'Oak Brook Park'."""
    import re
    return re.sub(rf"[\s,-]+{re.escape(city)}$", "", (name or "").strip(), flags=re.I).strip() or name


def census_name_wins(old: str, census: str, old_source: str | None, operator: str | None, city: str) -> bool:
    """Adopt the census name unless an operator feed named the building differently.

    'Vita Student Gough Street' and 'Gough Street' are the same name under a
    brand; 'Staniforth House' and 'Stanford House' are not, and the
    operator's spelling outranks a licensed pack's.
    """
    if noisy_name(old, city):
        return True
    brand = norm_name(operator or "") | {city.lower()}
    if (norm_name(old) - brand) == (norm_name(census) - brand):
        return True
    return old_source not in OPERATOR_FEEDS


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def census_names_from_merge_source(schemes: list[ExistingScheme]) -> dict[int, str]:
    """scheme.id -> census name, read from the merge source database if present."""
    ids = {s.id: (s.field_provenance or {}).get("_merge", {}).get("source_id") for s in schemes}
    ids = {k: v for k, v in ids.items() if v}
    if not ids:
        return {}
    url = settings.DATABASE_URL.rsplit("/", 1)[0] + "/uk_ops_bd_citywork"
    try:
        eng = create_engine(url)
        with eng.connect() as c:
            rows = c.execute(text("select id, name from existing_schemes where id = any(:ids)"),
                             {"ids": list(ids.values())}).fetchall()
    except Exception as exc:  # the working copy may have been dropped
        print(f"  merge source database not readable ({str(exc).splitlines()[0][:80]}); names pass skipped")
        return {}
    by_src = {r[0]: r[1] for r in rows}
    return {sid: by_src[src] for sid, src in ids.items() if src in by_src}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--slug", default=None)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    slug = args.slug or args.city.lower().replace(" ", "-")

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    schemes = (db.query(ExistingScheme)
               .filter(ExistingScheme.council_id == council.id, ExistingScheme.scheme_type == "PBSA",
                       ExistingScheme.merged_into_id.is_(None)).all())
    ops = {c.id: c for c in db.query(Company).filter(
        Company.id.in_([s.operator_company_id for s in schemes if s.operator_company_id] or [0]))}
    census = [s for s in schemes if s.operating_status]
    uni = {s.id for s in census if (ops.get(s.operator_company_id) and
                                    (ops[s.operator_company_id].company_type or "") == "University")}
    print(f"{council.name}: {len(schemes)} PBSA schemes, {len(census)} in the census ({len(uni)} university)")
    stats = Counter()

    # ---- names -----------------------------------------------------------
    print("\n== names")
    for sid, census_name in census_names_from_merge_source(schemes).items():
        s = db.get(ExistingScheme, sid)
        if alias_norm(census_name) == alias_norm(s.name):
            continue
        old = s.name
        op_name = ops[s.operator_company_id].name if s.operator_company_id in ops else None
        if census_name_wins(old, census_name, s.source, op_name, council.name):
            result = propose_name(db, s, census_name, "census")
            if result == "renamed":
                stats["renamed"] += 1
                print(f"  {old[:34]:34s} -> {census_name}")
            else:
                stats["rename_refused"] += 1
                print(f"  kept {old[:34]:34s} (census: {census_name}; name locked by "
                      f"{(s.locked_fields or {}).get('name')})")
        else:
            cleaned = clean_operator_name(old, council.name)
            # Same name to the registry (the city token is ignored), so this
            # is a display tidy-up rather than a rename: write it directly.
            if cleaned != old and set_field(s, "name", cleaned, "census", db):
                add_alias(db, s.id, old, s.source or "former_name")
                stats["renamed"] += 1
                print(f"  {old[:34]:34s} -> {cleaned}  (operator name kept, city suffix dropped)")
            stats["alias_added"] += add_alias(db, s.id, census_name, "census")
            stats["name_review"] += 1
            print(f"  review: operator calls it {old!r}, census {census_name!r} (alias added)")
    for s in census:
        # "Lakeside Birmingham" is a feed's habit, not the scheme's name.
        cleaned = clean_operator_name(s.name, council.name)
        if cleaned != s.name and (s.locked_fields or {}).get("name") != "manual" \
                and set_field(s, "name", cleaned, "census", db):
            add_alias(db, s.id, s.name, s.source or "former_name")
            stats["renamed"] += 1
            print(f"  {s.name[:34]:34s} -> {cleaned}  (city suffix dropped)")
        if (s.locked_fields or {}).get("name") not in ("manual", "registry", "census"):
            lock_field(s, "name", "census")
            stats["name_locked"] += 1

    # ---- aliases -----------------------------------------------------------
    print("\n== aliases")
    for s in schemes:
        stats["alias_added"] += add_alias(db, s.id, s.name, "registry")
    for log in (db.query(SchemeChangeLog)
                .filter(SchemeChangeLog.field_name == "name",
                        SchemeChangeLog.scheme_id.in_([s.id for s in schemes] or [0]))):
        stats["alias_added"] += add_alias(db, log.scheme_id, log.old_value, "former_name")
    print(f"  {stats['alias_added']} aliases added")

    # ---- source records -----------------------------------------------------
    print("\n== source records")
    sids = [s.id for s in schemes] or [0]
    for sid, source, ref in db.execute(text("""
        SELECT DISTINCT scheme_id, source, source_reference FROM scheme_rents
        WHERE scheme_id = ANY(:ids) AND source_reference LIKE 'http%'
        UNION
        SELECT DISTINCT scheme_id, source, source_reference FROM scheme_availability
        WHERE scheme_id = ANY(:ids) AND source_reference LIKE 'http%'
    """), {"ids": sids}):
        link_source_record(db, source or "unknown", ref, scheme_id=sid, match_method="url",
                           match_score=1.0, status="matched")
        stats["records:url"] += 1

    feeds = [
        ("afs_directory", read_jsonl(DATA / "afs" / f"{slug}.jsonl"), "operator", "url"),
        ("sturents", [r for r in read_jsonl(DATA / "sturents" / f"{slug}.jsonl")
                      if r.get("is_pbsa_candidate") and "/house/" not in (r.get("url") or "")], "agent", "url"),
    ]
    opdir = []
    for f in sorted(glob.glob(str(DATA / "operator_directories" / "*.jsonl"))):
        opdir += [r for r in read_jsonl(Path(f)) if args.city.lower() in (r.get("city") or "").lower()]
    feeds.append(("operator_directory", opdir, "operator", "url"))
    for source, rows, op_key, ref_key in feeds:
        for rec in rows:
            ref = rec.get(ref_key) or f"{rec.get('name')}|{rec.get('postcode')}"
            scheme, method, score = match_record(db, council.id, rec.get("name"), rec.get("postcode"),
                                                 candidates=schemes)
            status = "matched" if scheme else "unmatched"
            if scheme is None and source == "afs_directory" and not (
                    str(rec.get("advert_type") or "").lower() in AFS_PBSA
                    or str(rec.get("property_type") or "").lower() in AFS_PBSA
                    or rec.get("category") == "student-halls"):
                status = "ignored"   # an HMO flat or house, not a scheme
            link_source_record(db, source, ref, scheme_id=scheme.id if scheme else None,
                               raw_name=rec.get("name"), raw_postcode=rec.get("postcode"),
                               raw_operator=rec.get(op_key), raw_address=rec.get("address"),
                               match_method=method, match_score=score, status=status)
            stats[f"records:{source}:{status}"] += 1
            if scheme and rec.get("name"):
                stats["alias_added"] += add_alias(db, scheme.id, rec["name"], source)

    bench_beds: dict[int, tuple[int, str]] = {}
    for f in sorted(glob.glob(str(DATA / "benchmarks" / f"{slug}_*.json"))):
        pack = json.loads(Path(f).read_text(encoding="utf-8"))
        label = f"{pack.get('producer')} {pack.get('report_date')}"
        for section in ("private_stock", "university_stock"):
            for row in pack.get(section) or []:
                ref = f"{label}|{section}|{row.get('property')}|{row.get('postcode') or ''}"
                scheme, method, score = match_record(db, council.id, row.get("property"), row.get("postcode"),
                                                     candidates=schemes)
                link_source_record(db, "benchmark_report", ref, scheme_id=scheme.id if scheme else None,
                                   raw_name=row.get("property"), raw_postcode=row.get("postcode"),
                                   raw_operator=row.get("operator") or row.get("university"),
                                   payload=row, match_method=method, match_score=score)
                stats[f"records:benchmark:{'matched' if scheme else 'unmatched'}"] += 1
                if scheme:
                    stats["alias_added"] += add_alias(db, scheme.id, row.get("property"), "benchmark_report")
                    if row.get("beds") and section == "private_stock":
                        bench_beds[scheme.id] = (int(row["beds"]), label)
                else:
                    print(f"  benchmark row unmatched: {row.get('property')!r} {row.get('postcode')} "
                          f"({row.get('beds')} beds)")
    for k in sorted(stats):
        if k.startswith("records:"):
            print(f"  {k:44s} {stats[k]}")

    # ---- verification -------------------------------------------------------
    print("\n== verification")
    for s in census:
        if s.verification_status != "unverified" and not (s.verified_by or "").startswith("auto:"):
            stats["verify:kept_manual"] += 1
            continue
        prov = s.field_provenance or {}
        own = prov.get("beds_total") if (prov.get("beds_total") or {}).get("source") in PUBLISHABLE_SOURCES else None
        op_ok = (prov.get("operator") or {}).get("source") in PUBLISHABLE_SOURCES
        bench = bench_beds.get(s.id)
        status, note = "probable", "census scheme; awaiting scheme-by-scheme verification"
        if s.id in uni:
            note = "university property from the census"
        elif own and bench:
            delta = abs(own["value"] - bench[0]) / bench[0] if bench[0] else 1
            if delta <= 0.05 and op_ok:
                status = "verified"
                note = (f"own-source beds {own['value']} ({own['source']}) within 5% of benchmark "
                        f"{bench[0]} ({bench[1]}); operator corroborated by a listing")
            elif delta > 0.10:
                status = "disputed"
                note = f"own-source beds {own['value']} ({own['source']}) vs benchmark {bench[0]} ({bench[1]})"
        mark_verified(db, s, status, AUTO, note)
        stats[f"verify:{status}"] += 1
    for k in sorted(stats):
        if k.startswith("verify:"):
            print(f"  {k:30s} {stats[k]}")
    print(f"\n  renamed {stats['renamed']}, rename refused {stats['rename_refused']}, "
          f"names to review {stats['name_review']}, names locked {stats['name_locked']}, "
          f"aliases added {stats['alias_added']}")

    if args.apply:
        db.commit()
        print("\ncommitted")
        print("queue:", json.dumps(queue_counts(db, council.id)))
    else:
        db.rollback()
        print("\ndry run: rolled back, nothing written")
    db.close()


if __name__ == "__main__":
    main()
