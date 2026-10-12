"""Opening years for student schemes from the date their postcode was introduced.

A large new student building is normally given a postcode of its own when it
is built, and the Office for National Statistics records the month every
postcode was introduced (served free by postcodes.io). So a postcode first
introduced in, say, March 2016 points to a building that opened around 2016.

Checked on 10 October 2026 against the 42 student schemes that already had
an opening year and a postcode introduced in 2005 or later: the introduction
year was within one year of the known opening year for 38 of them (90%), and
the same year for 26.

It is an estimate, good to about a year, and is stored as one: source
"postcode_introduction", basis "derived". It is used only where

  - the scheme is a student scheme of 50 beds or more, operating, with no
    opening year,
  - its postcode was introduced in 2005 or later and is still live (an older
    postcode says nothing: the building may be newer than its street), and
  - no other scheme of ours shares that postcode.

    python date_schemes_from_postcode.py                # dry run, every student scheme
    python date_schemes_from_postcode.py --source national_code --apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
CACHE = os.path.join(OUT_DIR, "postcode_intro.json")
SOURCE = "postcode_introduction"
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
FIRST_USEFUL_YEAR = 2005
MIN_BEDS = 50
CONFIDENCE = 0.75


def introduction_year(raw) -> tuple[int, int] | None:
    """(year, month) from postcodes.io's 'YYYYMM'."""
    s = str(raw or "")
    if len(s) >= 6 and s[:6].isdigit():
        return int(s[:4]), int(s[4:6])
    return None


def judge(intro, terminated, shared: int, size, this_year: int) -> tuple[int | None, str]:
    """(opening year or None, reason)."""
    if terminated:
        return None, "postcode no longer live"
    ym = introduction_year(intro)
    if not ym:
        return None, "no introduction date on file"
    year, _ = ym
    if year < FIRST_USEFUL_YEAR:
        return None, "postcode older than 2005, says nothing"
    if year > this_year:
        return None, "introduction date in the future"
    if shared > 1:
        return None, "postcode shared with another scheme"
    if not size or size < MIN_BEDS:
        return None, f"under {MIN_BEDS} beds or size unknown"
    return year, "postcode introduced"


def lookup(postcodes: list[str]) -> dict[str, dict]:
    try:
        with open(CACHE, encoding="utf-8") as fh:
            cache: dict = json.load(fh)
    except (OSError, ValueError):
        cache = {}
    # Older cache entries hold only the date string.
    cache = {k: (v if isinstance(v, dict) else {"intro": v, "end": None}) for k, v in cache.items()}
    todo = sorted({p for p in postcodes if p not in cache})
    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(todo), 100):
            try:
                r = client.post("https://api.postcodes.io/postcodes", json={"postcodes": todo[i:i + 100]})
                r.raise_for_status()
            except httpx.HTTPError:
                continue
            for item in r.json().get("result", []):
                res = item.get("result")
                cache[item["query"]] = {"intro": (res or {}).get("date_of_introduction"),
                                        "end": (res or {}).get("date_of_termination"), "found": bool(res)}
            time.sleep(0.3)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CACHE, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)
    return cache


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="only schemes from this feed, for example national_code")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    db = SessionLocal()
    rows = db.execute(text(f"""
        SELECT es.id, es.name, upper(trim(es.postcode)) AS postcode, es.source, {SIZE} AS size
        FROM existing_schemes es
        WHERE es.merged_into_id IS NULL AND es.record_class = 'scheme' AND es.scheme_type = 'PBSA'
          AND coalesce(es.status, 'operational') = 'operational'
          AND es.build_year IS NULL AND es.completion_date IS NULL
          AND es.postcode ~ '^[A-Za-z]{{1,2}}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{{2}}$'
          AND (CAST(:src AS text) IS NULL OR es.source = :src)
        ORDER BY es.id"""), {"src": args.source}).mappings().all()
    shared = dict(db.execute(text("""
        SELECT upper(trim(postcode)), count(*) FROM existing_schemes
        WHERE merged_into_id IS NULL AND record_class = 'scheme' AND nullif(trim(postcode), '') IS NOT NULL
        GROUP BY 1""")).fetchall())
    print(f"student schemes with a postcode and no opening year: {len(rows)}")
    cache = lookup([r["postcode"] for r in rows])
    this_year = dt.date.today().year
    stats, found = Counter(), []
    for r in rows:
        c = cache.get(r["postcode"]) or {}
        year, why = judge(c.get("intro"), c.get("end"), shared.get(r["postcode"], 1), r["size"], this_year)
        if year is None:
            stats[why] += 1
            continue
        stats["opening year estimated"] += 1
        found.append({"scheme": r["id"], "name": r["name"], "postcode": r["postcode"], "source": r["source"],
                      "size": r["size"], "year": year, "introduced": str(c.get("intro"))})
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    print("  by feed:", Counter(f["source"] for f in found).most_common(8))
    print("  years:", sorted(Counter(f["year"] for f in found).items()))
    if args.apply:
        for f in found:
            scheme = db.get(ExistingScheme, f["scheme"])
            intro = f["introduced"]
            record_observation(db, scheme, "build_year", f["year"], SOURCE, basis="derived", confidence=CONFIDENCE,
                               reference=f"https://api.postcodes.io/postcodes/{f['postcode'].replace(' ', '')}",
                               quote=f"postcode {f['postcode']} was introduced in {intro[:4]}-{intro[4:6]}; "
                                     f"opening year estimated from it, good to about a year",
                               extractor="rule", refresh_cache=False)
            if scheme.build_year is None:
                set_field(scheme, "build_year", f["year"], SOURCE, db, changed_by="date_schemes_from_postcode.py")
        db.commit()
        print(f"written: {len(found)} opening years")
    else:
        db.rollback()
        print("dry run: nothing written")
    db.close()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"opening_years_postcode_{stamp}{'' if args.apply else '_dryrun'}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"applied": args.apply, "counts": dict(stats), "found": found}, fh, indent=1)
    print("run log:", path)


if __name__ == "__main__":
    main()
