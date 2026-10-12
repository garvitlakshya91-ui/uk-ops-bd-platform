"""Opening years for build-to-rent schemes from the ARL feed's completion date.

The ARL build-to-rent map, which already supplies these schemes, carries an
"Expected Completion" for each one. The scraper reads it and never stored
it. For schemes the feed marks completed, that date is when the scheme was
finished, to the feed's knowledge.

It is an approximate source: the field is sometimes the date expected when
planning was granted, not the date achieved. A date is refused when it is
not after the year of the planning application the feed quotes. A spot
check of 20 well-known schemes found 17 right to within a year.

Each year is stored as a reading that names the feed and quotes its raw
value, and fills the opening year only where the scheme has none.

    python date_btr_from_feed.py            # dry run on a fresh read of the feed
    python date_btr_from_feed.py --apply
    python date_btr_from_feed.py --snapshot data/enrichment/arl_feed_snapshot.json
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
SOURCE = "arl_btr_open_operating"
CONFIDENCE = 0.75


def planning_year(ref: str | None) -> int | None:
    """The year in a planning reference: '17/01234/FUL', '2016/1234', 'PA/14/02134'."""
    ref = ref or ""
    m = re.search(r"\b(20[0-3]\d)\b", ref)
    if m:
        return int(m.group(1))
    m = re.search(r"(?:^|[/\s])(\d{2})/\d{3,6}", ref)
    if m and 0 <= int(m.group(1)) <= 39:
        return 2000 + int(m.group(1))
    return None


def completion_year(raw: str | None) -> int | None:
    m = re.search(r"\b((?:19|20)\d{2})\b", raw or "")
    return int(m.group(1)) if m else None


async def read_feed() -> list[dict]:
    from app.scrapers.arl_btr_scraper import ARLBTRScraper
    async with ARLBTRScraper() as s:
        return await s.fetch_and_normalise()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--snapshot", help="a saved read of the feed, instead of fetching it again")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    if args.snapshot:
        with open(args.snapshot, encoding="utf-8") as fh:
            rows = json.load(fh)
    else:
        rows = asyncio.run(read_feed())
    feed = {r["source_reference"]: r for r in rows}
    this_year = dt.date.today().year
    db = SessionLocal()
    ours = db.execute(text("""
        SELECT es.id, es.name, es.source_reference, es.status, es.build_year, es.completion_date
        FROM existing_schemes es
        WHERE es.merged_into_id IS NULL AND es.record_class = 'scheme' AND es.source LIKE 'arl%'""")).mappings().all()
    stats, found = Counter(), []
    for o in ours:
        f = feed.get(o["source_reference"])
        if o["status"] != "operational":
            continue
        if o["build_year"] or o["completion_date"]:
            stats["already has an opening year"] += 1
            continue
        if not f:
            stats["no longer in the feed"] += 1
            continue
        if (f.get("status_raw") or "").strip().lower() != "completed":
            stats["feed does not mark it completed"] += 1
            continue
        year = completion_year(f.get("expected_completion"))
        if not year:
            stats["no completion date in the feed"] += 1
            continue
        if year > this_year:
            stats["completion date is in the future"] += 1
            continue
        pa = planning_year(f.get("planning_ref"))
        if pa and year <= pa:
            stats["completion date is not after its planning application (refused)"] += 1
            continue
        stats["opening year read"] += 1
        found.append({"scheme": o["id"], "name": o["name"], "year": year, "raw": f["expected_completion"],
                      "planning_ref": f.get("planning_ref")})
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    print("  years:", sorted(Counter(x["year"] for x in found).items()))
    if args.apply:
        for x in found:
            scheme = db.get(ExistingScheme, x["scheme"])
            record_observation(db, scheme, "build_year", x["year"], SOURCE, reference="https://btr-display-dev.realyse.com/",
                               basis="observed", confidence=CONFIDENCE, extractor="feed", refresh_cache=False,
                               quote=f"ARL build-to-rent map: status Completed, Expected Completion {x['raw']}")
            if scheme.build_year is None:
                set_field(scheme, "build_year", x["year"], SOURCE, db, changed_by="date_btr_from_feed.py")
        db.commit()
        print(f"written: {len(found)} opening years")
    else:
        db.rollback()
        print("dry run: nothing written")
    db.close()
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"opening_years_feed_{stamp}{'' if args.apply else '_dryrun'}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"applied": args.apply, "counts": dict(stats), "found": found}, fh, indent=1)
    print("run log:", path)


if __name__ == "__main__":
    main()
