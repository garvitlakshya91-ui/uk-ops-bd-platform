"""Energy ratings for schemes wrongly marked as having no certificates.

Until October 2026 the old energy-certificate API was failing, and a failed
lookup was stored on the scheme as "checked, no certificates". This asks the
register again for every scheme carrying that marker. Where the postcode does
have rated certificates the marker is replaced by the count in each band;
where it has none the marker stays as it is.

The old value of every scheme changed goes to the run log and the scheme
change log.

    python refill_epc_ratings.py              # dry run
    python refill_epc_ratings.py --apply
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme, SchemeChangeLog  # noqa: E402
from app.scrapers.epc_client import EPCApiError  # noqa: E402
from app.scrapers.epc_scraper import EPCScraper  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
SOURCE = "energy_certificate"


async def run(apply: bool, limit: int | None) -> None:
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT id, name, scheme_type, upper(trim(postcode)) AS postcode
        FROM existing_schemes
        WHERE epc_ratings::jsonb ->> 'checked' = 'true' AND nullif(trim(postcode), '') IS NOT NULL
        ORDER BY id""")).mappings().all()
    schemes = [dict(r) for r in rows][:limit]
    print(f"schemes marked as checked with no certificates: {len(schemes)}")
    stats = Counter()
    found = []
    at_postcode: dict[str, dict] = {}
    async with EPCScraper() as scraper:
        for s in schemes:
            pc = s["postcode"]
            if pc not in at_postcode:
                at_postcode[pc] = await scraper.get_rating_distribution(postcode=pc)
            ratings = at_postcode[pc]
            if not ratings["ratings"]:
                stats["really none on the register: marker kept"] += 1
                continue
            stats["has rated certificates: marker replaced"] += 1
            found.append({"scheme": s["id"], "name": s["name"], "type": s["scheme_type"], "postcode": pc,
                          "total": ratings["total"], "ratings": ratings["ratings"]})
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    for f in found[:10]:
        print(f"    {f['name'][:30]:30s} {f['postcode']:8s} {f['total']:>4} certificates {f['ratings']}")
    if apply:
        for f in found:
            scheme = db.get(ExistingScheme, f["scheme"])
            f["old_value"] = scheme.epc_ratings
            scheme.epc_ratings = at_postcode[f["postcode"]]
            db.add(SchemeChangeLog(
                scheme_id=scheme.id, field_name="epc_ratings", old_value=json.dumps(f["old_value"]),
                new_value=json.dumps({"ratings": f["ratings"], "total": f["total"]}),
                source=SOURCE, changed_by="refill_epc_ratings.py"))
        db.commit()
        print(f"written: {len(found)} schemes given their energy ratings")
    else:
        db.rollback()
        print("dry run: nothing written")
    db.close()
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"epc_ratings_refill_{stamp}{'' if apply else '_dryrun'}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"applied": apply, "counts": dict(stats), "found": found}, fh, indent=1)
    print("run log:", path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    try:
        asyncio.run(run(args.apply, args.limit))
    except EPCApiError as exc:
        raise SystemExit(f"stopped, nothing written: {exc}")


if __name__ == "__main__":
    main()
