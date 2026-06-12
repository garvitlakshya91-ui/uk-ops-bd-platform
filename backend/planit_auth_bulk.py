"""Per-council PlanIt ingest using the `auth=<council>` parameter.

The default PlanIt task pulls applications without a council filter,
which leaves big-city coverage thin (only a slice of nationwide records
get sampled per page). Using `auth=Manchester` returned 79× more
Manchester apps than the unfiltered call — same pattern likely applies
to every other major LPA.

This script iterates a curated list of top UK councils and ingests their
recent applications via the auth filter, last 365 days.

Usage::

    python planit_auth_bulk.py [--councils Manchester,Birmingham,Leeds] [--days 365]
"""
from __future__ import annotations
import argparse, json, os, sys, time
from datetime import datetime, date, timedelta, timezone

import httpx
from sqlalchemy import create_engine, text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)

from app.scrapers.base import BaseScraper

PLANIT_BASE = "https://www.planit.org.uk/api/applics/json"
UA = {"User-Agent": "ukops-bd-platform/1.0"}

# Application types we treat as noise (condition discharges, tree work, etc.)
SKIP_TYPES = (
    "trees", "tree work", "tree preservation", "conditions",
    "discharge condition", "discharge of conditions", "discharge of multiple",
    "non material amendment", "non-material amendment",
    "details reserved by condition", "approval of detail", "telecoms",
    "advertis", "listed building consent", "listed building", "tpo",
    "prior approval", "section 106",
)

STATUS_MAP = {
    "Undecided": "Pending", "Permitted": "Approved", "Conditions": "Approved",
    "Refused": "Refused", "Withdrawn": "Withdrawn", "Appeal": "Appeal",
    "Referred": "Pending", "Other": "Unknown", "Not Available": "Unknown",
    "Rejected": "Refused",
}

# Top UK councils where PlanIt's auth filter unlocks real volume.
DEFAULT_COUNCILS = [
    # Major cities first (large pipelines)
    "Manchester", "Birmingham", "Leeds", "Bristol", "Liverpool", "Sheffield",
    "Nottingham", "Newcastle upon Tyne", "Salford", "Bradford",
    # London boroughs with most BTR/PBSA
    "Tower Hamlets", "Southwark", "Lambeth", "Wandsworth", "Westminster",
    "Camden", "Hackney", "Newham", "Greenwich", "Lewisham", "Hammersmith and Fulham",
    "Islington", "Brent", "Ealing", "Hounslow",
    # Other key BD targets
    "Cambridge", "Oxford", "Edinburgh", "Glasgow", "Cardiff",
    "Bath and North East Somerset", "Brighton", "Reading",
]


def ingest_council(name: str, council_id: int, days: int, engine, *,
                   max_pages: int = 30) -> dict:
    """Pull PlanIt applications for one council and persist them."""
    start_date = (date.today() - timedelta(days=days)).isoformat()
    end_date = date.today().isoformat()
    seen = set()
    all_records = []

    with httpx.Client(timeout=30.0, headers=UA) as client:
        for page in range(1, max_pages + 1):
            try:
                r = client.get(PLANIT_BASE, params={
                    "auth": name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "pg_sz": 200,
                    "page": page,
                })
            except Exception as e:
                print(f"  {name} p{page} ERR {e}")
                break

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", "60"))
                print(f"  {name} p{page} 429 retry={retry}s")
                time.sleep(min(retry, 120))
                continue
            if r.status_code != 200:
                print(f"  {name} p{page} HTTP {r.status_code}")
                break

            data = r.json()
            records = data.get("records", []) or []
            if not records:
                break

            for rec in records:
                uid = (rec.get("uid") or "").strip()
                if uid and uid not in seen:
                    seen.add(uid)
                    all_records.append(rec)
            print(f"  {name} p{page}: {len(records)} records (cumulative {len(all_records)})")
            time.sleep(0.5)

    if not all_records:
        return {"council": name, "fetched": 0, "saved": 0, "noise_skipped": 0, "existing": 0}

    # Persist
    saved = noise_skipped = existing = 0
    with engine.connect() as c:
        for rec in all_records:
            uid = (rec.get("uid") or "").strip()
            if not uid:
                continue
            app_type = (rec.get("app_type") or "").lower()
            if any(n in app_type for n in SKIP_TYPES):
                noise_skipped += 1
                continue
            row = c.execute(
                text("SELECT 1 FROM planning_applications WHERE reference=:r AND council_id=:cid"),
                {"r": uid, "cid": council_id},
            ).fetchone()
            if row:
                existing += 1
                continue
            desc = rec.get("description") or ""
            status = STATUS_MAP.get(rec.get("app_state", ""), "Unknown")

            sd = None
            if rec.get("start_date"):
                try:
                    sd = datetime.fromisoformat(str(rec["start_date"])[:10]).date()
                except (ValueError, TypeError):
                    pass

            lat = lon = None
            loc = rec.get("location") or {}
            if isinstance(loc, dict) and len(loc.get("coordinates") or []) == 2:
                try:
                    lon, lat = float(loc["coordinates"][0]), float(loc["coordinates"][1])
                except (TypeError, ValueError):
                    pass

            scheme_type = BaseScraper.classify_scheme_type(desc)
            num_units = BaseScraper.extract_unit_count(desc)

            c.execute(text("""
                INSERT INTO planning_applications
                    (reference, council_id, address, description, application_type, status,
                     scheme_type, num_units, latitude, longitude, submission_date, source,
                     raw_data, created_at, updated_at)
                VALUES (:ref, :cid, :addr, :desc, :atype, :status, :stype, :units, :lat, :lon,
                        :sdate, 'planit_api', CAST(:raw AS jsonb), NOW(), NOW())
            """), {
                "ref": uid, "cid": council_id, "addr": rec.get("address"), "desc": desc,
                "atype": rec.get("app_type"), "status": status, "stype": scheme_type,
                "units": num_units, "lat": lat, "lon": lon, "sdate": sd,
                "raw": json.dumps(rec, default=str),
            })
            saved += 1
        c.commit()

    return {
        "council": name, "fetched": len(all_records),
        "saved": saved, "noise_skipped": noise_skipped, "existing": existing,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--councils", type=str, default=None,
                   help="Comma-separated; default = top 30")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--skip-manchester", action="store_true",
                   help="Skip Manchester (already done)")
    args = p.parse_args()

    councils = args.councils.split(",") if args.councils else DEFAULT_COUNCILS
    councils = [c.strip() for c in councils if c.strip()]
    if args.skip_manchester:
        councils = [c for c in councils if c.lower() != "manchester"]

    engine = create_engine(os.environ["DATABASE_URL"])
    # Resolve council_ids
    with engine.connect() as c:
        council_ids = {}
        for name in councils:
            row = c.execute(
                text("SELECT id FROM councils WHERE name = :n"), {"n": name}
            ).fetchone()
            council_ids[name] = row[0] if row else None

    print(f"Councils to ingest: {len(councils)}")
    missing = [n for n, cid in council_ids.items() if cid is None]
    if missing:
        print(f"  Missing in DB (will skip): {missing}")
    print()

    total = {"fetched": 0, "saved": 0, "noise_skipped": 0, "existing": 0}
    per_council = []
    for name in councils:
        cid = council_ids.get(name)
        if cid is None:
            continue
        print(f"=== {name} (id={cid}) ===")
        stats = ingest_council(name, cid, args.days, engine)
        per_council.append(stats)
        for k in ("fetched", "saved", "noise_skipped", "existing"):
            total[k] += stats.get(k, 0)
        print(f"  -> saved={stats['saved']:,} noise={stats['noise_skipped']:,} existing={stats['existing']:,}")
        print()
        # Brief pause between councils to be polite to PlanIt
        time.sleep(2.0)

    print("=" * 60)
    print(f"GRAND TOTAL: fetched={total['fetched']:,}  saved={total['saved']:,}  noise={total['noise_skipped']:,}  existing={total['existing']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
