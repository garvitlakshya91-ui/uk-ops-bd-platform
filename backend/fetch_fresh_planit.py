"""Focused PlanIt ingest — last N days of all applications, incremental saves.

Avoids the keyword-fan-out problem of the celery task by making a single
flat fetch over the date window and saving every batch immediately. Designed
for the BD use case: surface application-stage opportunities ASAP.

Usage::

    python fetch_fresh_planit.py [--days 30] [--limit 50000] [--undecided-only]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

PLANIT_BASE = "https://www.planit.org.uk/api/applics/json"
PAGE_SIZE = 200  # PlanIt max is 500 but smaller pages reduce 429 risk
COMMIT_EVERY = 500
SLEEP_BETWEEN = 0.6  # seconds between requests (~1.6 req/s)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30, help="How many days back to scan (default 30)")
    p.add_argument("--limit", type=int, default=50000, help="Max records to fetch")
    p.add_argument(
        "--undecided-only", action="store_true",
        help="Only fetch apps in Undecided state (BD-relevant — still in planning)",
    )
    p.add_argument("--dry-run", action="store_true", help="Fetch but don't write")
    args = p.parse_args()

    end_date = date.today()
    start_date = end_date - timedelta(days=args.days)

    print(f"Fetching PlanIt apps: {start_date} to {end_date}")
    print(f"  undecided_only={args.undecided_only}  limit={args.limit}  dry_run={args.dry_run}")
    print()

    engine = create_engine(os.environ["DATABASE_URL"])
    SessionLocal = sessionmaker(bind=engine)

    # Build council name -> id lookup
    council_lookup: dict[str, int] = {}
    with engine.connect() as c:
        for r in c.execute(text("SELECT id, name FROM councils")):
            council_lookup[r[1].lower().strip()] = r[0]
    print(f"Loaded {len(council_lookup):,} councils")

    saved = 0
    skipped_no_council = 0
    skipped_existing = 0
    page = 1  # PlanIt pages are 1-indexed
    fetched = 0

    headers = {"User-Agent": "ukops-bd-platform/1.0 (focused-ingest)"}
    client_kwargs: dict[str, object] = {"timeout": 60.0, "headers": headers}
    proxy_url = os.environ.get("PROXY_URL")
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
        print(f"  Using PROXY_URL=*****@{proxy_url.split('@')[-1]}")
    base_params: dict[str, object] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "pg_sz": PAGE_SIZE,
    }
    if args.undecided_only:
        base_params["app_state"] = "Undecided"

    db = SessionLocal()
    pending_new = 0
    consecutive_429s = 0

    with httpx.Client(**client_kwargs) as client:
        while fetched < args.limit:
            params = {**base_params, "page": page}
            try:
                r = client.get(PLANIT_BASE, params=params)
            except Exception as e:
                print(f"  ERR page={page}: {e}")
                time.sleep(5)
                continue

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", "60"))
                consecutive_429s += 1
                print(f"  429 page={page} retry={retry}s (consecutive 429s: {consecutive_429s})")
                if consecutive_429s >= 5:
                    print("  too many 429s in a row — stopping")
                    break
                time.sleep(retry)
                continue
            consecutive_429s = 0

            if r.status_code != 200:
                print(f"  HTTP {r.status_code} page={page}: {r.text[:200]}")
                break

            data = r.json()
            records = data.get("records", [])
            total = data.get("total")

            if page == 1:
                print(f"  total available in window: {total:,}")
                if args.limit < total:
                    print(f"  will cap at {args.limit:,}")

            if not records:
                print(f"  empty page → done (page={page})")
                break

            for rec in records:
                fetched += 1
                if fetched > args.limit:
                    break
                if args.dry_run:
                    continue
                ok, status_tag = save_planit_record(db, rec, council_lookup)
                if ok:
                    saved += 1
                    pending_new += 1
                elif status_tag == "no_council":
                    skipped_no_council += 1
                elif status_tag == "existing":
                    skipped_existing += 1

                if pending_new >= COMMIT_EVERY:
                    db.commit()
                    pending_new = 0
                    print(f"    committed batch (cumulative saved={saved:,})")

            page += 1
            if page % 10 == 0:
                print(f"  page={page} fetched={fetched:,} saved={saved:,} skipped_no_council={skipped_no_council:,} skipped_existing={skipped_existing:,}")
            time.sleep(SLEEP_BETWEEN)

    if not args.dry_run and pending_new > 0:
        db.commit()
    db.close()

    print()
    print("=" * 60)
    print(f"Fetched:             {fetched:,}")
    print(f"Newly saved:         {saved:,}")
    print(f"Skipped (no council mapping): {skipped_no_council:,}")
    print(f"Skipped (existing):  {skipped_existing:,}")
    return 0


def save_planit_record(db, rec: dict, council_lookup: dict[str, int]) -> tuple[bool, str]:
    """Insert a PlanIt record into planning_applications if not already present.

    Returns (saved, status_tag) where status_tag ∈ {'saved', 'no_council',
    'existing', 'error'}.
    """
    uid = (rec.get("uid") or "").strip()
    area = (rec.get("area_name") or "").strip()
    if not uid or not area:
        return False, "error"

    council_id = council_lookup.get(area.lower())
    if not council_id:
        return False, "no_council"

    # Check existence by (reference, council_id) — unique constraint
    existing = db.execute(
        text("""
            SELECT 1 FROM planning_applications
            WHERE reference = :ref AND council_id = :cid
        """),
        {"ref": uid, "cid": council_id},
    ).fetchone()
    if existing:
        return False, "existing"

    # Map PlanIt fields → planning_applications
    lat, lon = None, None
    loc = rec.get("location") or {}
    coords = loc.get("coordinates") if isinstance(loc, dict) else None
    if coords and len(coords) == 2:
        try:
            lon, lat = float(coords[0]), float(coords[1])
        except (TypeError, ValueError):
            pass

    status_map = {
        "Undecided": "Pending",
        "Permitted": "Approved",
        "Conditions": "Approved",
        "Refused": "Refused",
        "Withdrawn": "Withdrawn",
        "Appeal": "Appeal",
        "Referred": "Pending",
        "Other": "Unknown",
        "Not Available": "Unknown",
    }
    status = status_map.get(rec.get("app_state", ""), "Unknown")

    # Parse start_date (PlanIt's start_date is the submission date)
    submission_date = None
    raw_sd = rec.get("start_date") or rec.get("consulted_date")
    if raw_sd:
        try:
            submission_date = datetime.fromisoformat(raw_sd[:10]).date()
        except (ValueError, TypeError):
            pass

    description = rec.get("description") or ""
    applicant_name = rec.get("applicant_name") or ""
    agent_name = rec.get("agent_name") or ""

    # Use the broadened classifier
    from app.scrapers.base import BaseScraper
    scheme_type = BaseScraper.classify_scheme_type(
        description, applicant_name=applicant_name, agent_name=agent_name
    )
    num_units = BaseScraper.extract_unit_count(description)

    now = datetime.now(timezone.utc)
    db.execute(
        text("""
            INSERT INTO planning_applications
                (reference, council_id, address, description, applicant_name,
                 agent_name, application_type, status, scheme_type, num_units,
                 latitude, longitude, submission_date, source, raw_data,
                 created_at, updated_at)
            VALUES
                (:ref, :cid, :addr, :desc, :app, :agent, :atype, :status,
                 :stype, :units, :lat, :lon, :sdate, :source,
                 CAST(:raw AS jsonb), :now, :now)
        """),
        {
            "ref": uid,
            "cid": council_id,
            "addr": rec.get("address"),
            "desc": description,
            "app": applicant_name or None,
            "agent": agent_name or None,
            "atype": rec.get("app_type"),
            "status": status,
            "stype": scheme_type,
            "units": num_units,
            "lat": lat,
            "lon": lon,
            "sdate": submission_date,
            "source": "planit_api",
            "raw": _to_json(rec),
            "now": now,
        },
    )
    return True, "saved"


def _to_json(d: dict) -> str:
    import json
    return json.dumps(d, default=str)


if __name__ == "__main__":
    sys.exit(main())
