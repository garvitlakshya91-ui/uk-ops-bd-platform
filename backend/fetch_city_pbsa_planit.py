"""Keyword-targeted PlanIt ingest for one city's PBSA/BTR pipeline.

planit_auth_bulk.py pulls everything recent for a council but caps at
~6,000 records, which reaches back months, not years. A city report
needs the full student/BTR pipeline history, which is small when
filtered by keyword — so this script runs auth=<council> + q=<term>
over a multi-year window, one query per term, and persists the union.

Usage:
    python fetch_city_pbsa_planit.py --council Birmingham --years 8
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)

import httpx
from sqlalchemy import create_engine, text

from app.scrapers.base import BaseScraper

PLANIT_BASE = "https://www.planit.org.uk/api/applics/json"
UA = {"User-Agent": "ukops-bd-platform/1.0"}

SEARCH_TERMS = [
    "student accommodation",
    "purpose built student",
    "purpose-built student",
    "student bedspaces",
    "student beds",
    "PBSA",
    "build to rent",
    "build-to-rent",
    "co-living",
]

STATUS_MAP = {
    "Undecided": "Pending", "Permitted": "Approved", "Conditions": "Approved",
    "Refused": "Refused", "Withdrawn": "Withdrawn", "Appeal": "Appeal",
    "Referred": "Pending", "Other": "Unknown", "Not Available": "Unknown",
    "Rejected": "Refused",
}


def _date(v):
    try:
        return datetime.fromisoformat(str(v)[:10]).date()
    except (ValueError, TypeError):
        return None


def fetch_term(client, council, term, start_date, end_date, max_pages=25):
    records = []
    for page in range(1, max_pages + 1):
        try:
            r = client.get(PLANIT_BASE, params={
                "auth": council, "q": term,
                "start_date": start_date, "end_date": end_date,
                "pg_sz": 200, "page": page,
            })
        except Exception as exc:
            print(f"  [{term}] p{page} ERR {exc}")
            break
        if r.status_code == 429:
            wait = min(int(r.headers.get("Retry-After", "60")), 120)
            print(f"  [{term}] p{page} 429, waiting {wait}s")
            time.sleep(wait)
            continue
        if r.status_code != 200:
            print(f"  [{term}] p{page} HTTP {r.status_code}")
            break
        batch = r.json().get("records", []) or []
        if not batch:
            break
        records.extend(batch)
        print(f"  [{term}] p{page}: {len(batch)} (term total {len(records)})")
        time.sleep(0.6)
    return records


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--years", type=int, default=8)
    args = ap.parse_args()

    engine = create_engine(os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"))
    with engine.connect() as c:
        row = c.execute(text("SELECT id FROM councils WHERE name ILIKE :n"),
                        {"n": args.council}).fetchone()
    if row is None:
        raise SystemExit(f"Council {args.council!r} not found")
    council_id = row[0]

    start_date = (date.today() - timedelta(days=365 * args.years)).isoformat()
    end_date = date.today().isoformat()

    by_uid = {}
    with httpx.Client(timeout=30.0, headers=UA) as client:
        for term in SEARCH_TERMS:
            for rec in fetch_term(client, args.council, term, start_date, end_date):
                uid = (rec.get("uid") or "").strip()
                if uid:
                    by_uid.setdefault(uid, rec)
    print(f"\n{len(by_uid)} unique applications fetched")

    saved = existing = 0
    with engine.connect() as c:
        for uid, rec in by_uid.items():
            if c.execute(text(
                "SELECT 1 FROM planning_applications WHERE reference=:r AND council_id=:cid"
            ), {"r": uid, "cid": council_id}).fetchone():
                existing += 1
                continue
            desc = rec.get("description") or ""
            scheme_type = BaseScraper.classify_scheme_type(desc)
            lat = lon = None
            loc = rec.get("location") or {}
            if isinstance(loc, dict) and len(loc.get("coordinates") or []) == 2:
                try:
                    lon, lat = float(loc["coordinates"][0]), float(loc["coordinates"][1])
                except (TypeError, ValueError):
                    pass
            c.execute(text("""
                INSERT INTO planning_applications
                    (reference, council_id, address, description, application_type,
                     status, decision, scheme_type, is_pbsa, is_btr, num_units,
                     latitude, longitude, submission_date, decision_date,
                     portal_url, source, raw_data, created_at, updated_at)
                VALUES (:ref, :cid, :addr, :desc, :atype, :status, :decision,
                        :stype, :pbsa, :btr, :units, :lat, :lon, :sdate, :ddate,
                        :url, 'planit_api', CAST(:raw AS jsonb), NOW(), NOW())
            """), {
                "ref": uid, "cid": council_id,
                "addr": rec.get("address"), "desc": desc,
                "atype": rec.get("app_type"),
                "status": STATUS_MAP.get(rec.get("app_state", ""), "Unknown"),
                "decision": rec.get("app_state"),
                "stype": scheme_type,
                "pbsa": scheme_type == "PBSA",
                "btr": scheme_type == "BTR",
                "units": BaseScraper.extract_unit_count(desc),
                "lat": lat, "lon": lon,
                "sdate": _date(rec.get("start_date")),
                "ddate": _date(rec.get("decided_date")),
                "url": rec.get("link"),
                "raw": json.dumps(rec, default=str),
            })
            saved += 1
        c.commit()
    print(f"saved {saved}, already present {existing}")


if __name__ == "__main__":
    main()
