"""Fetch specific planning references from PlanIt into planning_applications.

Reference-led collection: a benchmark pack, a news story or an analyst
names a planning reference; this pulls the full public record for each
from PlanIt (id_match) and saves it, flagged PBSA. The stored data comes
from the public planning register, not the benchmark.

Usage:
    python backfill_pipeline_refs.py --council Birmingham \
        --benchmark data/benchmarks/birmingham_student_source_jan26.json
    python backfill_pipeline_refs.py --council Birmingham --refs 2024/06155/PA,...
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)

import httpx
from sqlalchemy import create_engine, text

from app.scrapers.base import BaseScraper
from app.scrapers.pbsa_beds import expected_delivery_year, parse_beds

PLANIT = "https://www.planit.org.uk/api/applics/json"
UA = {"User-Agent": "ukops-bd-platform/1.0"}

STATUS_MAP = {
    "Undecided": "Pending", "Permitted": "Approved", "Conditions": "Approved",
    "Refused": "Refused", "Withdrawn": "Withdrawn", "Appeal": "Appeal",
    "Referred": "Pending", "Rejected": "Refused",
}


def _date(v):
    try:
        return datetime.fromisoformat(str(v)[:10]).date()
    except (ValueError, TypeError):
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--benchmark", help="Benchmark JSON whose pipeline refs to fetch")
    ap.add_argument("--refs", help="Comma-separated planning references")
    args = ap.parse_args()

    refs = []
    if args.benchmark:
        bench = json.loads(Path(args.benchmark).read_text())
        refs += [p["planning_ref"].strip() for p in bench.get("pipeline", [])
                 if p.get("planning_ref")]
    if args.refs:
        refs += [r.strip() for r in args.refs.split(",") if r.strip()]
    refs = list(dict.fromkeys(refs))
    if not refs:
        raise SystemExit("no references given")

    engine = create_engine(os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"))
    with engine.connect() as c:
        row = c.execute(text("SELECT id FROM councils WHERE name ILIKE :n"),
                        {"n": args.council}).fetchone()
    if row is None:
        raise SystemExit(f"Council {args.council!r} not found")
    council_id = row[0]

    def variants(ref: str) -> list[str]:
        """Cleaned lookup candidates for a possibly dirty reference."""
        import re as _re
        base = _re.sub(r"[^0-9A-Za-z/]", "", ref).upper()
        out = [base]
        if _re.fullmatch(r"\d{4}/\d{3,6}", base):
            out.append(base + "/PA")
        if base.endswith("/PA"):
            out.append(base[:-3])
        return list(dict.fromkeys(out))

    saved = updated = missing = 0
    with httpx.Client(timeout=30.0, headers=UA) as client, engine.connect() as c:
        for ref in refs:
            recs = []
            for cand in variants(ref):
                for attempt in range(3):
                    try:
                        r = client.get(PLANIT, params={"auth": args.council,
                                                       "id_match": cand})
                    except Exception as exc:
                        print(f"  {cand}: ERR {exc}")
                        break
                    if r.status_code == 429:
                        wait = min(int(r.headers.get("Retry-After", "30")), 120)
                        print(f"  {cand}: 429, waiting {wait}s")
                        time.sleep(wait)
                        continue
                    recs = r.json().get("records", []) if r.status_code == 200 else []
                    break
                if recs:
                    break
                time.sleep(0.4)
            if not recs:
                print(f"  {ref}: not found on PlanIt")
                missing += 1
                continue
            rec = recs[0]
            uid = (rec.get("uid") or ref).strip()
            desc = rec.get("description") or ""
            status = STATUS_MAP.get(rec.get("app_state", ""), "Unknown")
            beds = parse_beds(desc)
            delivery = expected_delivery_year(status, _date(rec.get("decided_date")))
            lat = lon = None
            loc = rec.get("location") or {}
            if isinstance(loc, dict) and len(loc.get("coordinates") or []) == 2:
                try:
                    lon, lat = float(loc["coordinates"][0]), float(loc["coordinates"][1])
                except (TypeError, ValueError):
                    pass
            exists = c.execute(text(
                "SELECT id FROM planning_applications WHERE reference=:r AND council_id=:cid"
            ), {"r": uid, "cid": council_id}).fetchone()
            if exists:
                c.execute(text("""
                    UPDATE planning_applications
                       SET is_pbsa = TRUE, scheme_type = 'PBSA',
                           pbsa_beds = COALESCE(pbsa_beds, :beds),
                           expected_delivery_year = COALESCE(expected_delivery_year, :dy),
                           updated_at = NOW()
                     WHERE id = :id
                """), {"id": exists[0], "beds": beds, "dy": delivery})
                updated += 1
            else:
                c.execute(text("""
                    INSERT INTO planning_applications
                        (reference, council_id, address, description, application_type,
                         status, decision, scheme_type, is_pbsa, is_btr, num_units,
                         pbsa_beds, expected_delivery_year, latitude, longitude,
                         submission_date, decision_date, portal_url, source, raw_data,
                         created_at, updated_at)
                    VALUES (:ref, :cid, :addr, :desc, :atype, :status, :decision,
                            'PBSA', TRUE, FALSE, :units, :beds, :dy, :lat, :lon,
                            :sdate, :ddate, :url, 'planit_api', CAST(:raw AS jsonb),
                            NOW(), NOW())
                """), {
                    "ref": uid, "cid": council_id,
                    "addr": rec.get("address"), "desc": desc,
                    "atype": rec.get("app_type"), "status": status,
                    "decision": rec.get("app_state"),
                    "units": BaseScraper.extract_unit_count(desc),
                    "beds": beds, "dy": delivery, "lat": lat, "lon": lon,
                    "sdate": _date(rec.get("start_date")),
                    "ddate": _date(rec.get("decided_date")),
                    "url": rec.get("link"),
                    "raw": json.dumps(rec, default=str),
                })
                saved += 1
            print(f"  {ref}: {'updated' if exists else 'saved'}"
                  f" beds={beds} delivery={delivery} status={status}")
            time.sleep(0.6)
        c.commit()
    print(f"\nsaved {saved}, updated {updated}, not found {missing}")


if __name__ == "__main__":
    main()
