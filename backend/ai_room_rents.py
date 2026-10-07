"""Room-level rents, room mix, beds, amenities and incentives from operator
property pages, extracted by Claude.

The sellable rent grain is scheme x room type x tier (Classic/Gold/
Platinum) x tenancy weeks. Operator sites publish it on property detail
pages in two dozen layouts; instead of a parser per brand, this fetches
each page (httpx, Chromium fallback for bot-protected brands) and has
Claude extract the room table plus the scheme facts on the same page.
Everything lands as dated, append-only observations.

Requires ANTHROPIC_API_KEY, plus ANTHROPIC_WORKSPACE_ID for a user-scoped
key.

Usage:
    python ai_room_rents.py --city Birmingham [--limit 60] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import text as sqltext

from app.database import SessionLocal
from app.models.models import Council, SchemeRoomType
from app.models.observations import record_observation
from app.models.rent_history import record_rent

MODEL = "claude-haiku-4-5"
SOURCE = "operator_page"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

PROMPT = """From this student-accommodation property page text, extract what the
page states. Rules: never invent a value; use null when the page does not
say.

rooms: every room option advertised with a price.
  room_type: one of Studio, En-suite, Non en-suite, Twin, 1-bed apartment,
    2-bed apartment, Duplex (map the page's wording to the closest).
  sub_classification: the operator's own tier name (e.g. "Classic", "Gold",
    "Premium Studio"), or null.
  rent_ppw: price per person per week in GBP (convert per-month by *12/52).
  tenancy_weeks: contract length in weeks if stated.
  bills_included: true/false/null.
  rooms_count: number of rooms of this type if stated.
  room_size_sqm: if stated.
  academic_year: the letting year the price is for, as "2027-28", if stated.
total_beds: total bed spaces in the building if stated.
amenities: list of amenities named on the page (gym, cinema, study rooms...).
incentives: list of offers (cashback, "first month free", discount %).
booking_status: "sold_out", "limited" or "available" if the page says.

Respond with ONLY a JSON object:
{"rooms":[{...}], "total_beds": <int|null>, "amenities":[...], "incentives":[...],
 "booking_status": <string|null>}

Page text:
"""


def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None)


def page_text(html: str, cap: int = 14000) -> str:
    html = re.sub(r"(?s)<(script|style|svg|noscript)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text[:cap]


def fetch(url: str, browser) -> str | None:
    try:
        r = httpx.get(url, headers=UA, timeout=30.0, follow_redirects=True)
        if r.status_code == 200 and len(r.text) > 2000:
            return r.text
    except Exception:
        pass
    return browser.get(url) if browser else None


def extract(client, html: str) -> dict:
    msg = client.messages.create(
        model=MODEL, max_tokens=4000,
        messages=[{"role": "user", "content": PROMPT + page_text(html)}],
    )
    raw = msg.content[0].text
    raw = raw[raw.find("{"):raw.rfind("}") + 1]
    return json.loads(raw)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--limit", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set — add it to the environment")
    client = anthropic_client()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")

    # One operator/listing URL per scheme, freshest first; operator sites
    # preferred over listing aggregators when both exist.
    rows = db.execute(sqltext("""
        SELECT DISTINCT ON (x.scheme_id) x.scheme_id, es.name, x.source_reference
        FROM (
            SELECT scheme_id, source_reference, scraped_at AS at,
                   CASE WHEN source IN ('operator_directory','operator_page',
                                        'operator_unite_students') THEN 0 ELSE 1 END AS pref
            FROM scheme_rents WHERE source_reference LIKE 'http%%'
            UNION ALL
            SELECT scheme_id, source_reference, captured_at, 1
            FROM scheme_availability WHERE source_reference LIKE 'http%%'
        ) x JOIN existing_schemes es ON es.id = x.scheme_id
        WHERE es.council_id = :cid
        ORDER BY x.scheme_id, x.pref, x.at DESC
        LIMIT :lim
    """), {"cid": council.id, "lim": args.limit}).fetchall()
    print(f"{len(rows)} schemes with operator/listing URLs")

    browser = None
    if not args.no_browser:
        from app.scrapers.browser_fetch import BrowserFetcher
        browser = BrowserFetcher()

    stats = {"schemes": 0, "room_tiers": 0, "beds": 0, "amenities": 0, "incentives": 0}
    try:
        for scheme_id, name, url in rows:
            html = fetch(url, browser)
            if not html:
                print(f"  {name}: page fetch failed ({url[:60]})")
                continue
            try:
                data = extract(client, html)
            except Exception as exc:
                print(f"  {name}: AI ERR {str(exc)[:120]}")
                continue

            rooms = [r for r in data.get("rooms") or []
                     if isinstance(r.get("rent_ppw"), (int, float)) and 40 <= r["rent_ppw"] <= 1200]
            if not args.dry_run:
                for room in rooms:
                    record_rent(
                        db, scheme_id=scheme_id, source=SOURCE,
                        room_type=room.get("room_type"),
                        sub_classification=room.get("sub_classification"),
                        academic_year=room.get("academic_year"),
                        rent_per_week=round(float(room["rent_ppw"]), 2),
                        contract_length_weeks=room.get("tenancy_weeks"),
                        source_reference=url,
                    )
                    if room.get("bills_included") is not None:
                        record_observation(db, scheme_id, "bills_included",
                                           bool(room["bills_included"]), SOURCE,
                                           reference=url, refresh_cache=False)
                if rooms:
                    db.query(SchemeRoomType).filter_by(
                        scheme_id=scheme_id, is_current=True, source=SOURCE
                    ).update({"is_current": False})
                    seen = set()
                    for room in rooms:
                        key = (room.get("room_type"), room.get("sub_classification"))
                        if key in seen:
                            continue
                        seen.add(key)
                        db.add(SchemeRoomType(
                            scheme_id=scheme_id, room_type=room.get("room_type") or "Unknown",
                            sub_classification=room.get("sub_classification"),
                            rooms_count=room.get("rooms_count"),
                            room_size_sqm=room.get("room_size_sqm"),
                            source=SOURCE, source_reference=url,
                        ))
                beds = data.get("total_beds")
                if isinstance(beds, int) and 20 <= beds <= 5000:
                    record_observation(db, scheme_id, "beds_total", beds, SOURCE, reference=url)
                    stats["beds"] += 1
                if data.get("amenities"):
                    record_observation(db, scheme_id, "amenities",
                                       [str(a)[:60] for a in data["amenities"]][:30],
                                       SOURCE, reference=url)
                    stats["amenities"] += 1
                for offer in data.get("incentives") or []:
                    record_observation(db, scheme_id, "incentive", str(offer)[:200],
                                       SOURCE, reference=url, refresh_cache=False)
                    stats["incentives"] += 1
                db.commit()
            stats["room_tiers"] += len(rooms)
            if rooms or data.get("total_beds"):
                stats["schemes"] += 1
            print(f"  {name}: {len(rooms)} priced rooms, beds={data.get('total_beds')}, "
                  f"{len(data.get('amenities') or [])} amenities, "
                  f"{len(data.get('incentives') or [])} incentives")
    finally:
        if browser:
            browser.close()
        db.close()
    print("\n" + ", ".join(f"{k}={v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
