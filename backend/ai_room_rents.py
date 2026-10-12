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
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import text as sqltext

from pydantic import BaseModel, Field

from app.database import SessionLocal
from app.extraction.units import extract_units
from app.models.models import Council, SchemeIncentive, SchemeRoomType, UnitExtractionRecord
from app.models.observations import CACHE_MIN_CONFIDENCE, record_observation
from app.models.rents import record_incentive, record_room_rent, supersede

# Bulk page extraction runs on the current Haiku.
MODEL = "claude-haiku-5-5"
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
total_beds: total bed spaces in the building if the page states a number; null if it
  only says "over 200 students" or similar, or says nothing.
total_beds_quote: the exact phrase from the page the number came from, else null.
amenities: list of amenities named on the page (gym, cinema, study rooms...).
incentives: list of offers (cashback, "first month free", discount %).
booking_status: "sold_out", "limited" or "available" if the page says.

Page text:
"""


class Room(BaseModel):
    room_type: str
    sub_classification: Optional[str] = None
    rent_ppw: Optional[float] = None
    tenancy_weeks: Optional[int] = None
    bills_included: Optional[bool] = None
    rooms_count: Optional[int] = None
    room_size_sqm: Optional[float] = None
    academic_year: Optional[str] = None


class PageReading(BaseModel):
    rooms: list[Room] = Field(default_factory=list)
    total_beds: Optional[int] = None
    total_beds_quote: Optional[str] = None
    amenities: list[str] = Field(default_factory=list)
    incentives: list[str] = Field(default_factory=list)
    booking_status: Optional[str] = None


def beds_confidence(reading: PageReading, text: str) -> tuple[float, str | None]:
    """Score the page's bed count by what its quoted phrase actually says."""
    if not reading.total_beds:
        return 0.0, None
    quote = reading.total_beds_quote or ""
    squash = lambda s: re.sub(r"\s+", " ", s or "").strip().lower()  # noqa: E731
    verified = bool(quote) and squash(quote) in squash(text)
    if not verified:
        return 0.3, quote or None
    fig = next((f for f in extract_units(quote).of_kind("beds") if f.value == reading.total_beds), None)
    if fig is not None:
        return fig.confidence, quote        # a plain statement scores high, a round-up low
    return (0.7 if str(reading.total_beds) in quote.replace(",", "") else 0.3), quote


def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None, max_retries=3)


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


def extract(client, html: str) -> tuple[Optional[PageReading], str]:
    """The page's structured reading and the text it was read from."""
    text = page_text(html)
    resp = client.messages.parse(
        model=MODEL, max_tokens=4000,
        messages=[{"role": "user", "content": PROMPT + text}],
        output_format=PageReading,
    )
    if resp.stop_reason == "refusal":
        return None, text
    return resp.parsed_output, text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--limit", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--ids", type=int, nargs="+", default=None,
                    help="only these scheme ids (a reviewed target list)")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set — add it to the environment")
    client = anthropic_client()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")

    # One operator/listing URL per scheme, freshest first; operator sites
    # preferred over listing aggregators when both exist. Student schemes
    # only: the council also holds BTR and mixed-use records with URLs.
    params = {"cid": council.id, "lim": args.limit}
    id_filter = ""
    if args.ids:
        id_filter = " AND x.scheme_id = ANY(:ids)"
        params["ids"] = args.ids
    rows = db.execute(sqltext("""
        SELECT DISTINCT ON (x.scheme_id) x.scheme_id, es.name, x.source_reference
        FROM (
            SELECT scheme_id, source_reference, captured_at AS at,
                   CASE WHEN source IN ('operator_directory','operator_page',
                                        'operator_unite_students') THEN 0 ELSE 1 END AS pref
            FROM scheme_room_rents WHERE source_reference LIKE 'http%%'
            UNION ALL
            SELECT scheme_id, source_reference, captured_at,
                   CASE WHEN source = 'operator_directory' THEN 0 ELSE 1 END
            FROM scheme_advertised_rents WHERE source_reference LIKE 'http%%'
            UNION ALL
            SELECT scheme_id, source_reference, captured_at, 1
            FROM scheme_availability WHERE source_reference LIKE 'http%%'
            UNION ALL
            SELECT scheme_id, source_reference, last_seen,
                   CASE WHEN source = 'operator_page' THEN 0 ELSE 1 END
            FROM scheme_source_records WHERE status = 'matched' AND source_reference LIKE 'http%%'
        ) x JOIN existing_schemes es ON es.id = x.scheme_id
        WHERE es.council_id = :cid AND es.scheme_type = 'PBSA'""" + id_filter + """
        ORDER BY x.scheme_id, x.pref, x.at DESC
        LIMIT :lim
    """), params).fetchall()
    print(f"{len(rows)} schemes with operator/listing URLs")

    browser = None
    if not args.no_browser:
        from app.scrapers.browser_fetch import BrowserFetcher
        browser = BrowserFetcher()

    stats = {"schemes": 0, "room_tiers": 0, "beds": 0, "beds_low_confidence": 0,
             "amenities": 0, "incentives": 0}
    try:
        for scheme_id, name, url in rows:
            html = fetch(url, browser)
            if not html:
                print(f"  {name}: page fetch failed ({url[:60]})")
                continue
            try:
                data, text = extract(client, html)
            except Exception as exc:
                print(f"  {name}: AI ERR {type(exc).__name__}: {str(exc)[:120]}")
                continue
            if data is None:
                print(f"  {name}: the model declined to read this page")
                continue

            rooms = [r for r in data.rooms if r.rent_ppw is not None and 40 <= r.rent_ppw <= 1200]
            beds = data.total_beds if data.total_beds and 20 <= data.total_beds <= 5000 else None
            conf, quote = beds_confidence(data, text) if beds else (0.0, None)
            if not args.dry_run:
                for room in rooms:
                    record_room_rent(
                        db, scheme_id=scheme_id, source=SOURCE,
                        room_type=room.room_type,
                        sub_classification=room.sub_classification,
                        academic_year=room.academic_year,
                        rent_ppw=round(float(room.rent_ppw), 2),
                        contract_length_weeks=room.tenancy_weeks,
                        bills_included=room.bills_included,
                        source_reference=url,
                    )
                if rooms:
                    db.query(SchemeRoomType).filter_by(
                        scheme_id=scheme_id, is_current=True, source=SOURCE
                    ).update({"is_current": False})
                    seen = set()
                    for room in rooms:
                        key = (room.room_type, room.sub_classification)
                        if key in seen:
                            continue
                        seen.add(key)
                        db.add(SchemeRoomType(
                            scheme_id=scheme_id, room_type=room.room_type or "Unknown",
                            sub_classification=room.sub_classification,
                            rooms_count=room.rooms_count,
                            room_size_sqm=room.room_size_sqm,
                            source=SOURCE, source_reference=url,
                        ))
                if beds:
                    # The observation always lands; the cache only when the
                    # phrase is a firm statement (record_observation applies the bar).
                    record_observation(db, scheme_id, "beds_total", beds, SOURCE, reference=url,
                                       confidence=conf, quote=quote, extractor="ai", model=MODEL)
                    db.add(UnitExtractionRecord(
                        entity_type="scheme", entity_id=scheme_id, kind="beds", value=beds,
                        is_approximate=conf < CACHE_MIN_CONFIDENCE, confidence=conf,
                        quote=(quote or "")[:500], method="ai", model=MODEL,
                        source="operator_page", source_reference=url,
                    ))
                    stats["beds" if conf >= CACHE_MIN_CONFIDENCE else "beds_low_confidence"] += 1
                if data.amenities:
                    record_observation(db, scheme_id, "amenities",
                                       [str(a)[:60] for a in data.amenities][:30],
                                       SOURCE, reference=url, extractor="ai", model=MODEL)
                    stats["amenities"] += 1
                # A fresh capture: offers no longer on the page leave the
                # current set but stay in history.
                supersede(db, SchemeIncentive, [SOURCE], [scheme_id])
                for offer in data.incentives:
                    record_incentive(db, scheme_id=scheme_id, source=SOURCE, text=str(offer),
                                     source_reference=url)
                    stats["incentives"] += 1
                db.commit()
            stats["room_tiers"] += len(rooms)
            if rooms or beds:
                stats["schemes"] += 1
            print(f"  {name}: {len(rooms)} priced rooms, beds={beds}"
                  f"{f' (conf {conf})' if beds else ''}, "
                  f"{len(data.amenities)} amenities, {len(data.incentives)} incentives")
    finally:
        if browser:
            browser.close()
        db.close()
    print("\n" + ", ".join(f"{k}={v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
