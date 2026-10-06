"""Room-level rent tiers extracted from operator property pages by Claude.

The sellable rent grain is scheme x room type x tier (Classic/Gold/
Platinum) x tenancy weeks. Operator sites publish it on property detail
pages in two dozen different layouts; instead of a parser per brand,
this fetches each page (httpx, Chromium fallback for bot-protected
brands) and has Claude extract the room table. Rows are stored through
the append-only path with sub_classification filled.

Requires ANTHROPIC_API_KEY.

Usage:
    python ai_room_rents.py --city Birmingham [--limit 30] [--dry-run]
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
from app.models.models import Council
from app.models.rent_history import record_rent

MODEL = "claude-haiku-4-5"
SOURCE = "operator_detail_ai"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

PROMPT = """From this student-accommodation property page text, extract every room
option currently advertised with a price. Rules:
- room_type: one of Studio, En-suite, Non en-suite, Twin, 1-bed apartment,
  2-bed apartment, Duplex (map the page's wording to the closest).
- sub_classification: the operator's own tier/brand name for the room
  (e.g. "Classic", "Gold", "Premium Studio"), or null if none.
- rent_ppw: price per person per week in GBP. Convert "per month" by *12/52.
- tenancy_weeks: contract length in weeks if stated, else null.
- Only rooms with explicit prices on the page. Never invent values.
Respond with ONLY a JSON array:
[{"room_type": "...", "sub_classification": ..., "rent_ppw": <number>,
  "tenancy_weeks": <int or null>}, ...]
Return [] if no priced rooms are shown.

Page text:
"""


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set — add it to the environment")

    import anthropic
    client = anthropic.Anthropic()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")

    # One operator URL per scheme, from the freshest current rent row.
    rows = db.execute(sqltext("""
        SELECT DISTINCT ON (sr.scheme_id) sr.scheme_id, es.name, sr.source_reference
        FROM scheme_rents sr
        JOIN existing_schemes es ON es.id = sr.scheme_id
        WHERE es.council_id = :cid AND sr.is_current
          AND sr.source_reference LIKE 'http%%'
        ORDER BY sr.scheme_id, sr.scraped_at DESC
        LIMIT :lim
    """), {"cid": council.id, "lim": args.limit}).fetchall()
    print(f"{len(rows)} schemes with operator/listing URLs")

    browser = None
    if not args.no_browser:
        from app.scrapers.browser_fetch import BrowserFetcher
        browser = BrowserFetcher()

    schemes_done = tiers = 0
    try:
        for scheme_id, name, url in rows:
            html = fetch(url, browser)
            if not html:
                print(f"  {name}: page fetch failed ({url[:60]})")
                continue
            try:
                msg = client.messages.create(
                    model=MODEL, max_tokens=1500,
                    messages=[{"role": "user",
                               "content": PROMPT + page_text(html)}],
                )
                raw = msg.content[0].text
                raw = raw[raw.find("["):raw.rfind("]") + 1]
                rooms = json.loads(raw)
            except Exception as exc:
                print(f"  {name}: AI ERR {str(exc)[:100]}")
                continue
            kept = 0
            for room in rooms:
                ppw = room.get("rent_ppw")
                if not isinstance(ppw, (int, float)) or not 40 <= ppw <= 1200:
                    continue
                kept += 1
                if not args.dry_run:
                    record_rent(
                        db,
                        scheme_id=scheme_id,
                        source=SOURCE,
                        room_type=room.get("room_type"),
                        sub_classification=room.get("sub_classification"),
                        rent_per_week=round(float(ppw), 2),
                        contract_length_weeks=room.get("tenancy_weeks"),
                        source_reference=url,
                    )
            if kept:
                schemes_done += 1
                tiers += kept
            print(f"  {name}: {kept} priced rooms")
        if not args.dry_run:
            db.commit()
    finally:
        if browser:
            browser.close()
        db.close()
    print(f"\n{tiers} room tiers across {schemes_done} schemes")


if __name__ == "__main__":
    main()
