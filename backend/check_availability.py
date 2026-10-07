"""Capture per-scheme availability states — the revealed-demand series.

For every census scheme with an operator/listing URL, fetch the page
(httpx, Chromium fallback) and classify the letting state from on-page
wording. Appends one scheme_availability row per scheme per run; run
weekly through the Aug-Oct letting window, and at each quarterly rent
capture otherwise.

Usage:
    python check_availability.py --city Birmingham [--limit 60] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import text as sqltext

from app.database import SessionLocal
from app.models.models import Council, SchemeAvailability

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

SOLD_OUT_RE = re.compile(
    r"sold\s*out|fully\s*(?:booked|let|reserved)|no\s+(?:rooms?|availability)\s+"
    r"(?:available|left|remaining)|join\s+(?:the\s+)?wait\s*list|waiting\s+list\s+only",
    re.I,
)
LIMITED_RE = re.compile(
    r"last\s+(?:few\s+)?rooms?|limited\s+availability|selling\s+fast|"
    r"only\s+\d+\s+(?:rooms?|studios?)\s+left|low\s+availability",
    re.I,
)
# Academic-year the page is letting for ("2027/28", "2027-28", "27/28")
AY_RE = re.compile(r"\b(20\d{2})\s*[/–-]\s*(?:20)?(\d{2})\b")


def classify(html: str) -> tuple[str, str | None]:
    text = re.sub(r"(?s)<(script|style)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    state = "available"
    if SOLD_OUT_RE.search(text):
        state = "sold_out"
    elif LIMITED_RE.search(text):
        state = "limited"
    year = None
    m = AY_RE.search(text)
    if m:
        year = f"{m.group(1)}-{m.group(2)[-2:]}"
    return state, year


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--limit", type=int, default=80)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")

    rows = db.execute(sqltext("""
        SELECT DISTINCT ON (sr.scheme_id) sr.scheme_id, es.name, sr.source_reference
        FROM scheme_rents sr
        JOIN existing_schemes es ON es.id = sr.scheme_id
        WHERE es.council_id = :cid AND sr.is_current
          AND sr.source_reference LIKE 'http%%'
        ORDER BY sr.scheme_id, sr.scraped_at DESC
        LIMIT :lim
    """), {"cid": council.id, "lim": args.limit}).fetchall()
    print(f"{args.city}: checking {len(rows)} schemes with listing URLs")

    browser = None
    if not args.no_browser:
        from app.scrapers.browser_fetch import BrowserFetcher
        browser = BrowserFetcher()

    counts: dict[str, int] = {}
    try:
        for scheme_id, name, url in rows:
            html = None
            try:
                r = httpx.get(url, headers=UA, timeout=25.0, follow_redirects=True)
                if r.status_code == 200 and len(r.text) > 1500:
                    html = r.text
            except Exception:
                pass
            if html is None and browser is not None:
                html = browser.get(url)
            if html is None:
                state, year = "no_listing", None
            else:
                state, year = classify(html)
            counts[state] = counts.get(state, 0) + 1
            print(f"  {name}: {state}" + (f" ({year})" if year else ""))
            if not args.dry_run:
                db.add(SchemeAvailability(
                    scheme_id=scheme_id, state=state, academic_year=year,
                    source="listing_page_check", source_reference=url,
                ))
        if not args.dry_run:
            db.commit()
    finally:
        if browser:
            browser.close()
        db.close()
    print("\nstates:", dict(sorted(counts.items())))


if __name__ == "__main__":
    main()
