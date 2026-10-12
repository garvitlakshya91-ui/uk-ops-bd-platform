"""Find a page for every census scheme the registry has no URL for.

A scheme with no operator or listing page cannot be read for beds, rents
or letting state. For each such scheme this asks Claude, with web
search, for the operator's own property page, then verifies the answer
itself: the page must load, and its text must carry the scheme's name
tokens or its postcode. Verified pages become matched source records
(source "operator_page", method "web_search"); unverified answers are
kept as proposals for a person. Aggregator pages are accepted only as a
fallback and ranked below operator pages.

Requires ANTHROPIC_API_KEY.

Usage:
    python find_operator_pages.py --city Birmingham [--dry-run] [--ids 1 2]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import text

from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme
from app.registry.core import link_source_record
from app.scrapers.scheme_matching import norm_name, norm_pc

MODEL = "claude-opus-5-5"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      "Accept-Language": "en-GB,en;q=0.9"}
AGGREGATORS = ("accommodationforstudents", "sturents", "studentcrowd", "rightmove", "zoopla", "unilodgers",
               "uniplaces", "amberstudent", "universityliving", "student.com", "google.", "facebook.", "instagram.")
NO_PAGE_SQL = """
    select es.id from existing_schemes es left join companies co on co.id = es.operator_company_id
    where es.council_id = :cid and es.scheme_type = 'PBSA' and es.operating_status is not null
      and es.merged_into_id is null and coalesce(co.company_type, '') <> 'University'
      and not exists (select 1 from scheme_room_rents r where r.scheme_id = es.id and r.source_reference like 'http%')
      and not exists (select 1 from scheme_advertised_rents a where a.scheme_id = es.id and a.source_reference like 'http%')
      and not exists (select 1 from scheme_availability v where v.scheme_id = es.id and v.source_reference like 'http%')
      and not exists (select 1 from scheme_source_records s where s.scheme_id = es.id
                      and s.source_reference like 'http%' and s.status in ('matched', 'proposed'))
"""


def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None, max_retries=3)


def ask_for_page(client, scheme: ExistingScheme, operator: str | None, city: str) -> str | None:
    prompt = (f"Find the operator's own web page for this UK purpose-built student accommodation scheme "
              f"(the page where rooms are listed or booked), not a listings site.\n"
              f"Scheme: {scheme.name}\nOperator: {operator or 'unknown'}\nCity: {city}\n"
              f"Postcode: {scheme.postcode or 'unknown'}\nAddress: {(scheme.address or '')[:120]}\n\n"
              f"Reply with exactly one line: the full URL, or NONE if you cannot find the operator's page. "
              f"If only a listings-site page exists, give that URL.")
    resp = client.messages.create(
        model=MODEL, max_tokens=1500,
        tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 4}],
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.stop_reason == "refusal":
        return None
    answer = " ".join(b.text for b in resp.content if b.type == "text")
    m = re.search(r"https?://[^\s<>\"')\]]+", answer)
    return m.group(0).rstrip(".,") if m else None


def verify(url: str, scheme: ExistingScheme) -> tuple[bool, str]:
    """Does the page exist and talk about this scheme?"""
    try:
        r = httpx.get(url, headers=UA, timeout=30, follow_redirects=True)
    except httpx.HTTPError as exc:
        return False, f"fetch failed: {type(exc).__name__}"
    if r.status_code != 200 or len(r.text) < 500:
        return False, f"HTTP {r.status_code}"
    body = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r.text)).lower()
    tokens = norm_name(scheme.name)
    hits = sum(1 for t in tokens if t in body)
    pc = norm_pc(scheme.postcode)
    pc_hit = bool(pc) and (pc.lower() in body.replace(" ", "") or pc[:-3].lower() + " " in body)
    title = re.search(r"<title>(.*?)</title>", r.text, re.S | re.I)
    title = re.sub(r"\s+", " ", title.group(1)).strip()[:120] if title else ""
    ok = (tokens and hits >= max(1, (len(tokens) + 1) // 2)) or pc_hit
    return bool(ok), f"name tokens {hits}/{len(tokens)}, postcode {'yes' if pc_hit else 'no'}; title {title!r}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--ids", type=int, nargs="+", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set")
    client = anthropic_client()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    ids = args.ids or [r[0] for r in db.execute(text(NO_PAGE_SQL), {"cid": council.id})]
    schemes = db.query(ExistingScheme).filter(ExistingScheme.id.in_(ids or [0])).all()
    ops = {c.id: c.name for c in db.query(Company).filter(
        Company.id.in_([s.operator_company_id for s in schemes if s.operator_company_id] or [0]))}
    print(f"{council.name}: {len(schemes)} census schemes without a page")

    stats = {"matched": 0, "proposed": 0, "none": 0}
    for s in sorted(schemes, key=lambda x: -(x.beds_total or 0)):
        url = ask_for_page(client, s, ops.get(s.operator_company_id), council.name)
        if not url:
            stats["none"] += 1
            print(f"  #{s.id} {s.name[:32]:32s} no page found")
            continue
        ok, why = verify(url, s)
        aggregator = any(a in url.lower() for a in AGGREGATORS)
        status = "matched" if ok else "proposed"
        source = "listing_page" if aggregator else "operator_page"
        stats[status] += 1
        print(f"  #{s.id} {s.name[:32]:32s} {status:8s} {url[:70]}  ({why})")
        if not args.dry_run:
            link_source_record(db, source, url, scheme_id=s.id, raw_name=s.name, raw_postcode=s.postcode,
                               raw_operator=ops.get(s.operator_company_id), match_method="web_search",
                               match_score=1.0 if ok else 0.5, status=status)
            db.commit()
        time.sleep(1.0)
    db.close()
    print("\n" + ", ".join(f"{k}={v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
