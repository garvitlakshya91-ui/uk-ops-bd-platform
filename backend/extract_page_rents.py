"""Generic property-page rent extractor (credit-free lever 3+4).

For rent-less schemes whose ``source_reference`` is a property URL
(operator property pages, university hall fee pages), fetch the page
and extract advertised prices via three strategies:

  1. JSON-LD ``offers`` / ``priceSpecification`` blocks
  2. Embedded JSON price fields (``rent``, ``price``, ``pricePerWeek``…)
  3. Text regex: £X per week / pw / pppw and £X pcm / per month

Weekly prices kept in a plausible band (£60–£550); monthly £300–£4,000.
Writes min/max as 'From'/'To' rows, source='page_price_extract'.
Idempotent per source label. Polite: 1.2s between requests.

Usage:
    python extract_page_rents.py [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
SOURCE = "page_price_extract"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

PW_RE = re.compile(
    r"£\s*([\d,]+(?:\.\d{1,2})?)\s*(?:per\s+week|pw\b|p/w|pppw|/\s*week|a\s+week|weekly)",
    re.I)
PCM_RE = re.compile(
    r"£\s*([\d,]+(?:\.\d{1,2})?)\s*(?:per\s+month|pcm\b|p/m|pppm|/\s*month|a\s+month|monthly)",
    re.I)
JSONLD_RE = re.compile(
    r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)
# generic embedded-JSON price keys
KEY_RE = re.compile(
    r'"(?:price_?per_?week|weekly_?(?:price|rent)|rent_?ppw|price_?pw|pricePerWeek)"'
    r'\s*:\s*"?(\d{2,3}(?:\.\d{1,2})?)"?', re.I)


def _nums(matches):
    out = []
    for m in matches:
        try:
            out.append(float(str(m).replace(",", "")))
        except ValueError:
            pass
    return out


def extract_prices(html: str):
    """Return (weekly_prices, monthly_prices) found on the page."""
    weekly, monthly = [], []

    # 1) JSON-LD offers
    for blob in JSONLD_RE.findall(html):
        try:
            data = json.loads(blob.strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                price = node.get("price") or node.get("lowPrice") or node.get("minPrice")
                if price is not None:
                    try:
                        p = float(str(price).replace(",", ""))
                        unit = json.dumps(node).lower()
                        (weekly if ("week" in unit or "wee" in str(
                            node.get("unitText", "")).lower()) else monthly
                         ).append(p)
                    except ValueError:
                        pass
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)

    # 2) embedded JSON keys
    weekly += _nums(KEY_RE.findall(html))

    # 3) text regex
    weekly += _nums(PW_RE.findall(html))
    monthly += _nums(PCM_RE.findall(html))

    weekly = sorted({p for p in weekly if 60 <= p <= 550})
    monthly = sorted({p for p in monthly if 300 <= p <= 4000})
    return weekly, monthly


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT es.id, es.name, es.source_reference
            FROM existing_schemes es
            WHERE es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
              AND es.source_reference LIKE 'http%'
              AND NOT EXISTS (SELECT 1 FROM scheme_rents sr
                              WHERE sr.scheme_id = es.id
                                AND sr.source NOT IN ('ons_pipr_area'))
            ORDER BY es.id
        """)).fetchall()
    if args.limit:
        rows = rows[: args.limit]
    print(f"{len(rows):,} rent-less schemes with property URLs")

    stats = Counter()
    payload = []
    client = httpx.Client(headers={"User-Agent": UA}, timeout=15,
                          follow_redirects=True)
    for i, (sid, name, url) in enumerate(rows, 1):
        try:
            resp = client.get(url)
            if resp.status_code != 200:
                stats[f"http_{resp.status_code}"] += 1
                continue
            weekly, monthly = extract_prices(resp.text)
        except Exception:
            stats["fetch_error"] += 1
            continue
        finally:
            time.sleep(1.2)

        if not weekly and not monthly:
            stats["no_price_found"] += 1
            continue
        stats["schemes_with_prices"] += 1
        pairs = []
        if weekly:
            pairs.append(("From", weekly[0], round(weekly[0] * 52 / 12, 2)))
            if len(weekly) > 1:
                pairs.append(("To", weekly[-1], round(weekly[-1] * 52 / 12, 2)))
        elif monthly:
            pairs.append(("From", round(monthly[0] * 12 / 52, 2), monthly[0]))
            if len(monthly) > 1:
                pairs.append(("To", round(monthly[-1] * 12 / 52, 2), monthly[-1]))
        for rt, ppw, pcm in pairs:
            payload.append({"sid": sid, "rt": rt, "ppw": ppw, "pcm": pcm,
                            "ref": url[:500]})
        if i % 50 == 0:
            print(f"  {i}/{len(rows)} fetched, "
                  f"{stats['schemes_with_prices']} with prices", flush=True)

    print(f"\n{stats['schemes_with_prices']:,} schemes with prices, "
          f"{len(payload):,} rows")
    for k, v in stats.most_common():
        print(f"   {k:22} {v:,}")
    if args.dry_run or not payload:
        return

    with engine.begin() as c:
        c.execute(text(
            "DELETE FROM scheme_rents WHERE source = :s AND scheme_id = ANY(:ids)"
        ), {"s": SOURCE, "ids": list({p["sid"] for p in payload})})
        for p in payload:
            c.execute(text("""
                INSERT INTO scheme_rents
                    (scheme_id, room_type, rent_per_week, rent_per_month,
                     currency, source, source_reference, scraped_at, created_at)
                VALUES (:sid, :rt, :ppw, :pcm, 'GBP', :src, :ref, NOW(), NOW())
            """), {**p, "src": SOURCE})
    print(f"saved {len(payload):,} rent rows")


if __name__ == "__main__":
    main()
