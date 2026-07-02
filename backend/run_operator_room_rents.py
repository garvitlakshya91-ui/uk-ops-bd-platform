"""Run operator room-level rent scrapers and persist to ``scheme_rents``.

Generic runner for BaseOperatorScraper subclasses: fetches RentRecords,
matches each to an existing scheme (postcode first, then name-token
match within the postcode / council), and inserts rent rows. Never
creates schemes. Idempotent per operator source label (wipe + reload).

Usage:
    python run_operator_room_rents.py --operator unite
    python run_operator_room_rents.py --operator unite --limit 3   # smoke test
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

SCRAPERS = {
    "unite": ("app.scrapers.operators.unite_students", "UniteScraper",
              "operator_unite_students"),
}

STOP = {"the", "and", "house", "court", "student", "students", "hall",
        "halls", "studios", "apartments", "living", "residence"}


def toks(s: str) -> set[str]:
    return {w for w in re.findall(r"[a-z]{3,}", (s or "").lower())
            if w not in STOP}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--operator", required=True, choices=sorted(SCRAPERS))
    ap.add_argument("--limit", type=int, default=0, help="stop after N records")
    args = ap.parse_args()

    mod_name, cls_name, source = SCRAPERS[args.operator]
    mod = __import__(mod_name, fromlist=[cls_name])
    scraper_cls = getattr(mod, cls_name)

    engine = create_engine(DB_URL)
    with engine.connect() as c:
        # postcode -> [(id, name)]
        pc_map: dict[str, list] = {}
        for sid, nm, pck in c.execute(text("""
            SELECT id, name, REPLACE(UPPER(COALESCE(postcode,'')),' ','')
            FROM existing_schemes WHERE COALESCE(postcode,'') <> ''
        """)).fetchall():
            pc_map.setdefault(pck, []).append((sid, nm))

    stats = Counter()
    rows = []
    with scraper_cls() as scraper:
        for rec in scraper.fetch_all():
            stats["records"] += 1
            if args.limit and stats["records"] > args.limit:
                break
            pck = (rec.postcode or "").upper().replace(" ", "")
            cands = pc_map.get(pck, [])
            sid = None
            if len(cands) == 1:
                sid = cands[0][0]
            elif cands:
                rt = toks(rec.scheme_name)
                best, score = None, 0
                for cid, cname in cands:
                    s = len(rt & toks(cname))
                    if s > score:
                        best, score = cid, s
                sid = best if score >= 1 else cands[0][0]
            if not sid:
                stats["no_scheme_at_pc"] += 1
                continue
            ppw = rec.rent_per_week or rec.rent_min_per_week
            if not ppw:
                stats["no_rent"] += 1
                continue
            stats["matched"] += 1
            rows.append({
                "sid": sid, "rt": (rec.room_type or "Advertised")[:100],
                "ppw": ppw,
                "pm": rec.rent_per_month or round(ppw * 52 / 12, 2),
                "ay": rec.academic_year,
                "wk": rec.contract_length_weeks,
                "src": source, "ref": (rec.source_url or "")[:500],
            })

    print(f"\nfetched={stats['records']:,} matched_rows={len(rows):,} "
          f"schemes={len({r['sid'] for r in rows}):,}")
    for k, v in stats.most_common():
        print(f"   {k:20} {v:,}")

    if not rows:
        print("nothing to save")
        return
    with engine.begin() as c:
        existing = c.execute(text(
            "SELECT COUNT(*) FROM scheme_rents WHERE source = :s"),
            {"s": source}).scalar()
        if existing and len(rows) < existing * 0.5:
            print(f"ABORT: new scrape ({len(rows)} rows) is <50% of existing "
                  f"({existing}) — parser likely degraded; not replacing.")
            return
        # replace only the schemes we re-scraped, keep the rest
        c.execute(text("""
            DELETE FROM scheme_rents WHERE source = :s
            AND scheme_id = ANY(:sids)
        """), {"s": source, "sids": list({r["sid"] for r in rows})})
        for r in rows:
            c.execute(text("""
                INSERT INTO scheme_rents
                    (scheme_id, room_type, rent_per_week, rent_per_month,
                     currency, academic_year, contract_length_weeks,
                     source, source_reference, scraped_at, created_at)
                VALUES (:sid, :rt, :ppw, :pm, 'GBP', :ay, :wk,
                        :src, :ref, NOW(), NOW())
            """), r)
    print(f"saved {len(rows):,} rent rows "
          f"({len({r['sid'] for r in rows}):,} schemes) as {source}")


if __name__ == "__main__":
    main()
