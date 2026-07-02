"""Load already-scraped rents into ``scheme_rents`` (rent backfill).

The AFS, operator-directory and StuRents scrapers captured advertised
rents. Those records that became schemes carry the listing URL in
``existing_schemes.source_reference``; the rent files carry the same
``url``. We join on that exact key — so a rent is only ever attached to
the scheme it actually belongs to. Market-comparable HMO listings that
never became schemes simply don't match (correct).

Rents are normalised to per-week and per-month. Ranges (min/max) become
two rows ("From" / "To"); single advertised rents become one row.

Idempotent: clears the three source labels it owns, then reloads.

Usage:
    python load_scheme_rents.py --dry-run
    python load_scheme_rents.py
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

PPW_TO_PCM = 52.0 / 12.0  # weeks/year ÷ months/year

# source label -> (glob, parser)  — labels are owned by this loader so we
# can safely wipe + reload them without touching other rent sources.
AFS = "afs_directory"
OP = "operator_directory"
ST = "sturents"


def _f(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def rows_afs(d: dict):
    """AFS halls: single advertised rent_ppw, plus contract weeks/year."""
    ppw = _f(d.get("rent_ppw"))
    if not ppw:
        return
    weeks = None
    contracts = d.get("contracts") or []
    wks = [c.get("numberOfWeeks") for c in contracts if c.get("numberOfWeeks")]
    if wks:
        weeks = max(wks)
    ay = ((d.get("raw") or {}).get("academicYearLabel")) or None
    yield {
        "room_type": "Advertised", "rent_per_week": ppw,
        "rent_per_month": round(ppw * PPW_TO_PCM, 2),
        "academic_year": ay, "contract_length_weeks": weeks,
    }


def rows_op(d: dict):
    """Operator directory: rent_ppw_min / rent_ppw_max range."""
    lo, hi = _f(d.get("rent_ppw_min")), _f(d.get("rent_ppw_max"))
    if not lo and not hi:
        return
    if lo:
        yield {"room_type": "From", "rent_per_week": lo,
               "rent_per_month": round(lo * PPW_TO_PCM, 2),
               "academic_year": None, "contract_length_weeks": None}
    if hi and hi != lo:
        yield {"room_type": "To", "rent_per_week": hi,
               "rent_per_month": round(hi * PPW_TO_PCM, 2),
               "academic_year": None, "contract_length_weeks": None}


def rows_st(d: dict):
    """StuRents: per-person-per-week range + optional pcm, lease weeks."""
    lo, hi = _f(d.get("rent_pppw_min")), _f(d.get("rent_pppw_max"))
    pcm = _f(d.get("rent_pcm_min"))
    weeks = d.get("lease_weeks") or None
    if not lo and not hi and not pcm:
        return
    if lo:
        yield {"room_type": "From", "rent_per_week": lo,
               "rent_per_month": pcm or round(lo * PPW_TO_PCM, 2),
               "academic_year": None, "contract_length_weeks": weeks}
    if hi and hi != lo:
        yield {"room_type": "To", "rent_per_week": hi,
               "rent_per_month": round(hi * PPW_TO_PCM, 2),
               "academic_year": None, "contract_length_weeks": weeks}


# (label, glob, parser, allow postcode+name fallback matching)
# StuRents stays URL-only: it is mostly HMO market comparables and
# postcode matching would attach house rents to unrelated schemes.
GROUPS = [
    (AFS, "data/afs/*.jsonl", rows_afs, True),   # AFS = halls directory
    (OP, "data/operator_directories/*.jsonl", rows_op, True),
    (ST, "data/sturents/*.jsonl", rows_st, False),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    with engine.connect() as c:
        ref2id = dict(c.execute(text(
            "SELECT source_reference, id FROM existing_schemes "
            "WHERE source_reference IS NOT NULL")).fetchall())
        pc2schemes: dict[str, list] = {}
        for sid, nm, pck in c.execute(text("""
            SELECT id, name, REPLACE(UPPER(COALESCE(postcode,'')),' ','')
            FROM existing_schemes WHERE COALESCE(postcode,'') <> ''
        """)).fetchall():
            pc2schemes.setdefault(pck, []).append((sid, nm))
    print(f"{len(ref2id):,} schemes carry a source URL\n")

    def _toks(s):
        return {w for w in re.findall(r"[a-z]{3,}", (s or "").lower())
                if w not in ("the", "and", "house", "court", "student",
                             "students", "hall", "halls", "apartments")}

    def match_scheme(d: dict):
        """URL join first; else postcode + name-token match."""
        sid = ref2id.get(d.get("url"))
        if sid:
            return sid
        pck = (d.get("postcode") or "").upper().replace(" ", "")
        cands = pc2schemes.get(pck, [])
        if not cands:
            return None
        nt = _toks(d.get("name"))
        best, score = None, 0
        for cid, cname in cands:
            s = len(nt & _toks(cname))
            if s > score:
                best, score = cid, s
        if score >= 1:
            return best
        # postcode unique to one scheme and no name conflict -> accept
        if len(cands) == 1 and not nt:
            return cands[0][0]
        return None

    stats = Counter()
    payload: list[dict] = []          # rows to insert
    schemes_touched: set[int] = set()
    for label, pattern, parser, fallback in GROUPS:
        for fp in glob.glob(pattern):
            for line in open(fp, encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                sid = match_scheme(d) if fallback else ref2id.get(d.get("url"))
                if not sid:
                    continue
                got = False
                for r in parser(d):
                    got = True
                    payload.append({
                        "scheme_id": sid, "source": label,
                        "source_reference": (d.get("url") or "")[:500],
                        **r,
                    })
                if got:
                    stats[f"{label}_schemes"] = stats[f"{label}_schemes"]
                    schemes_touched.add(sid)
                    stats[f"{label}_rent_rows"] += sum(1 for _ in parser(d))

    by_scheme = Counter(p["scheme_id"] for p in payload)
    print(f"Rent rows to load: {len(payload):,} across "
          f"{len(by_scheme):,} schemes")
    for label, _, _, _ in GROUPS:
        n = sum(1 for p in payload if p["source"] == label)
        ns = len({p["scheme_id"] for p in payload if p["source"] == label})
        print(f"   {label:20} {n:>6,} rows  /  {ns:>5,} schemes")

    if args.dry_run:
        print("\n[dry-run] sample:")
        for p in payload[:8]:
            print(f"   #{p['scheme_id']} [{p['source']}] {p['room_type']:>9} "
                  f"£{p['rent_per_week']}/wk  £{p['rent_per_month']}/mo")
        return

    with engine.begin() as c:
        c.execute(text("DELETE FROM scheme_rents WHERE source IN (:a,:b,:d)"),
                  {"a": AFS, "b": OP, "d": ST})
        for p in payload:
            c.execute(text("""
                INSERT INTO scheme_rents
                    (scheme_id, room_type, rent_per_week, rent_per_month,
                     currency, academic_year, contract_length_weeks,
                     source, source_reference, scraped_at, created_at)
                VALUES (:scheme_id, :room_type, :rent_per_week, :rent_per_month,
                     'GBP', :academic_year, :contract_length_weeks,
                     :source, :source_reference, NOW(), NOW())
            """), p)
    print(f"\nLoaded {len(payload):,} rent rows for {len(by_scheme):,} schemes.")

    with engine.connect() as c:
        tot = c.execute(text(
            "SELECT COUNT(DISTINCT scheme_id) FROM scheme_rents")).scalar()
        bd = c.execute(text("""
            SELECT COUNT(DISTINCT sr.scheme_id) FROM scheme_rents sr
            JOIN existing_schemes es ON es.id = sr.scheme_id
            WHERE es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
        """)).scalar()
    print(f"Total schemes with rents now: {tot:,}  (BD: {bd:,})")


if __name__ == "__main__":
    main()
