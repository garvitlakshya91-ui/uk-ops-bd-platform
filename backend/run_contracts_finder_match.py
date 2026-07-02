"""Contracts Finder → existing-scheme contract matcher (credit-free).

Fetches housing/PBSA contract notices from the free Contracts Finder
OCDS API and attaches them to EXISTING schemes only. Unlike the stock
``ingest_contracts_finder`` pipeline this NEVER creates schemes (the
old pipeline invented junk schemes from tender notices, since cleaned).

Match rules, conservative by design:

  A  POSTCODE: a full postcode found in the notice title/description
     matches a scheme's postcode  ->  attach to that scheme.
  B  BUYER=OWNER: the contracting authority matches a company that
     owns BD schemes (council / housing association) and the notice is
     a housing-management type  ->  stock-wide contract; attach to that
     owner's BD schemes.
  C  UNIVERSITY NOMINATION: buyer is a university, supplier matches a
     known operator  ->  attach to that operator's PBSA schemes in the
     university's city.

Each match writes a ``scheme_contracts`` row and fills the scheme's
``contract_start_date`` / ``contract_end_date`` / ``contract_type``
where empty. Idempotent per notice (source_reference).

Usage:
    python run_contracts_finder_match.py --days 1095 [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from collections import Counter
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

PC_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b")

MGMT_HINTS = (
    "housing management", "property management", "estate management",
    "managing agent", "management of", "facilities management",
    "repairs and maintenance", "responsive repairs", "housing maintenance",
    "concierge", "cleaning and caretaking", "grounds maintenance",
    "student accommodation", "nomination", "halls of residence",
    "accommodation services", "lettings", "tenancy management",
)


def norm(s: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    s = re.sub(r"\b(the|of|and|ltd|limited|plc|llp|group|trust|council"
               r"|borough|city|london|mayor|burgesses)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# Focused, award-stage terms: awards carry supplier + start/end dates
# (tender notices don't), and these terms map to our BD scheme types.
FOCUS_TERMS = [
    "housing management", "property management", "estate management",
    "student accommodation", "purpose built student accommodation",
    "halls of residence", "nomination agreement",
    "build to rent", "residential lettings management",
]

CACHE = os.path.join(os.path.dirname(__file__), "data", "contracts_finder")


def _load_jsonl(fp):
    import json as _json
    with open(fp, encoding="utf-8") as fh:
        return [_json.loads(l) for l in fh if l.strip()]


def fetch(days: int, use_cache: bool = False):
    """Fetch award notices in quarterly windows, one cache file each.

    Resumable: a window with a ``.done`` marker is loaded from disk and
    skipped. Rate-limit-aware: cooldown pauses between windows and a
    long sleep after any term failure, so the free API's limiter is
    respected over an overnight run. ``--use-cache`` never fetches.
    """
    import json as _json
    import time

    os.makedirs(CACHE, exist_ok=True)

    windows = []
    end = date.today()
    start_all = end - timedelta(days=days)
    while end > start_all:
        begin = max(start_all, end - timedelta(days=91))
        windows.append((begin, end))
        end = begin - timedelta(days=1)

    from app.scrapers.contracts_finder import ContractsFinderScraper

    async def _fetch_window(w_from, w_to, wf):
        seen: set[str] = set()
        with open(wf, "w", encoding="utf-8") as fh:
            for term in FOCUS_TERMS:
                scraper = ContractsFinderScraper()
                try:
                    batch = await scraper.run(
                        search_terms=[term],
                        published_from=w_from,
                        published_to=w_to,
                        stages="award",
                        max_pages=40,
                    )
                except Exception as exc:
                    print(f"  [term FAILED] {w_from} {term!r}: {exc} "
                          f"— cooling down 300s", flush=True)
                    time.sleep(300)
                    continue
                for b in batch:
                    ref = b.get("source_reference")
                    if ref in seen:
                        continue
                    seen.add(ref)
                    b.pop("raw_release", None)
                    fh.write(_json.dumps(b, default=str) + "\n")
                fh.flush()
        return len(seen)

    out = []
    for w_from, w_to in windows:
        wf = os.path.join(CACHE, f"win_{w_from:%Y%m%d}.jsonl")
        done = wf + ".done"
        if os.path.exists(done):
            out.extend(_load_jsonl(wf))
            print(f"  [cached window] {w_from} → {w_to}", flush=True)
            continue
        if use_cache:
            continue
        n = asyncio.run(_fetch_window(w_from, w_to, wf))
        open(done, "w").close()
        out.extend(_load_jsonl(wf))
        print(f"  [window done] {w_from} → {w_to}: {n:,} notices "
              f"— cooldown 120s", flush=True)
        time.sleep(120)

    # legacy single-file cache (first partial run) — merge if present
    legacy = os.path.join(CACHE, f"notices_{days}d.jsonl")
    if os.path.exists(legacy):
        seen = {b.get("source_reference") for b in out}
        out.extend(b for b in _load_jsonl(legacy)
                   if b.get("source_reference") not in seen)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1095)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--use-cache", action="store_true",
                    help="reuse cached notices instead of fetching")
    args = ap.parse_args()

    print(f"Fetching Contracts Finder notices ({args.days} days back)...")
    notices = fetch(args.days, use_cache=args.use_cache)
    print(f"  {len(notices):,} housing-related notices fetched")

    engine = create_engine(DB_URL)
    stats = Counter()
    with engine.begin() as c:
        # lookup maps
        pc2scheme = {}
        for sid, pck in c.execute(text("""
            SELECT id, REPLACE(UPPER(postcode),' ','') FROM existing_schemes
            WHERE COALESCE(postcode,'') <> ''""")).fetchall():
            pc2scheme.setdefault(pck, []).append(sid)

        owners = c.execute(text("""
            SELECT co.id, co.name, COUNT(es.id)
            FROM companies co JOIN existing_schemes es ON es.owner_company_id = co.id
            WHERE es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
            GROUP BY co.id, co.name""")).fetchall()
        owner_by_norm = {norm(nm): (cid, cnt) for cid, nm, cnt in owners if norm(nm)}

        operators = c.execute(text("""
            SELECT co.id, co.name FROM companies co
            WHERE EXISTS (SELECT 1 FROM existing_schemes es
                          WHERE es.operator_company_id = co.id)""")).fetchall()
        op_by_norm = {norm(nm): cid for cid, nm in operators if norm(nm)}

        already = {r[0] for r in c.execute(text(
            "SELECT source_reference FROM scheme_contracts WHERE source='contracts_finder'"
        )).fetchall()}

        def add_contract(sid, n, ctype, op_id=None, client_id=None):
            stats[f"contract_{ctype}"] += 1
            if args.dry_run:
                return
            c.execute(text("""
                INSERT INTO scheme_contracts
                    (scheme_id, contract_reference, contract_type,
                     operator_company_id, client_company_id,
                     contract_start_date, contract_end_date, contract_value,
                     currency, source, source_reference, is_current,
                     created_at, updated_at)
                VALUES (:sid, :ref, :ct, :op, :cl, :sd, :ed, :val, 'GBP',
                        'contracts_finder', :ref, :cur, NOW(), NOW())
            """), {"sid": sid, "ref": n["source_reference"], "ct": ctype,
                   "op": op_id, "cl": client_id,
                   "sd": n.get("contract_start_date"),
                   "ed": n.get("contract_end_date"),
                   "val": n.get("contract_value"),
                   "cur": bool(n.get("contract_end_date")
                               and str(n["contract_end_date"]) >= str(date.today()))})
            c.execute(text("""
                UPDATE existing_schemes SET
                    contract_start_date = COALESCE(contract_start_date, :sd),
                    contract_end_date = COALESCE(contract_end_date, :ed),
                    contract_type = COALESCE(contract_type, :ct),
                    updated_at = NOW()
                WHERE id = :sid
            """), {"sid": sid, "sd": n.get("contract_start_date"),
                   "ed": n.get("contract_end_date"), "ct": ctype})

        for n in notices:
            ref = n.get("source_reference")
            if not ref or ref in already:
                stats["skipped_dupe"] += 1
                continue
            if not (n.get("contract_end_date") or n.get("contract_start_date")):
                stats["skipped_no_dates"] += 1
                continue
            blob = f"{n.get('title','')} {n.get('description','')}"
            low = blob.lower()
            if not any(h in low for h in MGMT_HINTS):
                stats["skipped_not_mgmt"] += 1
                continue

            buyer, supplier = n.get("contracting_authority", ""), n.get("supplier", "")
            op_id = op_by_norm.get(norm(supplier)) if supplier else None
            matched = False

            # Rule A — postcode in notice text -> specific scheme(s)
            pcs = {f"{a}{b}" for a, b in PC_RE.findall(blob.upper())}
            hit_sids = {sid for pc in pcs for sid in pc2scheme.get(pc, [])}
            if hit_sids and len(hit_sids) <= 5:
                for sid in hit_sids:
                    add_contract(sid, n, "Management (postcode match)", op_id)
                matched = True

            # Rule B — buyer owns BD stock (council / HA stock-wide contract)
            if not matched and buyer:
                own = owner_by_norm.get(norm(buyer))
                if own and n.get("contract_end_date"):
                    cid, cnt = own
                    sids = [r[0] for r in c.execute(text("""
                        SELECT id FROM existing_schemes
                        WHERE owner_company_id = :o
                          AND scheme_type IN ('BTR','PBSA','Co-living','Senior')
                    """), {"o": cid}).fetchall()]
                    for sid in sids:
                        add_contract(sid, n, "Stock management (buyer=owner)",
                                     op_id, cid)
                    stats["ruleB_owners"] += 1
                    matched = True

            # Rule C — university buyer + known operator supplier -> PBSA
            if not matched and "universit" in (buyer or "").lower() and op_id:
                city_tokens = [t for t in norm(buyer).split()
                               if t not in ("university", "universities")]
                sids = [r[0] for r in c.execute(text("""
                    SELECT es.id FROM existing_schemes es
                    JOIN councils cc ON cc.id = es.council_id
                    WHERE es.operator_company_id = :op AND es.scheme_type='PBSA'
                      AND LOWER(cc.name) LIKE ANY(:pats)
                """), {"op": op_id,
                       "pats": [f"%{t}%" for t in city_tokens] or ["%none%"]}
                ).fetchall()]
                for sid in sids:
                    add_contract(sid, n, "Nomination (university)", op_id)
                if sids:
                    stats["ruleC_universities"] += 1
                    matched = True

            if not matched:
                stats["unmatched"] += 1

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, v in stats.most_common():
        print(f"   {k:36} {v:,}")


if __name__ == "__main__":
    main()
