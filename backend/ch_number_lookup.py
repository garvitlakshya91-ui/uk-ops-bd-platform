"""Strict Companies House number lookup for BD-linked companies.

Fills ``companies.companies_house_number`` by CH name search for companies
attached to BD schemes that lack one. STRICT matching only: a normalised
name must be equal or near-equal (difflib >= 0.92) to the CH title — a
wrong CH number poisons arrears scores and ownership chains, so uncertain
matches are skipped.

Statutory bodies are excluded up front: councils, universities and most
housing associations (registered societies) are not on Companies House,
so searching them wastes quota and risks false positives.

Usage:
    python ch_number_lookup.py --dry-run [--limit 50]
    python ch_number_lookup.py
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from ownership_pilot import CH, load_api_key

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

# Not on Companies House — don't burn quota searching for these.
STATUTORY_PAT = re.compile(
    r"\b(council|borough|mayor and burgesses|county of|city of|university"
    r"|college|nhs|diocese|the crown)\b", re.I)


def name_key(name: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower())
    s = re.sub(r"\b(limited|ltd|plc|llp|the|of|and|company|co|uk|group)\b",
               " ", s)
    return re.sub(r"\s+", " ", s).strip()


def close(a: str, b: str) -> bool:
    ka, kb = name_key(a), name_key(b)
    if not ka or not kb:
        return False
    return ka == kb or SequenceMatcher(None, ka, kb).ratio() >= 0.92


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    ch = CH(load_api_key())
    engine = create_engine(DB_URL)
    stats = Counter()

    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT DISTINCT co.id, co.name FROM companies co
            WHERE COALESCE(co.companies_house_number,'') = ''
              AND COALESCE(co.name,'') <> ''
              AND EXISTS (SELECT 1 FROM existing_schemes es
                  WHERE co.id IN (es.operator_company_id, es.owner_company_id)
                    AND es.scheme_type IN ('BTR','PBSA','Co-living','Senior'))
            ORDER BY co.id
        """)).fetchall()
    print(f"{len(rows):,} BD-linked companies missing CH number")

    done = 0
    for cid, name in rows:
        if args.limit and done >= args.limit:
            break
        if STATUTORY_PAT.search(name or ""):
            stats["skipped_statutory"] += 1
            continue
        done += 1
        from urllib.parse import quote
        hits = ch.get(f"/search/companies?q={quote(name)}"
                      f"&items_per_page=5") or {}
        items = hits.get("items") or []
        match = None
        for it in items:
            if close(name, it.get("title", "")):
                match = it
                if (it.get("company_status") or "").lower() == "active":
                    break
        if not match:
            stats["no_close_match"] += 1
            continue
        num = (match.get("company_number") or "").strip().upper()
        if not num:
            stats["no_number"] += 1
            continue
        stats["matched"] += 1
        if args.dry_run:
            if stats["matched"] <= 10:
                print(f"  [WOULD SET] {name[:45]:47} -> {num} "
                      f"({match.get('title','')[:40]})")
            continue
        with engine.begin() as c:
            # another row may already own this CH number (unique constraint)
            taken = c.execute(text(
                "SELECT id FROM companies WHERE companies_house_number = :n"
            ), {"n": num}).first()
            if taken:
                stats["number_already_taken"] += 1
                continue
            c.execute(text("""
                UPDATE companies SET companies_house_number = :n,
                    updated_at = NOW()
                WHERE id = :i AND COALESCE(companies_house_number,'') = ''
            """), {"n": num, "i": cid})
        if done % 100 == 0:
            print(f"  {done:,} searched (api calls {ch.calls:,})")

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} "
          f"({done:,} searched, {ch.calls:,} API calls) ===")
    for k, v in stats.most_common():
        print(f"   {k:24} {v:,}")


if __name__ == "__main__":
    main()
