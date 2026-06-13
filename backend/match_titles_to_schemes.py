"""Title → scheme owner matcher (step 9).

Fills ``existing_schemes.owner_company_id`` from HMLR title data for
schemes that have no owner recorded.

Matching rules, conservative by design (titles are legal records but
addresses are messy):

  1. Exact postcode match (pc_key) between scheme and title.
  2. If several titles share the postcode, prefer ones whose address
     mentions the scheme's name token or street number.
  3. The chosen proprietor must be a company (CCOD/OCOD always is).
  4. Creates a companies row (with CH number when present) if new,
     then sets owner_company_id. Never overwrites an existing owner.

Usage:
    python match_titles_to_schemes.py --dry-run
    python match_titles_to_schemes.py            # apply
    python match_titles_to_schemes.py --prospect "ORKA INVESTMENTS"   # reverse lookup
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from merge_afs_schemes import find_or_create_company

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

STOP = {
    "the", "and", "house", "court", "street", "road", "lane", "place",
    "park", "view", "halls", "hall", "student", "studios", "apartments",
    "building", "block", "phase", "north", "south", "east", "west",
}


def name_tokens(name: str) -> list[str]:
    return [w for w in re.findall(r"[a-z]{4,}", (name or "").lower())
            if w not in STOP]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--prospect", help="Reverse lookup: list titles for proprietor name")
    args = ap.parse_args()

    engine = create_engine(DB_URL)

    if args.prospect:
        with engine.connect() as c:
            rows = c.execute(text("""
                SELECT source, title_number, address, postcode,
                       proprietor_name_1, ch_number_1, price_paid
                FROM title_ownership
                WHERE LOWER(proprietor_name_1) LIKE LOWER(:p)
                   OR LOWER(proprietor_name_2) LIKE LOWER(:p)
                ORDER BY postcode LIMIT 50
            """), {"p": f"%{args.prospect.lower()}%"}).fetchall()
            print(f"{len(rows)} titles for proprietor ~ {args.prospect!r}:")
            for r in rows:
                price = f" £{r[6]:,}" if r[6] else ""
                print(f"  [{r[0]}] {r[1]:<10} {(r[2] or '')[:70]:<72} "
                      f"{r[3] or '':<9} {r[4][:40]}{price}")
        return

    stats = Counter()
    with engine.begin() as c:
        schemes = c.execute(text("""
            SELECT es.id, es.name, es.address,
                   REPLACE(UPPER(COALESCE(es.postcode,'')),' ','') AS pck
            FROM existing_schemes es
            WHERE es.owner_company_id IS NULL
              AND es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
              AND COALESCE(es.postcode,'') <> ''
        """)).fetchall()
        print(f"{len(schemes):,} ownerless BD schemes with postcodes")

        for sid, sname, saddr, pck in schemes:
            titles = c.execute(text("""
                SELECT title_number, address, proprietor_name_1, ch_number_1,
                       category_1, source
                FROM title_ownership WHERE pc_key = :pc
            """), {"pc": pck}).fetchall()
            if not titles:
                stats["no_title_at_pc"] += 1
                continue

            chosen = None
            if len(titles) == 1:
                chosen = titles[0]
                stats["matched_single_title"] += 1
            else:
                toks = name_tokens(sname)
                num_m = re.match(r"^\s*(\d+[a-zA-Z]?)[,\s]", (saddr or sname) or "")
                snum = num_m.group(1).lower() if num_m else None
                best, best_score = None, 0
                for t in titles:
                    taddr = (t[1] or "").lower()
                    score = sum(2 for tok in toks if tok in taddr)
                    if snum and re.search(rf"\b{re.escape(snum)}\b", taddr):
                        score += 3
                    if score > best_score:
                        best, best_score = t, score
                if best and best_score >= 2:
                    chosen = best
                    stats["matched_by_address"] += 1
                else:
                    stats["ambiguous_pc"] += 1
                    continue

            prop, chnum = chosen[2], (chosen[3] or "").strip()
            if not prop:
                stats["no_proprietor"] += 1
                continue
            if args.dry_run:
                stats["would_set_owner"] += 1
                if stats["would_set_owner"] <= 12:
                    print(f"  [WOULD SET] #{sid} {sname[:40]!r} -> {prop[:45]} "
                          f"({chnum or 'no CH#'}) [{chosen[5]}]")
                continue
            # Prefer matching on CH number (the stable key) so we reuse an
            # existing company row instead of creating a name-duplicate.
            cid, created = None, False
            if chnum:
                row = c.execute(text(
                    "SELECT id FROM companies WHERE companies_house_number = :n LIMIT 1"
                ), {"n": chnum}).first()
                if row:
                    cid = row[0]
            if cid is None:
                cid, created = find_or_create_company(c, prop, False)
                if chnum:
                    c.execute(text("""
                        UPDATE companies SET companies_house_number = :n, updated_at = NOW()
                        WHERE id = :i AND COALESCE(companies_house_number,'') = ''
                    """), {"n": chnum, "i": cid})
            c.execute(text("""
                UPDATE existing_schemes
                SET owner_company_id = :o,
                    notes = LEFT(COALESCE(notes,'') ||
                            ' Owner from HMLR title ' || :t || '.', 1000),
                    updated_at = NOW()
                WHERE id = :s AND owner_company_id IS NULL
            """), {"o": cid, "s": sid, "t": chosen[0]})
            stats["owner_set"] += 1
            if created:
                stats["companies_created"] += 1

    print("\n=== Summary ===")
    for k, v in stats.most_common():
        print(f"  {k:<22}: {v:,}")


if __name__ == "__main__":
    main()
