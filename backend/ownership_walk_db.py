"""Walk + persist ownership chains for ALL BD-scheme owners (scaled step 11).

Streaming, resumable version of ownership_pilot.py + load_ownership_chains.py:
picks owner companies of BD schemes that have a CH number but no
ownership_checked_at, walks each PSC chain, and writes the chain + the
classification straight into the DB. Resumable — re-running only processes
companies not yet stamped.

Usage:
    python ownership_walk_db.py --limit 500     # batch
    python ownership_walk_db.py                 # all remaining
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from ownership_pilot import load_api_key, CH, walk_chain, classify_top, addr_key

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 = all remaining")
    args = ap.parse_args()

    ch = CH(load_api_key())
    engine = create_engine(DB_URL)

    with engine.connect() as c:
        q = """
            SELECT DISTINCT co.id, co.companies_house_number, co.name
            FROM companies co
            JOIN existing_schemes es ON es.owner_company_id = co.id
            WHERE es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
              AND co.ownership_checked_at IS NULL
              AND COALESCE(co.companies_house_number,'') <> ''
            ORDER BY co.id
        """
        if args.limit:
            q += f" LIMIT {args.limit}"
        owners = c.execute(text(q)).fetchall()
    print(f"Walking {len(owners):,} owner companies\n")

    done = 0
    for cid, raw_num, name in owners:
        num = raw_num.strip().upper()
        if num.isdigit():
            num = num.zfill(8)
        prof = ch.profile(num)
        with engine.begin() as c:
            if not prof:
                # registered societies / bad numbers — stamp so we skip next time
                c.execute(text("""
                    UPDATE companies SET ultimate_owner_type = 'NO CH PROFILE',
                        ownership_checked_at = NOW() WHERE id = :i
                """), {"i": cid})
                continue
            sics = prof.get("sic_codes", []) or []
            office = prof.get("registered_office_address", {}) or {}
            office_str = ", ".join(filter(None, [
                office.get("address_line_1"), office.get("locality"),
                office.get("postal_code")]))
            chain = walk_chain(ch, num)
            label = classify_top(chain, prof)
            c.execute(text("""
                UPDATE companies SET
                    ultimate_owner_name = :uon, ultimate_owner_type = :uot,
                    is_spv_candidate = :spv, office_cluster_key = :ock,
                    registered_address = COALESCE(NULLIF(:off,''), registered_address),
                    sic_codes = :sic, ownership_checked_at = NOW(), updated_at = NOW()
                WHERE id = :i
            """), {
                "uon": next((n["name"] for n in reversed(chain)
                             if n.get("kind") == "corporate"), None),
                "uot": label[:60],
                "spv": bool(set(sics) & {"68100", "68209", "68320", "68201"}),
                "ock": addr_key(office),
                "off": office_str, "sic": json.dumps(sics), "i": cid,
            })
            c.execute(text("DELETE FROM ownership_chain_nodes WHERE company_id = :i"),
                      {"i": cid})
            for n in chain:
                c.execute(text("""
                    INSERT INTO ownership_chain_nodes
                        (company_id, level, node_name, node_kind,
                         node_ch_number, node_country, natures_of_control)
                    VALUES (:cid,:lvl,:nm,:kind,:num,:ctry,:nat)
                """), {"cid": cid, "lvl": n.get("level", 1),
                       "nm": (n.get("name") or "?")[:500],
                       "kind": (n.get("kind") or "unknown")[:40],
                       "num": (n.get("reg_number") or None) and n["reg_number"][:80],
                       "ctry": (n.get("country") or None) and n["country"][:100],
                       "nat": json.dumps(n.get("natures", []))})
        done += 1
        if done % 100 == 0:
            print(f"  {done:,}/{len(owners):,}  (API calls {ch.calls:,})")

    print(f"\nWalked {done:,} companies, {ch.calls:,} API calls")


if __name__ == "__main__":
    main()
