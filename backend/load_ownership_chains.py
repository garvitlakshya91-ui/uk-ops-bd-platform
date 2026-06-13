"""Load walked ownership chains (data/ownership_pilot.json) into the DB.

Idempotent: clears + rewrites chain nodes per company on each run, and
updates the classification columns on companies.

Usage:
    python load_ownership_chains.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
SRC = Path(__file__).parent / "data" / "ownership_pilot.json"


def office_key(office: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (office or "").lower())[:120]


def main():
    data = json.loads(SRC.read_text(encoding="utf-8"))
    results = data["results"]
    print(f"Loading {len(results)} ownership records from {SRC.name}")

    engine = create_engine(DB_URL)
    loaded = nodes_written = missing = 0

    with engine.begin() as c:
        for r in results:
            num = r["ch_number"]
            row = c.execute(text("""
                SELECT id FROM companies
                WHERE UPPER(REPLACE(companies_house_number,' ','')) = :n
                   OR UPPER(REPLACE(companies_house_number,' ','')) = LTRIM(:n,'0')
                LIMIT 1
            """), {"n": num}).first()
            if not row:
                missing += 1
                continue
            cid = row[0]
            c.execute(text("""
                UPDATE companies SET
                    ultimate_owner_name = :uon,
                    ultimate_owner_type = :uot,
                    is_spv_candidate = :spv,
                    office_cluster_key = :ock,
                    registered_address = COALESCE(NULLIF(:office,''), registered_address),
                    sic_codes = :sic,
                    ownership_checked_at = NOW(),
                    updated_at = NOW()
                WHERE id = :i
            """), {
                "uon": next((n["name"] for n in reversed(r["chain"])
                             if n.get("kind") == "corporate"), None),
                "uot": r["ultimate_owner_type"][:60],
                "spv": r["is_spv_candidate"],
                "ock": office_key(r.get("registered_office", "")),
                "office": r.get("registered_office", ""),
                "sic": json.dumps(r.get("sic_codes", [])),
                "i": cid,
            })
            c.execute(text(
                "DELETE FROM ownership_chain_nodes WHERE company_id = :i"
            ), {"i": cid})
            for n in r["chain"]:
                c.execute(text("""
                    INSERT INTO ownership_chain_nodes
                        (company_id, level, node_name, node_kind,
                         node_ch_number, node_country, natures_of_control)
                    VALUES (:cid, :lvl, :nm, :kind, :num, :ctry, :nat)
                """), {
                    "cid": cid,
                    "lvl": n.get("level", 1),
                    "nm": (n.get("name") or "?")[:500],
                    "kind": (n.get("kind") or "unknown")[:40],
                    "num": (n.get("reg_number") or None),
                    "ctry": (n.get("country") or None),
                    "nat": json.dumps(n.get("natures", [])),
                })
                nodes_written += 1
            loaded += 1

    print(f"=== Summary ===")
    print(f"  Companies updated : {loaded}")
    print(f"  Chain nodes       : {nodes_written}")
    print(f"  CH number missing : {missing}")


if __name__ == "__main__":
    main()
