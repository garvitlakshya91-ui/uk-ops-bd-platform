"""Ownership-intelligence PILOT — scheme → SPV → platform → ultimate owner.

Proof-of-concept for the director's ask: identify PE investment firms,
white-label SPVs and asset-management platforms behind our schemes —
using ONLY data we already have (owner companies with CH numbers) plus
the free Companies House API (key already configured for arrears).

For each owner company:
  1. Company profile  -> SIC codes, registered office, status
  2. SPV detection    -> property SIC (68xxx) + small officer count +
                         name-similarity to its scheme(s)
  3. PSC chain walk   -> corporate PSCs recursed upward (max depth 4)
                         until an individual, a fund, or an overseas
                         entity terminates the chain
  4. Clustering       -> owner companies grouped by registered office
                         address; >=3 SPVs at one address = platform

Usage:
    python ownership_pilot.py --limit 60            # pilot sample
    python ownership_pilot.py --limit 0             # all (slow)
    python ownership_pilot.py --company 12345678    # single chain
Output: console report + data/ownership_pilot.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
CH_BASE = "https://api.company-information.service.gov.uk"

FOCUS_COUNCILS = [
    "Leeds", "Cardiff", "Exeter", "Southampton", "Middlesbrough", "Colchester",
    "Canterbury", "Lincoln", "Cheshire West and Chester", "Worcester",
    "Winchester", "Lancaster", "Durham", "Gwynedd", "Ceredigion", "York",
]

# Name signals for PE / fund / institutional capital at chain tops
_PE_NAME_PAT = re.compile(
    r"\b(capital|partners|equity|invest(?:ment|ments)?|fund|asset management|"
    r"holdings?|ventures|real estate|reit|infrastructure|pension|"
    r"l\.?p\.?|g\.?p\.?|sarl|s\.a\.r\.l)\b", re.I,
)
# SIC codes: property SPVs and fund/asset managers
SPV_SICS = {"68100", "68209", "68320", "68201"}
FUND_SICS = {"64205", "64209", "64301", "64303", "64999", "66300", "70100"}

_PC_PAT = re.compile(r"[A-Z]{1,2}\d[0-9A-Z]?\s*\d[A-Z]{2}")


def load_api_key() -> str:
    key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("COMPANIES_HOUSE_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        sys.exit("COMPANIES_HOUSE_API_KEY not found (env or backend/.env)")
    return key


class CH:
    """Minimal throttled Companies House API client."""

    def __init__(self, key: str):
        self.client = httpx.Client(auth=(key, ""), timeout=30)
        self._last = 0.0
        self.calls = 0
        self.cache: dict[str, Any] = {}

    def get(self, path: str) -> Optional[dict]:
        if path in self.cache:
            return self.cache[path]
        delta = time.monotonic() - self._last
        if delta < 0.55:                     # ~600 req / 5 min limit
            time.sleep(0.55 - delta)
        self._last = time.monotonic()
        self.calls += 1
        try:
            r = self.client.get(CH_BASE + path)
        except Exception:
            return None
        if r.status_code == 429:
            time.sleep(20)
            return self.get(path)
        if r.status_code != 200:
            self.cache[path] = None
            return None
        d = r.json()
        self.cache[path] = d
        return d

    def profile(self, number: str) -> Optional[dict]:
        return self.get(f"/company/{number}")

    def pscs(self, number: str) -> list[dict]:
        d = self.get(f"/company/{number}/persons-with-significant-control")
        return (d or {}).get("items", [])


def addr_key(office: dict) -> str:
    """Normalised registered-office key for clustering."""
    parts = [office.get("address_line_1"), office.get("postal_code")]
    return re.sub(r"[^a-z0-9]+", "", " ".join(p for p in parts if p).lower())


def walk_chain(ch: CH, number: str, depth: int = 0, seen=None) -> list[dict]:
    """Walk corporate PSCs upward; return chain nodes above this company."""
    if seen is None:
        seen = set()
    if depth >= 4 or number in seen:
        return []
    seen.add(number)
    chain = []
    for psc in ch.pscs(number):
        kind = psc.get("kind", "")
        if psc.get("ceased_on"):
            continue
        if kind.startswith("corporate-entity"):
            ident = psc.get("identification") or {}
            reg = (ident.get("registration_number") or "").strip().upper()
            node = {
                "level": depth + 1,
                "name": psc.get("name"),
                "kind": "corporate",
                "reg_number": reg,
                "country": ident.get("country_registered")
                            or ident.get("place_registered") or "",
                "natures": psc.get("natures_of_control", []),
            }
            chain.append(node)
            # Recurse only into UK-registered parents (8-char CH numbers)
            if reg and re.fullmatch(r"[A-Z0-9]{8}", reg):
                chain.extend(walk_chain(ch, reg, depth + 1, seen))
        elif kind.startswith("individual"):
            chain.append({
                "level": depth + 1,
                "name": psc.get("name"),
                "kind": "individual",
                "natures": psc.get("natures_of_control", []),
            })
        elif kind.startswith("super-secure"):
            chain.append({"level": depth + 1, "name": "(super-secure PSC)",
                          "kind": "super-secure"})
    if not chain and depth == 0:
        # PSC-exempt (e.g. listed co) or statement-only
        st = ch.get(f"/company/{number}/persons-with-significant-control-statements")
        if st and st.get("items"):
            chain.append({"level": 1, "kind": "statement",
                          "name": st["items"][0].get("statement", "")})
    return chain


def classify_top(chain: list[dict], profile: dict) -> str:
    """Label the likely ultimate-owner type for BD."""
    tops = [n for n in chain if n.get("kind") == "corporate"]
    top = tops[-1] if tops else None
    name = (top or {}).get("name", "") or profile.get("company_name", "")
    country = (top or {}).get("country", "")
    if country and not re.search(r"england|wales|scotland|united kingdom|uk", country, re.I):
        if _PE_NAME_PAT.search(name):
            return f"OFFSHORE FUND ({country})"
        return f"OFFSHORE ENTITY ({country})"
    if _PE_NAME_PAT.search(name):
        return "PE / FUND / INSTITUTIONAL"
    if any(n.get("kind") == "individual" for n in chain):
        return "PRIVATE INDIVIDUAL(S)"
    if not chain:
        return "NO PSC DATA (listed/exempt?)"
    return "CORPORATE GROUP"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=60,
                    help="Owner companies to walk (0 = all)")
    ap.add_argument("--company", help="Single CH number to walk")
    args = ap.parse_args()

    ch = CH(load_api_key())

    if args.company:
        prof = ch.profile(args.company) or {}
        print(json.dumps(prof.get("company_name"), indent=1))
        for n in walk_chain(ch, args.company):
            print("  " * n["level"], "->", n.get("name"),
                  f"[{n.get('kind')}]", n.get("country", ""))
        return

    engine = create_engine(DB_URL)
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT co.id, co.name, co.companies_house_number,
                   COUNT(es.id) AS schemes,
                   ARRAY_AGG(DISTINCT cc.name) AS councils,
                   ARRAY_AGG(es.name ORDER BY es.id) AS scheme_names
            FROM companies co
            JOIN existing_schemes es ON es.owner_company_id = co.id
            JOIN councils cc ON cc.id = es.council_id
            WHERE COALESCE(co.companies_house_number,'') <> ''
              AND es.scheme_type IN ('BTR','PBSA','Co-living','Senior')
              AND cc.name = ANY(:councils)
            GROUP BY co.id, co.name, co.companies_house_number
            ORDER BY COUNT(es.id) DESC
        """), {"councils": FOCUS_COUNCILS}).fetchall()

    if args.limit:
        rows = rows[: args.limit]
    print(f"Walking ownership for {len(rows)} owner companies "
          f"(focus cities, largest portfolios first)\n")

    results = []
    clusters = defaultdict(list)
    top_types = Counter()

    for i, (cid, name, number, n_schemes, councils, scheme_names) in enumerate(rows, 1):
        number = number.strip().upper()
        # Registered societies (housing associations) carry mutuals-register
        # numbers like 30938R — they are not on Companies House.
        if re.fullmatch(r"\d{1,5}R|(IP|RS|SP|NI)\d+R?", number):
            top_types["REGISTERED SOCIETY (mutuals register, not CH)"] += 1
            print(f"[{i}] {name[:48]:<50} {n_schemes:>3} schemes       "
                  f"REGISTERED SOCIETY — not on CH")
            continue
        if number.isdigit():
            number = number.zfill(8)   # CH numbers are zero-padded to 8
        prof = ch.profile(number)
        if not prof:
            print(f"[{i}] {name} ({number}) — profile fetch failed")
            continue
        sics = prof.get("sic_codes", []) or []
        office = prof.get("registered_office_address", {}) or {}
        is_spv = (
            bool(set(sics) & SPV_SICS)
            and n_schemes <= 3
        )
        chain = walk_chain(ch, number)
        label = classify_top(chain, prof)
        top_types[label] += 1
        clusters[addr_key(office)].append({
            "company": prof.get("company_name", name), "number": number,
            "schemes": n_schemes, "is_spv": is_spv,
        })
        rec = {
            "owner": prof.get("company_name", name),
            "ch_number": number,
            "schemes": n_schemes,
            "councils": councils,
            "example_scheme": (scheme_names or [None])[0],
            "sic_codes": sics,
            "registered_office": ", ".join(filter(None, [
                office.get("address_line_1"), office.get("locality"),
                office.get("postal_code")])),
            "is_spv_candidate": is_spv,
            "chain": chain,
            "ultimate_owner_type": label,
        }
        results.append(rec)
        top = next((n["name"] for n in reversed(chain)
                    if n.get("kind") == "corporate"), None)
        arrow = f" => {top}" if top else ""
        print(f"[{i}] {rec['owner'][:46]:<48} {n_schemes:>3} schemes  "
              f"{'SPV' if is_spv else '   '}  {label}{arrow[:60]}")

    # ---- platform clustering ----
    print("\n=== ASSET-MANAGEMENT PLATFORM CANDIDATES "
          "(>=3 owner companies sharing a registered office) ===")
    platforms = []
    for key, members in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        if len(members) < 3 or not key:
            continue
        total = sum(m["schemes"] for m in members)
        names = ", ".join(m["company"][:30] for m in members[:4])
        office = next((r["registered_office"] for r in results
                       if addr_key({"address_line_1": "", "postal_code": ""}) != key
                       and re.sub(r"[^a-z0-9]+", "", r["registered_office"].lower()).startswith(key[:12])), key)
        platforms.append({"office_key": key, "companies": len(members),
                          "schemes": total, "members": members})
        print(f"  {len(members):>2} companies / {total:>3} schemes @ {key[:40]:<42} e.g. {names}")

    print("\n=== ULTIMATE-OWNER TYPES ===")
    for label, n in top_types.most_common():
        print(f"  {n:>4}  {label}")
    print(f"\nAPI calls used: {ch.calls}")

    out = Path(__file__).parent / "data" / "ownership_pilot.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(
        {"results": results, "platforms": platforms}, indent=1), encoding="utf-8")
    print(f"Full chains written to {out}")


if __name__ == "__main__":
    main()
