"""Inverse SPV / PE discovery — find scheme-operating companies NOT in our system.

Instead of walking up from schemes we know, search Companies House
*sideways*:

  1. Advanced company search by property SIC codes registered in a
     target city:
        55900  other accommodation (student halls staple)
        68209  letting & operating of own/leased real estate (SPV staple)
        68320  management of real estate on a fee basis (asset managers)
  2. Score relevance by name (student/halls/living/residence/...).
  3. Cross-reference CH numbers against our companies table → NEW vs known.
  4. For new, relevant companies: read the charges register — lender
     charges name the actual property ("land at 12 High St, Lincoln").
     Extract postcodes, cross-ref against existing_schemes → candidate
     schemes MISSING from the platform.

Usage:
    python ch_spv_discovery.py --city Lincoln --city Canterbury
    python ch_spv_discovery.py --city York --max-companies 150
Output: data/spv_discovery/<city>.jsonl + console report.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

from ownership_pilot import load_api_key, CH  # reuse throttled client

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
OUT = Path(__file__).parent / "data" / "spv_discovery"

SIC_TARGETS = ["55900", "68209", "68320"]

_RELEVANT_PAT = re.compile(
    r"student|halls?\b|living|residence|accommodat|lodging|housing|"
    r"lettings?|stays?\b|rooms?\b|co.?living|apart", re.I)
_PC_PAT = re.compile(r"[A-Z]{1,2}\d[0-9A-Z]?\s*\d[A-Z]{2}")

# city → postcode area prefixes for charge-address matching.
# Single-letter areas (B, L, M, G) use letter+digit prefixes so they
# don't swallow BA/LL/ML/GL etc. Cities absent from the map accept all
# charge postcodes (useful for London's many areas).
_DIGITS = [str(d) for d in range(1, 10)]
CITY_PC_AREAS = {
    "Lincoln": ["LN"], "Canterbury": ["CT"], "York": ["YO"],
    "Durham": ["DH"], "Lancaster": ["LA"], "Chester": ["CH"],
    "Worcester": ["WR"], "Winchester": ["SO2"], "Bangor": ["LL5"],
    "Aberystwyth": ["SY23"], "Middlesbrough": ["TS"],
    "Exeter": ["EX"], "Colchester": ["CO"], "Southampton": ["SO"],
    "Cardiff": ["CF"], "Leeds": ["LS"],
    "Bristol": ["BS"], "Edinburgh": ["EH"],
    "Glasgow": ["G" + d for d in _DIGITS],
    "Birmingham": ["B" + d for d in _DIGITS],
    "Liverpool": ["L" + d for d in _DIGITS],
    "Manchester": ["M" + d for d in _DIGITS],
}


def advanced_search(ch: CH, city: str, sic: str, max_results: int) -> list[dict]:
    items, start = [], 0
    while start < max_results:
        size = min(100, max_results - start)
        d = ch.get(f"/advanced-search/companies?location={city}"
                   f"&sic_codes={sic}&company_status=active"
                   f"&size={size}&start_index={start}")
        if not d or not d.get("items"):
            break
        items.extend(d["items"])
        if len(d["items"]) < size:
            break
        start += size
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", action="append", required=True)
    ap.add_argument("--max-companies", type=int, default=150,
                    help="Max companies per city+SIC search")
    ap.add_argument("--max-charges", type=int, default=40,
                    help="Max charge lookups per city")
    args = ap.parse_args()

    ch = CH(load_api_key())
    engine = create_engine(DB_URL)
    OUT.mkdir(exist_ok=True)

    with engine.connect() as conn:
        known_numbers = {
            (r[0] or "").upper().lstrip("0")
            for r in conn.execute(text(
                "SELECT companies_house_number FROM companies "
                "WHERE COALESCE(companies_house_number,'') <> ''"))
        }
        known_postcodes = {
            (r[0] or "").upper().replace(" ", "")
            for r in conn.execute(text(
                "SELECT postcode FROM existing_schemes "
                "WHERE COALESCE(postcode,'') <> ''"))
        }
    print(f"Known CH numbers: {len(known_numbers):,} | "
          f"known scheme postcodes: {len(known_postcodes):,}\n")

    for city in args.city:
        print(f"================ {city} ================")
        seen: dict[str, dict] = {}
        for sic in SIC_TARGETS:
            found = advanced_search(ch, city, sic, args.max_companies)
            print(f"  SIC {sic}: {len(found)} active companies")
            for it in found:
                num = (it.get("company_number") or "").upper()
                if num and num not in seen:
                    it["_sic_hit"] = sic
                    seen[num] = it

        results = []
        new_relevant = []
        for num, it in seen.items():
            name = it.get("company_name", "")
            addr = it.get("registered_office_address", {}) or {}
            addr_str = ", ".join(filter(None, [
                addr.get("address_line_1"), addr.get("locality"),
                addr.get("postal_code")]))
            is_known = num.lstrip("0") in known_numbers
            relevant = bool(_RELEVANT_PAT.search(name))
            rec = {
                "city": city,
                "company_name": name,
                "ch_number": num,
                "sic_hit": it["_sic_hit"],
                "incorporated": it.get("date_of_creation"),
                "registered_office": addr_str,
                "already_in_system": is_known,
                "name_relevant": relevant,
                "charge_addresses": [],
                "new_scheme_postcodes": [],
            }
            results.append(rec)
            if relevant and not is_known:
                new_relevant.append(rec)

        print(f"  unique companies: {len(results)} | "
              f"name-relevant + NEW to system: {len(new_relevant)}")

        # Charges → property addresses for the most promising new companies
        pc_areas = CITY_PC_AREAS.get(city, [])
        charge_budget = args.max_charges
        candidates = 0
        for rec in new_relevant:
            if charge_budget <= 0:
                break
            charge_budget -= 1
            d = ch.get(f"/company/{rec['ch_number']}/charges")
            for item in (d or {}).get("items", []):
                partic = (item.get("particulars") or {}).get("description", "")
                if not partic:
                    continue
                pcs = _PC_PAT.findall(partic.upper())
                if partic and len(partic) < 300:
                    rec["charge_addresses"].append(partic[:200])
                for pc in pcs:
                    pck = pc.replace(" ", "")
                    in_city = not pc_areas or any(
                        pck.startswith(a.replace(" ", "")) for a in pc_areas)
                    if in_city and pck not in known_postcodes:
                        rec["new_scheme_postcodes"].append(pc)
            if rec["new_scheme_postcodes"]:
                candidates += 1

        out_path = OUT / f"{city.lower().replace(' ', '_')}.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in results:
                f.write(json.dumps(rec) + "\n")

        print(f"  charge lookups done; {candidates} companies with NEW-postcode properties")
        print(f"  -> {out_path}")
        top = [r for r in new_relevant if r["new_scheme_postcodes"]][:8]
        for r in top:
            print(f"     {r['company_name'][:48]:<50} {r['ch_number']} "
                  f"pcs={','.join(r['new_scheme_postcodes'][:3])}")
        more = [r for r in new_relevant if not r["new_scheme_postcodes"]][:5]
        if more:
            print("     (new+relevant without charge addresses, sample):")
            for r in more:
                print(f"       {r['company_name'][:60]}")
        print()

    print(f"API calls used: {ch.calls}")


if __name__ == "__main__":
    main()
