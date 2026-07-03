"""Backfill scheme coordinates from postcodes via postcodes.io (free).

Fills both coordinate column pairs (lat/lng and latitude/longitude) for
schemes that have a postcode but no coordinates. Uses the bulk endpoint
(100 postcodes per request, no API key). Terminated postcodes are
retried against the /terminated_postcodes endpoint singly.

Usage:
    python geo_backfill.py [--bd-only]
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
API = "https://api.postcodes.io"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bd-only", action="store_true")
    args = ap.parse_args()

    bd = ("AND scheme_type IN ('BTR','PBSA','Co-living','Senior')"
          if args.bd_only else "")
    engine = create_engine(DB_URL)
    with engine.connect() as c:
        rows = c.execute(text(f"""
            SELECT id, UPPER(TRIM(postcode)) FROM existing_schemes
            WHERE COALESCE(lat, latitude) IS NULL
              AND COALESCE(postcode,'') <> '' {bd}
        """)).fetchall()
    print(f"{len(rows):,} schemes missing coordinates (with postcode)")

    # postcode -> [scheme ids]
    pc_map: dict[str, list[int]] = {}
    for sid, pc in rows:
        pc_map.setdefault(pc, []).append(sid)
    pcs = list(pc_map)
    print(f"{len(pcs):,} distinct postcodes to look up")

    client = httpx.Client(timeout=30)
    found: dict[str, tuple[float, float]] = {}
    misses: list[str] = []
    for i in range(0, len(pcs), 100):
        batch = pcs[i:i + 100]
        r = client.post(f"{API}/postcodes", json={"postcodes": batch})
        r.raise_for_status()
        for item in r.json()["result"]:
            res = item.get("result")
            if res and res.get("latitude") is not None:
                found[item["query"].upper()] = (res["latitude"], res["longitude"])
            else:
                misses.append(item["query"].upper())
        if (i // 100) % 10 == 9:
            print(f"  {i + len(batch):,}/{len(pcs):,} looked up "
                  f"({len(found):,} found)")
        time.sleep(0.15)

    # terminated postcodes still have coordinates
    term_found = 0
    for pc in misses[:2000]:
        try:
            r = client.get(f"{API}/terminated_postcodes/{pc.replace(' ', '%20')}")
            if r.status_code == 200:
                res = r.json()["result"]
                if res.get("latitude") is not None:
                    found[pc] = (res["latitude"], res["longitude"])
                    term_found += 1
        except httpx.HTTPError:
            pass
        time.sleep(0.12)

    print(f"\nresolved {len(found):,}/{len(pcs):,} postcodes "
          f"({term_found} via terminated register)")

    n = 0
    with engine.begin() as c:
        for pc, (la, lo) in found.items():
            for sid in pc_map[pc]:
                c.execute(text("""
                    UPDATE existing_schemes
                    SET lat = :la, lng = :lo,
                        latitude = COALESCE(latitude, :la),
                        longitude = COALESCE(longitude, :lo),
                        updated_at = NOW()
                    WHERE id = :s AND COALESCE(lat, latitude) IS NULL
                """), {"la": la, "lo": lo, "s": sid})
                n += 1
    print(f"updated {n:,} schemes with coordinates")


if __name__ == "__main__":
    main()
