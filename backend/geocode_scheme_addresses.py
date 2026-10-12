"""Put schemes that have a street address but no map position on the map.

Uses OpenStreetMap's public geocoder (Nominatim) at one request a second,
with every answer kept in a local cache so a re-run asks nothing twice.
Then asks postcodes.io which council each position falls in.

A position is taken only when the geocoder's answer is in the scheme's own
town and is a building or a street, never a whole town. The postcode is
kept only for a building-level answer; a street-level answer gives a map
position and a council but no postcode.

Nothing is overwritten: only schemes with no position are touched, and a
council or postcode is filled only where the scheme has none.

    python geocode_scheme_addresses.py --source national_code            # dry run
    python geocode_scheme_addresses.py --source national_code --apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm.attributes import flag_modified  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
CACHE = os.path.join(OUT_DIR, "geocode_cache.json")
UA = {"User-Agent": "UK-Ops-BD-Platform/1.0 (scheme address lookup)", "Accept-Language": "en-GB"}
POSTCODE_RE = re.compile(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")
BUILDING_CLASSES = {"building", "amenity", "tourism", "office", "shop", "leisure", "historic", "man_made"}
TOWN_KEYS = ("city", "town", "village", "suburb", "municipality", "county", "state_district", "city_district",
             "borough", "neighbourhood", "hamlet")


def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", " ", (s or "").lower())).strip()


def judge(result: dict, town: str, has_street: bool) -> tuple[str | None, str]:
    """('building' | 'street' | None, reason) for one geocoder answer."""
    addr = result.get("address") or {}
    if (addr.get("country_code") or "").lower() != "gb":
        return None, "not in the United Kingdom"
    want = norm(town)
    places = " | ".join(norm(addr.get(k)) for k in TOWN_KEYS if addr.get(k)) + " | " + norm(result.get("display_name"))
    words = [w for w in want.split() if w not in ("upon", "on", "under", "the", "of", "greater")]
    if want and not (want in places or (words and all(w in places for w in words))):
        return None, "answer is in another town"
    cls, typ = result.get("class") or result.get("category") or "", result.get("type") or ""
    # A road is a road whatever its type says ("residential" is a kind of road as well as a kind of
    # building), unless the answer pins a house number on it.
    if cls == "highway":
        if addr.get("house_number"):
            return "building", f"{cls}/{typ} with house number"
        return ("street", f"{cls}/{typ}") if has_street else (None, f"a road, for a query with no street ({cls}/{typ})")
    if cls in BUILDING_CLASSES or addr.get("house_number") or \
            (cls == "place" and typ in ("house", "building")) or typ in ("apartments", "dormitory", "university", "college"):
        return "building", f"{cls}/{typ}"
    return None, f"too coarse ({cls}/{typ})"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="national_code")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT es.id, es.name, es.address, es.council_id FROM existing_schemes es
        WHERE es.merged_into_id IS NULL AND es.record_class = 'scheme' AND es.source = :src
          AND es.lat IS NULL AND nullif(trim(es.address), '') IS NOT NULL ORDER BY es.id"""),
        {"src": args.source}).mappings().all()
    rows = [dict(r) for r in rows][:args.limit]
    print(f"schemes from {args.source} with an address and no map position: {len(rows)}")
    try:
        with open(CACHE, encoding="utf-8") as fh:
            cache: dict[str, list] = json.load(fh)
    except (OSError, ValueError):
        cache = {}
    stats, found = Counter(), []
    asked = 0
    with httpx.Client(headers=UA, timeout=30.0) as client:
        for n, s in enumerate(rows, 1):
            parts = [p.strip() for p in s["address"].split(",") if p.strip()]
            town = parts[-1] if parts else ""
            has_street = len(parts) >= 2
            query = f"{s['address']}, United Kingdom" if has_street else f"{s['name']}, {town}, United Kingdom"
            if query not in cache:
                try:
                    r = client.get("https://nominatim.openstreetmap.org/search",
                                   params={"q": query, "format": "jsonv2", "addressdetails": 1, "countrycodes": "gb",
                                           "limit": 3})
                    if r.status_code == 429:
                        time.sleep(60)
                        r = client.get("https://nominatim.openstreetmap.org/search",
                                       params={"q": query, "format": "jsonv2", "addressdetails": 1,
                                               "countrycodes": "gb", "limit": 3})
                    r.raise_for_status()
                    cache[query] = r.json()
                except Exception as exc:                  # noqa: BLE001
                    stats[f"geocoder request failed ({type(exc).__name__})"] += 1
                    time.sleep(1.1)
                    continue
                asked += 1
                time.sleep(1.1)
                if asked % 50 == 0:
                    with open(CACHE, "w", encoding="utf-8") as fh:
                        json.dump(cache, fh)
                    print(f"  {n}/{len(rows)} looked up", flush=True)
            best = None
            for res in cache[query]:
                precision, why = judge(res, town, has_street)
                if precision:
                    best = (precision, res, why)
                    break
            if not best:
                stats["no usable answer" if cache[query] else "address not found"] += 1
                continue
            precision, res, why = best
            pc = ((res.get("address") or {}).get("postcode") or "").upper().strip()
            stats[f"placed at {precision} level"] += 1
            found.append({"scheme": s["id"], "name": s["name"], "address": s["address"], "lat": float(res["lat"]),
                          "lng": float(res["lon"]), "precision": precision, "kind": why, "needs_council": not s["council_id"],
                          "postcode": pc if precision == "building" and POSTCODE_RE.match(pc) else None,
                          "matched": (res.get("display_name") or "")[:160]})
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CACHE, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)

    # Which council is each position in? (postcodes.io, nearest postcode within 500 m)
    districts: dict[int, str] = {}
    todo = [f for f in found if f["needs_council"]]
    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(todo), 100):
            batch = todo[i:i + 100]
            try:
                r = client.post("https://api.postcodes.io/postcodes", json={"geolocations": [
                    {"longitude": f["lng"], "latitude": f["lat"], "limit": 1, "radius": 500} for f in batch]})
                r.raise_for_status()
                for f, item in zip(batch, r.json().get("result", [])):
                    hits = item.get("result") or []
                    if hits and hits[0].get("admin_district"):
                        districts[f["scheme"]] = hits[0]["admin_district"]
            except Exception as exc:                      # noqa: BLE001
                stats[f"council lookup failed ({type(exc).__name__})"] += 1
            time.sleep(0.3)
    councils = {norm(n): cid for cid, n in db.execute(text("SELECT id, name FROM councils"))}
    alias = {"city of edinburgh": "city of edinburgh", "glasgow city": "glasgow city", "kingston upon hull city of": "hull",
             "bristol city of": "bristol", "newcastle upon tyne": "newcastle", "herefordshire county of": "herefordshire",
             "city of westminster": "westminster", "durham": "durham", "county durham": "durham"}
    for f in found:
        d = norm(districts.get(f["scheme"]))
        f["council_id"] = councils.get(d) or councils.get(alias.get(d, "")) if d else None
        f["district"] = districts.get(f["scheme"])
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    print(f"  with a postcode: {sum(1 for f in found if f['postcode'])} | council found for "
          f"{sum(1 for f in found if f['needs_council'] and f['council_id'])} of {len(todo)} that had none")
    unmapped = Counter(f["district"] for f in found if f["needs_council"] and f["district"] and not f["council_id"])
    if unmapped:
        print("  districts with no council row of that name:", unmapped.most_common(12))
    for f in found[:8]:
        print(f"    {f['name'][:28]:28s} {f['address'][:34]:34s} -> {f['lat']:.5f},{f['lng']:.5f} {f['precision']:8s} "
              f"{f['postcode'] or '':8s} {f['matched'][:60]}")
    if args.apply:
        for f in found:
            scheme = db.get(ExistingScheme, f["scheme"])
            if scheme.lat is not None:
                continue
            scheme.lat, scheme.lng = f["lat"], f["lng"]
            scheme.latitude, scheme.longitude = f["lat"], f["lng"]
            if f["postcode"] and not (scheme.postcode or "").strip():
                scheme.postcode = f["postcode"]
            if f["council_id"] and not scheme.council_id:
                scheme.council_id = f["council_id"]
            prov = dict(scheme.field_provenance or {})
            prov["position"] = {"source": "osm_geocode", "precision": f["precision"], "matched": f["matched"],
                                "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
            scheme.field_provenance = prov
            flag_modified(scheme, "field_provenance")
        db.commit()
        print(f"written: {len(found)} map positions")
    else:
        db.rollback()
        print("dry run: nothing written")
    db.close()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"geocode_{args.source}_{stamp}{'' if args.apply else '_dryrun'}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"applied": args.apply, "counts": dict(stats), "found": found}, fh, indent=1)
    print(f"geocoder requests made: {asked}\nrun log: {path}")


if __name__ == "__main__":
    main()
