"""Rents, postcodes and map positions for student schemes from Accommodation for Students.

accommodationforstudents.com lists purpose-built student buildings city by
city. Each city page carries every building's name, postcode, coordinates,
operator and "from" weekly rent; each building's own page carries its room
types with a price for each tenancy length. This uses the scraper the
platform already has for it (robots.txt allows these pages; one request
every 1.5 seconds).

  --fetch-cities   Read every city's listing into data/afs/national/.
  --match          Match listed buildings to our student schemes and report.
                   With --apply: link the listing, store the "from" rent for
                   schemes that have none, and fill a missing postcode,
                   council and map position.
  --rooms          Read each linked building's page for prices by room type.

A listing is matched to a scheme by postcode plus a shared name word, or by
the same name in the same city, or by two shared words in the same city
backed by the operator or the street. Anything weaker goes to a review file.

Nothing is overwritten, with one stated exception: a map position this
project earlier placed only "to the street" is replaced by the listing's
exact position, and the old one is kept in the run log.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from urllib import robotparser

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm.attributes import flag_modified  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.rents import norm_academic_year, record_advertised_rent, record_room_rent  # noqa: E402
from app.registry.core import link_source_record  # noqa: E402
from app.scrapers.afs_scraper import BASE, CATEGORY_PATHS, AfsScraper, _NEXT_DATA_PAT  # noqa: E402
from app.scrapers.scheme_matching import norm_name, norm_pc  # noqa: E402
from scrape_national_code import (brand_tokens, city_key, distinct_words, joined_name, loose_words,  # noqa: E402
                                  scheme_city_keys, street_parts)

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data", "afs", "national")
OUT_DIR = os.path.join(HERE, "data", "enrichment")
SOURCE = "afs_directory"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"
NO_RENT = """NOT EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)
         AND NOT EXISTS (SELECT 1 FROM scheme_advertised_rents a WHERE a.scheme_id = es.id AND a.is_current)"""
POSTCODE_OK = re.compile(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")


def tidy_postcode(raw: str | None) -> str | None:
    s = re.sub(r"\s+", "", (raw or "").upper())
    if len(s) < 5:
        return None
    s = f"{s[:-3]} {s[-3:]}"
    return s if POSTCODE_OK.match(s) else None


# --------------------------------------------------------------------- fetch
def fetch_cities(limit: int | None) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(HERE, "data", "afs", "_cities.json"), encoding="utf-8") as fh:
        cities = json.load(fh)[:limit]
    rp = robotparser.RobotFileParser()
    try:
        rp.parse(httpx.get(BASE + "/robots.txt", timeout=20).text.splitlines())
    except httpx.HTTPError:
        rp.parse([])
    total = 0
    with AfsScraper() as afs:
        for n, city in enumerate(cities, 1):
            slug = city["slug"]
            out = os.path.join(DATA_DIR, f"{slug}.jsonl")
            if os.path.exists(out):
                with open(out, encoding="utf-8") as fh:
                    total += sum(1 for _ in fh)
                continue
            if not all(rp.can_fetch("*", f"{BASE}/{slug}/{c}") for c in CATEGORY_PATHS):
                print(f"  {slug}: robots.txt disallows, skipped", flush=True)
                continue
            rows = [p.to_json() for p in afs.scrape_city(slug)]
            with open(out, "w", encoding="utf-8") as fh:
                for r in rows:
                    r["city_name"] = city["name"]
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            total += len(rows)
            if n % 10 == 0:
                print(f"  {n}/{len(cities)} cities, {total} listings", flush=True)
    print(f"cities read: {len(cities)}; listings: {total}; saved under {DATA_DIR}")


def load_listings() -> list[dict]:
    rows = []
    for name in sorted(os.listdir(DATA_DIR)):
        if name.endswith(".jsonl"):
            with open(os.path.join(DATA_DIR, name), encoding="utf-8") as fh:
                rows.extend(json.loads(line) for line in fh if line.strip())
    # The site also lists ordinary houses and flats. Those are kept, marked, and matched only on
    # postcode and name together: some small student blocks are advertised that way.
    for r in rows:
        r["purpose_built"] = (r.get("advert_type") or "").lower() == "pbsa" or (r.get("property_type") or "") == "halls"
    return rows


# --------------------------------------------------------------------- match
def plan_matches(db) -> tuple[list[dict], list[dict], list[dict]]:
    listings = load_listings()
    schemes = db.execute(text(f"""
        SELECT es.id, es.name, es.address, es.postcode, es.lat, es.lng, es.council_id, es.source, cc.name AS council,
               co.name AS operator, ({NO_RENT}) AS no_rent, es.field_provenance -> 'position' ->> 'precision' AS precision,
               EXISTS (SELECT 1 FROM scheme_source_records sr WHERE sr.scheme_id = es.id AND sr.source = '{SOURCE}'
                       AND sr.status = 'matched') AS linked
        FROM existing_schemes es LEFT JOIN councils cc ON cc.id = es.council_id
        LEFT JOIN companies co ON co.id = es.operator_company_id
        WHERE {NAMED} AND es.scheme_type IN ('PBSA', 'Co-living')""")).mappings().all()
    known = frozenset(city_key(r.get("city_name") or r["city_slug"].replace("-", " ")) for r in listings)
    place_words = {w for k in known for w in k.split()}
    prepared, by_pc, by_city = [], defaultdict(list), defaultdict(list)
    for s in schemes:
        op = brand_tokens(s["operator"])
        words, numbers = street_parts(s["address"])
        p = {**dict(s), "op": op, "tokens": (norm_name(s["name"]) - op - place_words) or norm_name(s["name"]),
             "raw": norm_name(s["name"]) - place_words,
             "joined": joined_name(s["name"], op | place_words), "loose": loose_words(s["name"]) - place_words - op,
             "distinct": distinct_words(s["name"], place_words | op), "street": words - place_words,
             "cities": scheme_city_keys(s["council"], s["address"], s["name"], known)}
        prepared.append(p)
        if s["postcode"]:
            by_pc[norm_pc(s["postcode"])].append(p)
        for c in p["cities"]:
            by_city[c].append(p)

    pairs = []
    for r in listings:
        op = brand_tokens(r.get("operator"))
        city = city_key(r.get("city_name") or r["city_slug"].replace("-", " "))
        tokens = (norm_name(r["name"]) - op - place_words) or norm_name(r["name"])
        raw = norm_name(r["name"]) - place_words
        joined = joined_name(r["name"], op | place_words)
        loose = loose_words(r["name"]) - place_words - op
        distinct = distinct_words(r["name"], place_words | op)
        street, _ = street_parts(r.get("address"))
        street -= place_words
        pc = norm_pc(r.get("postcode") or "")
        cands = {p["id"]: p for p in by_pc.get(pc, [])} if pc else {}
        for p in by_city.get(city, []):
            cands.setdefault(p["id"], p)
        for p in cands.values():
            same_pc = bool(pc) and bool(p["postcode"]) and norm_pc(p["postcode"]) == pc
            same_city = city in p["cities"] or any(city in c or c in city for c in p["cities"])
            if p["postcode"] and pc and not same_pc and norm_pc(p["postcode"])[:-3] != pc[:-3] and not same_city:
                continue
            # Two feeds can name different operators for one building ("Canvas City Point" under Greystar,
            # "Canvas Coventry City Point" under Canvas Student): compare with both brands taken out.
            both = op | p["op"]
            bare_a, bare_b = raw - both, p["raw"] - both
            name_equal = bool(tokens) and (tokens == p["tokens"] or (len(joined) >= 6 and joined == p["joined"])
                                           or (len(bare_a) >= 2 and bare_a == bare_b))
            shared_loose = loose & p["loose"]
            shared_distinct = distinct & p["distinct"]
            op_same = bool(op & p["op"])
            street_same = bool(street & p["street"])
            grade, why = None, ""
            if same_pc and (name_equal or shared_loose or shared_distinct):
                grade, why = "A", "same postcode and name"
            elif same_pc and op_same and len(by_pc.get(pc, [])) == 1:
                grade, why = "B", "same postcode and operator, the only scheme there"
            elif same_city and name_equal and not (p["postcode"] and pc and norm_pc(p["postcode"])[:-3] != pc[:-3]
                                                    and not op_same):
                grade, why = "A", "same name in the same city"
            elif same_city and (len(shared_loose) >= 2 or any(len(t) >= 6 for t in shared_distinct)) and \
                    (op_same or street_same):
                grade, why = "B", "shared name words in the same city, with the " + ("operator" if op_same else "street")
            elif same_city and (name_equal or len(shared_loose) >= 2 or shared_distinct):
                grade, why = "C", "name resemblance only"
            if grade and not r["purpose_built"] and not (same_pc and name_equal and len(tokens) >= 2):
                grade = None          # an ordinary house or flat advert: only an exact match on both counts
            if grade:
                score = (same_pc * 4) + (name_equal * 3) + len(shared_loose) + op_same + street_same
                pairs.append({"listing": r, "scheme": p, "grade": grade, "why": why, "score": score})

    best_for_listing: dict[str, list[dict]] = defaultdict(list)
    best_for_scheme: dict[int, list[dict]] = defaultdict(list)
    for x in sorted(pairs, key=lambda x: (x["grade"], -x["score"])):
        best_for_listing[x["listing"]["afs_id"]].append(x)
        best_for_scheme[x["scheme"]["id"]].append(x)
    accepted, review = [], []
    for sid, xs in best_for_scheme.items():
        top = xs[0]
        if best_for_listing[top["listing"]["afs_id"]][0] is not top:
            continue
        tie = len(xs) > 1 and (xs[1]["grade"], xs[1]["score"]) == (top["grade"], top["score"])
        (accepted if top["grade"] in ("A", "B") and not tie else review).append(top)
    matched_ids = {x["listing"]["afs_id"] for x in accepted}
    unmatched = [r for r in listings if r["afs_id"] not in matched_ids and r["purpose_built"]]
    return accepted, review, unmatched


def council_ids(db, postcodes: list[str]) -> dict[str, int]:
    """postcode -> our council id, via postcodes.io."""
    names = {re.sub(r"[^a-z ]", " ", n.lower()).strip(): cid for cid, n in db.execute(text("SELECT id, name FROM councils"))}
    alias = {"kingston upon hull city of": "hull", "bristol city of": "bristol", "newcastle upon tyne": "newcastle",
             "herefordshire county of": "herefordshire", "county durham": "durham"}
    out: dict[str, int] = {}
    uniq = sorted({p for p in postcodes if p})
    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(uniq), 100):
            try:
                r = client.post("https://api.postcodes.io/postcodes", json={"postcodes": uniq[i:i + 100]})
                r.raise_for_status()
            except httpx.HTTPError:
                continue
            for item in r.json().get("result", []):
                res = item.get("result") or {}
                d = re.sub(r"[^a-z ]", " ", (res.get("admin_district") or "").lower()).strip()
                cid = names.get(d) or names.get(alias.get(d, ""))
                if cid:
                    out[item["query"]] = cid
            time.sleep(0.3)
    return out


def match(db, apply: bool) -> None:
    accepted, review, unmatched = plan_matches(db)
    new = [x for x in accepted if x["scheme"]["source"] == "national_code"]
    fill = [x for x in accepted if x["scheme"]["no_rent"] and x["listing"].get("rent_ppw")]
    print(f"listed purpose-built buildings: {len(accepted) + len(unmatched)} | matched to our schemes: {len(accepted)} "
          f"({Counter(x['grade'] for x in accepted).most_common()})")
    print(f"  of the schemes added from the register: {len(new)} matched")
    print(f"  schemes with no rent that get a 'from' rent: {len(fill)} "
          f"({sum(1 for x in fill if x['scheme']['source'] == 'national_code')} of them register schemes)")
    print(f"  postcode filled: {sum(1 for x in accepted if not x['scheme']['postcode'] and tidy_postcode(x['listing'].get('postcode')))}"
          f" | map position filled: {sum(1 for x in accepted if x['scheme']['lat'] is None and x['listing'].get('latitude'))}"
          f" | street-level position made exact: {sum(1 for x in accepted if x['scheme']['precision'] == 'street' and x['listing'].get('latitude'))}")
    print(f"  left for review: {len(review)} | listings with no scheme of ours: {len(unmatched)}")
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"afs_match_{stamp}{'' if apply else '_dryrun'}.csv")
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["outcome", "grade", "why", "scheme_id", "scheme", "our_operator", "our_postcode", "listing_name",
                    "listing_operator", "listing_city", "listing_postcode", "rent_ppw_from", "page"])
        for label, xs in (("matched", accepted), ("review", review)):
            for x in xs:
                s, r = x["scheme"], x["listing"]
                w.writerow([label, x["grade"], x["why"], s["id"], s["name"], s["operator"], s["postcode"], r["name"],
                            r.get("operator"), r["city_slug"], r.get("postcode"), r.get("rent_ppw"), r["url"]])
        for r in unmatched:
            w.writerow(["not in our data", "", "", "", "", "", "", r["name"], r.get("operator"), r["city_slug"],
                        r.get("postcode"), r.get("rent_ppw"), r["url"]])
    print(f"  detail: {path}")
    for x in new[:14]:
        s, r = x["scheme"], x["listing"]
        print(f"    {s['name'][:28]:28s} <- {r['name'][:28]:28s} {r['city_slug'][:12]:12s} {str(r.get('postcode')):9s} "
              f"£{r.get('rent_ppw')} pw [{x['grade']}: {x['why'][:40]}]")
    if not apply:
        print("dry run: nothing written")
        return
    need_council = [tidy_postcode(x["listing"].get("postcode")) for x in accepted if not x["scheme"]["council_id"]]
    councils = council_ids(db, [p for p in need_council if p])
    log = []
    for x in accepted:
        s, r = x["scheme"], x["listing"]
        scheme = db.get(ExistingScheme, s["id"])
        link_source_record(db, SOURCE, r["url"], scheme_id=s["id"], raw_name=r["name"], raw_postcode=r.get("postcode"),
                           raw_operator=r.get("operator"), raw_address=r.get("address"),
                           payload={"matched_on": x["why"], "rent_ppw": r.get("rent_ppw")},
                           match_method="postcode+name" if "postcode" in x["why"] else "name+city",
                           match_score=0.95 if x["grade"] == "A" else 0.85, status="matched")
        entry = {"scheme": s["id"], "name": s["name"], "listing": r["url"]}
        pc = tidy_postcode(r.get("postcode"))
        if pc and not (scheme.postcode or "").strip():
            scheme.postcode = pc
            entry["postcode"] = pc
        if pc and not scheme.council_id and councils.get(pc):
            scheme.council_id = councils[pc]
            entry["council_id"] = councils[pc]
        lat, lng = r.get("latitude"), r.get("longitude")
        if lat and lng and (scheme.lat is None or s["precision"] == "street"):
            entry["position"] = {"old": [scheme.lat, scheme.lng], "new": [lat, lng]}
            scheme.lat, scheme.lng, scheme.latitude, scheme.longitude = lat, lng, lat, lng
            prov = dict(scheme.field_provenance or {})
            prov["position"] = {"source": SOURCE, "precision": "building", "ref": r["url"],
                                "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
            scheme.field_provenance = prov
            flag_modified(scheme, "field_provenance")
        if s["no_rent"] and r.get("rent_ppw"):
            year = norm_academic_year((r.get("raw") or {}).get("academicYearLabel"))
            record_advertised_rent(db, scheme_id=s["id"], source=SOURCE, kind="from", rent_ppw=float(r["rent_ppw"]),
                                   academic_year=year, source_reference=r["url"])
            entry["rent_ppw_from"] = r["rent_ppw"]
        log.append(entry)
    db.commit()
    with open(os.path.join(OUT_DIR, f"afs_match_{stamp}.json"), "w", encoding="utf-8") as fh:
        json.dump(log, fh, indent=1)
    print(f"written: {len(accepted)} listings linked, {sum(1 for e in log if 'rent_ppw_from' in e)} rents, "
          f"{sum(1 for e in log if 'postcode' in e)} postcodes, {sum(1 for e in log if 'position' in e)} positions, "
          f"{sum(1 for e in log if 'council_id' in e)} councils")


# --------------------------------------------------------------------- rooms
def room_kind(occupancy: str | None, name: str | None) -> str:
    """The room type in the spelling the rest of the database uses. The listing site says only
    "Entire Place" or "Private Room"; the room's own name usually says which kind it is."""
    n = (name or "").lower()
    if re.search(r"non[\s-]?en[\s-]?suite", n):
        return "Non en-suite"
    if re.search(r"en[\s-]?suite", n):
        return "Ensuite"
    if "studio" in n:
        return "Studio"
    m = re.search(r"\b(one|two|three|four|five|six|[1-6])[\s-]*bed", n)
    if m and re.search(r"apartment|flat|house|duplex", n):
        num = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6}.get(m.group(1)) or int(m.group(1))
        return f"{num} bed"
    occ = (occupancy or "").lower()
    if "entire" in occ:
        return "Studio"
    if "shared" in occ or "twin" in n:
        return "Shared room"
    return "Room"


def room_tiers(html: str) -> tuple[str | None, list[dict]]:
    """(academic year, room tiers) from a building's page. One tier per named room:
    its cheapest price and the tenancy length that price is for."""
    m = _NEXT_DATA_PAT.search(html or "")
    if not m:
        return None, []
    try:
        details = json.loads(m.group(1))["props"]["pageProps"]["viewModel"]["propertyDetails"]
    except (ValueError, KeyError, TypeError):
        return None, []
    # Pages for single flats and houses carry "rooms" as a plain list with no price tiers.
    rooms_block = details.get("rooms") if isinstance(details, dict) else None
    years = rooms_block.get("years") if isinstance(rooms_block, dict) else None
    year = years.get("currentMarketingYear") if isinstance(years, dict) else None
    if not isinstance(year, dict):
        return None, []
    tiers = []
    for group in year.get("roomTypes") or []:
        if not isinstance(group, dict):
            continue
        for room in group.get("available") or []:
            contracts = [c for f in room.get("floors") or [] for c in f.get("contracts") or []
                         if c.get("amount") and 40 <= float(c["amount"]) <= 1500]
            if not contracts:
                continue
            # A two-week summer let is not the room's rent: price the tier on a full-year tenancy
            # (38 weeks or more) whenever one is offered.
            full_year = [c for c in contracts if (c.get("weeks") or 0) >= 38]
            cheapest = min(full_year or contracts, key=lambda c: (float(c["amount"]), -(c.get("weeks") or 0)))
            tiers.append({"room_type": room_kind(room.get("roomType") or group.get("roomType"), room.get("roomName")),
                          "name": (room.get("roomName") or "").strip()[:150] or None,
                          "rent_ppw": float(cheapest["amount"]), "weeks": cheapest.get("weeks"),
                          "sold_out": bool(room.get("isSoldOut"))})
    return norm_academic_year(year.get("label")), tiers


def rooms(db, apply: bool, limit: int | None, refresh: bool = False) -> None:
    # Normally: linked schemes with no room prices at all. With refresh: the schemes whose room prices
    # came from this source, read again (a changed price supersedes the old row, which is kept).
    scope = (f"EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current AND r.source = '{SOURCE}')"
             if refresh else "NOT EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)")
    rows = db.execute(text(f"""
        SELECT es.id, es.name, sr.source_reference AS url FROM existing_schemes es
        JOIN scheme_source_records sr ON sr.scheme_id = es.id AND sr.source = '{SOURCE}' AND sr.status = 'matched'
        WHERE {NAMED} AND sr.source_reference LIKE '{BASE}/%' AND {scope}
        ORDER BY es.id""")).mappings().all()
    seen, todo = set(), []
    for r in rows:
        if r["id"] not in seen:
            seen.add(r["id"])
            todo.append(dict(r))
    todo = todo[:limit]
    print(f"linked schemes with no prices by room type: {len(todo)}")
    cache_dir = os.path.join(DATA_DIR, "pages")
    os.makedirs(cache_dir, exist_ok=True)
    stats, found = Counter(), []
    with AfsScraper() as afs:
        for n, s in enumerate(todo, 1):
            fname = os.path.join(cache_dir, re.sub(r"[^a-z0-9\-]", "_", s["url"].rsplit("/", 1)[-1]) + ".html")
            if os.path.exists(fname):
                with open(fname, encoding="utf-8", errors="replace") as fh:
                    html = fh.read()
            else:
                html = afs.get(s["url"])
                if html:
                    with open(fname, "w", encoding="utf-8", errors="replace") as fh:
                        fh.write(html)
            if not html:
                stats["page not readable"] += 1
                continue
            year, tiers = room_tiers(html)
            if not tiers:
                stats["no room prices on the page"] += 1
                continue
            stats["prices by room type read"] += 1
            found.append({"scheme": s["id"], "name": s["name"], "url": s["url"], "year": year, "tiers": tiers})
            if apply:
                keyed: dict[tuple, dict] = {}
                for t in tiers:                       # one current row per (type, name): keep the cheapest
                    k = (t["room_type"], t["name"])
                    if k not in keyed or t["rent_ppw"] < keyed[k]["rent_ppw"]:
                        keyed[k] = t
                for t in keyed.values():
                    record_room_rent(db, scheme_id=s["id"], source=SOURCE, room_type=t["room_type"],
                                     sub_classification=t["name"], academic_year=year, rent_ppw=t["rent_ppw"],
                                     contract_length_weeks=t["weeks"], source_reference=s["url"])
                db.commit()
            if n % 50 == 0:
                print(f"  {n}/{len(todo)} pages", flush=True)
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    for f in found[:8]:
        print(f"    {f['name'][:30]:30s} {f['year']} " + "; ".join(
            f"{t['room_type']}{' ' + t['name'] if t['name'] else ''} £{t['rent_ppw']:.0f}/{t['weeks']}w" for t in f["tiers"][:4]))
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(OUT_DIR, f"afs_rooms_{stamp}{'' if apply else '_dryrun'}.json"), "w", encoding="utf-8") as fh:
        json.dump({"applied": apply, "counts": dict(stats), "found": found}, fh, indent=1)
    print("written" if apply else "dry run: nothing written")


OPENED_RE = re.compile(
    r"\b(open(?:ed|ing|s)|complet(?:ed|ion)|built|launch(?:ed|ing))\s+(?:in\s+|for\s+)?"
    r"(?:(?:january|february|march|april|may|june|july|august|september|october|november|december|spring|summer|"
    r"autumn|winter|early|late)\s+)?(19[89]\d|20[0-3]\d)\b", re.I)
NOT_THE_BUILDING = re.compile(r"phase\s*(?:2|two|ii|3|three)|refurbish|renovat|extension|new wing|new (?:luxury )?block|"
                              r"newly developed|university\s+(?:was\s+)?"
                              r"(?:founded|established|opened)", re.I)


def description_year(html: str, this_year: int) -> tuple[int, str] | None:
    """An opening year stated in a building's own description on the listing site."""
    m = _NEXT_DATA_PAT.search(html or "")
    if not m:
        return None
    try:
        desc = json.loads(m.group(1))["props"]["pageProps"]["viewModel"]["propertyDetails"].get("description") or ""
    except (ValueError, KeyError, TypeError, AttributeError):
        return None
    desc = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", desc))[:6000]
    years = {}
    for hit in OPENED_RE.finditer(desc):
        quote = desc[max(0, hit.start() - 90):hit.end() + 40].strip()
        year = int(hit.group(2))
        if 1985 <= year <= this_year and not NOT_THE_BUILDING.search(quote):
            years.setdefault(year, quote)
    if len(years) != 1:                 # no statement, or two different years: do not choose
        return None
    return next(iter(years.items()))


def opened(db, apply: bool) -> None:
    from app.models.observations import record_observation
    from app.scrapers.field_protection import set_field
    rows = db.execute(text(f"""
        SELECT DISTINCT es.id, es.name, sr.source_reference AS url FROM existing_schemes es
        JOIN scheme_source_records sr ON sr.scheme_id = es.id AND sr.source = '{SOURCE}' AND sr.status = 'matched'
        WHERE {NAMED} AND sr.source_reference LIKE '{BASE}/%' AND es.build_year IS NULL AND es.completion_date IS NULL
        ORDER BY es.id""")).mappings().all()
    cache_dir = os.path.join(DATA_DIR, "pages")
    this_year = dt.date.today().year
    found = []
    for s in rows:
        fname = os.path.join(cache_dir, re.sub(r"[^a-z0-9\-]", "_", s["url"].rsplit("/", 1)[-1]) + ".html")
        if not os.path.exists(fname):
            continue
        with open(fname, encoding="utf-8", errors="replace") as fh:
            got = description_year(fh.read(), this_year)
        if got:
            found.append({"scheme": s["id"], "name": s["name"], "url": s["url"], "year": got[0], "quote": got[1]})
    print(f"linked schemes with no opening year: {len(rows)}; descriptions that state one: {len(found)}")
    for f in found:
        print(f"    {f['name'][:30]:30s} {f['year']}  {f['quote'][:110]}")
    if apply:
        for f in found:
            scheme = db.get(ExistingScheme, f["scheme"])
            record_observation(db, scheme, "build_year", f["year"], SOURCE, reference=f["url"], confidence=0.85,
                               quote=f["quote"], extractor="regex")
            if scheme.build_year is None:
                set_field(scheme, "build_year", f["year"], SOURCE, db, changed_by="afs_fill_rents.py")
        db.commit()
        print(f"written: {len(found)} opening years")


def main() -> None:
    ap = argparse.ArgumentParser()
    for flag in ("--fetch-cities", "--match", "--rooms", "--refresh", "--opened", "--apply"):
        ap.add_argument(flag, action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if not (args.fetch_cities or args.match or args.rooms or args.opened):
        ap.error("use --fetch-cities, --match, --rooms and/or --opened")
    if args.fetch_cities:
        fetch_cities(args.limit)
    if args.match or args.rooms or args.opened:
        print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
        db = SessionLocal()
        try:
            if args.match:
                match(db, args.apply)
            if args.rooms:
                rooms(db, args.apply, args.limit, args.refresh)
            if args.opened:
                opened(db, args.apply)
        finally:
            db.close()


if __name__ == "__main__":
    main()
