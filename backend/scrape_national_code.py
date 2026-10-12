"""Bed counts from the National Code register of student accommodation.

The ANUK/Unipol National Code publishes, for every member, each building it
has signed up and the number of bed spaces in it (nationalcode.org). This
reads that register city by city and matches the buildings to our student
schemes.

  --fetch    Read the register (robots.txt respected, one request a second)
             into data/national_code/developments.jsonl.
  --match    Match buildings to schemes and report. With --apply, fill the
             bed count where a scheme has none, and store every reading as
             an observation with the register page it came from.

A building is matched only when its name agrees with a scheme's and at
least one other thing does too: the city, the street or the operator.
Weaker pairs are written to a review file, never applied.

Nothing is overwritten. Where a scheme already has a bed count the register
figure is stored beside it as a second reading, and the two are compared.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html as html_mod
import json
import os
import re
import sys
from collections import Counter, defaultdict
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.registry.core import link_source_record  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402
from app.scrapers.scheme_matching import norm_name, norm_pc  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data", "national_code")
OUT_DIR = os.path.join(HERE, "data", "enrichment")
BASE = "https://www.nationalcode.org"
SOURCE = "national_code"
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"

CARD_RE = re.compile(r'<article class="development-card[^"]*"\s+data-id="(\d+)">(.*?)</article>', re.S)
TITLE_RE = re.compile(r'development-card__title">(.*?)</h3>', re.S)
MEMBER_RE = re.compile(r'development-card__name">(.*?)</div>', re.S)
PIN_RE = re.compile(r'icon-pin"></i>(.*?)</li>', re.S)
BED_RE = re.compile(r'icon-bed"></i>\s*([\d,]+)\s*</li>', re.S)
SITE_RE = re.compile(r'<a href="([^"]+)" class="link" target="_blank">View Website', re.S)
CITY_LINK_RE = re.compile(r'href="(/cities/[a-z0-9\-]+/)"')
PAGE_RE = re.compile(r'href="\?p=(\d+)')
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)

LONDON = {"barking and dagenham", "barnet", "bexley", "brent", "bromley", "camden", "city of london", "croydon",
          "ealing", "enfield", "greenwich", "hackney", "hammersmith and fulham", "haringey", "harrow", "havering",
          "hillingdon", "hounslow", "islington", "kensington and chelsea", "kingston upon thames", "lambeth",
          "lewisham", "merton", "newham", "redbridge", "richmond upon thames", "southwark", "sutton",
          "tower hamlets", "waltham forest", "wandsworth", "westminster", "city of westminster"}
STREET_NOISE = {"road", "street", "st", "rd", "lane", "avenue", "ave", "way", "place", "square", "the", "of", "and",
                "close", "drive", "court", "house", "terrace", "row", "hill", "park", "gate", "walk", "quay"}
BRAND_NOISE = {"student", "students", "living", "accommodation", "homes", "housing", "limited", "ltd", "group", "uk",
               "plc", "llp", "management", "property", "properties", "services", "university", "the", "of", "and",
               "college", "residences", "residential", "halls"}


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def brand_tokens(name: str | None) -> set[str]:
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split() if t not in BRAND_NOISE and len(t) > 1}


def joined_name(name: str | None, drop: set[str] = frozenset()) -> str:
    """A name with its spaces closed up and filler words dropped, so that
    "The Leather Works" and "Leatherworks" compare equal."""
    toks = [t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
            if t not in drop and t not in {"the", "student", "students", "accommodation", "living", "at", "by", "and"}]
    return "".join(toks)


def city_key(name: str | None) -> str:
    s = re.sub(r"[^a-z ]", " ", (name or "").lower())
    s = re.sub(r"^.*\bdevelopments in\b", " ", s)      # the register's heading: "Search Our Developments in X"
    if re.search(r"\blondon\b", s):
        return "london"
    s = re.sub(r"\b(city|council|borough|metropolitan|district|county|of|upon|the|royal)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# --------------------------------------------------------------------- fetch
def fetch_register() -> None:
    from app.scrapers.operator_directory_scraper import OperatorDirectoryScraper
    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, "developments.jsonl")
    rows: dict[str, dict] = {}
    requests = 0
    with OperatorDirectoryScraper(timeout=30.0) as web:
        def get(url: str) -> str | None:
            nonlocal requests
            status, body = web._fetch(url)
            requests += 1
            return body if status == 200 else None

        cities: list[str] = []
        page = 1
        while page <= 40:
            body = get(f"{BASE}/cities/?p={page}") if page > 1 else get(f"{BASE}/cities/")
            found = [c for c in CITY_LINK_RE.findall(body or "") if c not in cities]
            if not found:
                break
            cities.extend(found)
            page += 1
        print(f"cities listed: {len(cities)}", flush=True)
        for n, path in enumerate(cities, 1):
            first = get(BASE + path)
            if not first:
                continue
            city = clean(H1_RE.search(first).group(1)) if H1_RE.search(first) else path.strip("/").split("/")[-1]
            last = max([int(p) for p in PAGE_RE.findall(first)] or [1])
            for p in range(1, min(last, 60) + 1):
                url = f"{BASE}{path}?p={p}&s=" if p > 1 else BASE + path
                body = first if p == 1 else get(url)
                for dev_id, block in CARD_RE.findall(body or ""):
                    title = clean(TITLE_RE.search(block).group(1)) if TITLE_RE.search(block) else ""
                    beds = BED_RE.search(block)
                    site = SITE_RE.search(block)
                    rows[dev_id] = {
                        "id": int(dev_id), "name": title,
                        "member": clean(MEMBER_RE.search(block).group(1)) if MEMBER_RE.search(block) else "",
                        "address": clean(PIN_RE.search(block).group(1)) if PIN_RE.search(block) else "",
                        "beds": int(beds.group(1).replace(",", "")) if beds else None,
                        "website": html_mod.unescape(site.group(1)) if site else None,
                        "city": city, "page": url}
            if n % 20 == 0:
                print(f"  {n}/{len(cities)} cities, {len(rows)} buildings, {requests} requests", flush=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        for r in rows.values():
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    with_beds = sum(1 for r in rows.values() if r["beds"])
    print(f"buildings: {len(rows)} ({with_beds} with a bed count, {sum(r['beds'] or 0 for r in rows.values()):,} beds) "
          f"in {len(cities)} cities; {requests} requests\nsaved {out_path}")


# --------------------------------------------------------------------- match
def load_register() -> list[dict]:
    with open(os.path.join(DATA_DIR, "developments.jsonl"), encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def scheme_city_keys(council: str | None, address: str | None, name: str | None = None,
                     known: frozenset[str] = frozenset()) -> set[str]:
    """The cities a scheme of ours can be said to be in: its council, and any
    register city its address or name spells out (Egham sits in Runnymede, so the
    council alone would never match the register's "Egham")."""
    keys = set()
    c = (council or "").lower().strip()
    if c:
        keys.add(city_key(c))
        if c in LONDON or "london" in c:
            keys.add("london")
    addr = (address or "").lower()
    if "london" in addr:
        keys.add("london")
    if known:
        text_ = " " + re.sub(r"[^a-z ]", " ", f"{address or ''} {name or ''}".lower()) + " "
        text_ = re.sub(r"\s+", " ", text_)
        keys.update(k for k in known if len(k) >= 4 and f" {k} " in text_)
    return {k for k in keys if k}


def match(db, apply: bool) -> None:
    devs = [d for d in load_register() if d.get("beds")]
    schemes = db.execute(text(f"""
        SELECT es.id, es.name, es.postcode, es.address, cc.name AS council, co.name AS operator, {SIZE} AS size,
               es.status
        FROM existing_schemes es LEFT JOIN councils cc ON cc.id = es.council_id
        LEFT JOIN companies co ON co.id = es.operator_company_id
        WHERE {NAMED} AND es.scheme_type = 'PBSA'""")).mappings().all()
    print(f"register buildings with a bed count: {len(devs)} | our student schemes: {len(schemes)} "
          f"({sum(1 for s in schemes if s['size'] is None)} with no bed count)")

    known_cities = frozenset(city_key(d["city"]) for d in devs)
    by_token: dict[str, list[dict]] = defaultdict(list)
    by_joined: dict[str, list[dict]] = defaultdict(list)
    prepared = []
    for s in schemes:
        op = brand_tokens(s["operator"])
        tokens = norm_name(s["name"]) - op or norm_name(s["name"])
        p = {**dict(s), "tokens": tokens, "op": op,
             "cities": scheme_city_keys(s["council"], s["address"], s["name"], known_cities),
             "addr": {t for t in re.sub(r"[^a-z0-9 ]", " ", (s["address"] or "").lower()).split()
                      if t not in STREET_NOISE and len(t) > 2 and not t.isdigit()}}
        p["joined"] = joined_name(s["name"], op)
        prepared.append(p)
        for t in tokens:
            by_token[t].append(p)
        if len(p["joined"]) >= 6:
            by_joined[p["joined"]].append(p)

    pairs = []
    for d in devs:
        member = brand_tokens(d["member"])
        dcity = city_key(d["city"])
        city_words = set(dcity.split())
        # A name made only of the city ("Southampton") is not a name.
        dt_tokens = (norm_name(d["name"]) - member or norm_name(d["name"])) - city_words
        if not dt_tokens:
            # The register calls it by operator and city alone ("Brighton" under iQ). It is our
            # scheme when that operator has exactly one scheme in that city named the same way.
            if member and dcity:
                same = [p for p in prepared if member & p["op"]
                        and (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"]))
                        and not (p["tokens"] - p["op"] - city_words - BRAND_NOISE)]
                if len(same) == 1:
                    pairs.append({"dev": d, "scheme": same[0], "grade": "B", "equal": False, "city": True,
                                  "street": False, "operator": True, "support": 2})
            continue
        dstreet = {t for t in re.sub(r"[^a-z0-9 ]", " ", d["address"].lower()).split()
                   if t not in STREET_NOISE and len(t) > 2 and not t.isdigit()}
        seen = {}
        for t in dt_tokens:
            for p in by_token.get(t, []):
                seen[p["id"]] = p
        # "The Leatherworks" and "The Leather Works" are one name written two ways.
        djoined = joined_name(d["name"], member | city_words)
        same_spelling = {p["id"] for p in by_joined.get(djoined, [])} if len(djoined) >= 6 else set()
        for p in by_joined.get(djoined, []) if same_spelling else []:
            seen[p["id"]] = p
        for p in seen.values():
            ours = p["tokens"] - city_words
            shared = dt_tokens & ours
            if p["id"] in same_spelling:
                shared = shared or {djoined}
            if not shared or not ours:
                continue
            equal = dt_tokens == ours or p["id"] in same_spelling
            partial = (dt_tokens <= ours or ours <= dt_tokens) and \
                (len(shared) >= 2 or max(len(t) for t in shared) >= 6)
            if not (equal or partial):
                continue
            city = bool(dcity) and (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"]))
            street = bool(dstreet & p["addr"])
            operator = bool(member & p["op"])
            support = city + street + operator
            # A different city is a different building, whatever the name says.
            if p["cities"] and dcity and not city and not street:
                continue
            grade = "A" if equal and support >= 1 else "B" if partial and support >= 2 else "C"
            pairs.append({"dev": d, "scheme": p, "grade": grade, "equal": equal, "city": city, "street": street,
                          "operator": operator, "support": support})

    # One building per scheme and one scheme per building: keep the best-supported pair, drop ties.
    def rank(x):
        return (x["grade"], -x["support"], -int(x["equal"]))
    best_for_scheme, best_for_dev = {}, {}
    for x in sorted(pairs, key=rank):
        best_for_scheme.setdefault(x["scheme"]["id"], []).append(x)
        best_for_dev.setdefault(x["dev"]["id"], []).append(x)
    accepted, review = [], []
    for sid, xs in best_for_scheme.items():
        top = xs[0]
        if best_for_dev[top["dev"]["id"]][0] is not top:
            continue                      # the building fits another scheme better
        tied = [x for x in xs[1:] if rank(x) == rank(top) and x["dev"]["beds"] != top["dev"]["beds"]]
        if top["grade"] in ("A", "B") and not tied:
            accepted.append(top)
        else:
            review.append(top)

    # How good is the matcher? Compare with the bed counts we already hold.
    have = [x for x in accepted if x["scheme"]["size"]]
    close = [x for x in have if abs(x["dev"]["beds"] - x["scheme"]["size"]) <= max(5, 0.05 * x["scheme"]["size"])]
    near = [x for x in have if abs(x["dev"]["beds"] - x["scheme"]["size"]) <= 0.15 * x["scheme"]["size"]]
    fill = [x for x in accepted if x["scheme"]["size"] is None]
    for g in ("A", "B"):
        sized = [x for x in have if x["grade"] == g]
        agree = [x for x in sized if x in close]
        print(f"  grade {g}: {len(sized)} already sized, {len(agree)} agree within 5% "
              f"({len(agree) / max(1, len(sized)):.0%}); {sum(1 for x in fill if x['grade'] == g)} would be filled")
    print(f"matched: {len(accepted)} schemes ({Counter(x['grade'] for x in accepted).most_common()})")
    print(f"  already had a bed count: {len(have)}; register agrees within 5%: {len(close)} "
          f"({len(close) / max(1, len(have)):.0%}), within 15%: {len(near)} ({len(near) / max(1, len(have)):.0%})")
    print(f"  had no bed count, now filled: {len(fill)} ({sum(x['dev']['beds'] for x in fill):,} beds)")
    print(f"  left for review (name-only or tied): {len(review)}")
    matched_devs = {x["dev"]["id"] for x in accepted}
    unmatched = [d for d in devs if d["id"] not in matched_devs]
    print(f"  register buildings with no scheme of ours: {len(unmatched)} ({sum(d['beds'] for d in unmatched):,} beds)")

    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(OUT_DIR, f"national_code_match_{stamp}{'' if apply else '_dryrun'}.csv"), "w",
              newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["outcome", "grade", "scheme_id", "scheme", "our_operator", "our_town", "our_beds", "register_name",
                    "register_member", "register_city", "register_address", "register_beds", "city", "street",
                    "operator", "page"])
        for label, xs in (("filled", fill), ("second reading", have), ("review", review)):
            for x in xs:
                s, d = x["scheme"], x["dev"]
                w.writerow([label, x["grade"], s["id"], s["name"], s["operator"], s["council"], s["size"], d["name"],
                            d["member"], d["city"], d["address"], d["beds"], x["city"], x["street"], x["operator"],
                            d["page"]])
        for d in unmatched:
            w.writerow(["not in our data", "", "", "", "", "", "", d["name"], d["member"], d["city"], d["address"],
                        d["beds"], "", "", "", d["page"]])
    print(f"  detail: {fh.name}")

    for x in sorted(have, key=lambda x: -abs(x["dev"]["beds"] - x["scheme"]["size"]))[:8]:
        print(f"    largest gaps: {x['scheme']['name'][:28]:28s} ours {x['scheme']['size']:>5} register {x['dev']['beds']:>5} "
              f"({x['dev']['name'][:28]}, {x['dev']['city']})")
    for x in fill[:10]:
        print(f"    fill: {x['scheme']['name'][:30]:30s} <- {x['dev']['name'][:30]:30s} {x['dev']['beds']:>5} beds "
              f"[{x['grade']}: city {x['city']}, street {x['street']}, operator {x['operator']}]")

    if not apply:
        print("dry run: nothing written")
        return
    for x in accepted:
        s, d = x["scheme"], x["dev"]
        scheme = db.get(ExistingScheme, s["id"])
        quote = f"{d['name']}, {d['address']}, {d['city']}: {d['beds']} bed spaces ({d['member']})"
        link_source_record(db, SOURCE, f"{BASE}/development/{d['id']}", scheme_id=s["id"], raw_name=d["name"],
                           raw_operator=d["member"], raw_address=f"{d['address']}, {d['city']}",
                           payload={"beds": d["beds"], "page": d["page"], "website": d["website"]},
                           match_method=f"name+{'city' if x['city'] else ''}{'+street' if x['street'] else ''}"
                                        f"{'+operator' if x['operator'] else ''}",
                           match_score=0.95 if x["grade"] == "A" else 0.85, status="matched")
        record_observation(db, scheme, "beds_total", d["beds"], SOURCE, reference=d["page"],
                           confidence=0.9 if x["grade"] == "A" else 0.8, quote=quote, extractor="register",
                           refresh_cache=False)
        if s["size"] is None and scheme.beds_total is None:
            set_field(scheme, "beds_total", d["beds"], SOURCE, db, changed_by="scrape_national_code.py")
    db.commit()
    print(f"written: {len(accepted)} readings stored, {len(fill)} bed counts filled")


# --------------------------------------------------------------------- members
MEMBER_RE = re.compile(r'member-card__title">(.*?)</h3>\s*<p>Operating\s+([\d,]+)\s+bedspaces\s+in\s+([\d,]+)\s+developments',
                       re.S)


def fetch_members() -> None:
    """Which members are private providers and which are universities or colleges."""
    from app.scrapers.operator_directory_scraper import OperatorDirectoryScraper
    os.makedirs(DATA_DIR, exist_ok=True)
    members: dict[str, dict] = {}
    with OperatorDirectoryScraper(timeout=30.0) as web:
        for code in ("privateprovider", "educationalprovider"):
            for page in range(1, 40):
                status, body = web._fetch(f"{BASE}/members/?code={code}&p={page}")
                found = MEMBER_RE.findall(body or "") if status == 200 else []
                new = [m for m in found if clean(m[0]) not in members]
                if not new:
                    break
                for name, beds, devs in new:
                    members[clean(name)] = {"name": clean(name), "code": code, "beds": int(beds.replace(",", "")),
                                            "developments": int(devs.replace(",", ""))}
    with open(os.path.join(DATA_DIR, "members.json"), "w", encoding="utf-8") as fh:
        json.dump(list(members.values()), fh, indent=1, ensure_ascii=False)
    print(f"members: {len(members)} ({Counter(m['code'] for m in members.values()).most_common()})")


# --------------------------------------------------------------------- new schemes
GENERIC_NAME = {"house", "court", "hall", "halls", "student", "students", "studios", "studio", "village", "place",
                "point", "tower", "towers", "apartments", "residence", "residences", "lodge", "accommodation",
                "living", "building", "buildings", "block", "campus", "park", "view", "gardens", "garden", "square",
                "yard", "works", "quarter", "central", "city", "new", "old", "north", "south", "east", "west",
                "upper", "lower", "street", "road", "lane", "phase", "college", "university", "flats", "rooms",
                "mews", "close", "terrace", "wharf", "quay", "mill", "mills", "exchange", "gate", "green", "one",
                "two", "three", "the", "and", "of", "at", "on", "st", "saint", "staircase", "hostel", "cottage",
                "cottages", "annexe", "main", "site", "centre", "center", "royal", "victoria", "albert", "kings",
                "queens", "king", "queen", "bridge", "station", "church", "castle", "market", "union", "crown"}


def distinct_words(name: str | None, drop: set[str] = frozenset()) -> set[str]:
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
            if len(t) >= 4 and not t.isdigit() and t not in GENERIC_NAME and t not in drop}


LOOSE_DROP = {"student", "students", "accommodation", "living", "the", "and", "for", "house", "hall", "halls",
              "court", "studios", "studio", "apartments", "residence", "residences", "village", "building", "phase",
              "block", "flats", "rooms", "college", "university", "street", "road", "lane"}


def loose_words(name: str | None) -> set[str]:
    """Every word of a name bar the ones nearly all student buildings share. Two of
    these in common ("old", "fire", "station") is a match that the stricter list misses."""
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
            if len(t) >= 3 and not t.isdigit() and t not in LOOSE_DROP}


def street_parts(address: str | None) -> tuple[set[str], set[str]]:
    """(street words, house numbers) of an address line."""
    toks = re.sub(r"[^a-z0-9 \-]", " ", (address or "").lower()).split()
    words = {t for t in toks if len(t) >= 5 and not any(ch.isdigit() for ch in t) and t not in STREET_NOISE
             and t not in GENERIC_NAME}
    numbers = {n for t in toks for n in re.findall(r"\d+", t)}
    return words, numbers


def display_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return name.title() if name.isupper() else name


def council_for(db, city: str, cache: dict) -> int | None:
    """The council a register city sits in, when the name settles it."""
    if city in cache:
        return cache[city]
    key = city_key(city)
    found = None
    if key and key != "london":
        rows = db.execute(text("SELECT id, name FROM councils")).fetchall()
        exact = [r for r in rows if city_key(r[1]) == key or city_key(r[1]) == key.split()[0]]
        if len(exact) == 1:
            found = exact[0][0]
        elif not exact:
            starts = [r for r in rows if r[1].lower().startswith(key + " ") or r[1].lower().startswith(key + ",")
                      or r[1].lower() in (f"city of {key}", f"{key} city")]
            if len(starts) == 1:
                found = starts[0][0]
    cache[city] = found
    return found


def plan_new_schemes(db, min_beds: int) -> tuple[list[dict], list[dict], list[dict]]:
    """(groups to create, possible duplicates for a person, buildings below the size line)."""
    devs = [d for d in load_register() if d.get("beds")]
    linked = {int(m.group(1)) for (ref,) in db.execute(text(
        "SELECT source_reference FROM scheme_source_records WHERE source = :s AND scheme_id IS NOT NULL"),
        {"s": SOURCE}) for m in [re.search(r"/development/(\d+)$", ref or "")] if m}
    existing = db.execute(text(f"""
        SELECT es.id, es.name, es.address, es.postcode, cc.name AS council, co.name AS operator, {SIZE} AS size,
               es.scheme_type
        FROM existing_schemes es LEFT JOIN councils cc ON cc.id = es.council_id
        LEFT JOIN companies co ON co.id = es.operator_company_id
        WHERE es.merged_into_id IS NULL AND es.record_class = 'scheme'""")).mappings().all()
    known_cities = frozenset(city_key(d["city"]) for d in devs)
    place_words = {w for k in known_cities for w in k.split()} | \
        {w for (n,) in db.execute(text("SELECT name FROM councils")) for w in city_key(n).split()}
    prepared = []
    for s in existing:
        words, numbers = street_parts(s["address"])
        prepared.append({**dict(s), "cities": scheme_city_keys(s["council"], s["address"], s["name"], known_cities),
                         "words": (distinct_words(s["name"]) | distinct_words(s["address"])) - STREET_NOISE - place_words,
                         "loose": loose_words(s["name"]) - place_words, "street": words - place_words,
                         "street_raw": words, "numbers": numbers, "op": brand_tokens(s["operator"]),
                         "joined": joined_name(s["name"], brand_tokens(s["operator"]) | place_words)})

    create, review, small = [], [], []
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for d in devs:
        if d["id"] in linked:
            continue
        if d["beds"] < min_beds:
            small.append(d)
            continue
        dcity = city_key(d["city"])
        member = brand_tokens(d["member"])
        d_words = distinct_words(d["name"], place_words | member)
        d_loose = loose_words(d["name"]) - place_words - member
        d_street_raw, d_numbers = street_parts(d["address"])
        d_street = d_street_raw - place_words
        d_joined = joined_name(d["name"], member | place_words)
        reason, cand = None, None
        for p in prepared:
            shared = d_words & p["words"]
            loose = d_loose & p["loose"]
            same_op = bool(member & p["op"])
            if len(d_joined) >= 6 and d_joined == p["joined"] and dcity and \
                    (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"])):
                reason, cand = "same name once the spaces are closed up", p
                break
            if same_op and not d_words and not d_loose and dcity and \
                    (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"])):
                reason, cand = "named only for its operator and city, where we already hold that operator", p
                break
            if d_numbers & p["numbers"] and d_street_raw & p["street_raw"] and dcity and \
                    (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"])):
                reason, cand = (f"same house number and road "
                                f"({', '.join(sorted(d_numbers & p['numbers']))} {', '.join(sorted(d_street_raw & p['street_raw']))})"), p
                break
            close10 = bool(p["size"]) and abs(d["beds"] - p["size"]) <= 0.10 * p["size"]
            close5 = bool(p["size"]) and abs(d["beds"] - p["size"]) <= 0.05 * p["size"]
            same_city = bool(dcity) and (dcity in p["cities"] or any(dcity in c or c in dcity for c in p["cities"]))
            if not same_city:
                # A town inside a larger council area, or a scheme of ours with no place at all:
                # two shared name words plus the operator or the size is the same building.
                if len(shared) >= 2 and (same_op or close10) and not (p["cities"] and dcity and len(shared) < 3
                                                                    and not same_op):
                    reason = f"same name words ({', '.join(sorted(shared))}) in a differently named place"
                elif not p["cities"] and shared and same_op and max(len(t) for t in shared) >= 6:
                    reason = f"same operator and name word ({', '.join(sorted(shared))}); our record has no place"
            elif shared and (len(shared) >= 2 or max(len(t) for t in shared) >= 5):
                reason = f"shares the name word(s) {', '.join(sorted(shared))}"
            elif len(loose) >= 2:
                reason = f"shares the words {', '.join(sorted(loose))}"
            elif loose and close5 and d["beds"] >= 50:
                reason = f"shares the word {', '.join(sorted(loose))} and the bed count is within 5%"
            elif d_street & p["street"] and (d_numbers & p["numbers"] or close10 or same_op):
                reason = f"same street ({', '.join(sorted(d_street & p['street']))})" + \
                    (" and number" if d_numbers & p["numbers"] else " and size" if close10 else " and operator")
            elif same_op and close10:
                reason = "same operator and a bed count within 10%"
            elif p["size"] and d["beds"] >= 50 and p["size"] == d["beds"]:
                reason = "exactly the same bed count"
            if reason:
                cand = p
                break
        if reason:
            review.append({**d, "why": reason, "scheme_id": cand["id"], "scheme": cand["name"],
                           "scheme_operator": cand["operator"], "scheme_beds": cand["size"]})
            continue
        groups[(display_name(d["name"]).lower(), d["address"].lower().strip(), dcity)].append(d)
    for parts in groups.values():
        parts.sort(key=lambda d: -d["beds"])
        create.append({"name": display_name(parts[0]["name"]), "address": parts[0]["address"], "city": parts[0]["city"],
                       "member": parts[0]["member"], "beds": sum(p["beds"] for p in parts), "parts": parts,
                       "website": parts[0].get("website")})
    return create, review, small


def operator_for(db, member: str, educational: bool):
    """The company row for a register member. A private operator is matched by brand,
    the way the rest of the platform does it. A university or college is matched by
    its exact name only: "Downing College" is not "Downing Students"."""
    from app.models.models import Company
    from enrich_operators import resolve_company
    if not educational:
        return resolve_company(db, member)
    row = db.execute(text("""
        SELECT id FROM companies WHERE merged_into_id IS NULL
          AND regexp_replace(lower(name), '[^a-z0-9]+', '', 'g') = regexp_replace(lower(:n), '[^a-z0-9]+', '', 'g')
        ORDER BY id LIMIT 1"""), {"n": member}).first()
    if row:
        return db.get(Company, row[0])
    co = Company(name=member, normalized_name=re.sub(r"[^a-z0-9]+", "", member.lower()), company_type="University",
                 is_active=True)
    db.add(co)
    db.flush()
    return co


def new_schemes(db, apply: bool, min_beds: int) -> None:
    create, review, small = plan_new_schemes(db, min_beds)
    try:
        with open(os.path.join(DATA_DIR, "members.json"), encoding="utf-8") as fh:
            codes = {m["name"]: m["code"] for m in json.load(fh)}
    except OSError:
        codes = {}
    educational = sum(1 for g in create if codes.get(g["member"]) == "educationalprovider")
    print(f"register buildings not yet linked to a scheme: {sum(len(g['parts']) for g in create) + len(review) + len(small)}")
    print(f"  to add as new schemes: {len(create)} ({sum(g['beds'] for g in create):,} beds; "
          f"{educational} run by universities or colleges; {sum(1 for g in create if len(g['parts']) > 1)} combine "
          f"several register entries at one address)")
    print(f"  held back, may already be in our data: {len(review)} ({sum(r['beds'] for r in review):,} beds)")
    print(f"  held back, under {min_beds} beds: {len(small)} ({sum(d['beds'] for d in small):,} beds)")
    print("  new by city:", Counter(city_key(g["city"]) for g in create).most_common(14))
    print("  new by operator:", Counter(g["member"] for g in create).most_common(14))
    print("  held-back reasons:", Counter(r["why"].split("(")[0].strip()[:42] for r in review).most_common(6))
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"national_code_new_schemes_{stamp}{'' if apply else '_dryrun'}.csv")
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["outcome", "register_name", "register_member", "city", "address", "beds", "why",
                    "our_scheme_id", "our_scheme", "our_operator", "our_beds", "page"])
        for g in create:
            w.writerow(["added" if apply else "to add", g["name"], g["member"], city_key(g["city"]), g["address"],
                        g["beds"], f"{len(g['parts'])} register entries" if len(g["parts"]) > 1 else "", "", "", "", "",
                        g["parts"][0]["page"]])
        for r in review:
            w.writerow(["held back: possible duplicate", r["name"], r["member"], city_key(r["city"]), r["address"],
                        r["beds"], r["why"], r["scheme_id"], r["scheme"], r["scheme_operator"], r["scheme_beds"],
                        r["page"]])
        for d in small:
            w.writerow([f"held back: under {min_beds} beds", d["name"], d["member"], city_key(d["city"]), d["address"],
                        d["beds"], "", "", "", "", "", d["page"]])
    print(f"  detail: {path}")
    for g in sorted(create, key=lambda g: -g["beds"])[:12]:
        print(f"    add: {g['name'][:34]:34s} {g['member'][:26]:26s} {city_key(g['city'])[:14]:14s} {g['beds']:>5} beds")
    for r in sorted(review, key=lambda r: -r["beds"])[:12]:
        print(f"    hold: {r['name'][:28]:28s} {r['beds']:>5} ~ #{r['scheme_id']} {r['scheme'][:28]:28s} "
              f"{str(r['scheme_beds']):>5} ({r['why'][:44]})")
    if not apply:
        print("dry run: nothing written")
        return
    councils: dict = {}
    companies: dict[str, int] = {}
    made, new_companies = [], 0
    before = db.execute(text("SELECT coalesce(max(id), 0) FROM companies")).scalar()
    for g in create:
        if g["member"] and g["member"] not in companies:
            companies[g["member"]] = operator_for(db, g["member"],
                                                  codes.get(g["member"]) == "educationalprovider").id
        ids = [p["id"] for p in g["parts"]]
        scheme = ExistingScheme(
            name=g["name"][:255], address=", ".join(x for x in (g["address"], city_key(g["city"]).title()) if x)[:500],
            council_id=council_for(db, g["city"], councils), scheme_type="PBSA", status="operational",
            beds_total=g["beds"], operator_company_id=companies.get(g["member"]), source=SOURCE,
            source_reference=f"nationalcode:{ids[0]}", record_class="scheme", locked_fields={})
        db.add(scheme)
        db.flush()
        note = ("National Code register: " + (f"{len(ids)} entries at this address combined. " if len(ids) > 1 else "")
                + ("University or college provider." if codes.get(g["member"]) == "educationalprovider"
                   else "Private provider."))
        db.execute(text("UPDATE existing_schemes SET notes = :n WHERE id = :id"), {"n": note, "id": scheme.id})
        for p in g["parts"]:
            link_source_record(db, SOURCE, f"{BASE}/development/{p['id']}", scheme_id=scheme.id, raw_name=p["name"],
                               raw_operator=p["member"], raw_address=f"{p['address']}, {p['city']}",
                               payload={"beds": p["beds"], "page": p["page"], "website": p.get("website")},
                               match_method="created from register", match_score=1.0, status="matched")
        record_observation(db, scheme, "beds_total", g["beds"], SOURCE, reference=g["parts"][0]["page"], confidence=0.9,
                           quote="; ".join(f"{p['name']}, {p['address']}: {p['beds']} bed spaces" for p in g["parts"])[:1500],
                           extractor="register", refresh_cache=False)
        made.append({"scheme_id": scheme.id, "name": g["name"], "beds": g["beds"], "register_ids": ids})
    new_companies = db.execute(text("SELECT count(*) FROM companies WHERE id > :b"), {"b": before}).scalar()
    db.commit()
    with open(os.path.join(OUT_DIR, f"national_code_new_schemes_{stamp}.json"), "w", encoding="utf-8") as fh:
        json.dump({"created": made, "new_companies_from_id": before + 1}, fh, indent=1)
    print(f"written: {len(made)} schemes added ({sum(m['beds'] for m in made):,} beds), {new_companies} new operator rows")


def member_websites(db, apply: bool) -> None:
    """The register links every building to its operator's website. Where an operator
    of register-sourced schemes has no website on file, take the one its buildings point to."""
    from urllib.parse import urlparse
    sites: dict[str, Counter] = defaultdict(Counter)
    for d in load_register():
        url = (d.get("website") or "").strip()
        host = urlparse(url if "://" in url else "https://" + url).netloc.lower() if url else ""
        if host and "." in host and not any(x in host for x in ("facebook.", "linkedin.", "instagram.", "google.",
                                                                "nationalcode.", "rightmove.", "zoopla.")):
            sites[d["member"]][f"https://{host}"] += 1
    rows = db.execute(text("""
        SELECT DISTINCT co.id, co.name, sr.raw_operator
        FROM existing_schemes es JOIN companies co ON co.id = es.operator_company_id
        JOIN scheme_source_records sr ON sr.scheme_id = es.id AND sr.source = :s
        WHERE es.source = :s AND es.merged_into_id IS NULL AND nullif(trim(co.website), '') IS NULL"""),
        {"s": SOURCE}).fetchall()
    filled, seen = [], set()
    for cid, name, member in rows:
        if cid in seen or member not in sites:
            continue
        site, n = sites[member].most_common(1)[0]
        if n / sum(sites[member].values()) < 0.6:
            continue                    # its buildings point to several sites: not one operator website
        seen.add(cid)
        filled.append({"company_id": cid, "company": name, "website": site, "buildings": n})
        if apply:
            db.execute(text("UPDATE companies SET website = :w, updated_at = now() WHERE id = :id "
                            "AND nullif(trim(website), '') IS NULL"), {"w": site, "id": cid})
    print(f"operators of register schemes with no website on file: {len({r[0] for r in rows})}; "
          f"website {'set' if apply else 'available'} for {len(filled)}")
    for f in filled[:8]:
        print(f"    {f['company'][:34]:34s} {f['website']}")
    if apply:
        db.commit()
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(os.path.join(OUT_DIR, f"national_code_member_websites_{stamp}.json"), "w", encoding="utf-8") as fh:
            json.dump(filled, fh, indent=1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--member-websites", action="store_true",
                    help="fill operator websites from the links the register gives for their buildings")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--members", action="store_true", help="read which members are private and which educational")
    ap.add_argument("--match", action="store_true")
    ap.add_argument("--new-schemes", action="store_true", help="add register buildings we do not hold")
    ap.add_argument("--min-beds", type=int, default=20)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if not (args.fetch or args.match or args.members or args.new_schemes or args.member_websites):
        ap.error("use --fetch, --members, --match, --new-schemes and/or --member-websites")
    if args.fetch:
        fetch_register()
    if args.members:
        fetch_members()
    if args.match or args.new_schemes or args.member_websites:
        db = SessionLocal()
        try:
            if args.match:
                match(db, args.apply)
            if args.new_schemes:
                new_schemes(db, args.apply, args.min_beds)
            if args.member_websites:
                member_websites(db, args.apply)
        finally:
            db.close()


if __name__ == "__main__":
    main()
