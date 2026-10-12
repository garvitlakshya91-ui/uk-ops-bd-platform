"""Opening years from energy certificates.

Every new home gets an energy certificate when it is completed, lodged as a
"new dwelling". For each operating scheme with no opening year this asks the
public energy-certificate register for the certificates at the scheme's
postcode and takes the year in which its new-dwelling certificates were
lodged. The register needs a bearer token (EPC_BEARER_TOKEN in .env).

A year is taken only when the certificates plausibly are the scheme:
  - enough of them for the scheme's size (at least 8, and at least 8% of
    its beds or homes, since a student flat of six rooms has one certificate),
  - and, where certificate addresses carry the scheme's name, only those
    certificates are counted,
  - and they single this scheme out: where another scheme shares the
    postcode they must carry this one's name and not the neighbour's, and
    where they carry no name there must not be more of them than the
    scheme has homes. A street word in a name ("New Walk") does not count.

The year is the one by which half the certificates had been lodged. It is
stored as a derived reading with the count behind it. The scheme's opening
year is filled only where it was empty and the reading is firm.

    python date_schemes_from_epc.py              # dry run
    python date_schemes_from_epc.py --apply
    python date_schemes_from_epc.py --cross-check   # compare with the years taken from the build-to-rent feed
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.scrapers.epc_client import EPC_API_BASE, EPCApiError, EPCClient, EPCQueryError  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402
from app.scrapers.scheme_matching import norm_name  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
CACHE = os.path.join(HERE, "data", "enrichment", "epc_by_postcode.json")
SOURCE = "energy_certificate"
FEED_SOURCE = "arl_btr_open_operating"
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"
POSTCODE_OK = re.compile(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")
FIRM = 0.75
# A street word in a scheme's name ("134 New Walk", "Unilife High Street") is in every address on that street.
STREET_WORDS = {"road", "street", "lane", "walk", "avenue", "terrace", "place", "square", "gate", "high",
                "way", "drive", "close", "row"}


def carrying(name: str, certs: list[dict]) -> list[dict]:
    """The certificates whose address carries enough of a scheme name's own words."""
    tokens = {t for t in norm_name(name) if len(t) >= 4 and t not in STREET_WORDS}
    if not tokens:
        return []
    found = []
    for c in certs:
        addr = " ".join(str(c.get(k) or "") for k in ("address1", "address2", "address3", "address")).lower()
        if sum(1 for t in tokens if t in addr) >= max(1, (len(tokens) + 1) // 2):
            found.append(c)
    return found


def judge(scheme: dict, certs: list[dict], this_year: int) -> dict | None:
    """The opening year the certificates at a scheme's postcode point to, or None.

    `scheme` may carry "neighbours": the names of the other schemes on its postcode.
    """
    new = [c for c in certs if (c.get("transaction-type") or "").lower().startswith("new dwelling")
           and (c.get("lodgement-date") or "")[:4].isdigit()]
    if not new:
        return None
    named = carrying(scheme["name"], new)
    by_name = len(named) >= 5
    used = named if by_name else new
    size = scheme["size"]
    need = max(8, int(0.08 * size)) if size else 20
    if len(used) < need and not by_name:
        return None
    years = sorted(int(c["lodgement-date"][:4]) for c in used)
    year = years[(len(years) - 1) // 2]
    if not 2008 <= year <= this_year:
        return None
    spread = Counter(years)
    share = spread[year] / len(years)
    # Certificates spread thinly over many years are resales and re-lets, not one completion.
    if share < 0.4 and not by_name:
        return None
    confidence = 0.9 if by_name and share >= 0.5 else 0.85 if (size and len(used) >= 0.3 * size and share >= 0.6) \
        else 0.8 if share >= 0.6 else 0.7
    # Certificates that may be a neighbour's are a note, not a date: another scheme shares the
    # postcode and the certificates do not single this one out, or there are more of them than
    # the scheme has homes.
    neighbours = scheme.get("neighbours") or ()
    doubt = None
    if by_name:
        mine = {id(c) for c in named}
        if any(2 * sum(id(c) in mine for c in carrying(other, new)) >= len(named) for other in neighbours):
            doubt = "the same certificates carry a neighbouring scheme's name too"
    elif neighbours:
        doubt = "another scheme shares the postcode"
    elif size and len(used) > 1.2 * size:
        doubt = "more certificates than the scheme has homes"
    if doubt:
        confidence = min(confidence, 0.7)
    return {"year": year, "certificates": len(used), "in_year": spread[year], "by_name": by_name,
            "confidence": confidence,
            "quote": f"{len(used)} new-dwelling energy certificates at {scheme['postcode']}"
                     f"{' carrying the scheme name' if by_name else ''}, {spread[year]} lodged in {year}"
                     f"{'; ' + doubt if doubt else ''}"}


def agreement(feed_year: int, certificate_year: int) -> str:
    """How an opening year from the build-to-rent feed compares with the certificates' year."""
    gap = abs(feed_year - certificate_year)
    return "same year" if gap == 0 else "a year apart" if gap == 1 else "more than a year apart"


async def run(apply: bool, limit: int | None, cross_check: bool = False) -> None:
    db = SessionLocal()
    if cross_check:
        rows = db.execute(text(f"""
            SELECT es.id, es.name, upper(trim(es.postcode)) AS postcode, es.scheme_type, {SIZE} AS size,
                   feed.year AS feed_year
            FROM existing_schemes es
            JOIN (SELECT DISTINCT ON (scheme_id) scheme_id, value_num::int AS year
                  FROM scheme_observations
                  WHERE field = 'build_year' AND source = :feed AND value_num IS NOT NULL
                  ORDER BY scheme_id, id DESC) feed ON feed.scheme_id = es.id
            WHERE {NAMED} AND nullif(trim(es.postcode), '') IS NOT NULL
            ORDER BY es.id"""), {"feed": FEED_SOURCE}).mappings().all()
    else:
        rows = db.execute(text(f"""
            SELECT es.id, es.name, upper(trim(es.postcode)) AS postcode, es.scheme_type, {SIZE} AS size
            FROM existing_schemes es
            WHERE {NAMED} AND coalesce(es.status, 'operational') = 'operational'
              AND es.completion_date IS NULL AND es.build_year IS NULL AND nullif(trim(es.postcode), '') IS NOT NULL
            ORDER BY es.id""")).mappings().all()
    schemes = [dict(r) for r in rows if POSTCODE_OK.match(r["postcode"])][:limit]
    at_postcode = defaultdict(list)
    for other_id, name, postcode in db.execute(text(f"""
            SELECT es.id, es.name, upper(trim(es.postcode)) FROM existing_schemes es
            WHERE {NAMED} AND nullif(trim(es.postcode), '') IS NOT NULL""")):
        at_postcode[postcode].append((other_id, name))
    for s in schemes:
        s["neighbours"] = [name for other_id, name in at_postcode[s["postcode"]] if other_id != s["id"]]
    print(("schemes dated from the build-to-rent feed, with a postcode: " if cross_check
           else "operating schemes with no opening year and a postcode: ") + str(len(schemes)))
    try:
        with open(CACHE, encoding="utf-8") as fh:
            cache: dict[str, list[dict]] = json.load(fh)
    except (OSError, ValueError):
        cache = {}
    keep = ("transaction-type", "lodgement-date", "address1", "address2", "address3", "address",
            "property-type", "schema-type")
    stats = Counter()
    found = []
    this_year = dt.date.today().year
    os.makedirs(OUT_DIR, exist_ok=True)
    # A failed lookup stops the run (the postcodes read so far stay cached), so
    # an outage is never counted as "no certificates".
    try:
        async with EPCClient() as client:
            for n, s in enumerate(schemes, 1):
                pc = s["postcode"]
                if pc not in cache:
                    try:
                        # The search gives every certificate at the postcode; the transaction
                        # type is read from the full certificate of each possible new dwelling.
                        data = await client.with_details(await client.search_domestic(postcode=pc))
                    except EPCQueryError:
                        stats["register refused the postcode"] += 1
                        continue
                    cache[pc] = [{k: c.get(k) for k in keep if c.get(k)} for c in data]
                    if n % 100 == 0:
                        with open(CACHE, "w", encoding="utf-8") as fh:
                            json.dump(cache, fh)
                        print(f"  {n}/{len(schemes)} postcodes read", flush=True)
                certs = cache[pc]
                if not certs:
                    stats["no certificates at the postcode"] += 1
                    continue
                got = judge(s, certs, this_year)
                if not got:
                    stats["certificates there, but not clearly this scheme's completion"] += 1
                    continue
                firm = got["confidence"] >= FIRM
                stats["opening year read (firm)" if firm else "opening year suggested (kept as a note only)"] += 1
                found.append({"scheme": s["id"], "name": s["name"], "type": s["scheme_type"], "postcode": pc,
                              "size": s["size"], **got, "firm": firm,
                              **({"feed_year": s["feed_year"]} if cross_check else {})})
            print(f"register requests made: {client.requests_made}")
    finally:
        with open(CACHE, "w", encoding="utf-8") as fh:
            json.dump(cache, fh)
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    firm_rows = [f for f in found if f["firm"]]
    print("  by type (firm):", Counter(f["type"] for f in firm_rows).most_common())
    print("  years (firm):", sorted(Counter(f["year"] for f in firm_rows).items()))
    for f in found[:14]:
        print(f"    {f['name'][:30]:30s} {f['postcode']:8s} size {str(f['size']):>5} -> {f['year']} "
              f"conf {f['confidence']} ({f['quote'][:70]})")
    if cross_check:
        verdicts = Counter(agreement(f["feed_year"], f["year"]) for f in firm_rows)
        print("  feed year against certificate year (firm readings):")
        for k in ("same year", "a year apart", "more than a year apart"):
            print(f"    {verdicts[k]:>5}  {k}")
        for f in sorted(firm_rows, key=lambda f: -abs(f["feed_year"] - f["year"])):
            if abs(f["feed_year"] - f["year"]) > 1:
                print(f"    {f['name'][:30]:30s} {f['postcode']:8s} feed {f['feed_year']}, certificates {f['year']} "
                      f"({f['certificates']} certificates)")
        db.rollback()
        print("cross-check: nothing written")
    elif apply:
        for f in found:
            scheme = db.get(ExistingScheme, f["scheme"])
            record_observation(db, scheme, "build_year", f["year"], SOURCE,
                               reference=f"{EPC_API_BASE}/api/domestic/search?postcode={f['postcode'].replace(' ', '')}",
                               basis="derived", confidence=f["confidence"], quote=f["quote"], extractor="rule",
                               refresh_cache=False)
            if f["firm"] and scheme.build_year is None:
                set_field(scheme, "build_year", f["year"], SOURCE, db, changed_by="date_schemes_from_epc.py")
        db.commit()
        print(f"written: {len(found)} readings stored, {len(firm_rows)} opening years filled")
    else:
        db.rollback()
        print("dry run: nothing written")
    db.close()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    kind = "_crosscheck" if cross_check else "" if apply else "_dryrun"
    path = os.path.join(OUT_DIR, f"opening_years_{stamp}{kind}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"applied": apply, "cross_check": cross_check, "counts": dict(stats), "found": found}, fh, indent=1)
    print("run log:", path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--cross-check", action="store_true",
                    help="compare the opening years taken from the build-to-rent feed with the certificates")
    args = ap.parse_args()
    if args.cross_check and args.apply:
        ap.error("--cross-check only reports; it cannot be combined with --apply")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("CROSS-CHECK (nothing is written)" if args.cross_check
          else "APPLYING" if args.apply else "DRY RUN (nothing is written)")
    try:
        asyncio.run(run(args.apply, args.limit, args.cross_check))
    except EPCApiError as exc:
        raise SystemExit(f"stopped, nothing written: {exc}")


if __name__ == "__main__":
    main()
