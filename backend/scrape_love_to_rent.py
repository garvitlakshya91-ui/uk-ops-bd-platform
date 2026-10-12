"""Asking rents from the Love to Rent directory of build-to-rent developments.

lovetorent.co.uk lists build-to-rent developments by operator, each with its
address, postcode and the homes currently advertised with their monthly
rents. This reads the development pages named in the site's sitemap and
matches them to our build-to-rent schemes by postcode and name.

  --fetch    Read the development pages (robots.txt respected, one request a
             second) into data/love_to_rent/developments.jsonl.
  --match    Match developments to schemes and report. With --apply, store
             the rents for schemes that have none.

A development is matched when its postcode is a scheme's postcode and the
names share a word, or when the names are the same and the town agrees.
Weaker pairs go to a review file and are not applied.

Rents are asking rents for the homes advertised on the day of capture. They
go into the rent tables with this directory as their source; nothing
existing is changed.
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
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models.rents import record_advertised_rent, record_room_rent  # noqa: E402
from app.registry.core import link_source_record  # noqa: E402
from app.scrapers.scheme_matching import norm_name, norm_pc  # noqa: E402
from read_scheme_pages import LOC_RE, NO_RENT, Pages, monthly_rents, page_text  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data", "love_to_rent")
OUT_DIR = os.path.join(HERE, "data", "enrichment")
BASE = "https://www.lovetorent.co.uk"
SOURCE = "lovetorent"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"
POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}[0-9][0-9A-Z]?)\s*([0-9][A-Z]{2})\b")
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
BRAND_NOISE = {"living", "homes", "limited", "ltd", "group", "uk", "plc", "llp", "management", "property", "the",
               "of", "and", "residential", "real", "estate", "developments", "lettings"}


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def brand_tokens(name: str | None) -> set[str]:
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split() if t not in BRAND_NOISE and len(t) > 1}


def fetch() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    pages = Pages()
    status, body = pages.get(BASE + "/sitemap.xml", keep=False)
    urls = [html_mod.unescape(u.strip()) for u in LOC_RE.findall(body or "")]
    # /companies/<operator>/<development>/ : three path parts
    devs = sorted({u for u in urls if re.fullmatch(r"/companies/[^/]+/[^/]+/?", urlparse(u).path)})
    print(f"sitemap: {len(urls)} addresses, {len(devs)} development pages", flush=True)
    rows = []
    for n, url in enumerate(devs, 1):
        status, html = pages.get(url)
        if status != 200 or not html:
            continue
        body_text = page_text(html, cap=200000)
        name = clean(H1_RE.search(html).group(1)) if H1_RE.search(html) else ""
        title = clean(TITLE_RE.search(html).group(1)) if TITLE_RE.search(html) else ""
        start = body_text.find(name, 200) if name else -1
        window = body_text[start:start + 400] if start >= 0 else body_text[:1500]
        pc = POSTCODE_RE.search(window.upper()) or POSTCODE_RE.search(body_text.upper())
        address = ""
        if pc and start >= 0:
            m = re.search(re.escape(name) + r"\s+(.{5,160}?)\b" + pc.group(1) + r"\s*" + pc.group(2), window, re.I)
            address = m.group(1).strip(" ,") if m else ""
        by = re.search(r"\bby\s+(.+?)\s+with\s+\d+", title) or re.search(r"\bby\s+(.+?)(?:\s*[|\-]|$)", title)
        rents = monthly_rents(html)
        rows.append({"url": url, "name": name, "operator": by.group(1).strip() if by else url.split("/")[4].replace("-", " "),
                     "operator_slug": url.split("/")[4], "address": address,
                     "postcode": f"{pc.group(1)} {pc.group(2)}" if pc else None,
                     "rents": [[label, value] for label, value, _ in rents],
                     "quote": rents[0][2][:200] if rents else None, "title": title[:200]})
        if n % 50 == 0:
            print(f"  {n}/{len(devs)} pages, {sum(1 for r in rows if r['rents'])} with rents", flush=True)
    # Operators' own landing pages on the directory: several wordings per development
    # ("apartments to rent at X", "houses to rent at X"); one of each is enough.
    groups: dict[tuple[str, str], str] = {}
    for u in urls:
        parts = urlparse(u).path.strip("/").split("/")
        if len(parts) == 2 and parts[0].startswith("development-landing-pages-"):
            m = re.search(r"to-rent-(?:at-|in-)?(.+)$", parts[1])
            groups.setdefault((parts[0].replace("development-landing-pages-", ""), m.group(1) if m else parts[1]), u)
    print(f"operator landing pages: {len(groups)} to read", flush=True)
    for n, ((op_slug, _), url) in enumerate(sorted(groups.items()), 1):
        status, html = pages.get(url)
        if status != 200 or not html:
            continue
        h1 = clean(H1_RE.search(html).group(1)) if H1_RE.search(html) else ""
        m = re.search(r"\bto rent\s+(?:at|in)?\s*(.+)$", h1, re.I)
        place = (m.group(1) if m else h1).strip()
        bits = [b.strip() for b in place.split(",") if b.strip()]
        district = bits[-1].upper() if bits and re.fullmatch(r"[A-Za-z]{1,2}\d[A-Za-z\d]?", bits[-1]) else None
        rents = monthly_rents(html)
        rows.append({"url": url, "name": bits[0] if bits else place, "title_place": place,
                     "operator": op_slug.replace("-", " ").replace("_", ""), "operator_slug": op_slug,
                     "address": ", ".join(bits[1:]), "postcode": None, "district": district,
                     "rents": [[label, value] for label, value, _ in rents],
                     "quote": rents[0][2][:200] if rents else None, "title": h1[:200]})
        if n % 50 == 0:
            print(f"  {n}/{len(groups)} landing pages", flush=True)
    pages.close()
    with open(os.path.join(DATA_DIR, "developments.jsonl"), "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"developments: {len(rows)}, with a postcode: {sum(1 for r in rows if r['postcode'])}, "
          f"with rents: {sum(1 for r in rows if r['rents'])}; requests made: {pages.fetched}")


def match(db, apply: bool) -> None:
    with open(os.path.join(DATA_DIR, "developments.jsonl"), encoding="utf-8") as fh:
        devs = [json.loads(line) for line in fh if line.strip()]
    schemes = db.execute(text(f"""
        SELECT es.id, es.name, es.postcode, es.address, es.status, es.scheme_type, cc.name AS council,
               co.name AS operator, ({NO_RENT}) AS no_rent
        FROM existing_schemes es LEFT JOIN councils cc ON cc.id = es.council_id
        LEFT JOIN companies co ON co.id = es.operator_company_id
        WHERE {NAMED} AND es.scheme_type IN ('BTR', 'Co-living', 'Mixed', 'Senior')""")).mappings().all()
    by_pc: dict[str, list[dict]] = defaultdict(list)
    by_tok: dict[str, list[dict]] = defaultdict(list)
    for s in schemes:
        p = {**dict(s), "tokens": norm_name(s["name"]) - brand_tokens(s["operator"]) or norm_name(s["name"])}
        if s["postcode"]:
            by_pc[norm_pc(s["postcode"])].append(p)
        for t in p["tokens"]:
            by_tok[t].append(p)
    print(f"directory developments: {len(devs)} ({sum(1 for d in devs if d['rents'])} with rents) | "
          f"our build-to-rent schemes: {len(schemes)} ({sum(1 for s in schemes if s['no_rent'])} with no rent)")

    accepted, review, used = [], [], set()
    for d in devs:
        tokens = norm_name(d["name"]) - brand_tokens(d["operator"]) or norm_name(d["name"])
        if not tokens:
            continue
        pc = norm_pc(d["postcode"]) if d["postcode"] else ""
        cands = []
        for p in by_pc.get(pc, []) if pc else []:
            shared = tokens & p["tokens"]
            op = bool(brand_tokens(d["operator"]) & brand_tokens(p["operator"]))
            if shared or op:
                cands.append(("A", len(shared) + op, p, "postcode + " + ("name" if shared else "operator")))
        if not cands:
            seen = {}
            for t in tokens:
                for p in by_tok.get(t, []):
                    seen[p["id"]] = p
            for p in seen.values():
                if tokens != p["tokens"]:
                    continue
                town = (p["council"] or "").lower()
                same_area = bool(pc) and bool(p["postcode"]) and norm_pc(p["postcode"])[:-3] == pc[:-3]
                same_town = bool(town) and town in (d["address"] or "").lower()
                op = bool(brand_tokens(d["operator"]) & brand_tokens(p["operator"]))
                if same_area or (same_town and op):
                    cands.append(("B", int(same_area) + int(same_town) + int(op), p, "same name + " +
                                  ("postcode district" if same_area else "town and operator")))
                elif same_town and len(tokens) >= 2:
                    # A two-word name in the same town is the same development ("One Eastside", Birmingham).
                    cands.append(("B", 1, p, "same two-word name + town"))
                elif same_town or op:
                    cands.append(("C", 1, p, "same name + " + ("town" if same_town else "operator")))
        if not cands and not pc:
            # A landing page gives the name, town and postcode district only.
            title_tokens = norm_name(d.get("title_place") or d["name"]) | tokens
            place = f"{d.get('title_place') or ''} {d.get('address') or ''}".lower()
            seen = {}
            for t in title_tokens:
                for p in by_tok.get(t, []):
                    seen[p["id"]] = p
            for p in seen.values():
                if not p["tokens"] or not p["tokens"] <= title_tokens:
                    continue
                op = bool(brand_tokens(d["operator"]) & brand_tokens(p["operator"]))
                district = bool(d.get("district")) and bool(p["postcode"]) and \
                    norm_pc(p["postcode"])[:-3] == d["district"]
                town = bool(p["council"]) and p["council"].lower() in place
                if op and (district or town):
                    cands.append(("B", len(p["tokens"]) + district + town, p,
                                  "operator's landing page: name + " + ("postcode district" if district else "town")))
                elif district or (op and len(p["tokens"]) >= 2):
                    cands.append(("C", 1, p, "landing page: name only partly confirmed"))
        if not cands:
            continue
        cands.sort(key=lambda c: (c[0], -c[1]))
        top = cands[0]
        tie = len(cands) > 1 and cands[1][0] == top[0] and cands[1][1] == top[1]
        row = {"dev": d, "scheme": top[2], "grade": top[0], "why": top[3]}
        if top[2]["id"] in used:
            continue                      # another wording of a page already matched to this scheme
        if top[0] in ("A", "B") and not tie:
            used.add(top[2]["id"])
            accepted.append(row)
        else:
            review.append(row)

    with_rent = [x for x in accepted if x["dev"]["rents"]]
    fill = [x for x in with_rent if x["scheme"]["no_rent"]]
    print(f"matched: {len(accepted)} schemes ({Counter(x['grade'] for x in accepted).most_common()}); "
          f"{len(with_rent)} of those pages print rents")
    print(f"  schemes with no rent that now get one: {len(fill)}")
    print(f"  by operator: {Counter((x['scheme']['operator'] or '')[:24] for x in fill).most_common(12)}")
    print(f"  left for review: {len(review)}")
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"love_to_rent_match_{stamp}{'' if apply else '_dryrun'}.csv")
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["outcome", "grade", "why", "scheme_id", "scheme", "our_operator", "our_postcode", "directory_name",
                    "directory_operator", "directory_postcode", "rent_from", "rent_to", "types", "page"])
        for label, xs in (("rent stored", fill), ("matched, already had rent or page has none",
                                                  [x for x in accepted if x not in fill]), ("review", review)):
            for x in xs:
                s, d = x["scheme"], x["dev"]
                vals = [v for _, v in d["rents"]]
                w.writerow([label, x["grade"], x["why"], s["id"], s["name"], s["operator"], s["postcode"], d["name"],
                            d["operator"], d["postcode"], min(vals) if vals else "", max(vals) if vals else "",
                            "; ".join(sorted({str(l) for l, _ in d["rents"] if l})), d["url"]])
    print(f"  detail: {path}")
    for x in fill[:12]:
        vals = [v for _, v in x["dev"]["rents"]]
        print(f"    {x['scheme']['name'][:28]:28s} <- {x['dev']['name'][:28]:28s} £{min(vals):,.0f} to £{max(vals):,.0f} pcm "
              f"[{x['grade']}: {x['why']}]")
    if not apply:
        print("dry run: nothing written")
        return
    for x in accepted:
        s, d = x["scheme"], x["dev"]
        link_source_record(db, SOURCE, d["url"], scheme_id=s["id"], raw_name=d["name"], raw_postcode=d["postcode"],
                           raw_operator=d["operator"], raw_address=d["address"],
                           payload={"matched_on": x["why"], "rents": d["rents"]},
                           match_method="postcode+name" if x["grade"] == "A" else "name+place",
                           match_score=0.95 if x["grade"] == "A" else 0.85, status="matched")
    for x in fill:
        s, d = x["scheme"], x["dev"]
        vals = [v for _, v in d["rents"]]
        record_advertised_rent(db, scheme_id=s["id"], source=SOURCE, kind="from", rent_pcm=min(vals),
                               source_reference=d["url"])
        if max(vals) > min(vals):
            record_advertised_rent(db, scheme_id=s["id"], source=SOURCE, kind="to", rent_pcm=max(vals),
                                   source_reference=d["url"])
        by_type: dict[str, float] = {}
        for label, value in d["rents"]:
            if label and label != "Home":
                by_type[label] = min(value, by_type.get(label, value))
        for label, value in by_type.items():
            record_room_rent(db, scheme_id=s["id"], source=SOURCE, room_type=label, rent_pcm=value,
                             source_reference=d["url"])
    db.commit()
    print(f"written: {len(accepted)} pages linked, rents stored for {len(fill)} schemes")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--match", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if not (args.fetch or args.match):
        ap.error("use --fetch and/or --match")
    if args.fetch:
        fetch()
    if args.match:
        db = SessionLocal()
        try:
            match(db, args.apply)
        finally:
            db.close()


if __name__ == "__main__":
    main()
