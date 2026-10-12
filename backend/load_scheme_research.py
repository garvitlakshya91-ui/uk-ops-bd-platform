"""Rents and opening years found by web research, checked against the pages they cite.

Research workers (web search and web fetch only) were given the schemes that still lacked a rent
or an opening year and asked for the weekly rent printed on the operator's or university's own
page and for a stated opening year, each with the page address and the words quoted.

Nothing a worker reports is stored on its say-so. For every claim this script re-reads the cited
page itself (politely, honouring robots.txt and refusals) and keeps the claim only when

  rent          the figure is printed on that page as a weekly price, and it is the scheme's:
                the page is headed with the scheme's name, or the figure stands right after it;
  room prices   the same test for each figure;
  opening year  the page states an opening in that year in a sentence naming the scheme, or on a
                page headed with the scheme's name, or the quoted sentence is on the page word
                for word and names the scheme;
  postcode      it is printed on the scheme's page and lies within 400 m of the scheme.

A claim whose page cannot be re-read (the site refuses automated requests, a PDF, a page drawn by
script) is left out and listed, never taken on trust. Only empty fields are filled.

    python load_scheme_research.py FILES...            # dry run
    python load_scheme_research.py FILES... --apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402
from sqlalchemy import text  # noqa: E402

import read_scheme_pages as R  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.models.rents import record_advertised_rent, record_room_rent  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "enrichment")
RENT_SOURCE = "operator_page"
YEAR_SOURCE = "web_research"
WEEK_NEAR = re.compile(r"per\s+week|weekly|\bpppw\b|\bppw\b|\bpw\b|p\s?/\s?w|p\.w\.|/\s?w(?:ee)?k\b|a\s+week|"
                       r"week(?:ly)?\s+(?:rent|price|cost|fee)", re.I)
# Words that describe a room or its price. In a fees table, any other capitalised word standing after
# a price is the name of the next row.
ROW_WORDS = set("""ensuite en suite studio studios room rooms flat flats bed beds bedroom bedrooms apartment apartments
    twin single double shared sharing standard medium large small premium premier deluxe classic superior plus luxury
    basic economy accessible adapted catered self part non postgraduate postgraduates undergraduate undergraduates
    returning students student special rate rates weeks week weekly cost total contract length per from and the with
    bathroom kitchen type price prices rent rents annual fee fees tenancy year years academic wks wks pw pppw ppw person
    bills included inclusive available sold out limited availability book now view find more bronze silver gold platinum
    diamond penthouse duplex twodio threedio club executive elite grand mini compact cosy corner balcony city for only
    just starting each occupancy let lets""".split())
FIELD_RE = re.compile(r"\b(?:completion|completed|date\s+opened|opened|year\s+built|built|opening)\s*[:\-–/]\s*"
                      r"(?:[A-Za-z]+\s+)?(1[89]\d\d|20[0-3]\d)\b", re.I)
NOT_WEEKLY_AFTER = re.compile(r"^\s*(?:pcm|per\s+(?:calendar\s+)?month|a\s+month|per\s+(?:year|annum|term)|total|deposit)", re.I)
OPEN_WORDS = re.compile(r"open(?:ed|ing|s)?\b|complet(?:ed|ion)|\bbuilt\b|constructed|welcom(?:ed|ing)|launch(?:ed|ing)|"
                        r"first\s+(?:students|residents)|handed\s+over|dates?\s+(?:from|back\s+to)|established|founded", re.I)
NOT_OPENING = re.compile(r"refurbish|renovat|acquir|purchas|bought|planning\s+(?:permission|approval|consent|application)|"
                         r"approved|granted|rebrand|renamed|sold\b", re.I)
POSTCODE_OK = re.compile(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")


def squash(s: str) -> str:
    """Lower case, plain quotes and dashes, single spaces: for comparing a quoted sentence with a page."""
    s = R.plain(s or "")
    s = s.translate(str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-",
                                   "—": "-", " ": " "}))
    return re.sub(r"\s+", " ", s.replace("'", "")).strip()


APOSTROPHES = str.maketrans("", "", "'`‘’")


def no_apostrophes(s: str) -> str:
    """"St Peter's House" and our "St Peters House" are one name."""
    return re.sub(r"&(?:#8217|#8216|#0?39|rsquo|lsquo|apos);", "", s or "").translate(APOSTROPHES)


def is_own_page(html: str, scheme: dict, url: str | None = None) -> bool:
    """Is the page the scheme's own: confirmed by a person, headed with the scheme's name, or a
    listing filtered to the one building ("?area=kexgill-tower")?"""
    if scheme.get("own_page") or R.headline_names(html, no_apostrophes(scheme["name"]), scheme.get("operator")):
        return True
    from urllib.parse import urlparse
    query = re.sub(r"[^a-z0-9]", "", R.plain(urlparse(url or "").query))
    words = [re.sub(r"[^a-z0-9]", "", w) for w in own_words(scheme) if len(w) >= 4 and w not in R.HEAD_FILLER]
    return bool(query and words) and all(w in query for w in words)


def figure_re(value: float) -> re.Pattern:
    """The price as a page would print it: £165, £165.00, £ 165; £132.05 only as £132.05."""
    whole, pence = int(value), round((value - int(value)) * 100)
    tail = rf"\.{pence:02d}" if pence else r"(?:\.00)?"
    return re.compile(rf"£\s?{whole}{tail}(?![\d,]|\.\d)")


def own_words(scheme: dict) -> list[str]:
    return [R.plain(w) for w in R.match_words(no_apostrophes(scheme["name"]), scheme.get("operator")) if len(w) >= 3]


def after_name(lead: str, words: list[str], own_tokens: set[str]) -> bool:
    """Does the figure these words lead up to belong to the scheme: is the scheme named before it
    (a row of a fees table, a price card) with nothing of another building's in between? Between the
    name and the figure there may be the scheme's own other prices, but no other building's name and,
    once a price has been passed, no word that could be the name of the next row."""
    low = R.plain(lead)
    if len(low) != len(lead):
        lead = low.upper()              # cannot line the two up: treat every word as capitalised
    if not words or not all(w in low for w in words):
        return False
    end = max(low.rfind(w) + len(w) for w in words)
    between = lead[end:]
    if R.names_another_building(lead, end, own_tokens) or between.count("£") > 6:
        return False
    first = between.find("£")
    if first >= 0:
        for token in re.findall(r"[A-Za-z][A-Za-z']+", between[first:]):
            if token[0].isupper() and len(token) >= 3 and token.lower() not in ROW_WORDS and token.lower() not in own_tokens:
                return False
    return True


def price_is_printed(html: str, scheme: dict, value: float, url: str | None = None) -> tuple[bool, str]:
    """Is this weekly price printed on the page, and is it the scheme's? (yes or no, why).
    `scheme["own_page"]` is set where a person has read the cited page and confirmed it is the
    scheme's own under another spelling ("Cranborne House" for our "Cranbourne House")."""
    html = no_apostrophes(html)
    body = R.page_text(html, cap=300000)
    words = own_words(scheme)
    own_tokens = set(re.findall(r"[a-z]+", R.plain(no_apostrophes(f"{scheme['name']} {scheme.get('address') or ''}")))) | set(words)
    own_page = is_own_page(html, scheme, url)
    seen = weekly = False
    for m in figure_re(value).finditer(body):
        seen = True
        if NOT_WEEKLY_AFTER.search(body[m.end():m.end() + 30]):
            continue
        # A weekly price: the unit follows the figure, or the figure sits in a table headed "per week".
        if not (WEEK_NEAR.search(body[m.end():m.end() + 45]) or WEEK_NEAR.search(body[max(0, m.start() - 2000):m.start()])):
            continue
        weekly = True
        lead = body[max(0, m.start() - 600):m.start()]
        if after_name(lead, words, own_tokens):
            return True, "printed right after the scheme's name"
        if own_page and not R.names_another_building(lead, max(0, len(lead) - R.NAMED_REACH), own_tokens):
            return True, "printed on the scheme's own page"
    if not seen:
        return False, "figure is not on the cited page"
    if not weekly:
        return False, "figure is on the page but not as a weekly price"
    return False, "figure is on the page but not tied to this scheme"


def year_is_stated(html: str, scheme: dict, year: int, quote: str | None, this_year: int) -> tuple[bool, str]:
    """Does the page say the scheme opened in this year? (yes or no, why)."""
    html = no_apostrophes(html)
    words = own_words(scheme)
    found = R.opening_year(html, this_year, set(words)) if words else None
    if found and found[0] == year:
        return True, "stated in a sentence naming the scheme"
    body = R.page_text(html, cap=300000)
    own_page = is_own_page(html, scheme)
    if own_page and (any(int(m.group(2)) == year for m in R.OPENED_RE.finditer(body))
                     or any(int(m.group(1)) == year for m in FIELD_RE.finditer(body))):
        return True, "stated on the scheme's own page"
    q = squash(quote or "")
    if len(q) >= 15 and str(year) in q and OPEN_WORDS.search(q):
        page = squash(body)
        at = page.find(q[:160]) if q[:160] in page else page.find(q[-160:])
        if at >= 0:
            near_year = q[max(0, q.find(str(year)) - 90):q.find(str(year)) + 30]
            if NOT_OPENING.search(near_year) and not R.OPENED_RE.search(q):
                return False, "the quoted sentence is about a refurbishment, sale or planning consent"
            # The sentence has to be about this scheme: it names it, the page is the scheme's own, or
            # the scheme is named in the few lines around the sentence.
            around = page[max(0, at - 600):at + len(q) + 300]
            if words and all(w in q for w in words) or own_page or (words and all(w in around for w in words)):
                return True, "quoted sentence is on the page word for word"
            return False, "quoted sentence is on the page but does not name the scheme"
        return False, "quoted sentence is not on the cited page"
    return False, "no opening in that year stated on the cited page"


_WIKI: dict[str, str | None] = {}


def wikipedia_html(url: str) -> str | None:
    """A Wikipedia article through Wikipedia's own interface for programs (its REST API). Wikipedia
    refuses automated readers that give no contact address (checked 10 October 2026), so this reads
    nothing unless RESEARCH_CONTACT is set in the environment to an address the owner is content to
    send. Without it the claim is left out, never taken on trust."""
    from urllib.parse import quote, unquote, urlparse
    contact = (os.environ.get("RESEARCH_CONTACT") or "").strip()
    if not contact:
        return None
    parts = urlparse(url)
    title = unquote(parts.path.split("/wiki/", 1)[-1]).strip()
    key = f"{parts.netloc}/{title}"
    if key not in _WIKI:
        try:
            r = httpx.get(f"https://{parts.netloc}/api/rest_v1/page/html/{quote(title, safe='')}", timeout=30.0,
                          headers={"User-Agent": f"uk-ops-bd-platform/1.0 (UK property data research; {contact})"},
                          follow_redirects=True)
            _WIKI[key] = r.text if r.status_code == 200 else None
        except httpx.HTTPError:
            _WIKI[key] = None
    return _WIKI[key]


def normal_room(raw: str) -> str | None:
    """One of our room types, or None: a price tier's marketing name ("Deluxe Plus") is not a room type."""
    m = R.ROOM_RE.search(raw or "")
    return R.room_label(m.group(1)) if m else None


def number(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if 60 <= f <= 900 else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--own-page", default="", help="scheme ids whose cited rent page a person has confirmed is the "
                    "scheme's own under another spelling; the figure must still be printed there as a weekly price")
    ap.add_argument("--skip-rent", default="", help="scheme ids whose rent claim is left out after review")
    ap.add_argument("--skip-year", default="", help="scheme ids whose opening-year claim is left out after review")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    paths = sorted({p for pat in args.files for p in glob.glob(pat)})
    claims: dict[int, dict] = {}
    for p in paths:
        try:
            with open(p, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError) as exc:
            print(f"  cannot read {os.path.basename(p)}: {exc}")
            continue
        for row in data if isinstance(data, list) else []:
            if isinstance(row, dict) and isinstance(row.get("id"), int):
                claims[row["id"]] = row
    print(f"research files: {len(paths)}, schemes reported on: {len(claims)}")
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT es.id, es.name, es.address, es.postcode, es.build_year, es.lat, es.lng,
               (SELECT co.name FROM companies co WHERE co.id = es.operator_company_id) AS operator,
               (EXISTS (SELECT 1 FROM scheme_advertised_rents a WHERE a.scheme_id = es.id AND a.is_current)
                OR EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)) AS has_rent
        FROM existing_schemes es WHERE es.id = ANY(:ids) AND es.merged_into_id IS NULL"""),
        {"ids": list(claims)}).mappings().all()
    schemes = {r["id"]: dict(r) for r in rows}
    pages = R.Pages()
    this_year = dt.date.today().year
    stats = Counter()
    ids = lambda raw: {int(x) for x in raw.replace(",", " ").split()}  # noqa: E731
    own_page, skip_rent, skip_year = ids(args.own_page), ids(args.skip_rent), ids(args.skip_year)
    for sid in own_page:
        if sid in schemes:
            schemes[sid]["own_page"] = True
    log: dict = {"applied": args.apply, "rents": [], "years": [], "postcodes": [], "left_out": [],
                 "by_hand": {"own_page": sorted(own_page), "skip_rent": sorted(skip_rent), "skip_year": sorted(skip_year)}}

    def read(url: str | None) -> tuple[str | None, str]:
        if not url or not str(url).startswith("http"):
            return None, "no page address given"
        if R.is_aggregator(url):
            return None, "an aggregator page, not the operator's own"
        if "wikipedia.org/wiki/" in url:
            html = wikipedia_html(url)
            return (html, "") if html else (None, "Wikipedia refuses automated readers that give no contact address")
        status, html = pages.get(url)
        if status == 200 and html and len(R.page_text(html, cap=5000)) > 200:
            return html, ""
        return None, {-1: "robots.txt disallows the page", 403: "site refuses automated requests",
                      200: "page has no readable text (drawn by script, or a PDF)"}.get(status, f"page could not be re-read (status {status})")

    want_pc: list[tuple[dict, str, str]] = []
    for sid, c in sorted(claims.items()):
        s = schemes.get(sid)
        if not s:
            stats["scheme not found"] += 1
            continue
        # ---- rent
        low = number(c.get("rent_from_ppw"))
        if low is not None and sid in skip_rent:
            stats["rent left out: by hand after review"] += 1
        elif low is not None and not s["has_rent"]:
            html, why = read(c.get("rent_url"))
            ok, how = (False, why) if html is None else price_is_printed(html, s, low, c.get("rent_url"))
            if not ok:
                stats[f"rent left out: {how}"] += 1
                log["left_out"].append({"scheme": sid, "name": s["name"], "what": "rent", "value": low,
                                        "url": c.get("rent_url"), "why": how})
            else:
                high = number(c.get("rent_to_ppw"))
                if high is not None and not (low < high <= 3 * low and price_is_printed(html, s, high, c.get("rent_url"))[0]):
                    high = None
                rooms: dict[str, float] = {}
                for room in c.get("rooms") or []:
                    v = number((room or {}).get("ppw")) if isinstance(room, dict) else None
                    label = normal_room(str(room.get("type") or "")) if isinstance(room, dict) else None
                    if v is None or not label or not low <= v <= (high or 3 * low):
                        continue
                    if price_is_printed(html, s, v, c.get("rent_url"))[0]:
                        rooms[label] = min(v, rooms.get(label, v))
                stats["rent kept"] += 1
                stats["  of which with prices by room type"] += 1 if rooms else 0
                log["rents"].append({"scheme": sid, "name": s["name"], "from": low, "to": high, "rooms": rooms,
                                     "url": c["rent_url"], "how": how, "year": c.get("academic_year")})
                if args.apply:
                    year = c.get("academic_year") if re.fullmatch(r"20\d\d/\d\d", str(c.get("academic_year") or "")) else None
                    record_advertised_rent(db, scheme_id=sid, source=RENT_SOURCE, kind="from", rent_ppw=low,
                                           academic_year=year, source_reference=c["rent_url"])
                    if high:
                        record_advertised_rent(db, scheme_id=sid, source=RENT_SOURCE, kind="to", rent_ppw=high,
                                               academic_year=year, source_reference=c["rent_url"])
                    for label, v in rooms.items():
                        record_room_rent(db, scheme_id=sid, source=RENT_SOURCE, room_type=label, rent_ppw=v,
                                         academic_year=year, source_reference=c["rent_url"])
                if c.get("postcode"):
                    want_pc.append((s, str(c["postcode"]), c["rent_url"]))
        elif low is not None:
            stats["rent not needed (scheme has one now)"] += 1
        # ---- opening year
        year = c.get("opened_year")
        if isinstance(year, int) and sid in skip_year:
            stats["opening year left out: by hand after review"] += 1
        elif isinstance(year, int) and s["build_year"] is None:
            if not 1850 <= year <= this_year:
                stats["opening year left out: not a plausible year"] += 1
                continue
            html, why = read(c.get("opened_url"))
            ok, how = (False, why) if html is None else year_is_stated(html, s, year, c.get("opened_quote"), this_year)
            if not ok:
                stats[f"opening year left out: {how}"] += 1
                log["left_out"].append({"scheme": sid, "name": s["name"], "what": "opening year", "value": year,
                                        "url": c.get("opened_url"), "why": how})
                continue
            stats["opening year kept"] += 1
            log["years"].append({"scheme": sid, "name": s["name"], "year": year, "url": c["opened_url"], "how": how,
                                 "quote": (c.get("opened_quote") or "")[:200]})
            if args.apply:
                scheme = db.get(ExistingScheme, sid)
                record_observation(db, scheme, "build_year", year, YEAR_SOURCE, reference=c["opened_url"],
                                   basis="observed", confidence=0.8, quote=(c.get("opened_quote") or "")[:300],
                                   extractor="research+rule", refresh_cache=False)
                if scheme.build_year is None:
                    set_field(scheme, "build_year", year, YEAR_SOURCE, db, changed_by="load_scheme_research.py")
    # ---- postcodes: printed on the page whose rent was kept, and at the scheme
    todo = [(s, f"{m.group(1)} {m.group(2)}", url) for s, pc, url in want_pc
            if not (s["postcode"] or "").strip() and s["lat"] is not None
            for m in [R.PAGE_PC_RE.search(pc.upper())] if m]
    where: dict[str, tuple[float, float]] = {}
    if todo:
        try:
            with httpx.Client(timeout=30.0) as client:
                r = client.post("https://api.postcodes.io/postcodes", json={"postcodes": sorted({pc for _, pc, _ in todo})})
                for item in r.json().get("result", []):
                    res = item.get("result") or {}
                    if res.get("latitude") is not None:
                        where[item["query"]] = (res["latitude"], res["longitude"])
        except (httpx.HTTPError, ValueError):
            pass
    for s, pc, url in todo:
        _, html = pages.get(url)
        if pc not in where or not html or pc not in R.page_postcodes(html):
            stats["postcode left out: not printed on the page or unknown"] += 1
            continue
        d = R.metres(s["lat"], s["lng"], *where[pc])
        if d > 400:
            stats["postcode left out: more than 400 m from the scheme"] += 1
            continue
        stats["postcode kept"] += 1
        log["postcodes"].append({"scheme": s["id"], "name": s["name"], "postcode": pc, "metres": round(d)})
        if args.apply:
            scheme = db.get(ExistingScheme, s["id"])
            if not (scheme.postcode or "").strip():
                set_field(scheme, "postcode", pc, RENT_SOURCE, db, changed_by="load_scheme_research.py")
    if args.apply:
        db.commit()
    else:
        db.rollback()
    pages.close()
    db.close()
    for k, v in sorted(stats.items(), key=lambda kv: (not kv[0].endswith("kept"), -kv[1])):
        print(f"    {v:>5}  {k}")
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"scheme_research_{stamp}{'' if args.apply else '_dryrun'}.json")
    log["counts"] = dict(stats)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(log, fh, indent=1, default=str)
    print(f"page requests made: {pages.fetched}\nrun log: {path}")


if __name__ == "__main__":
    main()
