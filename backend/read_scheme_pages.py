"""Read operators' own pages for facts missing on named schemes.

  --discover   Find each rent-less build-to-rent scheme's page on its
               operator's website, from the site's sitemap (or, where a
               site has none, from its location and development listings).
               A page is linked only when its address carries the scheme's
               name and its text names the scheme.
  --rents      Read monthly rents off the linked pages.
  --beds       Read a stated bed count off the pages already linked to
               student schemes that have none.
  --opened     Read a stated opening year off the same pages.

Free: plain page requests, robots.txt respected, one request a second per
site, no workaround for sites that block automated requests. Pages are kept
under data/enrichment/pages, so a re-run makes no requests for them.

Nothing is overwritten: values go in only where a scheme has none, and each
is stored with the page it came from and the words it was read from.

Dry run unless --apply.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html as html_mod
import json
import os
import re
import sys
from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.extraction.units import extract_units  # noqa: E402
from app.models.models import ExistingScheme  # noqa: E402
from app.models.observations import record_observation  # noqa: E402
from app.models.rents import record_advertised_rent, record_room_rent  # noqa: E402
from app.registry.core import link_source_record  # noqa: E402
from app.scrapers.field_protection import set_field  # noqa: E402
from app.scrapers.scheme_matching import norm_name, norm_pc  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE_DIR = os.path.join(HERE, "data", "enrichment", "pages")
OUT_DIR = os.path.join(HERE, "data", "enrichment")
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"
NO_RENT = """NOT EXISTS (SELECT 1 FROM scheme_room_rents r WHERE r.scheme_id = es.id AND r.is_current)
         AND NOT EXISTS (SELECT 1 FROM scheme_advertised_rents a WHERE a.scheme_id = es.id AND a.is_current)"""

AGGREGATORS = ("accommodationforstudents.com", "sturents.com", "rightmove.co.uk", "zoopla.co.uk", "onthemarket.com",
               "studentcrowd.com", "unihomes.co.uk", "student.com", "amberstudent.com", "universityliving.com",
               "homeviews.com", "lovetorent.co.uk", "buildtorentdirectory.co.uk", "uhomes.com", "casita.com")
LOC_RE = re.compile(r"<loc>\s*(?:<!\[CDATA\[)?\s*([^<\s\]]+)\s*(?:\]\]>)?\s*</loc>", re.I)
HREF_RE = re.compile(r'<a\b[^>]*href=["\']([^"\'#]+)["\']', re.I)
NOISE_PATH = re.compile(r"/(blog|news|press|article|insight|event|career|job|team|people|privacy|terms|cookie|legal|"
                        r"tag|category|author|wp-content|wp-json|faq|feed|cdn-cgi|login|account|basket|cart|case-stud(y|ies)|"
                        r"research|report|sector|service|consultanc(y|ies)|invest(or|ors|ment|ments)?|media|award)s?(/|$)|"
                        r"\.(pdf|jpg|jpeg|png|webp|svg|zip|mp4)$|"
                        # Old articles that still print years-old prices ("Facilities at Horton House": £60 a
                        # week against £80 on the building's own page) and dated posts.
                        r"/facilities-at-|/20[0-2]\d/[01]\d/", re.I)
HUB_PATH = re.compile(r"develop|location|propert|home|communit|neighbourhood|portfolio|building|apartment|rent|find|"
                      r"search|where|scheme|project|cit(y|ies)|live|residen|collection|our-", re.I)

# £1,250 pcm / per month / a month / p/m / /month / monthly
PCM_RE = re.compile(r"£\s?(\d{1,2},?\d{3}|\d{3})(?:\.\d{2})?\s*(?:pcm\b|p\.c\.m\.?|per\s+(?:calendar\s+)?month|p/m\b|"
                    r"/\s?(?:month|mo)\b|a\s+month|monthly|pm\b)", re.I)
BED_RE = re.compile(r"\b(studio|(?:(\d|one|two|three|four|five)[\s-]*bed(?:room)?s?))\b", re.I)
JSON_PCM_RE = re.compile(r'"[A-Za-z_]*(?:pcm|per_?month|permonth|monthly)[A-Za-z_]*"\s*:\s*"?£?\s?(\d{1,2},?\d{3}|\d{3})'
                         r'(?:\.\d+)?"?', re.I)
WORD_NUM = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
OPENED_RE = re.compile(
    r"\b(open(?:ed|ing|s)|complet(?:ed|ion|ing)|built|launch(?:ed|ing)|welcom(?:ed|ing)\s+(?:its|our|the)?\s*first\s+"
    r"(?:residents|students|tenants)|first\s+(?:residents|students)\s+mov(?:ed|ing)\s+in)\s+(?:in\s+|for\s+|during\s+)?"
    r"(?:(?:january|february|march|april|may|june|july|august|september|october|november|december|spring|summer|"
    r"autumn|winter|early|late|mid)[\s-]+)?(19[89]\d|20[0-3]\d)\b", re.I)


# --------------------------------------------------------------------- pages
class Pages:
    """Polite fetching with a disk cache keyed by address."""

    def __init__(self) -> None:
        from app.scrapers.operator_directory_scraper import OperatorDirectoryScraper
        self.web = OperatorDirectoryScraper(timeout=20.0)
        os.makedirs(PAGE_DIR, exist_ok=True)
        self.index_path = os.path.join(PAGE_DIR, "index.json")
        try:
            with open(self.index_path, encoding="utf-8") as fh:
                self.index: dict[str, int | None] = json.load(fh)
        except (OSError, ValueError):
            self.index = {}
        self.fetched = 0
        self.memory: dict[str, tuple[int | None, str | None]] = {}

    def _file(self, url: str) -> str:
        host = urlparse(url).netloc.lower().replace(":", "_") or "unknown"
        os.makedirs(os.path.join(PAGE_DIR, host), exist_ok=True)
        return os.path.join(PAGE_DIR, host, hashlib.sha1(url.encode("utf-8")).hexdigest() + ".html")

    def get(self, url: str, keep: bool = True) -> tuple[int | None, str | None]:
        """(status, html). status -1 means robots.txt disallows the page."""
        if url in self.memory:
            return self.memory[url]
        if url in self.index:
            status = self.index[url]
            if status == 200 and os.path.exists(self._file(url)):
                with open(self._file(url), encoding="utf-8", errors="replace") as fh:
                    return 200, fh.read()
            if status != 200:
                return status, None
        try:
            status, body = self.web._fetch(url)
        except Exception:                                 # noqa: BLE001
            status, body = None, None
        self.fetched += 1
        if not keep:
            self.memory[url] = (status, body)
            return status, body
        self.index[url] = status
        if status == 200 and body:
            with open(self._file(url), "w", encoding="utf-8", errors="replace") as fh:
                fh.write(body)
        if self.fetched % 25 == 0:
            self.save()
        return status, body if status == 200 else None

    def save(self) -> None:
        # Another run may be writing too: merge with what is on disk before saving.
        try:
            with open(self.index_path, encoding="utf-8") as fh:
                on_disk = json.load(fh)
        except (OSError, ValueError):
            on_disk = {}
        on_disk.update(self.index)
        self.index = on_disk
        tmp = self.index_path + f".{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.index, fh)
        os.replace(tmp, self.index_path)

    def close(self) -> None:
        self.save()
        self.web.client.close()


def page_text(html: str, cap: int = 80000) -> str:
    html = re.sub(r"(?is)<(script|style|svg|noscript)\b[^>]*>.*?</\1>", " ", html)
    txt = html_mod.unescape(re.sub(r"(?s)<[^>]+>", " ", html))
    return re.sub(r"\s+", " ", txt)[:cap]


def domain_of(url: str) -> str:
    return urlparse(url if "://" in url else "https://" + url).netloc.lower().removeprefix("www.")


def is_aggregator(url: str) -> bool:
    d = domain_of(url)
    return any(d == a or d.endswith("." + a) for a in AGGREGATORS)


# --------------------------------------------------------------------- finding a scheme's page
def site_urls(pages: Pages, site: str, cap: int = 6000) -> tuple[set[str], str]:
    """Every page address a site publishes, from its sitemap or else its listing pages."""
    root = f"{urlparse(site).scheme or 'https'}://{urlparse(site).netloc}"
    try:
        seeds = list(pages.web._robots_for(root + "/").site_maps() or [])
    except Exception:                                     # noqa: BLE001
        seeds = []
    seeds += [root + p for p in ("/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml", "/sitemap-index.xml")]
    queue, visited, urls = list(dict.fromkeys(seeds)), set(), set()
    while queue and len(visited) < 40 and len(urls) < cap:
        sm = queue.pop(0)
        if sm in visited:
            continue
        visited.add(sm)
        status, body = pages.get(sm, keep=False)
        if status != 200 or not body or "<loc" not in body.lower():
            continue
        for loc in LOC_RE.findall(body):
            loc = html_mod.unescape(loc.strip())
            leaf = loc.lower().rsplit("/", 1)[-1]
            if leaf.endswith((".xml", ".xml.gz")) or "sitemap" in leaf:
                queue.append(loc)
            elif domain_of(loc) == domain_of(root):
                urls.add(loc.split("#")[0])
    if urls:
        return urls, "sitemap"
    # No sitemap: read the home page and its listing pages, two levels, at most 25 requests.
    seen, frontier, budget = set(), [(site, 0)], 25
    while frontier and budget > 0:
        url, depth = frontier.pop(0)
        if url in seen:
            continue
        seen.add(url)
        status, body = pages.get(url)
        budget -= 1
        if status != 200 or not body:
            continue
        for href in HREF_RE.findall(body):
            link = urljoin(url, html_mod.unescape(href.strip())).split("#")[0].split("?")[0]
            if domain_of(link) != domain_of(root) or NOISE_PATH.search(urlparse(link).path):
                continue
            urls.add(link)
            if depth < 1 and HUB_PATH.search(urlparse(link).path) and link not in seen:
                frontier.append((link, depth + 1))
    return urls, "listing pages" if urls else "nothing found"


def path_tokens(url: str) -> tuple[set[str], str]:
    path = urlparse(url).path.lower()
    return {t for t in re.split(r"[^a-z0-9]+", path) if t}, re.sub(r"[^a-z0-9]", "", path)


def candidates(tokens: set[str], urls: set[str]) -> list[str]:
    """Addresses whose path carries every word of the scheme's name, best first."""
    out = []
    for url in urls:
        if NOISE_PATH.search(urlparse(url).path):
            continue
        toks, flat = path_tokens(url)
        if tokens <= toks or all(len(t) >= 4 and t in flat for t in tokens):
            extra = len(toks - tokens)
            out.append((extra, urlparse(url).path.count("/"), len(url), url))
    return [u for *_, u in sorted(out)]


def names_scheme(html: str, tokens: set[str], postcode: str | None, town: str | None) -> tuple[bool, str]:
    body = page_text(html).lower()
    hits = sum(1 for t in tokens if t in body)
    pc = norm_pc(postcode)
    pc_hit = bool(pc) and (pc.lower() in body.replace(" ", "") or (pc[:-3].lower() + " ") in body)
    town_hit = bool(town) and town.lower() in body
    ok = hits == len(tokens) and (len(tokens) >= 2 or pc_hit or town_hit)
    return ok, f"name words {hits}/{len(tokens)}, postcode {'yes' if pc_hit else 'no'}, town {'yes' if town_hit else 'no'}"


BRAND_FILLER = {"living", "homes", "limited", "ltd", "group", "uk", "plc", "llp", "management", "property", "the",
                "of", "and", "residential", "real", "estate", "developments", "apartments", "by", "at"}


def match_words(name: str, operator: str | None) -> set[str]:
    """The words a page address has to carry for a scheme. The operator's own
    brand does not count ("UNCLE Manchester" on uncle.co.uk has to be found by
    "manchester"), so where the name is only brand and place, the place is used."""
    brand = {t for t in re.sub(r"[^a-z0-9 ]", " ", (operator or "").lower()).split() if t not in BRAND_FILLER}
    words = norm_name(name) - brand
    if words:
        return words
    raw = {t for t in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split() if t not in BRAND_FILLER and len(t) > 2}
    return raw - brand


# --------------------------------------------------------------------- reading a page
def bed_label(raw: str) -> str:
    raw = raw.lower()
    if raw.startswith("studio"):
        return "Studio"
    m = re.match(r"(\d|one|two|three|four|five)", raw)
    n = WORD_NUM.get(m.group(1), None) if m and not m.group(1).isdigit() else int(m.group(1)) if m else None
    return f"{n} bed" if n else "Home"


SEP = r"[\s:,;|\-–—()→>]*"
# "Studio apartments from £900": only filler between the home type and the price.
GAP_BEFORE = re.compile(rf"{SEP}(?:plus)?{SEP}(?:apartments?|homes?|flats?|houses?|suites?|rooms?)?{SEP}(?:rents?|rental|prices?)?"
                        rf"{SEP}(?:start(?:ing|s)?)?{SEP}(?:from|at)?{SEP}", re.I)
# "£1,750 per month for a one bedroom" / "£840 pcm View all : Studio".
GAP_AFTER = re.compile(rf"{SEP}(?:view all)?{SEP}(?:for)?{SEP}(?:a|an|our)?{SEP}", re.I)


def nearest_label(body: str, start: int, end: int) -> str | None:
    """The home type a price belongs to: the one joined to it by filler words
    only. Where a type sits on both sides, the side with a joining word
    ("from" before, "for" or "view all" after) wins, else the one before."""
    before = body[max(0, start - 90):start]
    after = body[end:end + 45]
    prev = list(BED_RE.finditer(before))
    nxt = BED_RE.search(after)
    gap_b = before[prev[-1].end():] if prev else None
    gap_a = after[:nxt.start()] if nxt else None
    tied_b = gap_b is not None and GAP_BEFORE.fullmatch(gap_b) is not None
    tied_a = gap_a is not None and GAP_AFTER.fullmatch(gap_a) is not None
    if tied_b and tied_a:
        strong_b = bool(re.search(r"\b(from|at)\b|:", gap_b, re.I))
        strong_a = bool(re.search(r"\b(for|view all)\b|:", gap_a, re.I))
        tied_b, tied_a = (strong_b or not strong_a), (strong_a and not strong_b)
    if tied_b:
        return bed_label(prev[-1].group(1))
    if tied_a:
        return bed_label(nxt.group(1))
    return None


BARE_TYPED_RE = re.compile(r"£\s?(\d{1,2},?\d{3}|\d{3})(?:\.\d{2})?(?!\s*(?:pw|per\s+week|/\s?week|a\s+week))", re.I)


NOT_RENT = re.compile(r"membership|parking|car park|garage|storage|pet (?:rent|fee)|gym|bike|broadband|deposit", re.I)
NOT_RENT_AFTER = re.compile(r"membership|parking|car park|storage", re.I)


def monthly_rents(html: str) -> list[tuple[str | None, float, str]]:
    """(home type or None, monthly rent, the words it was read from)."""
    body = page_text(html, cap=200000)
    found: list[tuple[str | None, float, str]] = []
    spans = []
    for m in PCM_RE.finditer(body):
        value = float(m.group(1).replace(",", ""))
        if not 450 <= value <= 12000:
            continue
        # A monthly charge that is not rent: club membership, a parking space, storage, a pet.
        if NOT_RENT.search(body[max(0, m.start() - 60):m.start()]) or \
                NOT_RENT_AFTER.search(body[m.end():m.end() + 40]):
            continue
        spans.append((m.start(), m.end()))
        quote = body[max(0, m.start() - 60):m.end() + 30].strip()
        found.append((nearest_label(body, m.start(), m.end()), value, quote))
    if found:
        # The page quotes monthly rents. A bare price sitting right beside a home type
        # ("£1,500 View all : 1 bedroom apartments") is one more of them.
        lo, hi = min(v for _, v, _ in found), max(v for _, v, _ in found)
        for m in BARE_TYPED_RE.finditer(body):
            if any(a <= m.start() < b for a, b in spans):
                continue
            value = float(m.group(1).replace(",", ""))
            if not (0.5 * lo <= value <= 4 * hi and 400 <= value <= 12000):
                continue
            near = BED_RE.search(body[m.end():m.end() + 30])
            if near and not re.search(r"deposit|council tax|bills|voucher|save|off", body[max(0, m.start() - 40):m.start()], re.I):
                found.append((bed_label(near.group(1)), value, body[max(0, m.start() - 40):m.end() + 40].strip()))
    # Figures buried in a page's script data are not read: on a test they included
    # record numbers that only looked like prices.
    if found:
        lo, hi = min(v for _, v, _ in found), max(v for _, v, _ in found)
        if hi / lo > 8:        # a stray figure (a deposit, a house price) is in the mix
            typed = [f for f in found if f[0]]
            found = typed if typed else []
    return found


RENTAL = "es.scheme_type IN ('BTR', 'Co-living') AND es.status = 'operational'"
STUDENT = "es.scheme_type = 'PBSA' AND coalesce(es.status, 'operational') = 'operational'"

# £165 per week / pw / pppw / per person per week / a week / weekly
PPW_RE = re.compile(r"£\s?(\d{2,3})(?:\.(\d{2}))?\s*(?:per\s+person\s+)?(?:per\s+week|pppw\b|ppw\b|pp\s?/\s?pw\b|pw\b|p/w\b|p\.w\.|"
                    r"/\s?(?:week|wk)\b|a\s+week|weekly)", re.I)
PPW_RANGE_RE = re.compile(r"£\s?(\d{2,3})(?:\.(\d{2}))?\s*(?:-|–|—|to)\s*£\s?\d{2,3}(?:\.\d{2})?\s*(?:per\s+person\s+)?"
                          r"(?:per\s+week|pppw\b|ppw\b|pp\s?/\s?pw\b|pw\b|p/w\b|/\s?(?:week|wk)\b|a\s+week|weekly)", re.I)
ROOM_RE = re.compile(r"\b(non[\s-]?en[\s-]?suites?|en[\s-]?suites?|studios?|twodios?|duplex|penthouses?|"
                     r"(?:one|two|three|four|five|six|[1-9])[\s-]*bed(?:room)?s?|shared\s+(?:room|flat|apartment)|"
                     r"cluster\s+flat|standard\s+room)\b", re.I)
NOT_WEEKLY_RENT_BEFORE = re.compile(
    r"(?:deposit|cashback|cash back|voucher|save|saving|savings|discount|parking|storage|membership|reward|"
    r"insurance|allowance)\s*(?:of|is|are|worth|up to|from|:|-)?\s*(?:up to\s*)?$", re.I)


def room_label(raw: str) -> str:
    r = re.sub(r"[\s-]+", " ", raw.lower()).strip()
    if r.startswith("non en"):
        return "Non en-suite"
    if r.startswith("en suite") or r.startswith("ensuite"):
        return "Ensuite"
    if r.rstrip("s") in ("studio", "twodio", "penthouse") or r == "duplex":
        return (r if r == "duplex" else r.rstrip("s")).capitalize()
    if r.startswith(("shared", "cluster", "standard")):
        return r.capitalize()
    m = re.match(r"(\d|one|two|three|four|five|six)", r)
    n = (WORD_NUM.get(m.group(1)) or {"six": 6}.get(m.group(1))) if m and not m.group(1).isdigit() else int(m.group(1)) if m else None
    return f"{n} bed flat" if n else "Room"


BUILDING_NOUNS = {"house", "court", "hall", "halls", "village", "building", "buildings", "place", "point", "works",
                  "tower", "towers", "lodge", "heights", "mill", "mills", "yard", "quarter", "square", "gardens",
                  "residence", "residences", "street", "road", "lane", "way", "walk", "gate", "row", "wharf", "quay"}
FROM_BEFORE = re.compile(r"\b(?:from|starting at|start(?:s|ing)? from)\s*(?:just|only)?\s*$", re.I)
NAMED_REACH = 90


GENERIC_WORDS = {"student", "students", "the", "of", "and", "at"}
WORD_RE = re.compile(r"[A-Za-z0-9']+")


def names_another_building(raw: str, start: int, own_tokens: set[str]) -> bool:
    """Do these words, from `start` on, name a building or street that is not the scheme's own? A
    capitalised building word ("House", "Street") names one. It is the scheme's own only when both
    that word and the word before it are in the scheme's name or address: "Don Gratton House" is
    not "Lilian Knowles House", and "Moor Lane Halls" is not "The Moor"."""
    words = [(m.start(), m.group()) for m in WORD_RE.finditer(raw)]
    for i, (pos, w) in enumerate(words):
        if pos < start or w.lower() not in BUILDING_NOUNS or not w[0].isupper():
            continue
        j = i - 1
        while j >= 0 and words[j][1].lower() in GENERIC_WORDS:
            j -= 1
        if j < 0:
            continue
        if w.lower() not in own_tokens or words[j][1].lower() not in own_tokens:
            return True
    return False


def said_of(raw: str, name_words: list[str] | None, own_tokens: set[str]) -> int | None:
    """How far before a price the scheme's name stands, when the price is said of the scheme: every
    word of its name is there, close by, and no other building is named in between. None otherwise.
    `raw` is the text leading up to the price, as printed."""
    low = plain(raw)
    if len(low) != len(raw):
        raw = low.upper()               # cannot line the two up: treat every word as capitalised
    if not name_words or not all(w in low for w in name_words):
        return None
    end = max(low.rfind(w) + len(w) for w in name_words)
    if len(low) - end > NAMED_REACH or names_another_building(raw, end, own_tokens):
        return None
    return len(low) - end


def not_rent_before(before: str) -> bool:
    """Do the words right before a weekly figure make it something other than rent? In
    "£300 deposit £180 per week" the deposit already has its own amount, so the £180 is a rent."""
    hit = NOT_WEEKLY_RENT_BEFORE.search(before)
    return bool(hit) and not re.search(r"£\s?[\d,.]+\s*$", before[:hit.start()])


STRUCTURED_PRICE_RE = re.compile(r'"priceRange"\s*:\s*"([^"]{0,60})"')


def price_key(body: str, m: re.Match) -> str:
    """A price with the words leading up to it: the same key on two buildings' pages is shared
    furniture (a menu, a banner, a card for a neighbour), not either building's price."""
    return plain(body[max(0, m.start() - 50):m.end()])


def price_keys(html: str) -> set[str]:
    body = page_text(html, cap=200000)
    return {price_key(body, m) for rx in (PPW_RANGE_RE, PPW_RE) for m in rx.finditer(body)} | \
        {"pricerange " + plain(raw) for raw in STRUCTURED_PRICE_RE.findall(html)}


def weekly_rents(html: str, name_words: list[str] | None = None, name: str | None = None,
                 skip: set[str] | None = None) -> list[tuple[str | None, float, str]]:
    """(room type or None, weekly rent, the words it was read from) for a student building's page.
    `name_words` and `name` (its name, with its address where known) are the scheme's own, used to
    tell its price from a neighbour's; `skip` holds the prices that also stand on other buildings'
    pages of the same site."""
    body = page_text(html, cap=200000)
    own_tokens = set(re.findall(r"[a-z]+", plain(name or ""))) | set(name_words or [])
    # Each reading: [room type, rent, words, kind]. Kinds: "range", "plain" (a bare price),
    # "from" (a "from" price not said of this scheme) and "named" (a "from" price said of it).
    found: list[list] = []
    named: list[tuple[float, int]] = []
    # Every price in page order. "£138 - £264 per week" is read twice: as a range (its first figure;
    # the unit follows the second figure only) and as the single price its second figure makes.
    matches = sorted([(m.start(), 0, m) for m in PPW_RANGE_RE.finditer(body)] +
                     [(m.start(), 1, m) for m in PPW_RE.finditer(body)], key=lambda t: (t[0], t[1]))
    prev_end = range_end = range_lead_start = 0
    for _, single, m in matches:
        # The words leading up to a price run from the price before it; the second figure of a
        # range shares the range's.
        lead_start = range_lead_start if m.start() < range_end else prev_end
        if not single:
            range_end, range_lead_start = m.end(), lead_start
        prev_end = max(prev_end, m.end())
        value = float(m.group(1) + ("." + m.group(2) if m.group(2) else ""))
        if not 60 <= value <= 900 or (skip and price_key(body, m) in skip):
            continue
        # A weekly figure that is not rent: money off, a deposit, cashback, a parking space.
        # Only the words right against the figure count: "Save £20 per week" is a discount, but a rent
        # that merely follows a sentence about a deposit is still a rent.
        if not_rent_before(body[max(0, m.start() - 40):m.start()]) or \
                re.search(r"^\s*(?:off|discount|cashback|saving)", body[m.end():m.end() + 20], re.I):
            continue
        lead = body[max(lead_start, m.start() - 400):m.start()]
        if name_words and names_another_building(lead, 0, own_tokens) \
                and not all(w in plain(lead) for w in name_words):
            # A list of several buildings' rooms: this price stands under another building's name.
            continue
        words = body[max(0, m.start() - 60):m.end() + 30].strip()
        if not single:
            found.append([None, value, words, "range"])
            continue
        before = body[max(0, m.start() - 80):m.start()]
        after = body[m.end():m.end() + 70]
        # "Classic En-Suite, Tudor Studios, from £110": a room word in the scheme's own name is not a room type.
        prev = [p for p in ROOM_RE.finditer(before) if p.group(1).lower() not in own_tokens]
        nxt = ROOM_RE.search(after[:40])
        bracket = re.match(r"\s*\(([^)]{0,60})\)", after)
        label = None
        if bracket and ROOM_RE.search(bracket.group(1)):
            # "£198 per week (single occupancy en suite room); £223 per week (studio)": the type follows.
            label = room_label(ROOM_RE.search(bracket.group(1)).group(1))
        elif prev and GAP_BEFORE.fullmatch(before[prev[-1].end():]):
            label = room_label(prev[-1].group(1))
        elif nxt and GAP_AFTER.fullmatch(after[:nxt.start()]):
            label = room_label(nxt.group(1))
        kind = "plain"
        if FROM_BEFORE.search(before):
            # A "from" price is the building's headline price, a line of its price list, or a teaser
            # for another building. It is this scheme's when the scheme is the last building named
            # before it, with no other price in between.
            lead = lead[-250:]
            gap = said_of(lead, name_words, own_tokens)
            if gap is not None:
                kind = "named"
                named.append((value, gap))
            elif names_another_building(lead, max(0, len(lead) - NAMED_REACH), own_tokens):
                continue                # said of another building, named just before it: a card for a neighbour
            else:
                kind = "from"
        found.append([label, value, words, kind])
    # The page's structured description of itself: "priceRange": "From £165PW".
    for raw in STRUCTURED_PRICE_RE.findall(html):
        m = PPW_RE.search(html_mod.unescape(raw))
        if m and not (skip and "pricerange " + plain(raw) in skip):
            value = float(m.group(1) + ("." + m.group(2) if m.group(2) else ""))
            if 60 <= value <= 900 and not any(f[1] == value for f in found):
                found.append([None, value, f'"priceRange": "{raw}"', "from"])
    if not found:
        return []
    # Pages also carry teaser cards for the operator's other buildings ("Arkwright House from £75")
    # and one-off figures. The page's own "from" price is the anchor: the lowest one said of the
    # scheme, or the only untyped one on the page. Everything is then kept only within reach of
    # it. With several untyped "from" prices and none said of the scheme, or no "from" price and a
    # wide spread, the page cannot be read safely and nothing is taken.
    untyped = sorted({f[1] for f in found if f[3] == "from" and f[0] is None})
    if named:
        anchor = min(v for v, _ in named)       # the lowest price said of the scheme is its "from" price
        found = [f for f in found if not (f[3] == "from" and f[0] is None)]     # the other buildings' teasers
    elif len(untyped) == 1:
        anchor = untyped[0]
        typed_values = [f[1] for f in found if f[0]]
        if name_words and typed_values and anchor < 0.8 * min(typed_values):
            # A lone, cheaper "from" price not said of this scheme, on a page that prices its own
            # rooms by type: a teaser for another building. Read the typed prices instead.
            found = [f for f in found if f[0] or f[1] != anchor]
            anchor = min(typed_values)
    elif untyped:
        return []
    else:
        anchor = None
    if anchor is not None:
        found = [f for f in found if anchor <= f[1] <= (3.0 if f[0] else 2.5) * anchor]
    elif found:
        lo, hi = min(f[1] for f in found), max(f[1] for f in found)
        if hi / lo > 2.5:
            found = [f for f in found if f[0]]
    return [(f[0], f[1], f[2]) for f in found]


def plain(s: str) -> str:
    """Lower case with accents removed, so 'Haüs' reads as 'haus'."""
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c)).lower()


def opening_year(html: str, today_year: int, name_words: set[str] | None = None) -> tuple[int, str] | None:
    """A stated opening year. With `name_words`, only a statement that names the
    scheme counts: operator pages also carry blurbs about their other buildings."""
    body = page_text(html, cap=120000)
    years = Counter()
    quotes: dict[int, str] = {}
    for m in OPENED_RE.finditer(body):
        y = int(m.group(2))
        if 1985 <= y <= today_year + 3:
            quote = body[max(0, m.start() - 80):m.end() + 40].strip()
            if name_words is not None:
                # Every word of the scheme's name has to be in the statement: "Newton Court ...
                # opened in 2018" on Marshall Court's page is about Newton Court. A statement
                # about an office, shop or hotel in the same development is not the homes.
                near = plain(quote)
                words = [w for w in name_words if len(w) >= 4] or list(name_words)
                if not words or not all(w in near for w in words):
                    continue
                if re.search(r"\b(office|retail|hotel|commercial|school|station)\b", near):
                    continue
            years[y] += 1
            quotes.setdefault(y, quote)
    if not years:
        return None
    if len(years) > 1 and years.most_common(2)[0][1] == years.most_common(2)[1][1]:
        return None            # two different years equally stated: a page about several buildings
    y = years.most_common(1)[0][0]
    return y, quotes[y]


# --------------------------------------------------------------------- stages
def stage_discover(db, pages: Pages, apply: bool, limit: int | None, log: dict, student: bool = False) -> None:
    kind = STUDENT if student else RENTAL
    rows = db.execute(text(f"""
        SELECT es.id, es.name, es.postcode, coalesce(cc.name, '') AS town, co.id AS op_id, co.name AS operator, co.website
        FROM existing_schemes es JOIN companies co ON co.id = es.operator_company_id
        LEFT JOIN councils cc ON cc.id = es.council_id
        WHERE {NAMED} AND {kind} AND {NO_RENT}
          AND nullif(trim(co.website), '') IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM scheme_source_records sr WHERE sr.scheme_id = es.id
                          AND sr.source = 'operator_page' AND sr.status = 'matched')
        ORDER BY co.id, es.id""")).mappings().all()
    by_op: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        by_op[r["op_id"]].append(dict(r))
    ops = sorted(by_op.items(), key=lambda kv: -len(kv[1]))[:limit]
    print(f"rent-less {'student' if student else 'build-to-rent'} schemes whose operator has a website: "
          f"{len(rows)} across {len(by_op)} operators")
    stats = Counter()
    for op_id, schemes in ops:
        site = schemes[0]["website"].strip()
        site = site if "://" in site else "https://" + site
        urls, how = site_urls(pages, site)
        linked = 0
        found: list[dict] = []
        for s in schemes:
            tokens = match_words(s["name"], s["operator"])
            if not tokens or (len(tokens) == 1 and len(next(iter(tokens))) < 4):
                stats["name too short to match safely"] += 1
                continue
            cands = candidates(tokens, urls)
            if not cands:
                stats["no page with the scheme's name"] += 1
                continue
            done = False
            for url in cands[:3]:
                status, html = pages.get(url)
                if status != 200 or not html:
                    continue
                ok, why = names_scheme(html, tokens, s["postcode"], s["town"])
                if ok:
                    done = True
                    found.append({"scheme": s["id"], "name": s["name"], "url": url, "check": why,
                                  "more": [u for u in cands if u != url][:5], "tokens": sorted(tokens),
                                  "postcode": s["postcode"], "town": s["town"], "operator": s["operator"],
                                  "placed": "postcode yes" in why or "town yes" in why})
                    break
            if not done:
                stats["page found but it does not name the scheme"] += 1
        # One page claimed by two schemes: keep only the ones the page places (postcode or town).
        claims = Counter(f["url"] for f in found)
        for f in found:
            if claims[f["url"]] > 1 and not f["placed"]:
                stats["page shared with another scheme and not placed by postcode or town"] += 1
                continue
            stats["page linked"] += 1
            linked += 1
            log["links"].append(f)
            if apply:
                link_source_record(db, "operator_page", f["url"], scheme_id=f["scheme"], raw_name=f["name"],
                                   raw_postcode=f["postcode"], raw_operator=f["operator"],
                                   match_method="sitemap_slug", match_score=1.0, status="matched")
        print(f"  {schemes[0]['operator'][:30]:30s} {how:14s} {len(urls):>5} pages  {linked}/{len(schemes)} schemes linked",
              flush=True)
        if apply:
            db.commit()
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    log["counts"].update({f"discover: {k}": v for k, v in stats.items()})


HEADLINE_RE = re.compile(r"<(title|h1)\b[^>]*>(.*?)</\1>", re.I | re.S)
HEAD_FILLER = {"street", "road", "lane", "avenue", "house", "court", "hall", "halls", "student", "students",
               "accommodation", "village", "place", "studios", "studio", "apartments", "building", "campus", "city"}


def headline_names(html: str, name: str, operator: str | None = None) -> bool:
    """Is this the scheme's own page: is it headed (its title or first main heading) with the
    scheme's name? The operator's own words do not count, or every page of "York St John
    University" would pass for a hall called "St John Street"; and only the first heading is
    read, because a city page carries one heading per building it lists."""
    heads, seen_h1 = [], False
    for m in HEADLINE_RE.finditer(html[:400000]):
        if m.group(1).lower() == "h1":
            if seen_h1:
                continue
            seen_h1 = True
        heads.append(re.sub(r"<[^>]+>", " ", m.group(2)))
    head = plain(html_mod.unescape(" ".join(heads)))
    head_joined = re.sub(r"[^a-z0-9]", "", head)
    words = [plain(w) for w in match_words(name, operator) if len(w) >= 3]
    # Once the operator's words are out, something of the scheme's own has to be left to look for.
    if not [w for w in words if w not in HEAD_FILLER]:
        return False
    hits = sum(1 for w in words if w in head or w in head_joined)
    return hits == len(words) or (len(words) >= 3 and hits >= len(words) - 1)


def address_names(url: str, name: str, operator: str | None = None) -> bool:
    """Does the page's address carry a word of the scheme's own name?"""
    path = re.sub(r"[^a-z0-9]", "", plain(urlparse(url).path))
    words = [plain(w) for w in match_words(name, operator) if len(w) >= 4 and plain(w) not in HEAD_FILLER]
    return any(re.sub(r"[^a-z0-9]", "", w) in path for w in words)


def other_house_number(url: str, address: str | None) -> bool:
    """Is the page for another door on the same street: its address opens with a house number
    ("/134-north-sherwood-street/") that the scheme's own address ("138 - 142 North Sherwood St")
    does not cover?"""
    last = [p for p in urlparse(url).path.split("/") if p][-1:] or [""]
    m = re.match(r"(\d{1,4})[-_]([a-z]+)", last[0].lower())
    if not m or m.group(2) in ("bed", "bedroom", "beds", "bedrooms", "person", "room", "rooms"):
        return False
    lead = re.match(r"\s*(\d{1,4})\s*(?:-|–|to)?\s*(\d{1,4})?", address or "")
    if not lead:
        return False
    low = int(lead.group(1))
    high = int(lead.group(2)) if lead.group(2) else low
    return not min(low, high) <= int(m.group(1)) <= max(low, high)


def scheme_urls(db, where: str) -> dict[int, dict]:
    """Schemes matching `where` with every page linked to them, the operator's own first."""
    rows = db.execute(text(f"""
        SELECT es.id, es.name, es.postcode, es.scheme_type, u.src, u.url,
               (SELECT co.name FROM companies co WHERE co.id = es.operator_company_id) AS operator, es.address
        FROM existing_schemes es
        CROSS JOIN LATERAL (
            SELECT es.source src, es.source_reference url
            UNION ALL SELECT sr.source, sr.source_reference FROM scheme_source_records sr
                      WHERE sr.scheme_id = es.id AND sr.status = 'matched'
            UNION ALL SELECT r.source, r.source_reference FROM scheme_room_rents r WHERE r.scheme_id = es.id
            UNION ALL SELECT a.source, a.source_reference FROM scheme_advertised_rents a WHERE a.scheme_id = es.id) u
        WHERE {NAMED} AND {where} AND u.url LIKE 'http%'""")).fetchall()
    out: dict[int, dict] = {}
    for sid, name, pc, stype, src, url, operator, address in rows:
        s = out.setdefault(sid, {"id": sid, "name": name, "postcode": pc, "type": stype, "operator": operator,
                                 "address": address, "urls": []})
        if (src, url) not in s["urls"]:
            s["urls"].append((src, url))
    for s in out.values():
        s["urls"].sort(key=lambda su: (is_aggregator(su[1]), su[0] != "operator_page", len(su[1])))
    return out


def stage_rents(db, pages: Pages, apply: bool, limit: int | None, log: dict, student: bool = False,
                skip_ids: set[int] | None = None) -> None:
    schemes = scheme_urls(db, f"{STUDENT if student else RENTAL} AND {NO_RENT}")
    read = weekly_rents if student else monthly_rents
    unit = "rent_ppw" if student else "rent_pcm"
    for link in log["links"]:        # pages found in this run (a dry run has not stored them)
        s = schemes.setdefault(link["scheme"], {"id": link["scheme"], "name": link["name"], "postcode": None,
                                                "type": "BTR", "urls": []})
        if ("operator_page", link["url"]) not in s["urls"]:
            s["urls"].insert(0, ("operator_page", link["url"]))
        s["more"] = link.get("more", [])
    todo = [s for s in schemes.values() if s["id"] not in (skip_ids or set())][:limit]
    print(f"rent-less {'student' if student else 'build-to-rent'} schemes with a linked page: {len(todo)}")
    stats = Counter()
    results = []

    def to_try(s: dict) -> list[str]:
        return [u for _, u in s["urls"][:3] if not is_aggregator(u)] + list(s.get("more", []))[:2]

    # A price that stands, with the same words before it, on two buildings' pages of one site is
    # shared furniture: a menu of every building, a banner, a card for a neighbour. Find those first.
    shared_keys: dict[str, set[str]] = {}
    if student:
        on_pages: dict[str, Counter] = defaultdict(Counter)
        counted: set[str] = set()
        for s in todo:
            for url in to_try(s):
                if url.rstrip("/") in counted or not address_names(url, s["name"], s.get("operator")):
                    continue
                counted.add(url.rstrip("/"))
                status, html = pages.get(url)
                if status == 200 and html and headline_names(html, s["name"], s.get("operator")):
                    on_pages[domain_of(url)].update(price_keys(html))
        shared_keys = {d: {k for k, n in c.items() if n > 1} for d, c in on_pages.items()}
    for s in todo:
        got = None
        for url in to_try(s):
            if student and not address_names(url, s["name"], s.get("operator")):
                # A city page or a fees table lists several buildings' prices. A scheme's own page
                # carries its name in its address.
                stats["page address does not carry the scheme's name"] += 1
                continue
            if student and other_house_number(url, s.get("address")):
                stats["page is for another house number on the street"] += 1
                log.setdefault("other_house_number", []).append([s["id"], s["name"], s.get("address"), url])
                continue
            status, html = pages.get(url)
            if status != 200 or not html:
                stats[{-1: "robots.txt disallows", 403: "site blocks automated requests"}.get(status, "page not readable")] += 1
                continue
            if student and not headline_names(html, s["name"], s.get("operator")):
                stats["page is not headed with the scheme's name"] += 1
                continue
            if student:
                own = [plain(w) for w in match_words(s["name"], s.get("operator")) if len(w) >= 3]
                rents = weekly_rents(html, own, f"{s['name']} {s.get('address') or ''}",
                                     shared_keys.get(domain_of(url)))
            else:
                rents = read(html)
            if rents:
                got = (url, rents)
                break
        if not got:
            stats["no rent printed on the page"] += 1
            continue
        url, rents = got
        lo, hi = min(v for _, v, _ in rents), max(v for _, v, _ in rents)
        by_type: dict[str, float] = {}
        for label, value, _ in rents:
            if label and label != "Home":
                by_type[label] = min(value, by_type.get(label, value))
        results.append({"scheme": s["id"], "name": s["name"], "url": url, "from": lo, "to": hi,
                        "by_type": by_type, "quote": rents[0][2][:160],
                        "readings": [[label, value, words[:150]] for label, value, words in rents]})
    # A promotion banner repeated across a site reads as the same price range on different schemes'
    # pages. A range shared like that is the banner's, not either scheme's.
    shared = Counter((domain_of(r["url"]), r["from"], r["to"]) for r in {r["url"]: r for r in results}.values()
                     if r["to"] > r["from"])
    for r in results:
        if shared[(domain_of(r["url"]), r["from"], r["to"])] > 1:
            stats["same price range on several schemes' pages (a site banner)"] += 1
            continue
        s = {"id": r["scheme"], "name": r["name"]}
        url, lo, hi, by_type = r["url"], r["from"], r["to"], r["by_type"]
        stats["rent read"] += 1
        stats["of which with prices by home type"] += 1 if by_type else 0
        log["rents"].append(r)
        if apply:
            record_advertised_rent(db, scheme_id=s["id"], source="operator_page", kind="from", source_reference=url,
                                   **{unit: lo})
            if hi > lo:
                record_advertised_rent(db, scheme_id=s["id"], source="operator_page", kind="to", source_reference=url,
                                       **{unit: hi})
            for label, value in by_type.items():
                record_room_rent(db, scheme_id=s["id"], source="operator_page", room_type=label, source_reference=url,
                                 **{unit: value})
            db.commit()
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    log["counts"].update({f"rents: {k}": v for k, v in stats.items()})


def stage_beds(db, pages: Pages, apply: bool, limit: int | None, log: dict, min_conf: float) -> None:
    schemes = scheme_urls(db, f"es.scheme_type = 'PBSA' AND {SIZE} IS NULL")
    todo = list(schemes.values())[:limit]
    print(f"student schemes with no bed count and a linked page: {len(todo)}")
    stats = Counter()
    for s in todo:
        best = None
        for src, url in s["urls"][:3]:
            status, html = pages.get(url)
            if status != 200 or not html:
                stats[{-1: "robots.txt disallows", 403: "site blocks automated requests"}.get(status, "page not readable")] += 1
                continue
            fig = extract_units(page_text(html, cap=60000)).best("beds")
            if fig and fig.is_additional is False and 20 <= fig.value <= 5000:
                if best is None or fig.confidence > best[2].confidence:
                    best = (src, url, fig)
                if fig.confidence >= min_conf:
                    break
        if best is None:
            stats["no bed count stated on the page"] += 1
            continue
        src, url, fig = best
        firm = fig.confidence >= min_conf
        stats["bed count read (firm statement)" if firm else "bed count mentioned loosely (kept as a note only)"] += 1
        label = "operator_page" if not is_aggregator(url) else (src if src in ("afs_directory", "sturents") else "afs_directory")
        log["beds"].append({"scheme": s["id"], "name": s["name"], "url": url, "beds": fig.value,
                            "confidence": round(fig.confidence, 2), "quote": (fig.quote or "")[:160], "firm": firm})
        if apply:
            scheme = db.get(ExistingScheme, s["id"])
            record_observation(db, scheme, "beds_total", fig.value, label, reference=url, confidence=fig.confidence,
                               quote=fig.quote, extractor="regex", refresh_cache=firm)
            if firm and scheme.beds_total is None:
                set_field(scheme, "beds_total", fig.value, label, db, changed_by="read_scheme_pages.py")
            db.commit()
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    log["counts"].update({f"beds: {k}": v for k, v in stats.items()})


def stage_opened(db, pages: Pages, apply: bool, limit: int | None, log: dict) -> None:
    schemes = scheme_urls(db, "es.completion_date IS NULL AND es.build_year IS NULL "
                              "AND coalesce(es.status, 'operational') = 'operational'")
    todo = list(schemes.values())[:limit]
    print(f"operating schemes with no opening year and a linked page: {len(todo)}")
    stats = Counter()
    this_year = dt.date.today().year
    for s in todo:
        got = None
        for src, url in s["urls"][:3]:
            # Only a page of the scheme's own: a register or city page lists many buildings and
            # states no opening year, so it is not fetched at all.
            if is_aggregator(url) or "nationalcode.org" in url or not address_names(url, s["name"], s.get("operator")):
                continue
            status, html = pages.get(url)
            if status != 200 or not html:
                continue
            found = opening_year(html, this_year, {plain(w) for w in norm_name(s["name"])})
            if found:
                got = (url, found)
                break
        if not got:
            stats["no opening year stated"] += 1
            continue
        url, (year, quote) = got
        if year > this_year:
            stats["page gives a future opening date (skipped)"] += 1
            continue
        stats["opening year read"] += 1
        log["opened"].append({"scheme": s["id"], "name": s["name"], "url": url, "year": year, "quote": quote[:160]})
        if apply:
            scheme = db.get(ExistingScheme, s["id"])
            record_observation(db, scheme, "build_year", year, "operator_page", reference=url, confidence=0.8,
                               quote=quote, extractor="regex")
            if scheme.build_year is None:
                set_field(scheme, "build_year", year, "operator_page", db, changed_by="read_scheme_pages.py")
            db.commit()
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    log["counts"].update({f"opened: {k}": v for k, v in stats.items()})


PAGE_PC_RE = re.compile(r"\b([A-Z]{1,2}[0-9][0-9A-Z]?)\s?([0-9][A-Z]{2})\b")
JSONLD_PC_RE = re.compile(r'"postalCode"\s*:\s*"([^"]{5,9})"')


def page_postcodes(html: str) -> list[str]:
    """Postcodes printed on a page: those in its structured address first, then the rest by frequency."""
    out: list[str] = []
    for raw in JSONLD_PC_RE.findall(html):
        m = PAGE_PC_RE.search(raw.upper())
        if m:
            out.append(f"{m.group(1)} {m.group(2)}")
    counts = Counter(f"{a} {b}" for a, b in PAGE_PC_RE.findall(page_text(html, cap=200000).upper()))
    out += [pc for pc, _ in counts.most_common(8)]
    return list(dict.fromkeys(out))


def metres(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    import math
    p = math.pi / 180
    return 6371000 * math.hypot((lng2 - lng1) * p * math.cos((lat1 + lat2) / 2 * p), (lat2 - lat1) * p)


def stage_postcodes(db, pages: Pages, apply: bool, limit: int | None, log: dict) -> None:
    """A scheme placed on the map only to its street, with a page of its own: take the postcode
    printed on that page, provided the postcode really is where the scheme is (within 400 m).
    The distance test is what keeps an operator's head-office postcode out."""
    import httpx
    schemes = scheme_urls(db, "es.scheme_type = 'PBSA' AND nullif(trim(es.postcode), '') IS NULL AND es.lat IS NOT NULL")
    where = {r[0]: (r[1], r[2]) for r in db.execute(text(
        "SELECT id, lat, lng FROM existing_schemes WHERE id = ANY(:ids)"), {"ids": list(schemes)})}
    todo = list(schemes.values())[:limit]
    print(f"student schemes on the map with no postcode and a linked page: {len(todo)}")
    cands: dict[int, list[str]] = {}
    site_of: dict[int, str] = {}
    for s in todo:
        for _, url in [su for su in s["urls"] if not is_aggregator(su[1])][:3]:
            # Only the scheme's own page: a city page prints its neighbours' postcodes too.
            if not address_names(url, s["name"], s.get("operator")) or other_house_number(url, s.get("address")):
                continue
            status, html = pages.get(url)
            if status == 200 and html and headline_names(html, s["name"], s.get("operator")):
                cands[s["id"]] = page_postcodes(html)
                if cands[s["id"]]:
                    site_of[s["id"]] = domain_of(url)
                    break
    # A postcode printed on two schemes' pages of one site is the operator's office, in the footer.
    printed = Counter((site_of[sid], pc) for sid, pcs in cands.items() if sid in site_of for pc in set(pcs))
    cands = {sid: [pc for pc in pcs if printed[(site_of.get(sid), pc)] < 2] for sid, pcs in cands.items()}
    wanted = sorted({pc for pcs in cands.values() for pc in pcs})
    located: dict[str, tuple[float, float]] = {}
    with httpx.Client(timeout=30.0) as client:
        for i in range(0, len(wanted), 100):
            try:
                r = client.post("https://api.postcodes.io/postcodes", json={"postcodes": wanted[i:i + 100]})
                r.raise_for_status()
            except httpx.HTTPError:
                continue
            for item in r.json().get("result", []):
                res = item.get("result") or {}
                if res.get("latitude") is not None:
                    located[item["query"]] = (res["latitude"], res["longitude"])
    stats = Counter()
    for s in todo:
        lat, lng = where[s["id"]]
        near = sorted((metres(lat, lng, *located[pc]), pc) for pc in cands.get(s["id"], []) if pc in located)
        near = [(d, pc) for d, pc in near if d <= 400]
        if not near:
            stats["no postcode on the page that is at the scheme"] += 1
            continue
        if len(near) > 1 and near[1][0] - near[0][0] < 60:
            stats["two postcodes equally close (left)"] += 1
            continue
        d, pc = near[0]
        stats["postcode read from the scheme's page"] += 1
        log.setdefault("postcodes", []).append({"scheme": s["id"], "name": s["name"], "postcode": pc, "metres": round(d)})
        if apply:
            scheme = db.get(ExistingScheme, s["id"])
            if not (scheme.postcode or "").strip():
                set_field(scheme, "postcode", pc, "operator_page", db, changed_by="read_scheme_pages.py")
    if apply:
        db.commit()
    for k, v in stats.most_common():
        print(f"    {v:>5}  {k}")
    log["counts"].update({f"postcodes: {k}": v for k, v in stats.items()})


def main() -> None:
    ap = argparse.ArgumentParser()
    for flag in ("--discover", "--rents", "--beds", "--opened", "--postcodes", "--student", "--apply"):
        ap.add_argument(flag, action="store_true")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--min-bed-confidence", type=float, default=0.75)
    ap.add_argument("--skip-schemes", help="scheme ids to leave out of the rents stage, after a review of the dry run")
    args = ap.parse_args()
    if not (args.discover or args.rents or args.beds or args.opened or args.postcodes):
        ap.error("pick at least one stage")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    db = SessionLocal()
    pages = Pages()
    log: dict = {"applied": args.apply, "counts": {}, "links": [], "rents": [], "beds": [], "opened": []}
    try:
        if args.discover:
            print("\n== finding scheme pages on operator websites")
            stage_discover(db, pages, args.apply, args.limit, log, args.student)
        if args.rents:
            print("\n== monthly rents")
            skip_ids = {int(x) for x in (args.skip_schemes or "").replace(",", " ").split()}
            log["skipped_by_hand"] = sorted(skip_ids)
            stage_rents(db, pages, args.apply, args.limit, log, args.student, skip_ids)
        if args.beds:
            print("\n== bed counts")
            stage_beds(db, pages, args.apply, args.limit, log, args.min_bed_confidence)
        if args.opened:
            print("\n== opening years")
            stage_opened(db, pages, args.apply, args.limit, log)
        if args.postcodes:
            print("\n== postcodes from scheme pages")
            stage_postcodes(db, pages, args.apply, args.limit, log)
        if args.apply:
            db.commit()
        else:
            db.rollback()
    finally:
        pages.close()
        db.close()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    stages = "_".join(s for s in ("discover", "rents", "beds", "opened", "postcodes") if getattr(args, s))
    path = os.path.join(OUT_DIR, f"pages_{stages}_{stamp}{'' if args.apply else '_dryrun'}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(log, fh, indent=1, default=str)
    print(f"\npage requests made: {pages.fetched}\nrun log: {path}")


if __name__ == "__main__":
    main()
