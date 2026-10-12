"""One unit extractor for every asset type.

Planning descriptions and operator pages state capacity in many shapes:
"540 bedspaces", "up to 1,850 units of residential accommodation",
"part 5, part 9 storey", "student and/or co-living accommodation (864
beds)", "housing over 200 students". Each figure returned here is typed
(beds, homes, storeys...), flagged when it is an envelope ("up to") or
sits inside an either/or permission, and carries a confidence so callers
can decide what is published, what is cached and what goes to review.

    ex = extract_units(description)
    ex.beds          # committed student bed count, or None
    ex.beds_max      # "up to N" envelope, or None
    ex.either_or     # the permission offers alternative uses
    ex.best("homes") # the UnitFigure behind a number, with quote and confidence

Nothing here touches a database; `as_rows` shapes figures for the
unit_extractions table.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

KINDS = ("beds", "homes", "storeys", "studios", "cluster_flats", "coliving_rooms",
         "hotel_rooms", "care_beds", "hmo_beds", "rooms", "units")

# Plausible ranges per kind; anything outside is noise (a 4-bedroom
# extension, a 9,000-bed masterplan, a "2-bed flat" size).
PLAUSIBLE = {
    "beds": (10, 4000), "studios": (5, 3000), "cluster_flats": (2, 1000),
    "coliving_rooms": (5, 3000), "hotel_rooms": (5, 3000), "care_beds": (5, 1000),
    "hmo_beds": (3, 200), "rooms": (5, 3000), "units": (1, 10000),
    "homes": (1, 10000), "storeys": (1, 80),
}

_WORDS = {"single": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
          "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
          "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
          "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50}
_NUM = r"\d{1,3}(?:,\d{3})+|\d{1,5}"
_WORD = "|".join(_WORDS)
_MAX = (r"(?:up\s+to|(?:a\s+)?max(?:imum)?\.?\s+of|not\s+(?:more|greater)\s+than|"
        r"no\s+more\s+than)\s+(?:a\s+maximum\s+of\s+)?")
_APPROX = (r"(?:circa|c\.|approx(?:imately|\.)?|around|about|some|over|more\s+than|"
           r"in\s+excess\s+of|nearly|almost)\s+")
# A qualified number: optional envelope/approximation prefix, digits or a
# word, then "no."/"x" and a hyphen or space ("236no. bedspaces", "540-bed").
_NUMQ = (rf"(?P<max>{_MAX})?(?P<approx>{_APPROX})?(?:(?P<n>{_NUM})|(?P<w>\b(?:{_WORD})\b))"
         r"\s*(?:no\.?\s*|x\s*)?[-\s]?")
_ADJ = (r"(?:(?:new|additional|further|residential|affordable|private|market|attached|detached|"
        r"semi-detached|terraced|self[-\s]contained|build[-\s]to[-\s]rent|btr|co-?living|"
        r"purpose[-\s]built|en[-\s]?suite|ensuite|non[-\s]en[-\s]?suite|shared|accessible)\s+)*")

# Figure patterns, most specific first. Overlapping matches on the same
# number are resolved by confidence, so a phrase may hit several.
_PATTERNS: list[tuple[str, re.Pattern, float]] = [
    ("variation", re.compile(
        r"(?:from\s+)?(?P<a>\d{2,4})\s+to\s+(?P<n>\d{2,4})\s+(?:student\s+)?"
        r"bed(?:\s?spaces?|rooms?|s)?\b", re.I), 0.9),
    ("beds", re.compile(
        _NUMQ + _ADJ + r"(?P<noun>(?:student\s+|study\s+)?(?:bed(?:\s?spaces?|-?spaces?|rooms?|s)?))\b", re.I), 0.0),
    ("student_units", re.compile(
        _NUMQ + _ADJ + r"(?:(?:units?|rooms?|apartments?|flats?|studios?|cluster\s+(?:flats?|rooms?|apartments?))"
        r"\s+(?:of\s+)?(?:purpose[-\s]built\s+)?student|"
        r"(?:purpose[-\s]built\s+)?student\s+(?:accommodation\s+|housing\s+)?"
        r"(?:apartments?|rooms?|bedrooms?|units?|flats?|studios?))\b", re.I), 0.7),
    ("page_students", re.compile(
        r"(?:home\s+to|houses?|housing|accommodat(?:es|ing|ion\s+for)|hosts?|sleeps|room\s+for|"
        r"space\s+for|capacity\s+(?:of|for))\s+" + _NUMQ + r"(?:students?|residents?|guests?|people)\b",
        re.I), 0.6),
    ("studios", re.compile(
        _NUMQ + _ADJ + r"(?:\d[-\s]?bed(?:room)?\s+)?studio(?:s|\s+(?:flats?|apartments?|units?|rooms?))?\b",
        re.I), 0.8),
    ("cluster", re.compile(
        _NUMQ + _ADJ + r"cluster\s+(?:flats?|apartments?|rooms?)\b", re.I), 0.8),
    ("components", re.compile(
        r"(?P<n>\d{1,4})\s*(?:x|no\.?)\s*\d{1,2}[-\s]?bed(?:room)?\s*"
        r"(?:apartments?|flats?|houses?|dwellings?|units?|homes?)\b", re.I), 0.75),
    ("homes", re.compile(
        _NUMQ + _ADJ + r"(?P<noun>dwelling\s*houses?|dwellings?|homes?|houses?|residential\s+units?|"
        r"units\s+of\s+residential\s+accommodation|apartments?|flats?|maisonettes?|bungalows?|"
        r"townhouses?|units?)\b", re.I), 0.0),
    ("rooms", re.compile(
        _NUMQ + _ADJ + r"(?P<pre>[a-z-]+\s+)?rooms?\b", re.I), 0.0),
    ("storeys", re.compile(
        r"(?:part\s+)?" + _NUMQ + r"(?:and\s+a\s+half\s+)?store(?:y|ys|ies)\b", re.I), 0.0),
    ("storey_range", re.compile(
        r"(?:ranging\s+)?(?:from|between)\s+(?P<a>\d{1,2}|" + _WORD + r")\s+(?:to|and)\s+"
        r"(?P<n>\d{1,2}|" + _WORD + r")\s+store(?:y|ys|ies)\b", re.I), 0.85),
    # "Ground + 20 storeys" is 21 storeys: this reading must beat the plain
    # "20 storeys" match on the same number.
    ("ground_plus", re.compile(
        r"ground\s*(?:\+|plus|and)\s*(?P<n>\d{1,2}|" + _WORD + r")\s+(?:upper\s+)?(?:floors?|store(?:y|ys|ies))\b",
        re.I), 0.91),
    ("floors", re.compile(
        _NUMQ + r"(?:upper\s+)?floors\b", re.I), 0.5),
]
_ROOM_NOISE = {"study", "plant", "bin", "communal", "meeting", "common", "cycle", "laundry",
               "lounge", "games", "cinema", "storage", "changing", "consulting", "treatment",
               "class", "dining", "function", "press", "staff", "refuse", "store", "utility",
               "boiler", "server", "control", "waiting", "reception", "show"}
_UNIT_SIZE_BEFORE = re.compile(r"\d\s*x\s*$", re.I)          # "1 x 10-bed cluster"
_TOTAL_BEFORE = re.compile(r"total(?:\s+of)?\s*$", re.I)
# "an additional 53 bed spaces": the increment an extension adds, not a total.
_ADDITIONAL_NEAR = re.compile(r"\b(?:additional|further|extra|more)\b", re.I)
# "2026/27 rooms", "September 2026 bedrooms": a year beside a unit noun is not a count.
_YEAR_CONTEXT = re.compile(
    r"(?:(?:january|february|march|april|may|june|july|august|september|october|november|december|"
    r"academic\s+year|\bin|\bfor|\bfrom|\bsince)\s*$)", re.I)
_YEAR_SUFFIX = re.compile(r"^\s*[/–-]\s*\d{2}\b")
_YEAR_PREFIX = re.compile(r"(?:19|20)\d{2}\s*[/–-]\s*$")
_COUNT_FRAME = re.compile(r"(?:total|comprising|provid\w*|erection\s+of|accommodat\w*|\bof|\bto)\s*$", re.I)

# Context markers decide what a bare "N beds" or "N rooms" counts.
_MARKERS = {
    "student": re.compile(r"\bstudents?\b|\bpbsa\b|halls?\s+of\s+residence|bed-?\s?spaces?", re.I),
    "coliving": re.compile(r"co-?living|shared\s+living", re.I),
    "hotel": re.compile(r"\bhotels?\b|apart-?hotel|serviced\s+apartments?", re.I),
    "care": re.compile(r"care\s+home|nursing|respite|extra[-\s]care|\bc2\b|hospital|hospice", re.I),
    "hmo": re.compile(r"\bhmos?\b|multiple\s+occupation", re.I),
    "residential": re.compile(r"\bc3\b|\bresidential\b|dwellings?|\bhomes?\b|build[-\s]to[-\s]rent|\bbtr\b", re.I),
}
_EITHER_OR = [
    re.compile(r"\band/or\b", re.I),
    re.compile(r"\beither\b[^.;]{0,160}?\bor\b", re.I),
    re.compile(r"\b(?:or\s+)?alternatively\b", re.I),
    re.compile(r"\boption\s+[ab12]\b", re.I),
    re.compile(r"\bflexible\s+(?:use|residential|student|co-?living|accommodation)", re.I),
    re.compile(r"\bor\s+(?:purpose[-\s]built\s+)?(?:student|co-?living|residential|hotel|shared\s+living)\b", re.I),
    re.compile(r"\b(?:student|co-?living|residential|hotel)\b[^.;]{0,60}?\bor\b[^.;]{0,60}?"
               r"\b(?:student|co-?living|residential|hotel)\b", re.I),
]
_USE_NOUN = re.compile(r"student|co-?living|residential|hotel|dwellings?|homes?|accommodation|rooms?|"
                       r"units?|\bc[123]\b|sui\s+generis|shared\s+living|apartments?|flats?", re.I)


@dataclass
class UnitFigure:
    kind: str
    value: int
    confidence: float
    quote: str
    span: tuple[int, int]
    is_maximum: bool = False      # an envelope: "up to", "maximum of"
    is_approximate: bool = False  # "circa", "over", "around"
    alternative: bool = False     # inside an either/or permission
    component: bool = False       # one part of a larger total
    is_additional: bool = False   # an increment ("additional 53 beds"), not a scheme total
    variation_from: Optional[int] = None
    method: str = "regex"         # regex | variation | sum | from_studios | ai | manual
    context: Optional[str] = None

    def committed(self) -> bool:
        return not self.is_maximum and not self.component


@dataclass
class UnitExtraction:
    text: str
    figures: list[UnitFigure] = field(default_factory=list)
    either_or: bool = False

    # ------------------------------------------------------------ queries
    def of_kind(self, kind: str) -> list[UnitFigure]:
        return [f for f in self.figures if f.kind == kind]

    def best(self, kind: str) -> Optional[UnitFigure]:
        """Highest-confidence committed figure; alternatives only as a last resort."""
        cands = [f for f in self.of_kind(kind) if f.committed()]
        firm = [f for f in cands if not f.alternative] or cands
        return max(firm, key=lambda f: (f.confidence, f.value)) if firm else None

    def maximum(self, kind: str) -> Optional[UnitFigure]:
        cands = [f for f in self.of_kind(kind) if f.is_maximum]
        return max(cands, key=lambda f: (f.confidence, f.value)) if cands else None

    def _value(self, fig: Optional[UnitFigure]) -> Optional[int]:
        return fig.value if fig else None

    @property
    def beds(self) -> Optional[int]:
        return self._value(self.best("beds"))

    @property
    def beds_max(self) -> Optional[int]:
        return self._value(self.maximum("beds"))

    @property
    def homes(self) -> Optional[int]:
        return self._value(self.best("homes"))

    @property
    def homes_max(self) -> Optional[int]:
        return self._value(self.maximum("homes"))

    @property
    def storeys(self) -> Optional[int]:
        return self._value(self.best("storeys"))

    def summary(self) -> dict:
        b, h, s = self.best("beds"), self.best("homes"), self.best("storeys")
        return {
            "beds": b.value if b else None, "beds_confidence": round(b.confidence, 2) if b else None,
            "beds_quote": b.quote if b else None, "beds_max": self.beds_max,
            "homes": h.value if h else None, "homes_confidence": round(h.confidence, 2) if h else None,
            "homes_max": self.homes_max, "storeys": s.value if s else None,
            "either_or": self.either_or, "figures": len(self.figures),
        }


# ------------------------------------------------------------------ helpers
def _word_or_int(s: str) -> Optional[int]:
    s = s.lower().replace(",", "")
    return int(s) if s.isdigit() else _WORDS.get(s)


def _number(m: re.Match) -> Optional[int]:
    g = m.groupdict()
    if g.get("n"):
        return _word_or_int(g["n"])
    if g.get("w"):
        return _WORDS.get(g["w"].lower())
    return None


def _context(text: str, start: int, end: int) -> tuple[Optional[str], set[str]]:
    """The nearest use marker to a figure, and every marker in its window."""
    lo, hi = max(0, start - 150), min(len(text), end + 60)
    window = text[lo:hi]
    best, best_d, present = None, 10 ** 6, set()
    for name, rx in _MARKERS.items():
        for m in rx.finditer(window):
            present.add(name)
            ms, me = m.start() + lo, m.end() + lo
            d = 0 if (ms < end and me > start) else min(abs(ms - end), abs(start - me))
            if d < best_d:
                best, best_d = name, d
    return best, present


def _number_pos(m: re.Match) -> int:
    for g in ("n", "w"):
        if m.groupdict().get(g):
            return m.start(g)
    return m.start()


def _plausible(kind: str, value: int) -> bool:
    lo, hi = PLAUSIBLE.get(kind, (1, 10 ** 6))
    return lo <= value <= hi


# ------------------------------------------------------------------ core
def _classify_beds(noun: str, ctx: Optional[str], present: set[str]) -> tuple[str, float]:
    n = noun.lower()
    specific = "space" in n or n.startswith(("student", "study"))
    if ctx == "student" or ctx is None and specific:
        if specific:
            return "beds", 0.9
        return ("beds", 0.75) if "room" in n else ("beds", 0.85)
    if ctx == "coliving":
        # "student and/or co-living ... 864 beds": beds are PBSA vocabulary,
        # co-living counts rooms, so a student mention nearby keeps it beds.
        return ("beds", 0.8) if "student" in present else ("coliving_rooms", 0.8)
    if ctx == "hotel":
        return "hotel_rooms", 0.8
    if ctx == "care":
        return "care_beds", 0.8
    if ctx == "hmo":
        return "hmo_beds", 0.8
    if ctx == "residential":
        return ("beds", 0.35) if "room" in n else ("beds", 0.4)
    return ("beds", 0.45) if "room" in n else ("beds", 0.5)


def _classify_homes(noun: str, ctx: Optional[str], text: str, start: int) -> tuple[str, float]:
    n = re.sub(r"\s+", " ", noun.lower())
    if ctx == "student":
        return "beds", 0.65
    if ctx == "coliving":
        return "coliving_rooms", 0.8
    if ctx == "hotel" and n.startswith("apartment"):
        return "hotel_rooms", 0.6
    if n in ("dwelling", "dwellings", "home", "homes", "residential unit", "residential units",
             "units of residential accommodation") or n.startswith("dwelling"):
        return "homes", 0.85
    if n.startswith(("house", "maisonette", "bungalow", "townhouse")):
        return "homes", 0.8
    if n.startswith(("apartment", "flat")):
        return "homes", 0.7
    # bare "units": only residential when the text says so nearby
    near = text[max(0, start - 60):start + 60]
    if ctx == "residential" or re.search(r"residential|\bc3\b|dwelling|homes?", near, re.I):
        return "homes", 0.7
    return "units", 0.4


def _classify_rooms(pre: Optional[str], ctx: Optional[str]) -> Optional[tuple[str, float]]:
    word = (pre or "").strip().lower()
    if word in _ROOM_NOISE:
        return None
    if word.startswith(("co-living", "coliving")) or ctx == "coliving":
        return "coliving_rooms", 0.85
    if word in ("hotel", "guest") or ctx == "hotel":
        return "hotel_rooms", 0.85
    if ctx == "student":
        return "beds", 0.75
    if ctx == "care":
        return "care_beds", 0.7
    if ctx == "hmo":
        return "hmo_beds", 0.7
    return "rooms", 0.4


def extract_units(text: Optional[str]) -> UnitExtraction:
    """Every typed figure in `text`, with envelope/alternative flags and confidence."""
    ex = UnitExtraction(text=text or "")
    if not text:
        return ex
    by_pos: dict[int, UnitFigure] = {}

    def offer(fig: UnitFigure, pos: int) -> None:
        if not _plausible(fig.kind, fig.value):
            return
        cur = by_pos.get(pos)
        if cur is None or fig.confidence > cur.confidence:
            by_pos[pos] = fig

    for name, rx, base in _PATTERNS:
        for m in rx.finditer(text):
            pos = _number_pos(m)
            if _UNIT_SIZE_BEFORE.search(text[max(0, m.start() - 6):m.start()]) and name != "components":
                continue  # "1 x 10-bed cluster": the 10 is a flat size
            value = _number(m)
            if value is None:
                continue
            if m.groupdict().get("n") and _YEAR_PREFIX.search(text[max(0, m.start("n") - 8):m.start("n")]):
                continue   # the "27" of "2026/27"
            if 1950 <= value <= 2099 and m.groupdict().get("n"):
                # "2026/27", "September 2026", or a bare "2026 Student Rooms"
                # heading: a year. Counts that size are written "1,544" or
                # framed as one ("a total of 1968 bedspaces").
                after = text[m.end("n"):m.end("n") + 4]
                before = text[max(0, m.start("n") - 16):m.start("n")]
                framed = (_COUNT_FRAME.search(before)
                          or "space" in (m.groupdict().get("noun") or "").lower())
                if (_YEAR_SUFFIX.match(after) or _YEAR_CONTEXT.search(before)
                        or ("," not in m.group("n") and not framed)):
                    continue
            ctx, present = _context(text, m.start(), m.end())
            quote = text[m.start():m.end()].strip()
            is_max = bool(m.groupdict().get("max"))
            approx = bool(m.groupdict().get("approx"))
            kind, conf, extra = None, base, {}
            if name == "variation":
                kind, extra = "beds", {"variation_from": int(m.group("a")), "method": "variation"}
            elif name == "beds":
                kind, conf = _classify_beds(m.group("noun"), ctx, present)
            elif name == "student_units":
                kind = "beds"
            elif name == "page_students":
                kind = "beds"
            elif name == "studios":
                kind = "coliving_rooms" if ctx == "coliving" and "student" not in present else "studios"
            elif name == "cluster":
                kind = "cluster_flats"
            elif name == "components":
                kind, extra = "homes", {"component": True}
            elif name == "homes":
                kind, conf = _classify_homes(m.group("noun"), ctx, text, m.start())
            elif name == "rooms":
                r = _classify_rooms(m.group("pre"), ctx)
                if r is None:
                    continue
                kind, conf = r
            elif name == "storeys":
                kind, conf = "storeys", (0.9 if m.groupdict().get("n") else 0.85)
            elif name == "storey_range":
                kind = "storeys"
                low = _word_or_int(m.group("a"))
                value = _word_or_int(m.group("n"))
                if low is not None and value is not None and low < value:
                    offer(UnitFigure("storeys", low, 0.6, quote, (m.start(), m.end()),
                                     component=True, context=ctx), m.start("a"))
                if value is None:
                    continue
            elif name == "ground_plus":
                kind, value = "storeys", (_word_or_int(m.group("n")) or 0) + 1
                if value < 2:
                    continue
            elif name == "floors":
                kind = "storeys"
            if kind is None:
                continue
            if _TOTAL_BEFORE.search(text[max(0, m.start() - 12):m.start()]):
                conf = min(0.98, conf + 0.05)
            if approx:
                conf *= 0.85
            additional = kind != "storeys" and bool(
                _ADDITIONAL_NEAR.search(text[max(0, m.start() - 30):m.end()]))
            fig = UnitFigure(kind, value, round(conf, 3), quote, (m.start(), m.end()),
                             is_maximum=is_max, is_approximate=approx, is_additional=additional,
                             context=ctx, **extra)
            offer(fig, pos)

    figures = sorted(by_pos.values(), key=lambda f: f.span)

    # Either/or permissions: flag the clause and every figure in the same
    # sentence, since a count under an alternative use is conditional.
    windows = []
    for rx in _EITHER_OR:
        for m in rx.finditer(text):
            if not _USE_NOUN.search(text[max(0, m.start() - 80):m.end() + 80]):
                continue
            lo = max((text.rfind(ch, 0, m.start()) for ch in ".;"), default=-1) + 1
            nxt = [i for i in (text.find(ch, m.end()) for ch in ".;") if i >= 0]
            hi = min(nxt) if nxt else len(text)
            windows.append((lo, hi))
    if windows:
        ex.either_or = True
        for f in figures:
            if any(f.span[0] < hi and f.span[1] > lo for lo, hi in windows):
                f.alternative = True
                f.confidence = round(f.confidence * 0.8, 3)

    # Derived figures, clearly labelled: homes from listed components,
    # beds from an all-studio scheme.
    comps = [f for f in figures if f.kind == "homes" and f.component]
    if len(comps) >= 2 and not any(f.kind == "homes" and f.committed() for f in figures):
        figures.append(UnitFigure("homes", sum(c.value for c in comps), 0.65,
                                  "; ".join(c.quote for c in comps),
                                  (comps[0].span[0], comps[-1].span[1]), method="sum"))
    if not any(f.kind == "beds" and f.committed() for f in figures):
        studios = [f for f in figures if f.kind == "studios" and f.committed()
                   and f.context in ("student", None)]
        if studios:
            s = max(studios, key=lambda f: (f.confidence, f.value))
            figures.append(UnitFigure("beds", s.value, round(s.confidence * 0.7, 3), s.quote, s.span,
                                      alternative=s.alternative, method="from_studios", context=s.context))
    ex.figures = figures
    return ex


# ------------------------------------------------------------------ storage
def text_sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def as_rows(ex: UnitExtraction, entity_type: str, entity_id: int, source: str,
            source_reference: Optional[str] = None, model: Optional[str] = None,
            kinds: Optional[Iterable[str]] = None) -> list[dict]:
    """Shape figures for the unit_extractions table."""
    sha = text_sha1(ex.text)
    wanted = set(kinds) if kinds else None
    return [
        {
            "entity_type": entity_type, "entity_id": entity_id, "kind": f.kind, "value": f.value,
            "is_maximum": f.is_maximum, "is_approximate": f.is_approximate, "alternative": f.alternative,
            "component": f.component, "is_additional": f.is_additional,
            "variation_from": f.variation_from, "confidence": f.confidence,
            "quote": f.quote[:500], "method": f.method, "model": model, "source": source,
            "source_reference": source_reference, "text_sha1": sha,
        }
        for f in ex.figures if wanted is None or f.kind in wanted
    ]
