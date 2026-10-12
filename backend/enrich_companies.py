"""Fill company details for the companies behind named schemes.

Free sources only. Nothing is overwritten: a field is written when it is
empty, and every value written goes into a run log under data/enrichment/.

  --check-numbers   Look each stored Companies House number up. Where the
                    register's name agrees with ours, fill the registered
                    office and SIC codes. Where it does not, list the row
                    for review and write nothing.
  --officers        For numbers that passed the check, add the current
                    directors and secretaries as contacts (name and role
                    only; no address, birth date or nationality is kept).
  --find-numbers    For companies with no number whose name is a legal
                    name (ends Limited, LLP, PLC), search the register and
                    accept only an exact, active, single match.
  --websites        Take the operator's own domain from the web addresses
                    its schemes were collected from.
  --site-details    Fetch each stored website (robots.txt respected, one
                    request a second, no workaround for blocked sites) and
                    read the switchboard number, a general enquiries
                    address and the company number printed on the site.
  --load-research F Load a JSON file of researched websites, then verify
                    each claim against the cited page before storing it.

Dry run unless --apply is given.

    python enrich_companies.py --check-numbers --officers
    python enrich_companies.py --check-numbers --officers --apply
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
from difflib import SequenceMatcher
from urllib.parse import urljoin, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enrichment")
NAMED = "es.merged_into_id IS NULL AND es.record_class = 'scheme'"

# Companies in scope: anything a named scheme points at.
TARGETS = f"""
    SELECT x.cid AS id,
           count(DISTINCT es.id) FILTER (WHERE x.role = 'operator') AS n_op,
           count(DISTINCT es.id) FILTER (WHERE x.role <> 'operator') AS n_own
    FROM existing_schemes es
    CROSS JOIN LATERAL (VALUES (es.operator_company_id, 'operator'), (es.owner_company_id, 'owner'),
                               (es.asset_manager_company_id, 'asset_manager'),
                               (es.landlord_company_id, 'landlord')) x(cid, role)
    WHERE {NAMED} AND x.cid IS NOT NULL GROUP BY x.cid"""

LEGAL_END = re.compile(r"\b(ltd|limited|llp|plc|l\.?p\.?)\.?$", re.I)
NAME_NOISE = re.compile(r"\b(limited|ltd|plc|llp|lp|the|of|and|company|co|uk|group|holdings)\b")
PERSON_ROLES = {"director": "Director", "secretary": "Company secretary", "llp-member": "LLP member",
                "llp-designated-member": "LLP designated member", "member-of-a-management": "Management board member",
                "managing-officer": "Managing officer"}
GENERIC_OCCUPATIONS = {"", "none", "director", "company director", "co director", "companydirector", "n/a", "na",
                       "businessman", "businesswoman", "business person", "manager", "executive", "retired"}

# Sites that list other people's buildings: never an operator's own website.
AGGREGATORS = ("accommodationforstudents.com", "sturents.com", "rightmove.co.uk", "zoopla.co.uk", "onthemarket.com",
               "studentcrowd.com", "unihomes.co.uk", "student.com", "amberstudent.com", "universityliving.com",
               "homeviews.com", "lovetorent.co.uk", "buildtorentdirectory.co.uk", "facebook.com", "linkedin.com",
               "instagram.com", "twitter.com", "x.com", "google.com", "youtube.com", "gov.uk", "wikipedia.org",
               "uhomes.com", "casita.com", "mystudenthalls.com", "studentroomstay.com", "spareroom.co.uk",
               "openrent.co.uk", "primelocation.com", "housinganywhere.com", "unilodgers.com")
OWN_SITE_SOURCES = ("operator_directory", "pbsa_operator", "operator_unite_students", "operator_iq_student",
                    "university_halls", "operator_scraper")

GENERIC_MAILBOX = re.compile(r"^(info|hello|hi|enquir(y|ies)|inquir(y|ies)|contact(us)?|contactus|lettings?|sales|admin|"
                             r"office|mail|team|support|help|customer(service|services|care)?|reception|general|"
                             r"bookings?|accommodation|residents?|students?|press|media|marketing|invest(or|ors)?|"
                             r"ir|corporate|business|partnerships?|property|homes|living|rent|renting|welcome)$", re.I)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
TEL_HREF_RE = re.compile(r'href=["\']tel:([^"\']+)["\']', re.I)
MAILTO_RE = re.compile(r'href=["\']mailto:([^"\'?]+)', re.I)
PHONE_TEXT_RE = re.compile(r"(?<![\d.])(?:\+44\s?\(?0?\)?\s?|\(?0)(?:\d[\s\-()]{0,2}){9,10}(?![\d.])")
COMPANY_NO_RE = re.compile(
    r"(?:company|registration|registered|reg\.?)\s*(?:registration\s*)?(?:number|no\.?|num\.?|#)?\s*[:.]?\s*"
    r"(?:is\s*)?\b((?:OC|SC|NI|SO|LP|NC|FC|IP|RS)\s?\d{5,6}R?|\d{7,8})\b", re.I)
REGISTERED_IN_RE = re.compile(
    r"registered\s+in\s+(?:england(?:\s*(?:and|&)\s*wales)?|scotland|wales|northern\s+ireland|the\s+uk|the\s+united\s+kingdom)"
    r"[^.<>]{0,60}?\b((?:OC|SC|NI|SO|LP)\s?\d{6}|\d{7,8})\b", re.I)
LINK_RE = re.compile(r'<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', re.I | re.S)
DETAIL_PAGE_HINT = re.compile(r"contact|get[-\s]?in[-\s]?touch|about|privacy|terms|legal|cookie|company[-\s]?info|imprint",
                              re.I)


# --------------------------------------------------------------------- helpers
def name_key(name: str) -> str:
    s = re.sub(r"&", " and ", (name or "").lower())
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = NAME_NOISE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def name_verdict(ours: str, theirs: str, previous: list[str] | None = None) -> str:
    """'match' when the register's name is ours; 'contains' when ours is the
    leading words of a longer registered name; otherwise 'differs'."""
    a = name_key(ours)
    for cand in [theirs] + list(previous or []):
        b = name_key(cand)
        if not a or not b:
            continue
        if a == b or SequenceMatcher(None, a, b).ratio() >= 0.92:
            return "match"
    b = name_key(theirs)
    if a and b and len(a) >= 5 and (b.startswith(a + " ") or a.startswith(b + " ")):
        return "contains"
    return "differs"


def norm_number(raw: str | None) -> str | None:
    if not raw:
        return None
    s = re.sub(r"\s+", "", raw.upper())
    if re.fullmatch(r"\d{1,8}", s):
        return s.zfill(8)
    if re.fullmatch(r"(OC|SC|NI|SO|LP|NC|FC|OE|SL|NL|IP|RS|SP|NP|BR)\d{5,6}R?", s):
        return s
    return None     # registered societies written as 22981R and other forms the API does not serve


def format_address(addr: dict | None) -> str | None:
    if not addr:
        return None
    parts = [addr.get(k) for k in ("premises", "address_line_1", "address_line_2", "locality", "region",
                                   "postal_code", "country")]
    out = ", ".join(p.strip() for p in parts if p and p.strip())
    return out or None


def person_name(raw: str) -> str:
    """Companies House writes 'SURNAME, Forename Middle'. Returns 'Forename Middle Surname'."""
    raw = re.sub(r"\s+", " ", (raw or "").strip())
    if "," in raw:
        surname, rest = raw.split(",", 1)
        rest = re.sub(r"^(mr|mrs|ms|miss|dr|prof|professor|sir|dame|lord|lady)\.?\s+", "", rest.strip(), flags=re.I)
        raw = f"{rest} {surname.strip().title() if surname.isupper() else surname.strip()}".strip()
    return raw[:255]


def job_title(role: str, occupation: str | None) -> str:
    base = PERSON_ROLES.get(role, role.replace("-", " ").capitalize())
    occ = re.sub(r"\s+", " ", (occupation or "").strip())
    if occ.lower() in GENERIC_OCCUPATIONS or len(occ) > 60:
        return base
    return f"{base} ({occ.title() if occ.isupper() or occ.islower() else occ})"[:255]


def domain_of(url: str) -> str:
    return urlparse(url if "://" in url else "https://" + url).netloc.lower().removeprefix("www.")


def is_aggregator(domain: str) -> bool:
    return any(domain == a or domain.endswith("." + a) for a in AGGREGATORS)


def norm_phone(raw: str) -> str | None:
    """UK numbers as '020 7123 4567' style digits with a leading 0; None when not a plausible UK number."""
    digits = re.sub(r"[^\d+]", "", raw or "")
    if digits.startswith("+44"):
        digits = "0" + digits[3:].lstrip("0")
    elif digits.startswith("0044"):
        digits = "0" + digits[4:].lstrip("0")
    digits = digits.replace("+", "")
    if not re.fullmatch(r"0[1-9]\d{8,9}", digits):
        return None
    if digits.startswith("07"):
        return None        # a mobile is a person's number, not a switchboard
    return digits


def strip_tags(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript|svg)\b.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = html.replace("&nbsp;", " ").replace("&amp;", "&").replace("&#8211;", "-")
    return re.sub(r"\s+", " ", html)


def read_site_page(html: str, site_domain: str) -> dict:
    """Phones, generic mailboxes and company numbers printed on one page."""
    found = {"phones": Counter(), "emails": Counter(), "numbers": Counter()}
    body = strip_tags(html)
    for raw in TEL_HREF_RE.findall(html):
        p = norm_phone(raw)
        if p:
            found["phones"][p] += 3
    for m in PHONE_TEXT_RE.finditer(body):
        p = norm_phone(m.group(0))
        if p:
            found["phones"][p] += 1
    for raw in MAILTO_RE.findall(html) + EMAIL_RE.findall(body):
        e = raw.strip().lower().rstrip(".")
        if not EMAIL_RE.fullmatch(e):
            continue
        local, dom = e.split("@", 1)
        if dom.endswith((".png", ".jpg", ".gif", ".webp", ".svg")) or "sentry" in dom or "example" in dom:
            continue
        if GENERIC_MAILBOX.match(local):
            found["emails"][e] += 2 if dom.removeprefix("www.") == site_domain else 1
    for rx in (COMPANY_NO_RE, REGISTERED_IN_RE):
        for m in rx.finditer(body):
            n = norm_number(m.group(1))
            if n and not re.fullmatch(r"0{8}", n):
                found["numbers"][n] += 1
    return found


class RunLog:
    def __init__(self, apply: bool):
        self.apply, self.entries, self.counts = apply, [], Counter()
        self.review: list[dict] = []
        self.checked: list[dict] = []

    def wrote(self, table: str, row_id: int, field: str, value, source: str, **extra) -> None:
        self.entries.append({"table": table, "id": row_id, "field": field, "old": None, "new": value,
                             "source": source, **extra})

    def save(self, stage: str) -> str:
        os.makedirs(OUT_DIR, exist_ok=True)
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(OUT_DIR, f"companies_{stage}_{stamp}{'' if self.apply else '_dryrun'}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"applied": self.apply, "at": stamp, "counts": dict(self.counts), "writes": self.entries,
                       "review": self.review, "checked": self.checked}, fh, indent=1, default=str)
        return path


def fill(db, log: RunLog, company_id: int, field: str, value, source: str, **extra) -> bool:
    """Write companies.<field> only when it is empty. Returns True when written."""
    if value in (None, "", []):
        return False
    payload = json.dumps(value) if isinstance(value, (list, dict)) else value
    cast = "CAST(:v AS jsonb)" if isinstance(value, (list, dict)) else ":v"
    empty = f"({field} IS NULL OR {field}::text IN ('', 'null', '[]'))"
    n = db.execute(text(f"UPDATE companies SET {field} = {cast}, updated_at = now() WHERE id = :id AND {empty}"),
                   {"v": payload, "id": company_id}).rowcount
    if n:
        log.wrote("companies", company_id, field, value, source, **extra)
        log.counts[f"companies.{field}"] += 1
    return bool(n)


def add_contact(db, log: RunLog, company_id: int, full_name: str, title: str | None, source: str,
                email: str | None = None, phone: str | None = None, confidence: float = 0.95) -> bool:
    exists = db.execute(text("""
        SELECT 1 FROM contacts WHERE company_id = :c AND lower(full_name) = lower(:n)
          AND coalesce(lower(email), '') = coalesce(lower(:e), '') LIMIT 1"""),
        {"c": company_id, "n": full_name, "e": email}).first()
    if exists:
        return False
    db.execute(text("""
        INSERT INTO contacts (company_id, full_name, job_title, email, phone, source, confidence_score,
                              last_verified_at, created_at)
        VALUES (:c, :n, :t, :e, :p, :s, :conf, now(), now())"""),
        {"c": company_id, "n": full_name, "t": title, "e": email, "p": phone, "s": source, "conf": confidence})
    log.wrote("contacts", company_id, "contact", {"name": full_name, "title": title, "email": email, "phone": phone},
              source)
    log.counts[f"contacts from {source}"] += 1
    return True


def load_targets(db, where: str = "TRUE") -> list[dict]:
    rows = db.execute(text(f"""
        SELECT c.id, c.name, c.companies_house_number, c.registered_address, c.website, c.phone, c.company_type,
               t.n_op, t.n_own
        FROM ({TARGETS}) t JOIN companies c ON c.id = t.id
        WHERE c.merged_into_id IS NULL AND {where}
        ORDER BY (t.n_op + t.n_own) DESC, c.id""")).mappings().all()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------- Companies House
# Trades a landlord, developer, operator, fund, university or housing body files under.
PROPERTY_SIC = ("41", "55", "64", "66", "68", "70100", "81", "85", "87", "88", "94")
TOO_SMALL = {"micro-entity", "dormant", "null", "none", ""}
SUBSTANTIAL = {"group", "full", "medium", "large"}


def legal_name(name: str) -> str:
    """A registered name with case, punctuation and Ltd/Limited spelling removed, the legal form kept."""
    s = re.sub(r"&", " AND ", (name or "").upper())
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\bLTD\b", "LIMITED", s)
    s = re.sub(r"\bPUBLIC LIMITED COMPANY\b", "PLC", s)
    s = re.sub(r"^\s*THE\s+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def judge_number(co: dict, prof: dict, aliases: tuple[str, ...] = ()) -> tuple[str, str | None]:
    """(verdict, issue). A stored number is trusted when the register's legal
    name is ours, or when our brand name matches and the registered company is
    in a property trade. A recent or tiny company carrying many schemes is a
    look-alike registration and is never trusted."""
    register = prof.get("company_name", "")
    previous = [p.get("name", "") for p in prof.get("previous_company_names", [])]
    status = (prof.get("company_status") or "").lower()
    created = prof.get("date_of_creation") or ""
    sic = prof.get("sic_codes") or []
    accounts = str(((prof.get("accounts") or {}).get("last_accounts") or {}).get("type") or "").lower()
    schemes = co["n_op"] + co["n_own"]
    register_names = [legal_name(register)] + [legal_name(p) for p in previous]
    exact = legal_name(co["name"]) in register_names
    known_alias = any(legal_name(a) in register_names for a in aliases)
    loose = name_verdict(co["name"], register, previous)
    if not exact and not known_alias and loose == "differs":
        return loose, "name on the register differs"
    if re.match(r"(IP|RS|NP|SP)\d", prof.get("company_number", "") or ""):
        return "registered society", None        # the register holds a name and little else for these
    if status not in ("active", "open", "registered"):
        return loose, f"matched but {status or 'no status'}"
    if known_alias:
        return "legal name", None                # the entity its own website names
    if exact and LEGAL_END.search(co["name"].strip()):
        # Our name is the full legal name, as a land or company register feed writes it.
        if schemes >= 5 and created >= "2024-01-01":
            return loose, "too new for the schemes linked"
        return "legal name", None
    # Ours is a brand name: the register entry has to look like the business behind it.
    if schemes >= 5 and (created >= "2023-01-01" or accounts in TOO_SMALL):
        return loose, "too new or too small for the schemes linked"
    if created >= "2024-01-01":
        return loose, "brand name match, company formed very recently"
    if any(code.startswith(PROPERTY_SIC) for code in sic) or accounts in SUBSTANTIAL:
        return "brand name, property trade", None
    return loose, "brand name match, unrelated trade"


def load_aliases(db) -> dict[int, tuple[str, ...]]:
    out: dict[int, list[str]] = defaultdict(list)
    for cid, alias in db.execute(text("SELECT company_id, alias_name FROM company_aliases")):
        out[cid].append(alias)
    return {k: tuple(v) for k, v in out.items()}


def add_alias(db, log: RunLog, company_id: int, alias: str, source: str) -> None:
    if not alias:
        return
    seen = db.execute(text("SELECT 1 FROM company_aliases WHERE company_id = :c AND lower(alias_name) = lower(:a)"),
                      {"c": company_id, "a": alias}).first()
    if not seen:
        db.execute(text("INSERT INTO company_aliases (company_id, alias_name, source) VALUES (:c, :a, :s)"),
                   {"c": company_id, "a": alias[:500], "s": source[:50]})
        log.wrote("company_aliases", company_id, "alias_name", alias, source)
        log.counts["legal names recorded as aliases"] += 1


async def ch_profile(enricher, number: str) -> dict | None:
    return await enricher._request("GET", f"/company/{number}")


async def stage_numbers(db, log: RunLog, do_officers: bool, limit: int | None,
                        defer_brand: set[int] | None = None) -> None:
    from app.enrichment.companies_house import CompaniesHouseEnricher
    enricher = CompaniesHouseEnricher()
    targets = load_targets(db, "c.companies_house_number IS NOT NULL")[:limit]
    aliases = load_aliases(db)
    print(f"companies with a stored number: {len(targets)}")
    verdicts = Counter()
    try:
        for i, co in enumerate(targets, 1):
            number = norm_number(co["companies_house_number"])
            if not number:
                verdicts["number form the register API does not serve"] += 1
                continue
            try:
                prof = await ch_profile(enricher, number)
            except Exception as exc:                      # noqa: BLE001
                verdicts[f"lookup failed ({type(exc).__name__})"] += 1
                continue
            if not prof:
                verdicts["number not on the register"] += 1
                log.review.append({"id": co["id"], "ours": co["name"], "number": number, "issue": "not on the register",
                                   "schemes": co["n_op"] + co["n_own"]})
                continue
            verdict, issue = judge_number(co, prof, aliases.get(co["id"], ()))
            if not issue and verdict.startswith("brand") and defer_brand and co["id"] in defer_brand:
                verdicts["brand name match, held until its website is read"] += 1
                continue
            if issue:
                verdicts[issue] += 1
                log.review.append({"id": co["id"], "ours": co["name"], "number": number,
                                   "register_name": prof.get("company_name"),
                                   "status": prof.get("company_status"), "created": prof.get("date_of_creation"),
                                   "sic": prof.get("sic_codes"), "issue": issue,
                                   "accounts": ((prof.get("accounts") or {}).get("last_accounts") or {}).get("type"),
                                   "schemes": co["n_op"] + co["n_own"]})
                continue
            verdicts[f"confirmed ({verdict})"] += 1
            fill(db, log, co["id"], "registered_address", format_address(prof.get("registered_office_address")),
                 "companies_house", number=number)
            fill(db, log, co["id"], "sic_codes", prof.get("sic_codes") or None, "companies_house", number=number)
            log.checked.append({"id": co["id"], "ours": co["name"], "number": number, "verdict": verdict,
                                "register_name": prof.get("company_name"), "created": prof.get("date_of_creation")})
            if do_officers and verdict != "registered society":
                await add_officers(db, log, enricher, co["id"], number)
            if i % 50 == 0:
                print(f"  {i}/{len(targets)} checked", flush=True)
                if log.apply:
                    db.commit()
    finally:
        await enricher.close()
    print("  verdicts:")
    for k, v in verdicts.most_common():
        print(f"    {v:>5}  {k}")


async def add_officers(db, log: RunLog, enricher, company_id: int, number: str, cap: int = 12) -> int:
    try:
        officers = await enricher.get_officers(number, active_only=True, items_per_page=100)
    except Exception:                                     # noqa: BLE001
        log.counts["officer lookups failed"] += 1
        return 0
    people = [o for o in officers if o.officer_role in PERSON_ROLES and not o.officer_role.startswith("corporate")]
    people.sort(key=lambda o: (o.officer_role == "secretary", o.appointed_on or ""))
    added = 0
    for o in people[:cap]:
        name = person_name(o.name)
        if not name or len(name) < 4:
            continue
        added += add_contact(db, log, company_id, name, job_title(o.officer_role, o.occupation), "companies_house")
    if people:
        log.counts["companies given directors"] += 1 if added else 0
    return added


async def stage_find_numbers(db, log: RunLog, do_officers: bool, limit: int | None) -> None:
    """Legal names with no number: accept only one exact, active match."""
    from app.enrichment.companies_house import CompaniesHouseEnricher
    enricher = CompaniesHouseEnricher()
    targets = [c for c in load_targets(db, "c.companies_house_number IS NULL") if LEGAL_END.search(c["name"].strip())]
    targets = targets[:limit]
    print(f"legal names with no number: {len(targets)}")
    verdicts = Counter()
    try:
        for co in targets:
            try:
                hits = await enricher.search_company(co["name"], items_per_page=10)
            except Exception as exc:                      # noqa: BLE001
                verdicts[f"search failed ({type(exc).__name__})"] += 1
                continue
            exact = [h for h in hits if name_key(h.title) == name_key(co["name"])
                     and (h.company_status or "").lower() == "active"]
            if len(exact) != 1:
                verdicts["no single exact active match"] += 1
                continue
            hit = exact[0]
            taken = db.execute(text("SELECT id FROM companies WHERE companies_house_number = :n AND id <> :id LIMIT 1"),
                               {"n": hit.company_number, "id": co["id"]}).first()
            if taken:
                verdicts["number already on another company row"] += 1
                log.review.append({"id": co["id"], "ours": co["name"], "number": hit.company_number,
                                   "issue": f"number is already on company {taken[0]}"})
                continue
            verdicts["number found"] += 1
            fill(db, log, co["id"], "companies_house_number", hit.company_number, "companies_house_search",
                 register_name=hit.title)
            fill(db, log, co["id"], "registered_address", format_address(hit.registered_office_address),
                 "companies_house_search", number=hit.company_number)
            if do_officers:
                await add_officers(db, log, enricher, co["id"], hit.company_number)
    finally:
        await enricher.close()
    for k, v in verdicts.most_common():
        print(f"    {v:>5}  {k}")


# --------------------------------------------------------------------- websites
def stage_websites(db, log: RunLog) -> None:
    """An operator's own domain, read off the pages its schemes were collected from."""
    rows = db.execute(text(f"""
        SELECT es.operator_company_id, u.src, u.url FROM existing_schemes es
        CROSS JOIN LATERAL (
            SELECT es.source src, es.source_reference url
            UNION ALL SELECT r.source, r.source_reference FROM scheme_room_rents r WHERE r.scheme_id = es.id
            UNION ALL SELECT sr.source, sr.source_reference FROM scheme_source_records sr WHERE sr.scheme_id = es.id) u
        WHERE {NAMED} AND es.operator_company_id IS NOT NULL AND u.url LIKE 'http%'""")).fetchall()
    per: dict[int, Counter] = defaultdict(Counter)
    for cid, src, url in rows:
        d = domain_of(url)
        if d and not is_aggregator(d) and src in OWN_SITE_SOURCES:
            per[cid][d] += 1
    # A domain that serves several operators is a shared platform, not anyone's own site.
    owners_of = Counter(d for cnt in per.values() for d in cnt)
    names = {r[0]: r[1] for r in db.execute(text("SELECT id, name FROM companies WHERE id = ANY(:ids)"),
                                            {"ids": list(per)}).fetchall()}
    picked = 0
    for cid, cnt in per.items():
        d, n = cnt.most_common(1)[0]
        share = n / sum(cnt.values())
        brand = name_key(names.get(cid, "")).replace(" ", "")
        token = re.sub(r"[^a-z0-9]", "", d.split(".")[0])
        looks_own = bool(brand) and (token in brand or brand[:6] in token)
        if share < 0.6 or not (looks_own or (n >= 3 and owners_of[d] == 1)):
            log.review.append({"id": cid, "ours": names.get(cid), "issue": "no clear own domain",
                               "domains": cnt.most_common(3)})
            continue
        if fill(db, log, cid, "website", f"https://{d}", "scheme_feed_urls", pages=n):
            picked += 1
    print(f"operators with collected pages: {len(per)}, websites set: {picked}")


def stage_site_details(db, log: RunLog, limit: int | None, only_ids: list[int] | None = None) -> None:
    from app.scrapers.operator_directory_scraper import OperatorDirectoryScraper
    where = "nullif(trim(c.website), '') IS NOT NULL"
    targets = [c for c in load_targets(db, where) if not only_ids or c["id"] in only_ids][:limit]
    print(f"companies with a website to read: {len(targets)}")
    outcome = Counter()
    with OperatorDirectoryScraper(timeout=20.0) as web:
        for co in targets:
            site = co["website"].strip()
            site = site if "://" in site else "https://" + site
            dom = domain_of(site)
            status, html = web._fetch(site)
            if status != 200 or not html:
                outcome[{-1: "robots.txt disallows", 403: "blocked (403)", None: "unreachable"}.get(status, f"http {status}")] += 1
                continue
            pages = [html]
            links = []
            for href, label in LINK_RE.findall(html):
                url = urljoin(site, href.strip())
                if domain_of(url) != dom or url.lower().endswith((".pdf", ".jpg", ".png", ".zip")):
                    continue
                if DETAIL_PAGE_HINT.search(href) or DETAIL_PAGE_HINT.search(strip_tags(label)):
                    links.append(url.split("?")[0])
            seen, ordered = set(), []
            for url in sorted(set(links), key=lambda u: (0 if "contact" in u.lower() else 1 if "privacy" in u.lower()
                                                         or "legal" in u.lower() or "terms" in u.lower() else 2, len(u))):
                if url.rstrip("/") not in seen and url.rstrip("/") != site.rstrip("/"):
                    seen.add(url.rstrip("/"))
                    ordered.append(url)
            for url in ordered[:4]:
                st, sub = web._fetch(url)
                if st == 200 and sub:
                    pages.append(sub)
            merged = {"phones": Counter(), "emails": Counter(), "numbers": Counter()}
            for p in pages:
                got = read_site_page(p, dom)
                for k in merged:
                    merged[k].update(got[k])
            outcome["read"] += 1
            if merged["phones"]:
                phone = merged["phones"].most_common(1)[0][0]
                fill(db, log, co["id"], "phone", phone, "company_website", page=site)
            own = [e for e in merged["emails"] if e.split("@")[1].removeprefix("www.") == dom]
            if own:
                email = max(own, key=lambda e: merged["emails"][e])
                add_contact(db, log, co["id"], "General enquiries", None, "company_website", email=email,
                            phone=merged["phones"].most_common(1)[0][0] if merged["phones"] else None, confidence=0.9)
            if merged["numbers"] and not co["companies_house_number"]:
                if len(merged["numbers"]) == 1:
                    log.review.append({"id": co["id"], "ours": co["name"], "issue": "number printed on own website",
                                       "number": next(iter(merged["numbers"])), "site": site})
                else:
                    log.review.append({"id": co["id"], "ours": co["name"], "issue": "several numbers on own website",
                                       "numbers": merged["numbers"].most_common(5), "site": site})
            if log.apply:
                db.commit()
    for k, v in outcome.most_common():
        print(f"    {v:>5}  {k}")


# --------------------------------------------------------------------- researched websites
GENERIC_NAME_WORDS = {"student", "students", "living", "homes", "housing", "property", "properties", "university",
                      "developments", "development", "estates", "estate", "capital", "investments", "investment",
                      "management", "partners", "real", "london", "residential", "house", "college", "city", "life",
                      "home", "accommodation", "lettings", "england", "british", "north", "south", "east", "west",
                      "trust", "association", "society", "school", "royal", "national", "urban", "land", "asset"}


def only_digits(s: str | None) -> str:
    return re.sub(r"\D", "", s or "")


def name_tokens(name: str) -> list[str]:
    toks = [t for t in name_key(name).split() if len(t) >= 4 and t not in GENERIC_NAME_WORDS]
    return toks or [t for t in name_key(name).split() if len(t) >= 3]


def replace(db, log: RunLog, company_id: int, field: str, value, source: str, **extra) -> bool:
    """Overwrite companies.<field>, keeping the old value in the run log. Used only
    where the stored value failed an independent check."""
    old = db.execute(text(f"SELECT {field} FROM companies WHERE id = :id"), {"id": company_id}).scalar()
    if old == value or value in (None, ""):
        return False
    db.execute(text(f"UPDATE companies SET {field} = :v, updated_at = now() WHERE id = :id"),
               {"v": value, "id": company_id})
    log.entries.append({"table": "companies", "id": company_id, "field": field, "old": old, "new": value,
                        "source": source, **extra})
    log.counts[f"companies.{field} corrected"] += 1
    return True


async def stage_load_research(db, log: RunLog, paths: list[str]) -> None:
    """Store researched websites and details, re-checking each claim where the page can be fetched."""
    from app.enrichment.companies_house import CompaniesHouseEnricher
    from app.scrapers.operator_directory_scraper import OperatorDirectoryScraper, POSTCODE_RE
    rows = []
    for p in paths:
        try:
            with open(p, encoding="utf-8") as fh:
                data = json.load(fh)
            rows.extend(r for r in data if isinstance(r, dict))
        except (OSError, ValueError) as exc:
            print(f"  skipped {os.path.basename(p)}: {type(exc).__name__}")
    cos = {c["id"]: c for c in load_targets(db)}
    print(f"researched rows: {len(rows)}")
    outcome = Counter()
    pages: dict[str, str | None] = {}
    enricher = CompaniesHouseEnricher()

    def page(web, url: str | None) -> str | None:
        if not url or not url.startswith("http"):
            return None
        if url not in pages:
            status, html = web._fetch(url)
            pages[url] = html if status == 200 and html else None
        return pages[url]

    try:
        with OperatorDirectoryScraper(timeout=20.0) as web:
            for n, r in enumerate(rows, 1):
                co = cos.get(r.get("id"))
                site = (r.get("website") or "").strip()
                if not co:
                    outcome["id not in scope"] += 1
                    continue
                if not site:
                    outcome["no website found"] += 1
                    continue
                site = site if "://" in site else "https://" + site
                dom = domain_of(site)
                if not dom or is_aggregator(dom) or dom.endswith("company-information.service.gov.uk"):
                    outcome["not the company's own site"] += 1
                    log.review.append({"id": co["id"], "ours": co["name"], "issue": "researched site is a directory",
                                       "site": site})
                    continue
                if (r.get("website_confidence") or "").lower() != "high":
                    # The researcher was unsure it is the same business: a person decides.
                    outcome["researcher unsure, left for review"] += 1
                    log.review.append({"id": co["id"], "ours": co["name"], "issue": "researcher unsure of the match",
                                       "site": site, "confidence": r.get("website_confidence"),
                                       "notes": (r.get("notes") or "")[:300]})
                    continue
                home = page(web, site)
                toks = name_tokens(co["name"])
                flat_dom = re.sub(r"[^a-z0-9]", "", dom)
                by_domain = any(t in flat_dom for t in toks)
                by_page = bool(home) and any(t in strip_tags(home).lower() for t in toks)
                high = (r.get("website_confidence") or "").lower() == "high"
                if not (by_domain or by_page or (high and home is None)):
                    outcome["website not confirmed"] += 1
                    log.review.append({"id": co["id"], "ours": co["name"], "issue": "website does not name the company",
                                       "site": site, "confidence": r.get("website_confidence")})
                    continue
                checked = "re-checked" if home is not None else "site blocks automated requests, not re-checked"
                outcome[f"website accepted ({checked})"] += 1
                root = f"https://{urlparse(site).netloc}"
                path = urlparse(site).path.rstrip("/")
                fill(db, log, co["id"], "website", (root + path)[:500], "web_research", check=checked)

                def seen_on(url: str | None, needle: str, as_digits: bool) -> bool | None:
                    """True/False when a page could be read, None when none could."""
                    got = [h for h in (page(web, url) if url and domain_of(url) == dom else None, home) if h]
                    if not got:
                        return None
                    if as_digits:
                        return any(needle in only_digits(h) for h in got)
                    return any(needle.lower() in h.lower() for h in got)

                phone = norm_phone(r.get("phone") or "")
                if phone:
                    ok = seen_on(r.get("phone_source_url"), phone[1:], True)
                    if ok or (ok is None and high and domain_of(r.get("phone_source_url") or "") == dom):
                        fill(db, log, co["id"], "phone", phone, "company_website",
                             page=r.get("phone_source_url"), check="re-checked" if ok else "not re-checked")
                    else:
                        outcome["phone not on the cited page"] += 1
                        phone = None
                email = (r.get("email") or "").strip().lower()
                if email and EMAIL_RE.fullmatch(email) and GENERIC_MAILBOX.match(email.split("@")[0]):
                    ok = seen_on(r.get("email_source_url"), email, False)
                    if ok or (ok is None and high and domain_of(r.get("email_source_url") or "") == dom):
                        add_contact(db, log, co["id"], "General enquiries", None, "company_website", email=email,
                                    phone=phone, confidence=0.9 if ok else 0.7)
                    else:
                        outcome["email not on the cited page"] += 1
                address = re.sub(r"\s+", " ", (r.get("address") or "").strip())
                pc = POSTCODE_RE.search(address.upper())
                if address and pc:
                    ok = seen_on(r.get("address_source_url"), pc.group(1).replace(" ", ""), False) or \
                        seen_on(r.get("address_source_url"), pc.group(1), False)
                    if ok or (ok is None and high):
                        fill(db, log, co["id"], "headquarters_address", address[:500], "company_website",
                             page=r.get("address_source_url"))
                        fill(db, log, co["id"], "headquarters_postcode", pc.group(1), "company_website")
                number = norm_number(r.get("company_number"))
                if number:
                    await settle_number(db, log, enricher, co, number, r, outcome)
                if log.apply and n % 20 == 0:
                    db.commit()
    finally:
        await enricher.close()
    for k, v in outcome.most_common():
        print(f"    {v:>5}  {k}")


async def settle_number(db, log: RunLog, enricher, co: dict, number: str, r: dict, outcome: Counter) -> None:
    """A company number printed on the company's own site: check it on the register, then store it."""
    try:
        prof = await ch_profile(enricher, number)
    except Exception:                                     # noqa: BLE001
        outcome["register lookup failed"] += 1
        return
    if not prof:
        outcome["printed number not on the register"] += 1
        return
    register = prof.get("company_name", "")
    previous = [p.get("name", "") for p in prof.get("previous_company_names", [])]
    claimed = r.get("legal_entity") or ""
    agrees = (claimed and name_verdict(claimed, register, previous) != "differs") or \
        name_verdict(co["name"], register, previous) != "differs"
    if not agrees:
        outcome["printed number is a different company"] += 1
        log.review.append({"id": co["id"], "ours": co["name"], "issue": "printed number does not match the named entity",
                           "number": number, "register_name": register, "claimed": claimed})
        return
    if (prof.get("company_status") or "").lower() not in ("active", "open", "registered"):
        outcome["printed number is not an active company"] += 1
        return
    stored = norm_number(co["companies_house_number"])
    if stored == number:
        outcome["printed number agrees with the stored one"] += 1
        return
    other = db.execute(text("SELECT id, name FROM companies WHERE companies_house_number = :n AND id <> :id LIMIT 1"),
                       {"n": number, "id": co["id"]}).first()
    if other:
        outcome["printed number sits on another company row"] += 1
        log.review.append({"id": co["id"], "ours": co["name"], "issue": "same company as another row?",
                           "number": number, "register_name": register, "other_id": other[0], "other_name": other[1]})
        return
    source = "company_website_number"
    if not stored:
        fill(db, log, co["id"], "companies_house_number", number, source, register_name=register,
             page=r.get("company_number_source_url"))
        outcome["number added from own website"] += 1
    else:
        old_prof = None
        try:
            old_prof = await ch_profile(enricher, stored)
        except Exception:                                 # noqa: BLE001
            pass
        old_verdict, issue = ("", "not on the register") if not old_prof else judge_number(co, old_prof)
        if not issue and old_verdict.startswith("brand"):
            issue = "matched on brand name only; the company's own site names another entity"
        if not issue:
            outcome["site prints a different entity than the stored one (kept)"] += 1
            log.review.append({"id": co["id"], "ours": co["name"], "issue": "site prints a second entity",
                               "stored": stored, "printed": number, "register_name": register})
            return
        replace(db, log, co["id"], "companies_house_number", number, source, register_name=register,
                why=f"stored number failed the check: {issue}", page=r.get("company_number_source_url"))
        replace(db, log, co["id"], "registered_address", format_address(prof.get("registered_office_address")),
                source, number=number)
        outcome["wrong stored number corrected"] += 1
    add_alias(db, log, co["id"], register, "companies_house")
    fill(db, log, co["id"], "registered_address", format_address(prof.get("registered_office_address")), source,
         number=number)
    fill(db, log, co["id"], "sic_codes", prof.get("sic_codes") or None, source, number=number)
    await add_officers(db, log, enricher, co["id"], number)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load-research", nargs="+", metavar="FILE")
    ap.add_argument("--check-numbers", action="store_true")
    ap.add_argument("--officers", action="store_true")
    ap.add_argument("--find-numbers", action="store_true")
    ap.add_argument("--websites", action="store_true")
    ap.add_argument("--site-details", action="store_true")
    ap.add_argument("--hold-brand-ids", metavar="GLOB",
                    help="research batch files: brand-name matches for these companies wait for their website")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    stages = [s for s in ("check_numbers", "find_numbers", "websites", "site_details") if getattr(args, s)]
    if args.load_research:
        stages.append("research")
    if not stages:
        ap.error("pick at least one stage")
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    db = SessionLocal()
    log = RunLog(args.apply)
    try:
        if args.check_numbers:
            print("\n== stored Companies House numbers")
            held: set[int] = set()
            if args.hold_brand_ids:
                import glob
                for path in glob.glob(args.hold_brand_ids):
                    with open(path, encoding="utf-8") as fh:
                        held.update(r["id"] for r in json.load(fh))
            asyncio.run(stage_numbers(db, log, args.officers, args.limit, held))
        if args.find_numbers:
            print("\n== legal names with no number")
            asyncio.run(stage_find_numbers(db, log, args.officers, args.limit))
        if args.websites:
            print("\n== websites from collected pages")
            stage_websites(db, log)
        if args.site_details:
            print("\n== details printed on company websites")
            stage_site_details(db, log, args.limit)
        if args.load_research:
            print("\n== researched websites")
            asyncio.run(stage_load_research(db, log, args.load_research))
        print("\nwritten" if args.apply else "\nwould write")
        for k, v in sorted(log.counts.items()):
            print(f"  {v:>6,}  {k}")
        print(f"  {len(log.review):>6,}  rows listed for review")
        if args.apply:
            db.commit()
        else:
            db.rollback()
        print("run log:", log.save("_".join(stages)))
    finally:
        db.close()


if __name__ == "__main__":
    main()
