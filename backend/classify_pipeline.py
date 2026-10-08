"""Classify a city's PBSA planning rows by what they actually are.

The register holds consents, their children (condition discharges,
non-material amendments, S73 variations, listed-building consents,
screening requests, adverts) and envelopes (outline/hybrid "up to N").
Counting every approved row's beds as "approved pipeline" overstates it;
flagging every old consent as "overdue" misses the ones that were built.

Per row this sets:
  permission_type        full / outline / hybrid / child types / other
  parent_reference       the consent a child belongs to
  superseded_by_reference a later full consent for the same site
  beds / beds_max        committed count vs "up to" envelope (beds_basis says which)
  delivery_status (+basis, evidence, date)
      completed          site hosts a live census scheme, or dated evidence
      under_construction / pre_letting   dated evidence (operator pages)
      active_consent     children decided/submitted in the last 24 months
      consented_full     full permission inside the 3-year implementation window
      consented_outline  outline/hybrid permission (envelope only)
      submitted          awaiting decision
      dormant            full permission older than 3 years, no register activity
      superseded / child / refused / withdrawn   excluded from pipeline totals
  expected_delivery_year (+ delivery_year_basis observed/derived)

Evidence beyond the register comes from data/pipeline_evidence_<city>.json
(dated, sourced observations such as an operator's "opening September 2027").

Usage: .venv/bin/python classify_pipeline.py --city Birmingham [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
from collections import defaultdict
from pathlib import Path

from sqlalchemy import or_

from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme, PlanningApplication
from app.scrapers.pbsa_beds import parse_beds, parse_beds_max
from app.scrapers.scheme_matching import norm_name, norm_pc

REF_RE = re.compile(r"\b(\d{4}/\d{4,5}/PA)\b", re.I)
CHILD_PATTERNS = [
    ("conditions", re.compile(r"determine (?:the )?details|discharge of condition|details reserved by condition|"
                              r"approval of details|condition(?:s)? (?:no\.?|number)", re.I)),
    ("s73_variation", re.compile(r"variation of condition|section 73|\bs73\b|removal of condition", re.I)),
    ("nma", re.compile(r"non[-\s]?material amendment|minor material amendment", re.I)),
    ("listed_building", re.compile(r"listed building consent", re.I)),
    ("screening", re.compile(r"screening (?:request|opinion)|scoping opinion", re.I)),
    ("advert", re.compile(r"display of .{0,40}sign|advertisement consent|illuminated", re.I)),
    ("temporary_use", re.compile(r"temporary (?:use|occupation)", re.I)),
]
EXTENSION_RE = re.compile(r"\b(extension|additional|roof(?:top)? extension|infill)\b", re.I)
IMPLEMENT_YEARS = 3          # standard implementation window for a full consent
ACTIVE_MONTHS = 24           # register activity that marks a consent as alive
BUILD_YEARS_FULL = 2
BUILD_YEARS_OUTLINE = 4
MIN_SCHEME_BEDS = 50


def classify_status(status: str | None, decision: str | None) -> str:
    blob = f"{status or ''} {decision or ''}"
    if re.search(r"withdraw", blob, re.I):
        return "withdrawn"
    if re.search(r"refus|reject|dismiss", blob, re.I):
        return "refused"
    if re.search(r"approv|permit|grant|consent", blob, re.I):
        return "approved"
    if re.search(r"submit|pending|regist|await|valid|consult|undecided", blob, re.I):
        return "pending"
    return "other"


def permission_type_of(a: PlanningApplication) -> str:
    desc = a.description or ""
    at = (a.application_type or "").lower()
    for label, pat in CHILD_PATTERNS:
        if pat.search(desc):
            return label
    if at == "conditions":
        return "conditions"
    if at == "amendment":
        return "nma"
    if at == "heritage" and "listed" in desc.lower():
        return "listed_building"
    if re.search(r"\bhybrid\b", desc, re.I):
        return "hybrid"
    if at == "outline" or re.search(r"\boutline\b", desc, re.I):
        return "outline"
    if at == "full" or re.search(r"full (?:planning )?application|full planning", desc, re.I):
        return "full"
    return "other"


def site_key(a: PlanningApplication) -> str:
    pc = norm_pc(a.postcode)
    if not pc:
        m = re.search(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b", (a.address or "").upper())
        pc = norm_pc(m.group(0)) if m else ""
    return pc


def months_between(d1: datetime.date, d2: datetime.date) -> int:
    return (d2.year - d1.year) * 12 + (d2.month - d1.month)


STREET_STOP = {"birmingham", "city", "centre", "land", "at", "of", "and", "the", "to", "road", "street",
               "lane", "edgbaston", "selly", "oak", "aston", "nechells", "digbeth", "hockley", "newtown"}


def _leading_number(addr: str | None) -> str | None:
    m = re.match(r"\s*(?:land\s+(?:at|rear\s+of|adjacent\s+to)\s+)?(\d+[a-z]?)(?:\s*[-–]\s*\d+[a-z]?)?\b", (addr or "").lower())
    return m.group(1) if m else None


def same_site(a: PlanningApplication, b: PlanningApplication) -> bool:
    """Same postcode, same leading street number when both carry one, and a
    shared street token: two consents for one building, not two neighbours."""
    if not site_key(a) or site_key(a) != site_key(b):
        return False
    na, nb = _leading_number(a.address), _leading_number(b.address)
    if na and nb and na != nb:
        return False
    ta = {t for t in norm_name(a.address or "") if t not in STREET_STOP and not t.isdigit()}
    tb = {t for t in norm_name(b.address or "") if t not in STREET_STOP and not t.isdigit()}
    return bool(ta & tb) or not (ta and tb)


def implements(consent: PlanningApplication, scheme: ExistingScheme) -> bool:
    """A live scheme on the consent's postcode is that consent built out only
    when it is of the consent's size, or opened after the decision."""
    beds = consent.pbsa_beds or 0
    s_beds = scheme.beds_total or scheme.total_units or 0
    if consent.decision_date and scheme.build_year and scheme.build_year >= consent.decision_date.year - 1:
        return True
    return bool(beds and s_beds and 0.6 <= s_beds / beds <= 1.6)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    today = datetime.date.today()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    apps = (db.query(PlanningApplication)
            .filter(PlanningApplication.council_id == council.id,
                    or_(PlanningApplication.is_pbsa.is_(True),
                        PlanningApplication.scheme_type == "PBSA",
                        PlanningApplication.description.ilike("%student%")))
            .all())
    by_ref = {a.reference: a for a in apps if a.reference}

    # Evidence file: dated observations keyed by reference
    evidence = {}
    ev_path = Path(__file__).parent / "data" / f"pipeline_evidence_{args.city.lower().replace(' ', '_')}.json"
    if ev_path.exists():
        for e in json.loads(ev_path.read_text())["entries"]:
            evidence[e["reference"]] = e

    # Live schemes: a consent whose site hosts one is complete
    schemes = (db.query(ExistingScheme)
               .filter(ExistingScheme.council_id == council.id,
                       ExistingScheme.scheme_type == "PBSA").all())
    ops = {c.id: c for c in db.query(Company).filter(
        Company.id.in_([s.operator_company_id for s in schemes if s.operator_company_id] or [0]))}
    live_by_pc: dict[str, list[ExistingScheme]] = defaultdict(list)
    for s in schemes:
        if (s.operating_status or "live") in ("live", "no_letting_presence") and s.postcode:
            live_by_pc[norm_pc(s.postcode)].append(s)
    uni_names = [norm_name(s.name) for s in schemes
                 if (ops.get(s.operator_company_id) and (ops[s.operator_company_id].company_type or "") == "University")]

    # ---- pass 1: permission type, parents, beds ------------------------
    for a in apps:
        a.permission_type = permission_type_of(a)
    children = {"conditions", "s73_variation", "nma", "listed_building", "screening", "advert", "temporary_use"}
    for a in apps:
        if a.permission_type in children:
            refs = [r.upper() for r in REF_RE.findall(a.description or "") if r.upper() != (a.reference or "").upper()]
            parent = next((r for r in refs if r in by_ref and by_ref[r].permission_type not in children), None)
            if parent is None:
                key = site_key(a)
                cands = [b for b in apps if b is not a and b.permission_type not in children
                         and site_key(b) == key and key]
                cands.sort(key=lambda b: (b.decision_date or datetime.date.min), reverse=True)
                parent = cands[0].reference if cands else (refs[0] if refs else None)
            a.parent_reference = parent
        else:
            a.parent_reference = None

    for a in apps:
        if a.permission_type in children:
            continue
        desc = a.description or ""
        if a.permission_type in ("outline", "hybrid"):
            env = parse_beds_max(desc) or parse_beds(desc)
            a.beds_max = env
            if env:
                a.pbsa_beds, a.beds_basis = env, "up_to"
        elif a.pbsa_beds is None:
            n = parse_beds(desc)
            if n:
                a.pbsa_beds, a.beds_basis = n, "stated"
        elif not a.beds_basis:
            a.beds_basis = "stated"
    # S73 / NMA that states a new total moves it to the parent
    for a in apps:
        if a.permission_type in ("s73_variation", "nma") and a.parent_reference in by_ref:
            m = re.search(r"(?:from\s+)?(\d{2,4})\s+to\s+(\d{2,4})\s+(?:student\s+)?bed", a.description or "", re.I)
            parent = by_ref[a.parent_reference]
            if m:
                parent.pbsa_beds, parent.beds_basis = int(m.group(2)), "variation"
            a.pbsa_beds, a.beds_basis = None, "child"
        elif a.permission_type in children:
            a.pbsa_beds, a.beds_basis = None, "child"

    # Register activity per parent: children decided/submitted recently
    activity: dict[str, list[datetime.date]] = defaultdict(list)
    for a in apps:
        if a.permission_type in children and a.parent_reference:
            d = a.decision_date or a.submitted_date or a.submission_date or a.validated_date
            if d:
                activity[a.parent_reference].append(d)

    # ---- pass 2: superseded (older full consent, same site, newer consent) ----
    consents = [a for a in apps if a.permission_type not in children
                and classify_status(a.status, a.decision) == "approved"
                and (a.pbsa_beds or 0) >= MIN_SCHEME_BEDS]
    by_site: dict[str, list[PlanningApplication]] = defaultdict(list)
    for a in consents:
        k = site_key(a)
        if k:
            by_site[k].append(a)
    for k, rows in by_site.items():
        rows.sort(key=lambda b: (b.decision_date or datetime.date.min))
        for i, older in enumerate(rows[:-1]):
            newer = rows[-1]
            if EXTENSION_RE.search(newer.description or "") or older.reference in evidence:
                continue
            if not same_site(older, newer):
                continue
            older.superseded_by_reference = newer.reference

    # ---- pass 3: delivery status --------------------------------------
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"rows": 0, "beds": 0, "beds_max": 0})
    for a in apps:
        cls = classify_status(a.status, a.decision)
        status, basis, ev, year, ybasis = None, "derived", None, None, None
        e = evidence.get(a.reference)
        if a.permission_type in children:
            status, ev = "child", f"{a.permission_type} application under {a.parent_reference or 'an earlier consent'}"
        elif cls in ("refused", "withdrawn"):
            status, basis, ev = cls, "observed", f"{a.status} {a.decision or ''}".strip()
        elif e:
            status, basis = e["status"], e.get("basis", "observed")
            ev = f"{e['evidence']} [{e['source']}, {e['observed_at']}]"
            year, ybasis = e.get("opening_year"), (basis if e.get("opening_year") else None)
        elif a.superseded_by_reference:
            status, ev = "superseded", f"later consent {a.superseded_by_reference} for the same site"
        else:
            hits = [s for s in live_by_pc.get(site_key(a), []) if implements(a, s)]
            on_live_site = bool(live_by_pc.get(site_key(a)))
            if cls == "approved" and on_live_site and (a.pbsa_beds or 0) < MIN_SCHEME_BEDS:
                # Alterations, extensions and re-fits of a scheme that is
                # already operating: not pipeline, not a new consent.
                s = live_by_pc[site_key(a)][0]
                status, ev = "ancillary", f"works to the operating scheme {s.name} at the same postcode" + (f" ({a.pbsa_beds} beds)" if a.pbsa_beds else "")
            elif cls == "approved" and hits and (a.pbsa_beds or 0) >= MIN_SCHEME_BEDS \
                    and not EXTENSION_RE.search(a.description or ""):
                s = hits[0]
                status, ev = "completed", f"live scheme {s.name} ({s.beds_total or s.total_units or '?'} beds) at the same postcode"
            elif cls == "approved":
                recent = [d for d in activity.get(a.reference, []) if months_between(d, today) <= ACTIVE_MONTHS]
                age_years = (today - a.decision_date).days / 365.25 if a.decision_date else 0
                if a.permission_type in ("outline", "hybrid"):
                    status, ev = "consented_outline", f"{a.permission_type} permission; envelope up to {a.beds_max or a.pbsa_beds or '?'} beds"
                    if recent:
                        ev += f"; {len(recent)} child applications in the last {ACTIVE_MONTHS} months"
                elif recent:
                    status, ev = "active_consent", f"{len(recent)} child applications on the register in the last {ACTIVE_MONTHS} months (latest {max(recent).isoformat()})"
                elif age_years > IMPLEMENT_YEARS:
                    status, ev = "dormant", f"full permission {a.decision_date.isoformat()}; no register activity in {IMPLEMENT_YEARS}+ years, implementation window passed"
                else:
                    status, ev = "consented_full", f"full permission {a.decision_date.isoformat() if a.decision_date else ''}; inside the {IMPLEMENT_YEARS}-year implementation window"
            elif cls == "pending":
                status, basis, ev = "submitted", "observed", "awaiting decision"
            else:
                status, ev = "submitted", "no decision recorded on the register"
        # delivery year: evidence first, otherwise an earliest-possible derived year
        if year is None:
            # Earliest-possible year, never a forecast: a full consent needs
            # a build after its decision and cannot open before next year;
            # an outline envelope has no date until reserved matters appear.
            if status in ("consented_full", "active_consent") and a.decision_date:
                year, ybasis = max(a.decision_date.year + BUILD_YEARS_FULL, today.year + 1), "derived"
            elif status == "under_construction":
                year, ybasis = today.year + 1, "derived"
        if status in ("completed", "dormant", "superseded", "child", "ancillary", "refused", "withdrawn", "submitted") and ybasis != "observed":
            year, ybasis = None, None
        a.delivery_status, a.delivery_status_basis = status, basis
        a.delivery_status_evidence, a.delivery_status_at = ev, today
        a.expected_delivery_year, a.delivery_year_basis = year, ybasis
        if status == "completed":
            a.construction_status, a.construction_evidence_at = "complete", today
            a.construction_source = (e or {}).get("source") or "census"
        elif status == "under_construction":
            a.construction_status, a.construction_evidence_at = "under_construction", today
            a.construction_source = (e or {}).get("source")
        c = counts[status]
        c["rows"] += 1
        c["beds"] += a.pbsa_beds or 0
        c["beds_max"] += a.beds_max or 0

    print(f"{args.city}: {len(apps)} student applications classified")
    for st, c in sorted(counts.items(), key=lambda kv: -kv[1]["beds"]):
        print(f"  {st:20s} rows {c['rows']:4d}  beds {c['beds']:6,d}" + (f"  (envelopes {c['beds_max']:,})" if c["beds_max"] else ""))
    print("\nLargest rows by status:")
    for a in sorted(apps, key=lambda x: -(x.pbsa_beds or 0))[:40]:
        print(f"  {a.reference:16s} {a.delivery_status:18s} {str(a.pbsa_beds or '-'):>6s} {a.beds_basis or '':10s} "
              f"{str(a.expected_delivery_year or '-'):>5s} {a.delivery_year_basis or '':9s} {(a.address or '')[:48]}")
    if args.dry_run:
        db.rollback()
        print("\n(dry run, nothing written)")
    else:
        db.commit()
        print("\nwritten")
    db.close()


if __name__ == "__main__":
    main()
