"""Assemble the data context for a city Supply, Pipeline & Ownership report.

Everything degrades gracefully: a section whose underlying table or
column is absent (e.g. ownership chains in a fresh environment) renders
as "not available" rather than failing the report.
"""
from __future__ import annotations

import datetime
import json
import re
from collections import defaultdict
from pathlib import Path

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

from app.models.models import Company, Council, ExistingScheme, PlanningApplication
from app.census_sources import published_value
from report.demand import affordability, balance_scenarios, gather_demand_context
from report.finance import gather_finance_context

APPROVED_RE = re.compile(r"approv|permit|grant|consent", re.I)
PENDING_RE = re.compile(r"submit|pending|regist|await|valid|consult", re.I)
REFUSED_RE = re.compile(r"refus|reject|withdraw|dismiss", re.I)

# Status-aware pipeline (classify_pipeline.py). A permitted bed is not a
# pipeline bed: the weight is the stated delivery assumption applied to
# each status to turn "maximum permitted" into "risk-weighted expected".
LIVE_STATUSES = ("under_construction", "pre_letting", "active_consent",
                 "consented_full", "consented_outline")
DELIVERY_WEIGHTS = {"under_construction": 1.0, "pre_letting": 1.0,
                    "active_consent": 0.8, "consented_full": 0.6,
                    "consented_outline": 0.4}
STATUS_LABELS = {
    "under_construction": "Under construction",
    "pre_letting": "Pre-letting",
    "active_consent": "Consent active (conditions being discharged)",
    "consented_full": "Full permission, inside implementation window",
    "consented_outline": "Outline / hybrid envelope (up to)",
    "submitted": "Awaiting decision",
    "dormant": "Dormant consent (no register activity in 3+ years)",
    "completed": "Completed (in the operational census)",
    "superseded": "Superseded by a later consent",
    "child": "Child application (conditions, amendments, listed building)",
    "refused": "Refused", "withdrawn": "Withdrawn",
}
TIER_ROOM_TYPES_EXCLUDED = ("From (advertised)", "To (advertised)", "Range")
INCENTIVE_CASH_RE = re.compile(r"£\s?([\d,]{2,6})(?!\s*(?:/|per|a)\s*(?:week|wk|pw))", re.I)
INCENTIVE_WEEKLY_RE = re.compile(r"£\s?(\d{1,3})\s*(?:off\s+)?(?:a|per|/)\s*(?:week|wk)|£\s?(\d{1,3})\s*(?:pw|p/w)\s*off", re.I)


def _beds(s: ExistingScheme) -> int:
    return s.beds_total or s.num_units or s.total_units or 0


def _table_exists(db: Session, name: str) -> bool:
    return db.execute(text("SELECT to_regclass(:n)"), {"n": name}).scalar() is not None


def _column_exists(db: Session, table: str, column: str) -> bool:
    return bool(db.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = :t AND column_name = :c"
    ), {"t": table, "c": column}).scalar())


def classify_status(status: str | None, decision: str | None) -> str:
    blob = f"{status or ''} {decision or ''}"
    if REFUSED_RE.search(blob):
        return "refused"
    if APPROVED_RE.search(blob):
        return "approved"
    if PENDING_RE.search(blob):
        return "pending"
    return "other"


def gather_city_context(db: Session, council_name: str,
                        publication: bool = False) -> dict:
    """Assemble the report context.

    publication=True keeps only values from sources we may publish
    (app.census_sources): schemes our scrapers have not observed are
    left out, and benchmark-only fields are blank and counted as gaps.
    """
    council = db.query(Council).filter(Council.name.ilike(council_name)).first()
    if council is None:
        raise SystemExit(f"Council {council_name!r} not found")

    # ------------------------------------------------------------- census
    all_pbsa = (
        db.query(ExistingScheme)
        .filter(ExistingScheme.council_id == council.id,
                ExistingScheme.scheme_type == "PBSA")
        .all()
    )
    operator_rows = {
        c.id: c for c in db.query(Company).filter(
            Company.id.in_([s.operator_company_id for s in all_pbsa
                            if s.operator_company_id] or [0])
        )
    }
    operators = {cid: c.name for cid, c in operator_rows.items()}

    def _is_university(s: ExistingScheme) -> bool:
        op = operator_rows.get(s.operator_company_id)
        return op is not None and (op.company_type or "") == "University"

    def _beds_of(s: ExistingScheme) -> int:
        if publication:
            return published_value(s, "beds_total") or 0
        return _beds(s)

    def _build_year_of(s: ExistingScheme):
        return published_value(s, "build_year") if publication else s.build_year

    def _operator_of(s: ExistingScheme):
        if publication:
            return published_value(s, "operator")
        return operators.get(s.operator_company_id)

    candidates = [s for s in all_pbsa if not _is_university(s)]
    unis = [s for s in all_pbsa if _is_university(s)]
    coverage = None
    if publication:
        observed = [s for s in candidates if published_value(s, "observed")]
        coverage = {
            "schemes_known": len(candidates),
            "schemes_observed": len(observed),
            "beds_sourced": sum(1 for s in observed if published_value(s, "beds_total")),
            "operator_sourced": sum(1 for s in observed if published_value(s, "operator")),
            "build_year_sourced": sum(1 for s in observed if published_value(s, "build_year")),
            "universities_known": len(unis),
            "universities_observed": sum(1 for s in unis if published_value(s, "observed")),
        }
        candidates = observed
        unis = [s for s in unis if published_value(s, "observed")]
    schemes = candidates
    university_stock = sorted((
        {
            "name": s.name,
            "university": operators.get(s.operator_company_id),
            "postcode": s.postcode,
            "beds": _beds_of(s),
        }
        for s in unis
    ), key=lambda r: -r["beds"])

    # Cheapest current advertised rent per scheme (append-only rows)
    rent_rows = db.execute(text("""
        SELECT scheme_id, MIN(rent_per_week) FROM scheme_rents
        WHERE is_current AND rent_per_week IS NOT NULL
          AND scheme_id = ANY(:sids)
        GROUP BY scheme_id
    """), {"sids": [s.id for s in all_pbsa] or [0]}).fetchall()
    min_rent = {r[0]: r[1] for r in rent_rows}

    census = sorted((
        {
            "name": s.name,
            "operator": _operator_of(s),
            "postcode": s.postcode,
            "beds": _beds_of(s),
            "build_year": _build_year_of(s),
            "operating_status": s.operating_status or "live",
            "nominations": bool(s.nominations),
            "from_rent_ppw": min_rent.get(s.id),
        }
        for s in schemes
    ), key=lambda r: -r["beds"])
    live = [r for r in census if r["operating_status"] != "closed"]
    total_beds = sum(r["beds"] for r in live)

    # ---------------------------------------------------- operator shares
    by_operator = defaultdict(int)
    for r in live:
        by_operator[r["operator"] or "Operator unconfirmed"] += r["beds"]
    operator_shares = sorted(
        ({"operator": k, "beds": v,
          "share": round(100 * v / total_beds, 1) if total_beds else 0}
         for k, v in by_operator.items()),
        key=lambda r: -r["beds"],
    )[:10]

    # ------------------------------------------------------------ vintage
    bands = [("pre-2010", None, 2009), ("2010-2014", 2010, 2014),
             ("2015-2019", 2015, 2019), ("2020-2023", 2020, 2023),
             ("2024+", 2024, None)]
    vintage = []
    unknown_build = sum(r["beds"] for r in live if not r["build_year"])
    for label, lo, hi in bands:
        beds = sum(
            r["beds"] for r in live
            if r["build_year"]
            and (lo is None or r["build_year"] >= lo)
            and (hi is None or r["build_year"] <= hi)
        )
        vintage.append({"band": label, "beds": beds})
    if unknown_build:
        vintage.append({"band": "year unknown", "beds": unknown_build})

    # ----------------------------------------------------------- pipeline
    apps = (
        db.query(PlanningApplication)
        .filter(PlanningApplication.council_id == council.id,
                or_(PlanningApplication.is_pbsa.is_(True),
                    PlanningApplication.scheme_type == "PBSA"))
        .all()
    )
    this_year = datetime.date.today().year
    classified = _column_exists(db, "planning_applications", "delivery_status")

    def status_of(a: PlanningApplication) -> str:
        """Status-aware class when classify_pipeline.py has run; else the
        decision-based class with the old 'derived' labels."""
        if classified and a.delivery_status:
            return a.delivery_status
        cls = classify_status(a.status, a.decision)
        if cls == "approved":
            return "consented_full"
        if cls == "pending":
            return "submitted"
        return cls

    pipe_rollup = defaultdict(lambda: {"schemes": 0, "beds": 0})
    status_rollup = defaultdict(lambda: {"rows": 0, "beds": 0, "beds_max": 0, "expected": 0.0,
                                         "unquantified": 0})
    by_delivery = defaultdict(lambda: {"beds": 0, "observed": 0, "derived": 0})
    for a in apps:
        cls = classify_status(a.status, a.decision)
        st = status_of(a)
        beds = a.pbsa_beds or 0
        # legacy rollup kept for the KPI strip (approved = live consents)
        legacy = ("approved" if st in LIVE_STATUSES else
                  "pending" if st == "submitted" else
                  cls if cls in ("refused",) else "other")
        pipe_rollup[legacy]["schemes"] += 1
        pipe_rollup[legacy]["beds"] += beds
        r = status_rollup[st]
        r["rows"] += 1
        r["beds"] += beds
        r["beds_max"] += a.beds_max or 0
        r["expected"] += beds * DELIVERY_WEIGHTS.get(st, 0.0)
        if st in LIVE_STATUSES and not beds:
            r["unquantified"] += 1
        if st in LIVE_STATUSES and a.expected_delivery_year and a.expected_delivery_year >= this_year:
            d = by_delivery[a.expected_delivery_year]
            d["beds"] += beds
            d[(a.delivery_year_basis if classified else None) or "derived"] += beds
    live_total = sum(status_rollup[s]["beds"] for s in LIVE_STATUSES)
    expected_total = round(sum(status_rollup[s]["expected"] for s in LIVE_STATUSES))
    status_order = list(LIVE_STATUSES) + ["submitted", "dormant", "completed", "superseded",
                                          "child", "refused", "withdrawn"]
    status_table = [
        {"status": s, "label": STATUS_LABELS.get(s, s), "rows": status_rollup[s]["rows"],
         "beds": status_rollup[s]["beds"], "beds_max": status_rollup[s]["beds_max"],
         "weight": DELIVERY_WEIGHTS.get(s), "expected": round(status_rollup[s]["expected"]),
         "unquantified": status_rollup[s]["unquantified"], "live": s in LIVE_STATUSES}
        for s in status_order if status_rollup[s]["rows"]
    ]
    show = [a for a in apps if status_of(a) in LIVE_STATUSES + ("submitted", "dormant")]
    top_apps = sorted(show, key=lambda a: (-(a.pbsa_beds or 0), a.reference or ""))[:16]
    unquantified = [
        {"reference": a.reference, "address": (a.address or "")[:60], "status": STATUS_LABELS.get(status_of(a), status_of(a)),
         "evidence": (a.delivery_status_evidence or "")[:90] if classified else ""}
        for a in apps if status_of(a) in LIVE_STATUSES and not a.pbsa_beds
    ]

    pipeline = {
        "classified": classified,
        "rollup": dict(pipe_rollup),
        "status_table": status_table,
        "live_total": live_total,
        "expected_total": expected_total,
        "weights": DELIVERY_WEIGHTS,
        "by_delivery_year": sorted(
            (y, v["beds"], v["observed"], v["derived"]) for y, v in by_delivery.items()),
        "dormant_beds": status_rollup["dormant"]["beds"],
        "dormant_rows": status_rollup["dormant"]["rows"],
        "submitted_beds": status_rollup["submitted"]["beds"],
        "completed_beds": status_rollup["completed"]["beds"],
        "unquantified": unquantified,
        "overdue_beds": 0,
        "top": [
            {
                "reference": a.reference,
                "address": (a.address or (a.description or "")[:80]),
                "status": STATUS_LABELS.get(status_of(a), status_of(a)),
                "status_key": status_of(a),
                "status_basis": (a.delivery_status_basis if classified else None) or "derived",
                "evidence": ((a.delivery_status_evidence or "") if classified else "")[:110],
                "beds": a.pbsa_beds,
                "beds_basis": (a.beds_basis if classified else None) or "stated",
                "decision_date": a.decision_date.isoformat() if a.decision_date else None,
                "applicant": a.applicant_name,
                "delivery_year": a.expected_delivery_year
                if status_of(a) in LIVE_STATUSES else None,
                "year_basis": (a.delivery_year_basis if classified else None) or "derived",
            }
            for a in top_apps
        ],
    }

    # ---------------------------------------------------------- ownership
    ownership = {"available": False, "owners": [], "ultimate": [], "chains": 0}
    owner_ids = [s.owner_company_id for s in schemes if s.owner_company_id]
    if owner_ids:
        owner_names = {
            c.id: c for c in db.query(Company).filter(Company.id.in_(owner_ids))
        }
        by_owner = defaultdict(int)
        for s in schemes:
            if s.owner_company_id and s.owner_company_id in owner_names:
                by_owner[owner_names[s.owner_company_id].name] += _beds(s)
        ownership["owners"] = sorted(
            ({"owner": k, "beds": v} for k, v in by_owner.items()),
            key=lambda r: -r["beds"],
        )[:10]
        ownership["available"] = bool(ownership["owners"])
    if _column_exists(db, "companies", "ultimate_owner_name"):
        rows = db.execute(text("""
            SELECT co.ultimate_owner_name, COUNT(*) AS n
            FROM existing_schemes es
            JOIN companies co ON co.id = COALESCE(es.owner_company_id, es.operator_company_id)
            WHERE es.council_id = :cid AND co.ultimate_owner_name IS NOT NULL
            GROUP BY 1 ORDER BY n DESC LIMIT 10
        """), {"cid": council.id}).fetchall()
        ownership["ultimate"] = [{"owner": r[0], "schemes": r[1]} for r in rows]
        ownership["available"] = ownership["available"] or bool(ownership["ultimate"])
    if _table_exists(db, "ownership_chain_nodes"):
        ownership["chains"] = db.execute(
            text("SELECT COUNT(DISTINCT company_id) FROM ownership_chain_nodes")
        ).scalar() or 0

    # -------------------------------------------------------- btr context
    btr = (
        db.query(ExistingScheme)
        .filter(ExistingScheme.council_id == council.id,
                ExistingScheme.scheme_type == "BTR")
        .all()
    )

    # ------------------------------------------------------------- demand
    demand = gather_demand_context(db, council.id)
    balance = afford = None
    if demand and demand["adjusted_students"]:
        uni_beds = sum(r["beds"] for r in university_stock)
        # Pipeline cases: risk-weighted expected delivery, then every live
        # consent at its maximum plus what is awaiting decision.
        approved = pipeline["expected_total"]
        identified = pipeline["live_total"] + pipeline["submitted_beds"]
        balance = balance_scenarios(
            demand["adjusted_students"], total_beds + uni_beds,
            approved, identified,
            model_growth=demand.get("cagr"),
        )
        afford = affordability(db, council.id, demand["max_loan"])
        # Two distinct measures, never both "beds per student":
        #   provision_rate  = beds / all FT students   (BONARD's definition)
        #   coverage        = beds / estimated rental demand pool (ours, derived)
        demand["provision_rate_pct"] = round(
            100 * (total_beds + uni_beds) / demand["adjusted_students"], 1)
        if demand.get("pool") and demand["pool"]["pool"]:
            demand["pool"]["beds"] = total_beds + uni_beds
            demand["pool"]["coverage"] = round(
                (total_beds + uni_beds) / demand["pool"]["pool"], 3)
            demand["pool"]["coverage_pct"] = round(100 * demand["pool"]["coverage"], 1)

    # --------------------------------------------- online letting tracker
    # Latest letting-page state per scheme: a dated availability signal,
    # never an occupancy rate.
    letting = None
    if _table_exists(db, "scheme_availability"):
        rows = db.execute(text("""
            WITH latest AS (
              SELECT DISTINCT ON (scheme_id) scheme_id, state, captured_at
              FROM scheme_availability ORDER BY scheme_id, captured_at DESC)
            SELECT es.name, l.state, l.captured_at::date, es.beds_total, es.build_year
            FROM latest l JOIN existing_schemes es ON es.id = l.scheme_id
            WHERE es.council_id = :cid AND es.scheme_type = 'PBSA'
        """), {"cid": council.id}).fetchall()
        if rows:
            sold = [r for r in rows if r[1] == "sold_out"]
            letting = {
                "checked": len(rows), "sold_out": len(sold),
                "sold_out_beds": sum((r[3] or 0) for r in sold),
                "captured": max(r[2] for r in rows).isoformat(),
                "not_checked": max(len(schemes) - len(rows), 0),
                "sold_out_list": sorted(
                    ({"name": r[0], "beds": r[3], "build_year": r[4]} for r in sold),
                    key=lambda x: -(x["beds"] or 0)),
            }

    # --------------------------------------------------- rents by room type
    rent_segments = [
        {"room_type": r[0], "tiers": r[1], "schemes": r[2],
         "q1": round(float(r[3])), "median": round(float(r[4])), "q3": round(float(r[5]))}
        for r in db.execute(text("""
            SELECT sr.room_type, COUNT(*), COUNT(DISTINCT sr.scheme_id),
                   percentile_cont(0.25) WITHIN GROUP (ORDER BY sr.rent_per_week),
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY sr.rent_per_week),
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY sr.rent_per_week)
            FROM scheme_rents sr JOIN existing_schemes es ON es.id = sr.scheme_id
            WHERE es.council_id = :cid AND sr.is_current AND sr.source = 'operator_page'
              AND sr.rent_per_week BETWEEN 60 AND 500 AND sr.room_type IS NOT NULL
            GROUP BY sr.room_type HAVING COUNT(*) >= 3 ORDER BY COUNT(*) DESC
        """), {"cid": council.id}).fetchall()
    ]

    # ---------------------------------------------------- rent headline
    # The headline is the median of advertised room-tier observations, not
    # of scheme "from" prices: a from-price is each scheme's cheapest tier
    # and pulls any pooled median down.
    hl = db.execute(text("""
        SELECT COUNT(*), COUNT(DISTINCT sr.scheme_id),
               percentile_cont(0.5) WITHIN GROUP (ORDER BY sr.rent_per_week)
        FROM scheme_rents sr JOIN existing_schemes es ON es.id = sr.scheme_id
        WHERE es.council_id = :cid AND sr.is_current AND sr.rent_per_week BETWEEN 60 AND 700
          AND sr.room_type IS NOT NULL AND sr.room_type <> ALL(:excluded)
    """), {"cid": council.id, "excluded": list(TIER_ROOM_TYPES_EXCLUDED)}).fetchone()
    rent_headline = None
    if hl and hl[0]:
        rent_headline = {"n": hl[0], "schemes": hl[1], "median": round(float(hl[2]))}
    median_rent = rent_headline["median"] if rent_headline else None

    # -------------------------------------------- incentives, effective rent
    # Advertised cash incentives netted off the scheme's tier median: the
    # first step toward net effective rent (contract length and bills
    # follow as the series builds).
    effective_rents = []
    if _table_exists(db, "scheme_observations"):
        inc_rows = db.execute(text("""
            WITH tiers AS (
              SELECT scheme_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY rent_per_week) AS med,
                     MAX(contract_length_weeks) AS weeks
              FROM scheme_rents WHERE is_current AND rent_per_week BETWEEN 60 AND 700
                AND room_type <> ALL(:excluded) GROUP BY scheme_id),
            inc AS (
              SELECT DISTINCT ON (scheme_id, value_text) scheme_id, value_text, observed_at
              FROM scheme_observations WHERE field = 'incentive' AND value_text IS NOT NULL
              ORDER BY scheme_id, value_text, observed_at DESC)
            SELECT es.name, inc.value_text, t.med, t.weeks
            FROM inc JOIN existing_schemes es ON es.id = inc.scheme_id
            JOIN tiers t ON t.scheme_id = inc.scheme_id
            WHERE es.council_id = :cid ORDER BY es.name
        """), {"cid": council.id, "excluded": list(TIER_ROOM_TYPES_EXCLUDED)}).fetchall()
        by_scheme: dict[str, dict] = {}
        for name, txt, med, weeks in inc_rows:
            cash = sum(int(m.group(1).replace(",", "")) for m in INCENTIVE_CASH_RE.finditer(txt or "")
                       if re.search(r"cash|voucher|rebate|credit|free", txt or "", re.I))
            weekly = sum(int(m.group(1) or m.group(2)) for m in INCENTIVE_WEEKLY_RE.finditer(txt or ""))
            e = by_scheme.setdefault(name, {"scheme": name, "tier_median": round(float(med)),
                                            "weeks": int(weeks) if weeks else 51, "cash": 0,
                                            "weekly_off": 0, "incentives": []})
            # Offers are alternatives, not cumulative: take the largest.
            e["cash"] = max(e["cash"], cash)
            e["weekly_off"] = max(e["weekly_off"], weekly)
            e["incentives"].append(txt)
        for e in by_scheme.values():
            if e["cash"] or e["weekly_off"]:
                e["effective"] = round(e["tier_median"] - e["cash"] / e["weeks"] - e["weekly_off"])
                e["discount_pct"] = round(100 * (1 - e["effective"] / e["tier_median"]), 1) if e["tier_median"] else None
                e["incentives"] = "; ".join(e["incentives"])[:120]
                effective_rents.append(e)
        effective_rents.sort(key=lambda e: -(e["discount_pct"] or 0))

    # ------------------------------------------------------------ finance
    finance = gather_finance_context(
        db, council.id, float(median_rent) if median_rent else None, total_beds)

    # -------------------------------------------------------- HMO context
    # Advertised HMO sample from the StuRents crawl (file-based: listings
    # are market context, not census schemes). Honest framing: a crawl
    # sample, not a whole-market share.
    hmo = None
    sturents_file = Path(__file__).resolve().parent.parent / "data" / "sturents" / (
        council.name.lower().replace(" ", "-") + ".jsonl")
    if sturents_file.exists():
        hmo_rents, pbsa_rents, hmo_beds = [], [], []
        total = 0
        for line in sturents_file.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            total += 1
            price = rec.get("rent_pppw_min") or rec.get("rent_pppw_max")
            if rec.get("is_pbsa_candidate"):
                if price:
                    pbsa_rents.append(float(price))
            else:
                if price:
                    hmo_rents.append(float(price))
                if rec.get("beds"):
                    hmo_beds.append(int(rec["beds"]))
        if hmo_rents:
            hmo_rents.sort()
            hmo = {
                "sample": total,
                "hmo_listings": len(hmo_rents),
                "median_ppw": round(hmo_rents[len(hmo_rents) // 2]),
                "avg_ppw": round(sum(hmo_rents) / len(hmo_rents)),
                "avg_beds": round(sum(hmo_beds) / len(hmo_beds), 1) if hmo_beds else None,
                "pbsa_avg_ppw": round(sum(pbsa_rents) / len(pbsa_rents))
                if pbsa_rents else None,
            }

    # ------------------------------------------------------------ sources
    source_mix = defaultdict(int)
    for s in schemes:
        source_mix[s.source or "unknown"] += 1

    return {
        "city": council.name,
        "generated": datetime.date.today().isoformat(),
        "publication": publication,
        "coverage": coverage,
        "census": census,
        "kpis": {
            "pbsa_schemes": len(live),
            "pbsa_beds": total_beds,
            "beds_opened_2024_plus": sum(r["beds"] for r in live if (r["build_year"] or 0) >= 2024),
            "operators": len([k for k in by_operator if k != "Operator unconfirmed"]),
            "pipeline_approved_beds": pipeline["live_total"],
            "pipeline_expected_beds": pipeline["expected_total"],
            "pipeline_pending_beds": pipeline["rollup"].get("pending", {}).get("beds", 0),
            "btr_schemes": len(btr),
            "btr_units": sum(_beds(s) or 0 for s in btr),
        },
        "rent_headline": rent_headline,
        "effective_rents": effective_rents,
        "letting": letting,
        "operator_shares": operator_shares,
        "university_stock": university_stock,
        "rent_coverage": len([r for r in census if r["from_rent_ppw"]]),
        "vintage": vintage,
        "pipeline": pipeline,
        "demand": demand,
        "balance": balance,
        "afford": afford,
        "hmo": hmo,
        "finance": finance,
        "rent_segments": rent_segments,
        "ownership": ownership,
        "source_mix": sorted(source_mix.items(), key=lambda kv: -kv[1]),
    }
