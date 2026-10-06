"""Assemble the data context for a city Supply, Pipeline & Ownership report.

Everything degrades gracefully: a section whose underlying table or
column is absent (e.g. ownership chains in a fresh environment) renders
as "not available" rather than failing the report.
"""
from __future__ import annotations

import datetime
import re
from collections import defaultdict

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

from app.models.models import Company, Council, ExistingScheme, PlanningApplication

APPROVED_RE = re.compile(r"approv|permit|grant|consent", re.I)
PENDING_RE = re.compile(r"submit|pending|regist|await|valid|consult", re.I)
REFUSED_RE = re.compile(r"refus|reject|withdraw|dismiss", re.I)


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


def gather_city_context(db: Session, council_name: str) -> dict:
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

    schemes = [s for s in all_pbsa if not _is_university(s)]
    university_stock = sorted((
        {
            "name": s.name,
            "university": operators.get(s.operator_company_id),
            "postcode": s.postcode,
            "beds": _beds(s),
        }
        for s in all_pbsa if _is_university(s)
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
            "operator": operators.get(s.operator_company_id),
            "postcode": s.postcode,
            "beds": _beds(s),
            "build_year": s.build_year,
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
    pipe_rollup = defaultdict(lambda: {"schemes": 0, "beds": 0})
    by_delivery = defaultdict(int)
    for a in apps:
        cls = classify_status(a.status, a.decision)
        beds = a.pbsa_beds or 0
        pipe_rollup[cls]["schemes"] += 1
        pipe_rollup[cls]["beds"] += beds
        if (cls == "approved" and a.expected_delivery_year
                and a.expected_delivery_year >= datetime.date.today().year):
            by_delivery[a.expected_delivery_year] += beds
    top_apps = sorted(
        (a for a in apps if classify_status(a.status, a.decision) != "refused"),
        key=lambda a: -(a.pbsa_beds or 0),
    )[:12]
    pipeline = {
        "rollup": dict(pipe_rollup),
        "by_delivery_year": sorted(by_delivery.items()),
        "top": [
            {
                "reference": a.reference,
                "address": (a.address or (a.description or "")[:80]),
                "status": a.status or a.decision,
                "beds": a.pbsa_beds,
                "decision_date": a.decision_date.isoformat() if a.decision_date else None,
                "applicant": a.applicant_name,
                "delivery_year": a.expected_delivery_year,
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

    # ------------------------------------------------------------ sources
    source_mix = defaultdict(int)
    for s in schemes:
        source_mix[s.source or "unknown"] += 1

    return {
        "city": council.name,
        "generated": datetime.date.today().isoformat(),
        "census": census,
        "kpis": {
            "pbsa_schemes": len(live),
            "pbsa_beds": total_beds,
            "operators": len([k for k in by_operator if k != "Operator unconfirmed"]),
            "pipeline_approved_beds": pipeline["rollup"].get("approved", {}).get("beds", 0),
            "pipeline_pending_beds": pipeline["rollup"].get("pending", {}).get("beds", 0),
            "btr_schemes": len(btr),
            "btr_units": sum(_beds(s) or 0 for s in btr),
        },
        "operator_shares": operator_shares,
        "university_stock": university_stock,
        "rent_coverage": len([r for r in census if r["from_rent_ppw"]]),
        "vintage": vintage,
        "pipeline": pipeline,
        "ownership": ownership,
        "source_mix": sorted(source_mix.items(), key=lambda kv: -kv[1]),
    }
