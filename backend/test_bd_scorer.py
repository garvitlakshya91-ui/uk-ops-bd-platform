"""Sanity tests for the BD scorer.

Runs the scorer against:
1. A few hand-picked existing schemes (high CSAT gap, contract close, etc)
2. A few hand-picked planning applications (BTR vs Residential vs unknown)
3. Edge cases (NULL units, NULL dates, missing fields)

Asserts: scores are in [0,100], weighted composite matches sum of components,
known high-signal records score above threshold.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.scoring.bd_scorer import (
    BDScorer, _EXISTING_WEIGHTS, _APPLICATION_WEIGHTS,
)
from app.models.models import ExistingScheme, PlanningApplication

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)


def check(name: str, expr: bool, msg: str = "") -> bool:
    status = "PASS" if expr else "FAIL"
    print(f"  [{status}] {name}{(' — ' + msg) if msg else ''}")
    return expr


def main() -> None:
    engine = create_engine(DB_URL)
    failures = 0

    with Session(engine) as db:
        scorer = BDScorer(db_session=db)

        # ---------- Test 1: BTR/PBSA scheme scoring ----------
        print("\n=== Test 1: Existing scheme scoring ===")
        schemes = db.query(ExistingScheme).filter(
            ExistingScheme.scheme_type.in_(["BTR", "PBSA"]),
        ).limit(10).all()
        if not schemes:
            print("  [FAIL] no BTR/PBSA schemes")
            failures += 1
        for s in schemes[:5]:
            br = scorer.score_existing_scheme_breakdown(s)
            composite = br["composite"]
            # All four dimensions present
            ok_dims = all(k in br for k in _EXISTING_WEIGHTS)
            # Composite math right
            calc = sum(_EXISTING_WEIGHTS[k] * br[k] for k in _EXISTING_WEIGHTS)
            calc = round(min(max(calc, 0.0), 100.0), 1)
            if not check(f"scheme id={s.id} {s.name[:30]}: composite={composite}",
                         0 <= composite <= 100 and ok_dims and abs(calc - composite) < 0.2,
                         f"calc={calc} dims={list(br.keys())}"):
                failures += 1

        # ---------- Test 2: planning application scoring ----------
        print("\n=== Test 2: Application scoring ===")
        # Pick varied apps
        for filt, label in [
            ("scheme_type='BTR' AND num_units > 200", "BTR-large"),
            ("scheme_type='PBSA'", "PBSA"),
            ("scheme_type='Residential'", "Residential"),
            ("scheme_type IS NULL OR scheme_type='Unknown'", "Unknown"),
        ]:
            row = db.execute(text(f"""
                SELECT id FROM planning_applications
                WHERE {filt} ORDER BY submission_date DESC NULLS LAST LIMIT 1
            """)).first()
            if not row:
                print(f"  [SKIP] no {label} app found")
                continue
            app = db.get(PlanningApplication, row[0])
            br = scorer.score_planning_application_breakdown(app)
            composite = br["composite"]
            calc = sum(_APPLICATION_WEIGHTS[k] * br[k] for k in _APPLICATION_WEIGHTS)
            calc = round(min(max(calc, 0.0), 100.0), 1)
            if not check(f"app id={app.id} ({label}): composite={composite}",
                         0 <= composite <= 100 and abs(calc - composite) < 0.2,
                         f"calc={calc} st={app.scheme_type} u={app.num_units}"):
                failures += 1

        # ---------- Test 3: BTR-large should outscore Unknown ----------
        print("\n=== Test 3: BTR-large scores higher than Unknown ===")
        btr_row = db.execute(text(
            "SELECT id FROM planning_applications "
            "WHERE scheme_type='BTR' AND num_units > 200 "
            "AND submission_date >= NOW() - INTERVAL '6 months' "
            "ORDER BY num_units DESC LIMIT 1"
        )).first()
        unk_row = db.execute(text(
            "SELECT id FROM planning_applications "
            "WHERE (scheme_type IS NULL OR scheme_type='Unknown') "
            "AND num_units IS NULL "
            "AND submission_date < NOW() - INTERVAL '1 year' "
            "LIMIT 1"
        )).first()
        if btr_row and unk_row:
            btr_app = db.get(PlanningApplication, btr_row[0])
            unk_app = db.get(PlanningApplication, unk_row[0])
            btr_s = scorer.score_planning_application(btr_app)
            unk_s = scorer.score_planning_application(unk_app)
            if not check(f"BTR-large ({btr_s}) > Unknown-old ({unk_s})",
                         btr_s > unk_s + 20):
                failures += 1

        # ---------- Test 4: contract-close scheme outscores no-contract scheme ----------
        print("\n=== Test 4: contract-close scheme scores higher ===")
        close_row = db.execute(text("""
            SELECT id FROM existing_schemes
            WHERE contract_end_date IS NOT NULL
              AND contract_end_date BETWEEN NOW() AND NOW() + INTERVAL '6 months'
            LIMIT 1
        """)).first()
        far_row = db.execute(text("""
            SELECT id FROM existing_schemes
            WHERE contract_end_date IS NULL
              AND scheme_type IN ('BTR','PBSA') LIMIT 1
        """)).first()
        if close_row and far_row:
            c_s = scorer.score_existing_scheme(db.get(ExistingScheme, close_row[0]))
            f_s = scorer.score_existing_scheme(db.get(ExistingScheme, far_row[0]))
            if not check(
                f"contract-close ({c_s}) > no-contract ({f_s}) on urgency",
                c_s > f_s,
            ):
                failures += 1

        # ---------- Test 5: weights sum to 1.0 ----------
        print("\n=== Test 5: weights sum to 1.0 ===")
        if not check(
            f"existing weights sum={sum(_EXISTING_WEIGHTS.values())}",
            abs(sum(_EXISTING_WEIGHTS.values()) - 1.0) < 0.001,
        ):
            failures += 1
        if not check(
            f"application weights sum={sum(_APPLICATION_WEIGHTS.values())}",
            abs(sum(_APPLICATION_WEIGHTS.values()) - 1.0) < 0.001,
        ):
            failures += 1

    print(f"\n{'='*40}")
    print(f"Result: {failures} failures")
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
