"""Backfill bd_score + bd_score_breakdown for schemes and applications.

Usage:
    python backfill_bd_scores.py --schemes        # 32k BTR/PBSA schemes
    python backfill_bd_scores.py --applications   # applications (BTR/PBSA/Senior/Co-living + recent)
    python backfill_bd_scores.py --all            # both

Batched commits + progress logging.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.scoring.bd_scorer import BDScorer
from app.models.models import ExistingScheme, PlanningApplication

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
BATCH_SIZE = 500


def backfill_schemes(db: Session, scorer: BDScorer) -> dict[str, int]:
    """Score all BTR/PBSA schemes."""
    print("\n=== Backfilling BTR/PBSA schemes ===")
    start = time.time()
    stats = {"scored": 0, "skipped": 0}
    now = datetime.now(timezone.utc)

    # Pull IDs first, then iterate to avoid keeping ~32k objects in memory.
    # Exclude procurement-feed noise (find_a_tender / contracts_finder) — they
    # are service contracts, not BTR/PBSA buildings. Their contract_end_date
    # would otherwise dominate urgency scoring incorrectly.
    ids = [r[0] for r in db.execute(text("""
        SELECT id FROM existing_schemes
        WHERE scheme_type IN ('BTR','PBSA','Senior','Co-living')
          AND (source IS NULL OR source NOT IN ('find_a_tender','contracts_finder'))
        ORDER BY id
    """))]
    total = len(ids)
    print(f"  {total} schemes to score (procurement noise excluded)")

    # Null out scores on the noise rows so they don't leak into the dashboard
    db.execute(text("""
        UPDATE existing_schemes
        SET bd_score = NULL,
            bd_score_breakdown = NULL,
            bd_score_updated_at = NULL
        WHERE source IN ('find_a_tender','contracts_finder')
    """))
    db.commit()

    batch: list[dict] = []
    for i, sid in enumerate(ids, 1):
        s = db.get(ExistingScheme, sid)
        if s is None:
            stats["skipped"] += 1
            continue
        try:
            br = scorer.score_existing_scheme_breakdown(s)
        except Exception as e:
            print(f"  ! id={sid} error: {e}")
            stats["skipped"] += 1
            continue
        batch.append({
            "id": sid,
            "score": br["composite"],
            "breakdown": br,
            "ts": now,
        })
        stats["scored"] += 1

        if len(batch) >= BATCH_SIZE or i == total:
            db.execute(text("""
                UPDATE existing_schemes SET
                    bd_score = :score,
                    bd_score_breakdown = CAST(:breakdown AS jsonb),
                    bd_score_updated_at = :ts
                WHERE id = :id
            """), [
                {"id": r["id"], "score": r["score"],
                 "breakdown": _to_jsonb(r["breakdown"]),
                 "ts": r["ts"]}
                for r in batch
            ])
            db.commit()
            elapsed = time.time() - start
            rate = stats["scored"] / elapsed if elapsed else 0
            print(f"  ...{i}/{total} scored ({100*i/total:.0f}%) at {rate:.0f}/s")
            batch.clear()

    print(f"  Done in {time.time()-start:.0f}s")
    return stats


def backfill_applications(db: Session, scorer: BDScorer) -> dict[str, int]:
    """Score BTR/PBSA/Senior + recent (last 24mo) applications."""
    print("\n=== Backfilling planning applications ===")
    start = time.time()
    stats = {"scored": 0, "skipped": 0}
    now = datetime.now(timezone.utc)

    ids = [r[0] for r in db.execute(text("""
        SELECT id FROM planning_applications
        WHERE scheme_type IN ('BTR','PBSA','Senior','Co-living','Affordable','Mixed')
           OR (submission_date >= NOW() - INTERVAL '24 months'
               AND scheme_type IN ('Residential'))
        ORDER BY id
    """))]
    total = len(ids)
    print(f"  {total} applications to score")

    batch: list[dict] = []
    for i, aid in enumerate(ids, 1):
        a = db.get(PlanningApplication, aid)
        if a is None:
            stats["skipped"] += 1
            continue
        try:
            br = scorer.score_planning_application_breakdown(a)
        except Exception as e:
            print(f"  ! id={aid} error: {e}")
            stats["skipped"] += 1
            continue
        batch.append({
            "id": aid,
            "score": br["composite"],
            "breakdown": br,
            "ts": now,
        })
        stats["scored"] += 1

        if len(batch) >= BATCH_SIZE or i == total:
            db.execute(text("""
                UPDATE planning_applications SET
                    bd_score = :score,
                    bd_score_breakdown = CAST(:breakdown AS jsonb),
                    bd_score_updated_at = :ts
                WHERE id = :id
            """), [
                {"id": r["id"], "score": r["score"],
                 "breakdown": _to_jsonb(r["breakdown"]),
                 "ts": r["ts"]}
                for r in batch
            ])
            db.commit()
            elapsed = time.time() - start
            rate = stats["scored"] / elapsed if elapsed else 0
            eta = (total - i) / rate if rate > 0 else 0
            print(f"  ...{i}/{total} scored ({100*i/total:.0f}%) at {rate:.0f}/s "
                  f"ETA={eta:.0f}s")
            batch.clear()

    print(f"  Done in {time.time()-start:.0f}s")
    return stats


def _to_jsonb(d: dict) -> str:
    import json
    return json.dumps(d)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schemes", action="store_true")
    ap.add_argument("--applications", action="store_true")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    if args.all:
        args.schemes = args.applications = True
    if not (args.schemes or args.applications):
        print("Pass --schemes, --applications, or --all")
        sys.exit(1)

    engine = create_engine(DB_URL)
    overall_start = time.time()
    with Session(engine) as db:
        scorer = BDScorer(db_session=db)
        if args.schemes:
            s = backfill_schemes(db, scorer)
            print(f"\nSchemes: scored={s['scored']} skipped={s['skipped']}")
        if args.applications:
            s = backfill_applications(db, scorer)
            print(f"\nApplications: scored={s['scored']} skipped={s['skipped']}")
    print(f"\nTotal elapsed: {time.time()-overall_start:.0f}s")


if __name__ == "__main__":
    main()
