"""Re-classify planning_applications.scheme_type using the updated classifier.

Background: we extended ``BTR_APPLICANT_KEYWORDS`` with ~25 regional UK BTR
developers (Arena Central, Court Collaboration, Glenbrook, etc.) and added
high-signal description phrases. Existing rows still carry their old
classification, so this script re-runs the classifier and writes back any
rows whose label changed.

Modes:
    --council Birmingham     # only re-classify a specific council's rows
    --all                    # re-classify all apps (~1.35M, ~10-20 min)
    --dry-run                # report counts only, don't write

Output: per-direction transition counts (e.g. Residential→BTR: N).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.models.models import PlanningApplication
from app.scrapers.base import BaseScraper

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", help="Council name (e.g. Birmingham) to re-classify only that council's apps")
    ap.add_argument("--all", action="store_true", help="Re-classify all applications")
    ap.add_argument("--dry-run", action="store_true", help="Report counts without writing")
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    if not args.council and not args.all:
        print("Pass --council Birmingham OR --all")
        sys.exit(1)

    engine = create_engine(DB_URL)

    # Build id list
    with engine.connect() as c:
        if args.council:
            ids = [r[0] for r in c.execute(text("""
                SELECT pa.id FROM planning_applications pa
                JOIN councils co ON co.id = pa.council_id
                WHERE co.name = :n ORDER BY pa.id
            """), {"n": args.council})]
        else:
            ids = [r[0] for r in c.execute(text(
                "SELECT id FROM planning_applications ORDER BY id"
            ))]
    print(f"Candidates to re-classify: {len(ids):,}")

    # Process in batches
    transitions = Counter()
    total_changed = 0
    start = time.time()
    batch: list[tuple[int, str]] = []

    with Session(engine) as db:
        for i, aid in enumerate(ids, 1):
            app = db.get(PlanningApplication, aid)
            if app is None:
                continue
            old_type = (app.scheme_type or "Unknown")
            new_type = BaseScraper.classify_scheme_type(
                description=app.description,
                applicant_name=app.applicant_name,
                agent_name=app.agent_name,
            )
            if new_type != old_type:
                transitions[(old_type, new_type)] += 1
                batch.append((aid, new_type))

            if len(batch) >= args.batch_size or i == len(ids):
                if batch and not args.dry_run:
                    for app_id, new_st in batch:
                        a = db.get(PlanningApplication, app_id)
                        if a:
                            a.scheme_type = new_st
                    db.commit()
                total_changed += len(batch)
                batch.clear()
                if i % 5000 == 0 or i == len(ids):
                    elapsed = time.time() - start
                    rate = i / elapsed if elapsed else 0
                    eta = (len(ids) - i) / rate if rate else 0
                    print(
                        f"  ...{i:,}/{len(ids):,} ({100*i/len(ids):.1f}%)  "
                        f"changed={total_changed:,}  "
                        f"rate={rate:.0f}/s  eta={eta:.0f}s"
                    )

    # Summary
    print(f"\n=== Result ({'DRY-RUN' if args.dry_run else 'APPLIED'}) ===")
    print(f"  Total scanned: {len(ids):,}")
    print(f"  Total changed: {total_changed:,}")
    print(f"  Elapsed:       {time.time()-start:.0f}s")
    print(f"\n  Top transitions:")
    for (old_t, new_t), n in transitions.most_common(20):
        print(f"    {old_t or '<null>':<15} -> {new_t:<15} {n:>5}")


if __name__ == "__main__":
    main()
