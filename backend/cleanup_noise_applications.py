"""Delete obvious non-BD planning applications from the DB.

What gets removed (in order of confidence):
- Single-dwelling residential extensions (single/two-storey rear/side/loft/porch/garage)
- Tree work (felling, pruning, crown reduction, TPO)
- Telecoms masts and signage/advertising
- Listed Building Consent (LBC) and pure heritage works
- Discharge of conditions / approval of details / details pursuant
- Small changes of use (<5 units)
- Pure refurbishment / window replacement / cladding (<5 units)

What's protected (NEVER deleted):
- Any application referenced by a pipeline_opportunity (user-curated)
- Anything with num_units >= 10 (genuine multi-unit, even if mislabeled)
- Anything classified BTR / PBSA / Co-living / Senior / Affordable / Mixed

Usage::

    python cleanup_noise_applications.py [--dry-run] [--commit]

Default is dry-run-style: prints counts and asks before deleting. Add --commit
to actually perform the deletes (also requires --confirm flag for safety).
"""
from __future__ import annotations

import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

from sqlalchemy import create_engine, text


# Description-keyword patterns for clearly non-BD applications.
NOISE_PATTERNS = {
    "single-dwelling extensions": (
        r"single.storey|two.storey|rear extension|side extension|"
        r"front extension|loft conversion|conservatory|outbuilding|"
        r"porch|garage conversion|garden room"
    ),
    "tree work": (
        r"fell.{0,20}tree|prune|crown reduc|tree work|tree pres|tpo"
    ),
    "telecoms / signage": (
        r"telecom|antenna mast|signage|advertis|hoarding|fascia sign"
    ),
    "listed building / heritage": (
        r"listed building|heritage|conservation area"
    ),
    "discharge of conditions": (
        r"discharge of condition|approval of detail|details pursuant"
    ),
    "small change of use": (
        r"change of use"
    ),
    "works to existing dwelling": (
        r"replacement window|external rendering|cladding replac|"
        r"roof replac|reroofing"
    ),
}

# Always-protected scheme types
PROTECTED_SCHEME_TYPES = ("BTR", "PBSA", "Co-living", "Senior", "Affordable", "Mixed")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--commit", action="store_true",
                   help="Actually run the deletes. Without this, dry-run.")
    p.add_argument("--confirm", action="store_true",
                   help="Required alongside --commit for safety.")
    p.add_argument("--unit-floor", type=int, default=10,
                   help="Keep anything with num_units >= this (default 10).")
    args = p.parse_args()

    if args.commit and not args.confirm:
        print("ERROR: --commit requires --confirm too. This is destructive.")
        return 1

    engine = create_engine(os.environ["DATABASE_URL"])

    # Build a single CTE that identifies all noise rows safely.
    noise_or = " OR ".join(
        f"LOWER(COALESCE(description,'')) ~ :pat_{i}"
        for i in range(len(NOISE_PATTERNS))
    )
    where_noise = f"""
        ({noise_or})
        AND COALESCE(num_units, 0) < :floor
        AND (scheme_type IS NULL OR scheme_type NOT IN ({
            ', '.join(f"'{t}'" for t in PROTECTED_SCHEME_TYPES)
        }))
        AND NOT EXISTS (
            SELECT 1 FROM pipeline_opportunities po
            WHERE po.planning_application_id = planning_applications.id
        )
    """
    params: dict[str, object] = {"floor": args.unit_floor}
    for i, pat in enumerate(NOISE_PATTERNS.values()):
        params[f"pat_{i}"] = pat

    with engine.connect() as c:
        total = c.execute(text("SELECT COUNT(*) FROM planning_applications")).scalar() or 0
        print(f"Total applications before:                {total:,}")

        # Per-bucket counts for transparency
        print()
        print("Per-bucket counts (after applying unit-floor + protection):")
        per_bucket_total = 0
        for i, (label, pat) in enumerate(NOISE_PATTERNS.items()):
            n = c.execute(text(f"""
                SELECT COUNT(*) FROM planning_applications
                WHERE LOWER(COALESCE(description,'')) ~ :pat
                  AND COALESCE(num_units, 0) < :floor
                  AND (scheme_type IS NULL OR scheme_type NOT IN ({
                    ', '.join(f"'{t}'" for t in PROTECTED_SCHEME_TYPES)
                  }))
                  AND NOT EXISTS (
                    SELECT 1 FROM pipeline_opportunities po
                    WHERE po.planning_application_id = planning_applications.id
                  )
            """), {"pat": pat, "floor": args.unit_floor}).scalar() or 0
            per_bucket_total += n
            print(f"  {label:<35s} {n:>7,}")
        print(f"  {'(sum — may overlap)':<35s} {per_bucket_total:>7,}")

        # Distinct count of unique rows that match the union of all patterns
        noise_count = c.execute(
            text(f"SELECT COUNT(*) FROM planning_applications WHERE {where_noise}"),
            params,
        ).scalar() or 0

        # Also count "noise by app_type" — narrower, more confident
        noise_by_type = c.execute(text(f"""
            SELECT COUNT(*) FROM planning_applications
            WHERE LOWER(COALESCE(application_type,'')) ~
                'tree|conditions|discharge|telecom|advertis|listed|heritage|prior approv|non.?material'
              AND COALESCE(num_units, 0) < :floor
              AND (scheme_type IS NULL OR scheme_type NOT IN ({
                ', '.join(f"'{t}'" for t in PROTECTED_SCHEME_TYPES)
              }))
              AND NOT EXISTS (
                SELECT 1 FROM pipeline_opportunities po
                WHERE po.planning_application_id = planning_applications.id
              )
        """), {"floor": args.unit_floor}).scalar() or 0

        print()
        print(f"Distinct noise rows (description-based):  {noise_count:,}")
        print(f"Noise rows by application_type:           {noise_by_type:,}")

        keepers = c.execute(text(f"""
            SELECT COUNT(*) FROM planning_applications
            WHERE COALESCE(num_units, 0) >= :floor
               OR scheme_type IN ({', '.join(f"'{t}'" for t in PROTECTED_SCHEME_TYPES)})
               OR EXISTS (SELECT 1 FROM pipeline_opportunities po
                          WHERE po.planning_application_id = planning_applications.id)
        """), {"floor": args.unit_floor}).scalar() or 0
        print(f"Definite keepers (units>={args.unit_floor} or BD scheme or in pipeline): {keepers:,}")

        print()
        print(f"Projected delete:  ~{noise_count:,} rows")
        print(f"Projected after:   ~{total - noise_count:,} rows")

        if not args.commit:
            print()
            print("DRY-RUN. To execute: --commit --confirm")
            return 0

    # ---- Actually delete ----
    print()
    print("=== DELETING ===")
    with engine.begin() as c:
        # Drop alerts pointing at noise rows first (FK cascade safety)
        ad = c.execute(
            text(f"""
                DELETE FROM alerts
                WHERE planning_application_id IN (
                    SELECT id FROM planning_applications WHERE {where_noise}
                )
            """),
            params,
        ).rowcount
        print(f"  alerts removed:                  {ad:,}")

        pd = c.execute(
            text(f"DELETE FROM planning_applications WHERE {where_noise}"),
            params,
        ).rowcount
        print(f"  planning_applications removed:   {pd:,}")

        new_total = c.execute(text("SELECT COUNT(*) FROM planning_applications")).scalar() or 0
        print(f"  remaining applications:          {new_total:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
