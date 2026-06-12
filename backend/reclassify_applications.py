"""Reclassify existing planning_applications using the broadened classifier.

The current population has 102,984 Unknown / 81,364 Residential apps because
the old classifier only matched description keywords. The new
``BaseScraper.classify_scheme_type`` also matches applicant_name / agent_name
against curated BTR/PBSA/Senior operator lists.

Usage::

    python reclassify_applications.py [--dry-run] [--limit N] [--all]

By default, processes only rows where scheme_type IN ('Unknown', '',
'Residential') — these are the ones we expect to gain new classifications.
Use ``--all`` to re-evaluate every row (may change existing BTR labels).
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

from sqlalchemy import create_engine, text
from app.scrapers.base import BaseScraper


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--limit", type=int, default=None, help="Cap rows processed")
    parser.add_argument(
        "--all", action="store_true",
        help="Re-evaluate every row (default: only Unknown/Residential/empty)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=2000,
        help="Rows committed per transaction (default 2000)",
    )
    args = parser.parse_args()

    engine = create_engine(os.environ["DATABASE_URL"])

    where = (
        "TRUE" if args.all
        else "(scheme_type IN ('Unknown', 'Residential', '') OR scheme_type IS NULL)"
    )

    with engine.connect() as c:
        total_eligible = c.execute(
            text(f"SELECT COUNT(*) FROM planning_applications WHERE {where}")
        ).scalar() or 0
    print(f"Eligible rows: {total_eligible:,} (--all={args.all}, --dry-run={args.dry_run})")

    if args.limit:
        total_eligible = min(total_eligible, args.limit)

    if total_eligible == 0:
        print("Nothing to do.")
        return 0

    changes: Counter[tuple[str, str]] = Counter()  # (old, new) -> count
    skipped_demotions = 0
    scanned = 0
    updated = 0
    pending: list[tuple[str, int]] = []  # (new_scheme_type, id)

    # Specificity ranking — higher means more BD-relevant. Never overwrite a
    # more specific label with a less specific one (e.g. don't demote
    # Residential -> Unknown just because description/applicant are blank).
    specificity = {
        "Unknown": 0,
        "Residential": 1,
        "Affordable": 2,
        "Mixed": 3,
        "Senior": 4,
        "Co-living": 5,
        "PBSA": 6,
        "BTR": 7,
    }

    sql = f"""
        SELECT id, scheme_type, description, applicant_name, agent_name
        FROM planning_applications
        WHERE {where}
        ORDER BY id
    """
    if args.limit:
        sql += f" LIMIT {args.limit}"

    # Phase 1: read all rows, decide changes (no writes during streaming read).
    with engine.connect() as read_conn:
        result = read_conn.execution_options(stream_results=True).execute(text(sql))
        for row in result:
            scanned += 1
            old = row.scheme_type or "Unknown"
            new = BaseScraper.classify_scheme_type(
                row.description,
                applicant_name=row.applicant_name,
                agent_name=row.agent_name,
            )
            if new != old:
                if specificity.get(new, 0) < specificity.get(old, 0):
                    skipped_demotions += 1
                    continue
                changes[(old, new)] += 1
                pending.append((new, row.id))

            if scanned % 25000 == 0:
                print(f"  scanned {scanned:,}/{total_eligible:,}  pending_updates={len(pending):,}")

    print(f"  scan done: {scanned:,} rows, {len(pending):,} updates queued")

    # Phase 2: bulk-update in batches on a fresh connection.
    if not args.dry_run and pending:
        with engine.connect() as write_conn:
            for i in range(0, len(pending), args.batch_size):
                batch = pending[i:i + args.batch_size]
                _flush(write_conn, batch)
                updated += len(batch)
                if i % (args.batch_size * 5) == 0:
                    print(f"  updated {updated:,}/{len(pending):,}")
            write_conn.commit()

    print()
    print("=" * 60)
    print(f"Scanned: {scanned:,}")
    print(f"Reclassifications: {sum(changes.values()):,}")
    print(f"Skipped (would-be downgrades): {skipped_demotions:,}")
    if args.dry_run:
        print("(DRY RUN — no DB writes)")
    else:
        print(f"DB rows updated: {updated:,}")
    print()
    print("Change breakdown (old -> new):")
    for (old, new), n in sorted(changes.items(), key=lambda x: -x[1])[:20]:
        print(f"  {old:14s} -> {new:14s}  {n:>7,}")

    # Post-state snapshot
    with engine.connect() as c:
        print()
        print("Resulting scheme_type distribution:")
        for r in c.execute(text("""
            SELECT COALESCE(NULLIF(scheme_type, ''), '<null>') AS t, COUNT(*) AS n
            FROM planning_applications GROUP BY 1 ORDER BY n DESC
        """)):
            print(f"  {r[0]:20s} {r[1]:>7,}")
    return 0


def _flush(conn, pending: list[tuple[str, int]]) -> None:
    """Bulk-update scheme_type for a batch of rows."""
    conn.execute(
        text("""
            UPDATE planning_applications
            SET scheme_type = data.new_type
            FROM (
                SELECT UNNEST(CAST(:types AS text[]))  AS new_type,
                       UNNEST(CAST(:ids   AS integer[])) AS id
            ) AS data
            WHERE planning_applications.id = data.id
        """),
        {
            "types": [p[0] for p in pending],
            "ids":   [p[1] for p in pending],
        },
    )


if __name__ == "__main__":
    sys.exit(main())
