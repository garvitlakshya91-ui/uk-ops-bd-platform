"""Reclassify sub-20-unit EPC-sourced schemes out of the BD cohort.

The EPC new-dwelling import flooded the BD scheme set with ~26k small
blocks (mostly HA/council infill) that aren't institutional BD targets.
This moves EPC-sourced BD schemes with a KNOWN unit count < 20 to
scheme_type='Residential', so coverage metrics measure the real target
universe. Schemes with unknown unit counts are left in the cohort.

Reversible: old types are saved to ``scheme_type_reclass_backup`` and
``--revert`` restores them.

Usage:
    python reclassify_epc_longtail.py --dry-run
    python reclassify_epc_longtail.py
    python reclassify_epc_longtail.py --revert
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

RULE = """
    source = 'epc_new_dwelling'
    AND scheme_type IN ('BTR','PBSA','Co-living','Senior')
    AND COALESCE(num_units, total_units) < 20
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--revert", action="store_true")
    args = ap.parse_args()
    engine = create_engine(DB_URL)

    with engine.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS scheme_type_reclass_backup (
                scheme_id INT PRIMARY KEY,
                old_scheme_type VARCHAR(100),
                reclassified_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        if args.revert:
            n = c.execute(text("""
                UPDATE existing_schemes es
                SET scheme_type = b.old_scheme_type, updated_at = NOW()
                FROM scheme_type_reclass_backup b
                WHERE b.scheme_id = es.id
            """)).rowcount
            c.execute(text("DELETE FROM scheme_type_reclass_backup"))
            print(f"Reverted {n:,} schemes to their original type.")
            return

        rows = c.execute(text(f"""
            SELECT id, scheme_type FROM existing_schemes WHERE {RULE}
        """)).fetchall()
        by_type = {}
        for _, t in rows:
            by_type[t] = by_type.get(t, 0) + 1
        print(f"{len(rows):,} EPC schemes with known units < 20 to reclassify:")
        for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"   {t:12} {n:,}")

        if args.dry_run:
            print("[dry-run] no changes")
            return

        c.execute(text(f"""
            INSERT INTO scheme_type_reclass_backup (scheme_id, old_scheme_type)
            SELECT id, scheme_type FROM existing_schemes WHERE {RULE}
            ON CONFLICT (scheme_id) DO NOTHING
        """))
        n = c.execute(text(f"""
            UPDATE existing_schemes
            SET scheme_type = 'Residential',
                notes = LEFT(COALESCE(notes,'') ||
                        ' Reclassified from BD cohort (EPC long-tail <20 units).',
                        1000),
                updated_at = NOW()
            WHERE {RULE}
        """)).rowcount
        print(f"Reclassified {n:,} schemes to 'Residential' (backup saved).")


if __name__ == "__main__":
    main()
