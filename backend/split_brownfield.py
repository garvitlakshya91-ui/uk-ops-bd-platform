"""Split brownfield-register data out of planning_applications.

Creates a new ``brownfield_sites`` table, moves the 27,592 brownfield rows
into it, then deletes them from planning_applications. Idempotent — safe to
re-run if the table already exists.

Sites flagged as brownfield by the Brownfield Land Register scraper are
LA-curated land-bank entries, not actual planning applications. Keeping
them in planning_applications conflates two different concepts and breaks
the BD dashboards.
"""
from __future__ import annotations

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


def main() -> int:
    e = create_engine(os.environ["DATABASE_URL"])

    with e.begin() as c:
        # 1. Create brownfield_sites table if not exists.
        # Mirrors the subset of planning_applications columns brownfield needs.
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS brownfield_sites (
                id                  SERIAL PRIMARY KEY,
                reference           VARCHAR(255) NOT NULL,
                council_id          INTEGER REFERENCES councils(id) ON DELETE SET NULL,
                address             TEXT,
                postcode            VARCHAR(20),
                latitude            DOUBLE PRECISION,
                longitude           DOUBLE PRECISION,
                description         TEXT,
                num_units           INTEGER,
                status              VARCHAR(50),
                scheme_type         VARCHAR(50),
                source              VARCHAR(50) DEFAULT 'brownfield-register',
                source_reference    VARCHAR(255),
                raw_data            JSONB,
                last_verified_at    TIMESTAMP WITH TIME ZONE,
                created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (reference, council_id)
            )
        """))
        c.execute(text("CREATE INDEX IF NOT EXISTS ix_brownfield_sites_council_id ON brownfield_sites(council_id)"))
        c.execute(text("CREATE INDEX IF NOT EXISTS ix_brownfield_sites_postcode ON brownfield_sites(postcode)"))
        c.execute(text("CREATE INDEX IF NOT EXISTS ix_brownfield_sites_num_units ON brownfield_sites(num_units)"))
        print("Created brownfield_sites table (or it already existed)")

    # 2. Count before
    with e.connect() as c:
        before_brown = c.execute(text(
            "SELECT COUNT(*) FROM planning_applications WHERE reference LIKE 'brownfield:%'"
        )).scalar() or 0
        before_apps = c.execute(text("SELECT COUNT(*) FROM planning_applications")).scalar() or 0
        existing_brown = c.execute(text("SELECT COUNT(*) FROM brownfield_sites")).scalar() or 0
    print(f"Before:  planning_applications={before_apps:,}, of which brownfield={before_brown:,}")
    print(f"         brownfield_sites already populated={existing_brown:,}")

    # 3. Move data
    with e.begin() as c:
        moved = c.execute(text("""
            INSERT INTO brownfield_sites
                (reference, council_id, address, postcode, latitude, longitude,
                 description, num_units, status, scheme_type, source,
                 raw_data, created_at, updated_at)
            SELECT
                regexp_replace(reference, '^brownfield:', '') AS reference,
                council_id, address, postcode, latitude, longitude,
                description, num_units, status, scheme_type,
                COALESCE(NULLIF(source,''), 'brownfield-register') AS source,
                raw_data, created_at, updated_at
            FROM planning_applications
            WHERE reference LIKE 'brownfield:%'
            ON CONFLICT (reference, council_id) DO NOTHING
        """)).rowcount
        print(f"Moved into brownfield_sites: {moved:,}")

        # 4. Drop alerts pointing at brownfield rows (preserves FK integrity)
        alerts_deleted = c.execute(text("""
            DELETE FROM alerts
            WHERE planning_application_id IN (
                SELECT id FROM planning_applications WHERE reference LIKE 'brownfield:%'
            )
        """)).rowcount
        print(f"Alerts removed pointing at brownfield rows: {alerts_deleted:,}")

        # 5. Detach pipeline_opportunities (don't delete user-curated data) —
        #    set their planning_application_id to NULL so they survive.
        detached = c.execute(text("""
            UPDATE pipeline_opportunities
            SET planning_application_id = NULL
            WHERE planning_application_id IN (
                SELECT id FROM planning_applications WHERE reference LIKE 'brownfield:%'
            )
        """)).rowcount
        if detached:
            print(f"Pipeline opportunities detached from brownfield rows: {detached:,}")

        # 6. Delete from planning_applications
        deleted = c.execute(text(
            "DELETE FROM planning_applications WHERE reference LIKE 'brownfield:%'"
        )).rowcount
        print(f"Deleted from planning_applications: {deleted:,}")

    # 7. Verify
    with e.connect() as c:
        after_apps = c.execute(text("SELECT COUNT(*) FROM planning_applications")).scalar() or 0
        brown_total = c.execute(text("SELECT COUNT(*) FROM brownfield_sites")).scalar() or 0
        residual = c.execute(text(
            "SELECT COUNT(*) FROM planning_applications WHERE reference LIKE 'brownfield:%'"
        )).scalar() or 0

    print()
    print("=" * 60)
    print(f"After:   planning_applications={after_apps:,}")
    print(f"         brownfield_sites={brown_total:,}")
    print(f"         residual brownfield in apps: {residual:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
