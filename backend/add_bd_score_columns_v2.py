"""Add bd_score + bd_score_breakdown + bd_score_updated_at columns
to existing_schemes and planning_applications.

Idempotent: safe to re-run.
"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)


COLUMN_ADDS = [
    # existing_schemes
    ("existing_schemes", "bd_score",            "REAL"),
    ("existing_schemes", "bd_score_breakdown",  "JSONB"),
    ("existing_schemes", "bd_score_updated_at", "TIMESTAMP WITH TIME ZONE"),
    # planning_applications
    ("planning_applications", "bd_score",            "REAL"),
    ("planning_applications", "bd_score_breakdown",  "JSONB"),
    ("planning_applications", "bd_score_updated_at", "TIMESTAMP WITH TIME ZONE"),
]

INDEX_ADDS = [
    ("idx_existing_schemes_bd_score",
     "CREATE INDEX IF NOT EXISTS idx_existing_schemes_bd_score "
     "ON existing_schemes(bd_score DESC NULLS LAST) "
     "WHERE bd_score IS NOT NULL"),
    ("idx_planning_apps_bd_score",
     "CREATE INDEX IF NOT EXISTS idx_planning_apps_bd_score "
     "ON planning_applications(bd_score DESC NULLS LAST) "
     "WHERE bd_score IS NOT NULL"),
]


def main():
    engine = create_engine(DB_URL)
    with engine.begin() as c:
        for table, col, dtype in COLUMN_ADDS:
            c.execute(text(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
            ))
            print(f"  + {table}.{col} ({dtype})")
        for name, ddl in INDEX_ADDS:
            c.execute(text(ddl))
            print(f"  + index {name}")
    print("\nDone.")


if __name__ == "__main__":
    main()
