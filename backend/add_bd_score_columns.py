"""Add new BD-scoring columns to existing_schemes.

Adds:
- google_rating          float    (0-5, Google Places rating)
- google_review_count    int      (review count)
- google_place_id        varchar  (Google Place ID, for caching to avoid re-search)
- google_checked_at      timestamp
- occupancy_rate         float    (0.0-1.0, fraction occupied)
- occupancy_checked_at   timestamp
- arrears_risk_score     float    (0-100, derived from CH signals — higher = more distress)
- arrears_checked_at     timestamp
- bd_score_breakdown     jsonb    (cached per-dimension scores)

Safe to re-run.
"""
from sqlalchemy import create_engine, text

DDL = [
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS google_rating REAL",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS google_review_count INTEGER",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS google_place_id VARCHAR(100)",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS google_checked_at TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS occupancy_rate REAL",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS occupancy_checked_at TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS arrears_risk_score REAL",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS arrears_checked_at TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE existing_schemes ADD COLUMN IF NOT EXISTS bd_score_breakdown JSONB",
    "CREATE INDEX IF NOT EXISTS ix_existing_schemes_google_rating ON existing_schemes(google_rating)",
    "CREATE INDEX IF NOT EXISTS ix_existing_schemes_occupancy_rate ON existing_schemes(occupancy_rate)",
    "CREATE INDEX IF NOT EXISTS ix_existing_schemes_arrears_risk_score ON existing_schemes(arrears_risk_score)",
]

e = create_engine("postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
with e.begin() as c:
    for stmt in DDL:
        c.execute(text(stmt))
        print(f"  OK: {stmt[:80]}")

with e.connect() as c:
    cols = c.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='existing_schemes' AND column_name IN (
            'google_rating','google_review_count','google_place_id','google_checked_at',
            'occupancy_rate','occupancy_checked_at','arrears_risk_score',
            'arrears_checked_at','bd_score_breakdown'
        )
        ORDER BY column_name
    """)).fetchall()
print()
print(f"Verified {len(cols)} new columns:", [c[0] for c in cols])
