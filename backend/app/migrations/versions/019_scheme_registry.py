"""Scheme registry: one permanent ID per building, with its names and sources.

existing_schemes.id is the permanent identifier. This revision adds what
makes it a registry: every alternate name as an alias, every feed record
that was matched (or not) as a source record, and the verification state
an analyst sets scheme by scheme. A duplicate row is retired by pointing
merged_into_id at the survivor; nothing is deleted.

Revision ID: 019_scheme_registry
Revises: 018_extraction_confidence
Create Date: 2026-10-09
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "019_scheme_registry"
down_revision: Union[str, None] = "018_extraction_confidence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE existing_schemes
            ADD COLUMN IF NOT EXISTS verification_status VARCHAR(20) NOT NULL DEFAULT 'unverified',
            ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS verified_by VARCHAR(100),
            ADD COLUMN IF NOT EXISTS verification_note TEXT,
            ADD COLUMN IF NOT EXISTS shared_postcode_ok BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS merged_into_id INTEGER REFERENCES existing_schemes(id) ON DELETE SET NULL
    """)
    op.execute("COMMENT ON COLUMN existing_schemes.verification_status IS "
               "'unverified, probable, verified, disputed, retired'")
    op.execute("COMMENT ON COLUMN existing_schemes.shared_postcode_ok IS "
               "'An analyst confirmed this scheme is distinct from others at the same postcode'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_existing_schemes_verification "
               "ON existing_schemes (verification_status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_existing_schemes_merged_into "
               "ON existing_schemes (merged_into_id)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS scheme_aliases (
            id SERIAL PRIMARY KEY,
            scheme_id INTEGER NOT NULL REFERENCES existing_schemes(id) ON DELETE CASCADE,
            alias VARCHAR(500) NOT NULL,
            alias_norm VARCHAR(500) NOT NULL,
            source VARCHAR(100) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_scheme_alias UNIQUE (scheme_id, alias_norm)
        )
    """)
    op.execute("COMMENT ON COLUMN scheme_aliases.alias_norm IS "
               "'lower-cased, punctuation stripped, stop words removed; the match key'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_scheme_aliases_norm ON scheme_aliases (alias_norm)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS scheme_source_records (
            id SERIAL PRIMARY KEY,
            scheme_id INTEGER REFERENCES existing_schemes(id) ON DELETE SET NULL,
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500) NOT NULL,
            raw_name VARCHAR(500),
            raw_postcode VARCHAR(12),
            raw_operator VARCHAR(300),
            raw_address TEXT,
            payload JSONB,
            match_method VARCHAR(40),
            match_score DOUBLE PRECISION,
            status VARCHAR(20) NOT NULL DEFAULT 'unmatched',
            proposed_scheme_id INTEGER REFERENCES existing_schemes(id) ON DELETE SET NULL,
            proposal_confidence DOUBLE PRECISION,
            proposal_reason TEXT,
            confirmed_by VARCHAR(100),
            confirmed_at TIMESTAMPTZ,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_scheme_source_record UNIQUE (source, source_reference)
        )
    """)
    op.execute("COMMENT ON COLUMN scheme_source_records.status IS "
               "'matched, proposed, unmatched, rejected, ignored'")
    op.execute("COMMENT ON COLUMN scheme_source_records.match_method IS "
               "'postcode_name, name_exact, url, merge, manual, ai_proposed'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_scheme_source_records_scheme "
               "ON scheme_source_records (scheme_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_scheme_source_records_status "
               "ON scheme_source_records (status)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS scheme_source_records")
    op.execute("DROP TABLE IF EXISTS scheme_aliases")
    op.execute("DROP INDEX IF EXISTS ix_existing_schemes_merged_into")
    op.execute("DROP INDEX IF EXISTS ix_existing_schemes_verification")
    op.execute("""
        ALTER TABLE existing_schemes
            DROP COLUMN IF EXISTS merged_into_id,
            DROP COLUMN IF EXISTS shared_postcode_ok,
            DROP COLUMN IF EXISTS verification_note,
            DROP COLUMN IF EXISTS verified_by,
            DROP COLUMN IF EXISTS verified_at,
            DROP COLUMN IF EXISTS verification_status
    """)
