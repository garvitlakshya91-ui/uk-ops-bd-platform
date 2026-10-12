"""Extraction confidence: every machine-read number carries its evidence.

Adds confidence, the quoted phrase and the extractor to scheme
observations, a unit_extractions table holding every typed figure the
unit extractor (regex or AI) reads from a description, page or document,
and the confidence/quote behind a planning row's cached bed count.
Values read by a model never again land in a cache column without this
trail.

Revision ID: 018_extraction_confidence
Revises: 017_pipeline_status
Create Date: 2026-10-09
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "018_extraction_confidence"
down_revision: Union[str, None] = "017_pipeline_status"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE scheme_observations
            ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quote TEXT,
            ADD COLUMN IF NOT EXISTS extractor VARCHAR(40),
            ADD COLUMN IF NOT EXISTS model VARCHAR(60)
    """)
    op.execute("""
        ALTER TABLE planning_applications
            ADD COLUMN IF NOT EXISTS beds_confidence DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS beds_quote TEXT,
            ADD COLUMN IF NOT EXISTS beds_extractor VARCHAR(40)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS unit_extractions (
            id SERIAL PRIMARY KEY,
            entity_type VARCHAR(30) NOT NULL,
            entity_id INTEGER NOT NULL,
            kind VARCHAR(30) NOT NULL,
            value INTEGER NOT NULL,
            is_maximum BOOLEAN NOT NULL DEFAULT FALSE,
            is_approximate BOOLEAN NOT NULL DEFAULT FALSE,
            alternative BOOLEAN NOT NULL DEFAULT FALSE,
            component BOOLEAN NOT NULL DEFAULT FALSE,
            variation_from INTEGER,
            confidence DOUBLE PRECISION NOT NULL,
            quote VARCHAR(500),
            method VARCHAR(20) NOT NULL DEFAULT 'regex',
            model VARCHAR(60),
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500),
            text_sha1 CHAR(40),
            extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("COMMENT ON COLUMN unit_extractions.entity_type IS 'planning_application or scheme'")
    op.execute("COMMENT ON COLUMN unit_extractions.kind IS "
               "'beds, homes, storeys, studios, cluster_flats, coliving_rooms, hotel_rooms, care_beds, hmo_beds, rooms, units'")
    op.execute("COMMENT ON COLUMN unit_extractions.method IS 'regex, variation, sum, from_studios, ai, manual'")
    op.execute("COMMENT ON COLUMN unit_extractions.source IS 'description, operator_page, planning_document'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_unit_extractions_entity "
               "ON unit_extractions (entity_type, entity_id, kind)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_unit_extractions_sha ON unit_extractions (text_sha1)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS unit_extractions")
    op.execute("""
        ALTER TABLE planning_applications
            DROP COLUMN IF EXISTS beds_extractor,
            DROP COLUMN IF EXISTS beds_quote,
            DROP COLUMN IF EXISTS beds_confidence
    """)
    op.execute("""
        ALTER TABLE scheme_observations
            DROP COLUMN IF EXISTS model,
            DROP COLUMN IF EXISTS extractor,
            DROP COLUMN IF EXISTS quote,
            DROP COLUMN IF EXISTS confidence
    """)
