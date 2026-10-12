"""Record class: say what kind of thing a row in existing_schemes is.

Most rows are not operator-managed schemes. 95% are postcode clusters of
new-dwelling energy certificates; a few are procurement notices, listing
fragments ("Premium Studio", "Arrange a Viewing") or ingest artefacts.
record_class names the kind so lists, maps, counts and scores can tell
them apart instead of treating every row as a scheme. Nothing is removed.

Revision ID: 021_record_class
Revises: 020_rent_tables
Create Date: 2026-10-10
"""
from typing import Sequence, Union

from alembic import op


revision: str = "021_record_class"
down_revision: Union[str, None] = "020_rent_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE existing_schemes "
               "ADD COLUMN IF NOT EXISTS record_class VARCHAR(30) NOT NULL DEFAULT 'scheme'")
    op.execute("COMMENT ON COLUMN existing_schemes.record_class IS "
               "'scheme, development_cluster, procurement_notice, listing_fragment, ingest_artefact'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_existing_schemes_record_class "
               "ON existing_schemes (record_class)")
    # A merged company keeps its row; this says which company it became.
    op.execute("ALTER TABLE companies "
               "ADD COLUMN IF NOT EXISTS merged_into_id INTEGER REFERENCES companies(id) ON DELETE SET NULL")


def downgrade() -> None:
    op.execute("ALTER TABLE companies DROP COLUMN IF EXISTS merged_into_id")
    op.execute("DROP INDEX IF EXISTS ix_existing_schemes_record_class")
    op.execute("ALTER TABLE existing_schemes DROP COLUMN IF EXISTS record_class")
