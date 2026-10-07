"""Per-field provenance on existing_schemes.

Every published census value must trace to a source we are allowed to
publish. field_provenance maps field -> {"value", "source", "ref", "at",
"basis"}; licensed benchmark packs may populate it for QA but never as a
publishable source.

Revision ID: 015_field_provenance
Revises: 014_hesa_accommodation
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op


revision: str = "015_field_provenance"
down_revision: Union[str, None] = "014_hesa_accommodation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE existing_schemes "
               "ADD COLUMN IF NOT EXISTS field_provenance JSONB")


def downgrade() -> None:
    op.execute("ALTER TABLE existing_schemes DROP COLUMN IF EXISTS field_provenance")
