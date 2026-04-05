"""Add HMLR CCOD fields to schemes.

Revision ID: 003_add_hmlr_fields
Revises: 002_scheme_contracts
Create Date: 2026-03-29

Adds:
- schemes.hmlr_title_number  -- HMLR title number from CCOD dataset
- schemes.hmlr_tenure        -- Freehold / Leasehold from CCOD
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003_add_hmlr_fields"
down_revision: Union[str, None] = "002_scheme_contracts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "schemes",
        sa.Column(
            "hmlr_title_number",
            sa.String(20),
            nullable=True,
            comment="HMLR title number matched from CCOD dataset",
        ),
    )
    op.add_column(
        "schemes",
        sa.Column(
            "hmlr_tenure",
            sa.String(20),
            nullable=True,
            comment="Freehold or Leasehold from HMLR CCOD",
        ),
    )
    op.create_index(
        "ix_schemes_hmlr_title_number",
        "schemes",
        ["hmlr_title_number"],
    )


def downgrade() -> None:
    op.drop_index("ix_schemes_hmlr_title_number", table_name="schemes")
    op.drop_column("schemes", "hmlr_tenure")
    op.drop_column("schemes", "hmlr_title_number")
