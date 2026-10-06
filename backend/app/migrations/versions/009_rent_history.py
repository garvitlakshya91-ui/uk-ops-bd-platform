"""Make scheme_rents append-only: supersede flags + year-match key.

Rent observations are never deleted again. Loaders mark their previous
rows ``is_current = false`` and insert fresh ones, so the table
accumulates the rent time series. ``sub_classification`` carries the
operator tier (Gold/Platinum/...) used to match a room across years.

Revision ID: 009_rent_history
Revises: 008_add_ownership
Create Date: 2026-10-06
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "009_rent_history"
down_revision: Union[str, None] = "008_add_ownership"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "scheme_rents",
        sa.Column("is_current", sa.Boolean, nullable=False,
                  server_default=sa.text("true")),
    )
    op.add_column(
        "scheme_rents",
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "scheme_rents",
        sa.Column("sub_classification", sa.String(150), nullable=True),
    )
    op.create_index(
        "ix_scheme_rents_current", "scheme_rents", ["scheme_id", "is_current"]
    )
    op.create_index(
        "ix_scheme_rents_source_current", "scheme_rents", ["source", "is_current"]
    )


def downgrade() -> None:
    op.drop_index("ix_scheme_rents_source_current", table_name="scheme_rents")
    op.drop_index("ix_scheme_rents_current", table_name="scheme_rents")
    op.drop_column("scheme_rents", "sub_classification")
    op.drop_column("scheme_rents", "superseded_at")
    op.drop_column("scheme_rents", "is_current")
