"""Scheme availability observations — revealed demand.

Append-only by design, like scheme_rents: each capture writes a row per
scheme per source, so listing-depletion velocity (who sells out by
October, who discounts in January) accumulates as a time series. This
is the occupancy proxy consultancies need operator relationships for.

Revision ID: 012_availability
Revises: 011_demand_layer
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "012_availability"
down_revision: Union[str, None] = "011_demand_layer"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheme_availability",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("scheme_id", sa.Integer,
                  sa.ForeignKey("existing_schemes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
        sa.Column("state", sa.String(30), nullable=False,
                  comment="available / limited / sold_out / no_listing"),
        sa.Column("rooms_listed", sa.Integer, nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column("academic_year", sa.String(10), nullable=True,
                  comment="Letting year the state refers to, e.g. 2027-28"),
    )
    op.create_index("ix_availability_scheme_time", "scheme_availability",
                    ["scheme_id", "captured_at"])


def downgrade() -> None:
    op.drop_index("ix_availability_scheme_time", table_name="scheme_availability")
    op.drop_table("scheme_availability")
