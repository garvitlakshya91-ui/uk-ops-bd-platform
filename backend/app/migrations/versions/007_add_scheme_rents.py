"""Add scheme_rents table for per-scheme per-room-type rent tiers.

Revision ID: 007_add_scheme_rents
Revises: 006_add_locked_fields
Create Date: 2026-04-19
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "007_add_scheme_rents"
down_revision: Union[str, None] = "006_add_locked_fields"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheme_rents",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "scheme_id", sa.Integer,
            sa.ForeignKey("existing_schemes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("room_type", sa.String(100), nullable=True),
        sa.Column("rent_per_week", sa.Float, nullable=True),
        sa.Column("rent_per_month", sa.Float, nullable=True),
        sa.Column("currency", sa.String(3), nullable=False, server_default="GBP"),
        sa.Column("academic_year", sa.String(20), nullable=True),
        sa.Column("contract_length_weeks", sa.Integer, nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column(
            "scraped_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_scheme_rents_scheme_id", "scheme_rents", ["scheme_id"])
    op.create_index("ix_scheme_rents_scheme_room", "scheme_rents", ["scheme_id", "room_type"])


def downgrade() -> None:
    op.drop_index("ix_scheme_rents_scheme_room", table_name="scheme_rents")
    op.drop_index("ix_scheme_rents_scheme_id", table_name="scheme_rents")
    op.drop_table("scheme_rents")
