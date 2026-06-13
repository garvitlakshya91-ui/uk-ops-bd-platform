"""Add ownership-chain persistence and PE/platform classification.

ownership_chain_nodes: one row per node discovered walking a company's
PSC chain upward (level 1 = direct parent of the company).

companies gains ultimate-owner classification columns populated by the
ownership walker, plus an office-cluster key for platform grouping.

Revision ID: 008_add_ownership
Revises: 007_add_scheme_rents
Create Date: 2026-06-12
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "008_add_ownership"
down_revision: Union[str, None] = "007_add_scheme_rents"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ownership_chain_nodes",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "company_id", sa.Integer,
            sa.ForeignKey("companies.id", ondelete="CASCADE"),
            nullable=False, index=True,
        ),
        sa.Column("level", sa.Integer, nullable=False),
        sa.Column("node_name", sa.String(500), nullable=False),
        sa.Column("node_kind", sa.String(40), nullable=False),  # corporate/individual/super-secure/statement
        sa.Column("node_ch_number", sa.String(20), nullable=True),
        sa.Column("node_country", sa.String(100), nullable=True),
        sa.Column("natures_of_control", sa.JSON, nullable=True),
        sa.Column("walked_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
    )
    op.add_column("companies", sa.Column(
        "ultimate_owner_name", sa.String(500), nullable=True))
    op.add_column("companies", sa.Column(
        "ultimate_owner_type", sa.String(60), nullable=True, index=True))
    op.add_column("companies", sa.Column(
        "is_spv_candidate", sa.Boolean, nullable=True))
    op.add_column("companies", sa.Column(
        "office_cluster_key", sa.String(120), nullable=True, index=True))
    op.add_column("companies", sa.Column(
        "ownership_checked_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_table("ownership_chain_nodes")
    for col in ("ultimate_owner_name", "ultimate_owner_type",
                "is_spv_candidate", "office_cluster_key",
                "ownership_checked_at"):
        op.drop_column("companies", col)
