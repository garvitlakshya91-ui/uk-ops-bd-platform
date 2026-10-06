"""Demand layer: institutions, HESA enrolments, maintenance loans.

The foundation of every supply/demand chapter: full-time student numbers
per institution per academic year, mapped to councils (with a demand
adjustment for multi-campus institutions, e.g. Exeter's Penryn campus),
plus SLC maximum maintenance loans for affordability analysis.

Revision ID: 011_demand_layer
Revises: 010_census_fields
Create Date: 2026-10-06
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "011_demand_layer"
down_revision: Union[str, None] = "010_census_fields"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "institutions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        sa.Column("ukprn", sa.String(20), nullable=True),
        sa.Column("council_id", sa.Integer,
                  sa.ForeignKey("councils.id", ondelete="SET NULL"), nullable=True),
        sa.Column("city", sa.String(100), nullable=True),
        sa.Column(
            "demand_adjustment", sa.Float, nullable=False, server_default="1.0",
            comment="Share of FT students who study in this city "
                    "(multi-campus correction, e.g. Exeter ex-Penryn ~0.81)",
        ),
        sa.Column("campus_notes", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_institutions_council", "institutions", ["council_id"])

    op.create_table(
        "hesa_enrolments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("institution_id", sa.Integer,
                  sa.ForeignKey("institutions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("academic_year", sa.String(10), nullable=False,
                  comment="e.g. 2023-24"),
        sa.Column("full_time_students", sa.Integer, nullable=False),
        sa.Column("level", sa.String(30), nullable=False, server_default="all",
                  comment="all / undergraduate / postgraduate"),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("institution_id", "academic_year", "level",
                            name="uq_hesa_inst_year_level"),
    )

    op.create_table(
        "maintenance_loans",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("academic_year", sa.String(10), nullable=False),
        sa.Column("region", sa.String(30), nullable=False,
                  comment="outside_london / london / parental_home"),
        sa.Column("max_loan_gbp", sa.Integer, nullable=False),
        sa.Column("source", sa.String(200), nullable=True),
        sa.UniqueConstraint("academic_year", "region", name="uq_loan_year_region"),
    )


def downgrade() -> None:
    op.drop_table("maintenance_loans")
    op.drop_table("hesa_enrolments")
    op.drop_index("ix_institutions_council", table_name="institutions")
    op.drop_table("institutions")
