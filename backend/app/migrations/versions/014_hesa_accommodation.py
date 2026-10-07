"""HESA term-time accommodation (national) — demand-pool coefficients.

HESA chart 4: full-time students by term-time accommodation type,
entrant marker and academic year (UK level). Supplies the published
shares used to turn headcount into a PBSA-relevant demand pool
(deducting students at the parental home or in their own residence),
replacing invented propensity weights.

Revision ID: 014_hesa_accommodation
Revises: 013_visa_issuances
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "014_hesa_accommodation"
down_revision: Union[str, None] = "013_visa_issuances"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "hesa_term_time_accommodation",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("academic_year", sa.String(10), nullable=False),
        sa.Column("entrant_marker", sa.String(30), nullable=False,
                  comment="All / Entrant / Not an entrant"),
        sa.Column("accommodation", sa.String(100), nullable=False),
        sa.Column("students", sa.Integer, nullable=False),
        sa.Column("source", sa.String(200), nullable=True),
        sa.UniqueConstraint("academic_year", "entrant_marker", "accommodation",
                            name="uq_hesa_tta"),
    )


def downgrade() -> None:
    op.drop_table("hesa_term_time_accommodation")
