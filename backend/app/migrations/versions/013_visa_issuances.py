"""Sponsored-study visa issuances — the leading international signal.

Home Office quarterly entry-clearance statistics, national level. HESA
confirms international enrolment ~18 months late; visa grants move a
year or more earlier (the 2024 dependant-rule shock was visible here
four quarters before it reached enrolment data).

Revision ID: 013_visa_issuances
Revises: 012_availability
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "013_visa_issuances"
down_revision: Union[str, None] = "012_availability"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "visa_issuances",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("quarter", sa.String(10), nullable=False,
                  comment="e.g. 2026Q2"),
        sa.Column("nationality", sa.String(100), nullable=True,
                  comment="NULL = all nationalities"),
        sa.Column("visas_granted", sa.Integer, nullable=False),
        sa.Column("source", sa.String(200), nullable=True),
        sa.UniqueConstraint("quarter", "nationality",
                            name="uq_visa_quarter_nat"),
    )


def downgrade() -> None:
    op.drop_table("visa_issuances")
