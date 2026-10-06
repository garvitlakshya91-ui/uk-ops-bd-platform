"""Census fields for the reports product.

existing_schemes gains PBSA-census fields: beds_total (beds are not
units), build_year, nominations, operating_status. planning_applications
gains pbsa_beds (parsed from text) and expected_delivery_year.

ADD COLUMN IF NOT EXISTS throughout: the production database has columns
added outside alembic (e.g. total_units), and this migration must apply
cleanly both there and on ORM-created databases.

Revision ID: 010_census_fields
Revises: 009_rent_history
Create Date: 2026-10-06
"""
from typing import Sequence, Union

from alembic import op


revision: str = "010_census_fields"
down_revision: Union[str, None] = "009_rent_history"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE existing_schemes
            ADD COLUMN IF NOT EXISTS total_units INTEGER,
            ADD COLUMN IF NOT EXISTS beds_total INTEGER,
            ADD COLUMN IF NOT EXISTS build_year INTEGER,
            ADD COLUMN IF NOT EXISTS nominations BOOLEAN,
            ADD COLUMN IF NOT EXISTS operating_status VARCHAR(50)
    """)
    op.execute("""
        ALTER TABLE planning_applications
            ADD COLUMN IF NOT EXISTS pbsa_beds INTEGER,
            ADD COLUMN IF NOT EXISTS expected_delivery_year INTEGER
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE planning_applications
            DROP COLUMN IF EXISTS expected_delivery_year,
            DROP COLUMN IF EXISTS pbsa_beds
    """)
    op.execute("""
        ALTER TABLE existing_schemes
            DROP COLUMN IF EXISTS operating_status,
            DROP COLUMN IF EXISTS nominations,
            DROP COLUMN IF EXISTS build_year,
            DROP COLUMN IF EXISTS beds_total
    """)
