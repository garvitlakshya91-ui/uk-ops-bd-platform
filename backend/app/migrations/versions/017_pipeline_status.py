"""Pipeline status: permission type, parent links, delivery status, envelopes.

A planning register row is not a pipeline scheme. Condition discharges,
non-material amendments, S73 variations and listed-building consents are
children of a parent consent; outline and hybrid permissions carry an
"up to N" envelope, not a committed bed count; and a consent is only
"in the pipeline" while there is evidence it is alive. These columns let
the report classify every row by what it actually is.

Revision ID: 017_pipeline_status
Revises: 016_master_database
Create Date: 2026-10-08
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "017_pipeline_status"
down_revision: Union[str, None] = "016_master_database"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


COLUMNS = [
    sa.Column("permission_type", sa.String(40), nullable=True,
              comment="full, outline, hybrid, s73_variation, nma, conditions, "
                      "listed_building, screening, advert, temporary_use, other"),
    sa.Column("parent_reference", sa.String(60), nullable=True,
              comment="Reference of the consent this child application belongs to"),
    sa.Column("superseded_by_reference", sa.String(60), nullable=True,
              comment="Later consent/application for the same site"),
    sa.Column("delivery_status", sa.String(40), nullable=True,
              comment="completed, under_construction, pre_letting, active_consent, "
                      "consented_full, consented_outline, submitted, dormant, "
                      "superseded, child, refused, withdrawn"),
    sa.Column("delivery_status_basis", sa.String(20), nullable=True,
              comment="observed / derived"),
    sa.Column("delivery_status_evidence", sa.Text, nullable=True),
    sa.Column("delivery_status_at", sa.Date, nullable=True),
    sa.Column("beds_max", sa.Integer, nullable=True,
              comment="'Up to N' envelope on outline/hybrid permissions"),
    sa.Column("beds_basis", sa.String(30), nullable=True,
              comment="stated, up_to, units, studios, variation, ai, child"),
    sa.Column("delivery_year_basis", sa.String(20), nullable=True,
              comment="observed (operator/press statement) or derived (decision + build)"),
]


def upgrade() -> None:
    for col in COLUMNS:
        op.execute(
            f"ALTER TABLE planning_applications ADD COLUMN IF NOT EXISTS "
            f"{col.name} {col.type.compile(dialect=op.get_bind().dialect)}"
        )
    op.execute("CREATE INDEX IF NOT EXISTS ix_planning_applications_delivery_status "
               "ON planning_applications (delivery_status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_planning_applications_parent_reference "
               "ON planning_applications (parent_reference)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_planning_applications_parent_reference")
    op.execute("DROP INDEX IF EXISTS ix_planning_applications_delivery_status")
    for col in reversed(COLUMNS):
        op.execute(f"ALTER TABLE planning_applications DROP COLUMN IF EXISTS {col.name}")
