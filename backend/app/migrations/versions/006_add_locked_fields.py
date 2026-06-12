"""Add locked_fields JSONB column to existing_schemes for per-field source tracking.

Revision ID: 006_add_locked_fields
Revises: 005_add_assigned_to_user_id
Create Date: 2026-04-19

Adds ``locked_fields`` (JSONB, default {}) to track which source wrote each
protected field. Downstream writers compare their precedence against the lock
before overwriting. Backfills existing rows by seeding each protected field
with the row's existing ``source`` value.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "006_add_locked_fields"
down_revision: Union[str, None] = "005_add_assigned_to_user_id"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "existing_schemes",
        sa.Column(
            "locked_fields",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )

    # Backfill: for each protected field that is non-null, record its lock
    # as the scheme's existing `source` value (or 'unknown' if source is null).
    # Use jsonb_build_object + COALESCE + CASE WHEN <field> IS NOT NULL.
    op.execute(
        """
        UPDATE existing_schemes
        SET locked_fields = (
            COALESCE(
                CASE WHEN num_units IS NOT NULL
                     THEN jsonb_build_object('num_units', COALESCE(source, 'unknown'))
                     ELSE '{}'::jsonb END, '{}'::jsonb
            )
            || CASE WHEN operator_company_id IS NOT NULL
                    THEN jsonb_build_object('operator_company_id', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
            || CASE WHEN owner_company_id IS NOT NULL
                    THEN jsonb_build_object('owner_company_id', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
            || CASE WHEN asset_manager_company_id IS NOT NULL
                    THEN jsonb_build_object('asset_manager_company_id', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
            || CASE WHEN landlord_company_id IS NOT NULL
                    THEN jsonb_build_object('landlord_company_id', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
            || CASE WHEN contract_start_date IS NOT NULL
                    THEN jsonb_build_object('contract_start_date', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
            || CASE WHEN contract_end_date IS NOT NULL
                    THEN jsonb_build_object('contract_end_date', COALESCE(source, 'unknown'))
                    ELSE '{}'::jsonb END
        )
        """
    )

    op.create_index(
        "ix_existing_schemes_locked_fields",
        "existing_schemes",
        ["locked_fields"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_existing_schemes_locked_fields",
        table_name="existing_schemes",
    )
    op.drop_column("existing_schemes", "locked_fields")
