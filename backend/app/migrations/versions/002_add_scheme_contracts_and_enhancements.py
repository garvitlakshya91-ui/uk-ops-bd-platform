"""Add scheme_contracts, scheme_change_logs tables and enhance schemes.

Revision ID: 002_scheme_contracts
Revises: 001_initial
Create Date: 2026-03-26

Adds:
- New columns on schemes: asset_manager_company_id, landlord_company_id,
  status, source, source_reference, last_verified_at, data_confidence_score.
- scheme_contracts table for contract history tracking.
- scheme_change_logs table for audit trail on scheme field changes.
- Indexes on new columns and tables.
- Data migration: copies legacy contract data from schemes into
  scheme_contracts.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "002_scheme_contracts"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Add new columns to schemes (all nullable for backward
    #    compatibility with existing rows).
    # ------------------------------------------------------------------
    op.add_column(
        "schemes",
        sa.Column(
            "asset_manager_company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "schemes",
        sa.Column(
            "landlord_company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "schemes",
        sa.Column("source", sa.String(100), nullable=True),
    )
    op.add_column(
        "schemes",
        sa.Column("source_reference", sa.String(500), nullable=True),
    )
    op.add_column(
        "schemes",
        sa.Column(
            "last_verified_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "schemes",
        sa.Column("data_confidence_score", sa.Float(), nullable=True),
    )

    # Indexes on the new schemes columns
    op.create_index(
        "ix_schemes_source",
        "schemes",
        ["source"],
    )
    op.create_index(
        "ix_schemes_last_verified_at",
        "schemes",
        ["last_verified_at"],
    )
    op.create_index(
        "ix_schemes_asset_manager_company_id",
        "schemes",
        ["asset_manager_company_id"],
    )
    op.create_index(
        "ix_schemes_landlord_company_id",
        "schemes",
        ["landlord_company_id"],
    )

    # ------------------------------------------------------------------
    # 2. Create scheme_contracts table
    # ------------------------------------------------------------------
    op.create_table(
        "scheme_contracts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "scheme_id",
            sa.Integer(),
            sa.ForeignKey("schemes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("contract_reference", sa.String(500), nullable=True),
        sa.Column("contract_type", sa.String(100), nullable=True),
        sa.Column(
            "operator_company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "client_company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("contract_start_date", sa.Date(), nullable=True),
        sa.Column("contract_end_date", sa.Date(), nullable=True),
        sa.Column("contract_value", sa.Float(), nullable=True),
        sa.Column(
            "currency",
            sa.String(3),
            nullable=False,
            server_default="GBP",
        ),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column(
            "is_current",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("raw_data", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_index(
        "ix_scheme_contracts_scheme_id",
        "scheme_contracts",
        ["scheme_id"],
    )
    op.create_index(
        "ix_scheme_contracts_end_date",
        "scheme_contracts",
        ["contract_end_date"],
    )
    op.create_index(
        "ix_scheme_contracts_source_reference",
        "scheme_contracts",
        ["source_reference"],
    )
    op.create_index(
        "ix_scheme_contracts_is_current",
        "scheme_contracts",
        ["is_current"],
    )

    # ------------------------------------------------------------------
    # 3. Create scheme_change_logs table
    # ------------------------------------------------------------------
    op.create_table(
        "scheme_change_logs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "scheme_id",
            sa.Integer(),
            sa.ForeignKey("schemes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("field_name", sa.String(100), nullable=False),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("changed_by", sa.String(100), nullable=True),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_index(
        "ix_scheme_change_logs_scheme_id",
        "scheme_change_logs",
        ["scheme_id"],
    )
    op.create_index(
        "ix_scheme_change_logs_changed_at",
        "scheme_change_logs",
        ["changed_at"],
    )
    op.create_index(
        "ix_scheme_change_logs_field_name",
        "scheme_change_logs",
        ["field_name"],
    )

    # ------------------------------------------------------------------
    # 4. Data migration: copy legacy contract info from schemes
    #    into scheme_contracts so that historical data is preserved.
    # ------------------------------------------------------------------
    op.execute(
        sa.text(
            """
            INSERT INTO scheme_contracts
                (scheme_id, contract_start_date, contract_end_date,
                 operator_company_id, source, is_current)
            SELECT
                id,
                contract_start_date,
                contract_end_date,
                operator_company_id,
                'legacy_migration',
                TRUE
            FROM schemes
            WHERE contract_start_date IS NOT NULL
               OR contract_end_date IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    # Drop data created by the data migration (cascade handles the rows
    # when the table itself is dropped, so no explicit DELETE needed).

    # Drop new tables
    op.drop_table("scheme_change_logs")
    op.drop_table("scheme_contracts")

    # Drop indexes on schemes columns
    op.drop_index("ix_schemes_landlord_company_id", table_name="schemes")
    op.drop_index("ix_schemes_asset_manager_company_id", table_name="schemes")
    op.drop_index("ix_schemes_last_verified_at", table_name="schemes")
    op.drop_index("ix_schemes_source", table_name="schemes")

    # Drop new columns from schemes
    op.drop_column("schemes", "data_confidence_score")
    op.drop_column("schemes", "last_verified_at")
    op.drop_column("schemes", "source_reference")
    op.drop_column("schemes", "source")
    op.drop_column("schemes", "landlord_company_id")
    op.drop_column("schemes", "asset_manager_company_id")
