"""Master database: timestamped observations, room mix, events, finance.

The product is a scheme-level time series. Every fact about a scheme
arrives as an observation (never overwritten); existing_schemes keeps
only the latest value as a cache. Adds the room-mix, event and
transaction tables the underwriting product needs, published yield
benchmarks, and construction status on planning applications.

Revision ID: 016_master_database
Revises: 015_field_provenance
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "016_master_database"
down_revision: Union[str, None] = "015_field_provenance"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheme_observations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("scheme_id", sa.Integer,
                  sa.ForeignKey("existing_schemes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("field", sa.String(60), nullable=False,
                  comment="beds_total, operator, build_year, amenities, incentive, ..."),
        sa.Column("value_text", sa.Text, nullable=True),
        sa.Column("value_num", sa.Numeric(14, 2), nullable=True),
        sa.Column("value_json", JSONB, nullable=True),
        sa.Column("basis", sa.String(20), nullable=False, server_default="observed",
                  comment="observed / derived / forecast"),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column("academic_year", sa.String(10), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_obs_scheme_field_time", "scheme_observations",
                    ["scheme_id", "field", "observed_at"])

    op.create_table(
        "scheme_room_types",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("scheme_id", sa.Integer,
                  sa.ForeignKey("existing_schemes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("room_type", sa.String(60), nullable=False),
        sa.Column("sub_classification", sa.String(150), nullable=True),
        sa.Column("rooms_count", sa.Integer, nullable=True),
        sa.Column("room_size_sqm", sa.Float, nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("observed_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_room_types_scheme", "scheme_room_types", ["scheme_id", "is_current"])

    op.create_table(
        "scheme_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("scheme_id", sa.Integer,
                  sa.ForeignKey("existing_schemes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False,
                  comment="opened, closed, rebranded, operator_change, owner_change, "
                          "sold, refurbished, construction_start, construction_complete"),
        sa.Column("event_date", sa.Date, nullable=True),
        sa.Column("detail", JSONB, nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_events_scheme", "scheme_events", ["scheme_id", "event_date"])

    op.create_table(
        "transactions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("scheme_id", sa.Integer,
                  sa.ForeignKey("existing_schemes.id", ondelete="SET NULL"),
                  nullable=True),
        sa.Column("council_id", sa.Integer,
                  sa.ForeignKey("councils.id", ondelete="SET NULL"), nullable=True),
        sa.Column("asset_name", sa.String(255), nullable=False),
        sa.Column("transaction_date", sa.Date, nullable=True),
        sa.Column("price_gbp", sa.Numeric(14, 0), nullable=True),
        sa.Column("beds", sa.Integer, nullable=True),
        sa.Column("price_per_bed_gbp", sa.Numeric(12, 0), nullable=True),
        sa.Column("buyer", sa.String(255), nullable=True),
        sa.Column("seller", sa.String(255), nullable=True),
        sa.Column("yield_pct", sa.Float, nullable=True),
        sa.Column("asset_built_year", sa.Integer, nullable=True),
        sa.Column("deal_type", sa.String(40), nullable=True,
                  comment="asset, portfolio, forward_funding, refinance"),
        sa.Column("basis", sa.String(20), nullable=False, server_default="observed"),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("source_reference", sa.String(500), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_transactions_council_date", "transactions",
                    ["council_id", "transaction_date"])

    op.create_table(
        "yield_benchmarks",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("segment", sa.String(60), nullable=False,
                  comment="prime_london, super_prime_regional, prime_regional, secondary"),
        sa.Column("yield_pct", sa.Float, nullable=False),
        sa.Column("as_of", sa.Date, nullable=False),
        sa.Column("source", sa.String(200), nullable=False),
        sa.UniqueConstraint("segment", "as_of", "source", name="uq_yield_seg_date_src"),
    )

    op.execute("""
        ALTER TABLE planning_applications
            ADD COLUMN IF NOT EXISTS construction_status VARCHAR(40),
            ADD COLUMN IF NOT EXISTS construction_evidence_at DATE,
            ADD COLUMN IF NOT EXISTS construction_source VARCHAR(200)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE planning_applications
            DROP COLUMN IF EXISTS construction_source,
            DROP COLUMN IF EXISTS construction_evidence_at,
            DROP COLUMN IF EXISTS construction_status
    """)
    op.drop_table("yield_benchmarks")
    op.drop_index("ix_transactions_council_date", table_name="transactions")
    op.drop_table("transactions")
    op.drop_index("ix_events_scheme", table_name="scheme_events")
    op.drop_table("scheme_events")
    op.drop_index("ix_room_types_scheme", table_name="scheme_room_types")
    op.drop_table("scheme_room_types")
    op.drop_index("ix_obs_scheme_field_time", table_name="scheme_observations")
    op.drop_table("scheme_observations")
