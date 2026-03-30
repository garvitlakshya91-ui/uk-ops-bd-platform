"""Add assigned_to_user_id column to pipeline_opportunities.

Revision ID: 005_add_assigned_to_user_id
Revises: 004_add_users
Create Date: 2026-03-30

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "005_add_assigned_to_user_id"
down_revision: Union[str, None] = "004_add_users"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pipeline_opportunities",
        sa.Column(
            "assigned_to_user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_pipeline_opportunities_assigned_to_user_id",
        "pipeline_opportunities",
        ["assigned_to_user_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pipeline_opportunities_assigned_to_user_id",
        table_name="pipeline_opportunities",
    )
    op.drop_column("pipeline_opportunities", "assigned_to_user_id")
