"""Clone flag: mark planning rows that were copied across councils.

On 16 May 2026 an ingest fault wrote the same 5,000 applications under
about 260 councils each -- 1.25 million rows that share a reference and
description but belong to no particular council. is_clone marks them so
lists, counts, maps and scores can leave them out. Nothing is removed.

This migration only adds the column (a constant default, so no table
rewrite). The rows are marked by backend/sql/mark_cloned_planning_rows.sql,
run by hand after a backup.

Revision ID: 022_planning_is_clone
Revises: 021_record_class
Create Date: 2026-10-11
"""
from typing import Sequence, Union

from alembic import op


revision: str = "022_planning_is_clone"
down_revision: Union[str, None] = "021_record_class"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE planning_applications "
               "ADD COLUMN IF NOT EXISTS is_clone BOOLEAN NOT NULL DEFAULT false")
    op.execute("COMMENT ON COLUMN planning_applications.is_clone IS "
               "'Copied across councils by the 16 May 2026 ingest fault; excluded from reporting'")


def downgrade() -> None:
    op.execute("ALTER TABLE planning_applications DROP COLUMN IF EXISTS is_clone")
