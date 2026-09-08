"""m0 artifacts registry

Revision ID: 0002_artifacts
Revises: 0001_m0
Create Date: 2026-09-08
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_artifacts"
down_revision: str | None = "0001_m0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "artifacts",
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), primary_key=True),
        sa.Column("artifact_hash", sa.String(64), primary_key=True),
        sa.Column("work_unit_id", sa.String(64), nullable=True),
        sa.Column("run_id", sa.String(64), nullable=True),
        sa.Column("result_ref", sa.String(64), nullable=True),
        sa.Column("verdict", sa.String(16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_artifacts_work_unit_id", "artifacts", ["work_unit_id"])


def downgrade() -> None:
    op.drop_index("ix_artifacts_work_unit_id", table_name="artifacts")
    op.drop_table("artifacts")
