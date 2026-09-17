"""M2: append-only task message log for Planner cursors.

Revision ID: 0008_m2_messages
Revises: 0007_sandbox_identity
Create Date: 2026-09-17
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0008_m2_messages"
down_revision: str | None = "0007_sandbox_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "task_messages",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("sender_run_id", sa.String(length=64), nullable=True),
        sa.Column("sender_actor", sa.String(length=128), nullable=True),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("body_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.task_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "sequence_no", name="uq_task_messages_seq"),
    )
    op.create_index("ix_task_messages_task_id", "task_messages", ["task_id"])
    op.execute("UPDATE schema_meta SET value = 'm2' WHERE key = 'schema_version'")


def downgrade() -> None:
    op.drop_index("ix_task_messages_task_id", table_name="task_messages")
    op.drop_table("task_messages")
    op.execute("UPDATE schema_meta SET value = 'm1' WHERE key = 'schema_version'")
