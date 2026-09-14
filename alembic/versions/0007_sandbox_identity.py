"""Persist Run ↔ sandbox container identity for stop/reconcile confirmation.

Revision ID: 0007_sandbox_identity
Revises: 0006_artifact_uri
Create Date: 2026-09-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0007_sandbox_identity"
down_revision: str | None = "0006_artifact_uri"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "agent_runs", sa.Column("sandbox_container_id", sa.String(128), nullable=True)
    )
    op.add_column(
        "agent_runs",
        sa.Column(
            "sandbox_exit_unconfirmed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("agent_runs", "sandbox_exit_unconfirmed")
    op.drop_column("agent_runs", "sandbox_container_id")
