"""artifact content reference

Revision ID: 0006_artifact_uri
Revises: 0005_m1_execution
Create Date: 2026-09-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0006_artifact_uri"
down_revision: str | None = "0005_m1_execution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("artifacts", sa.Column("artifact_uri", sa.String(512), nullable=True))
    op.add_column("artifacts", sa.Column("size_bytes", sa.Integer(), nullable=True))
    op.add_column("artifacts", sa.Column("mime_type", sa.String(128), nullable=True))
    op.add_column("artifacts", sa.Column("source_path", sa.String(1024), nullable=True))


def downgrade() -> None:
    op.drop_column("artifacts", "source_path")
    op.drop_column("artifacts", "mime_type")
    op.drop_column("artifacts", "size_bytes")
    op.drop_column("artifacts", "artifact_uri")
