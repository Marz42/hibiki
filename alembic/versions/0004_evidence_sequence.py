"""acceptance evidence sequence_no

Revision ID: 0004_evidence_sequence
Revises: 0003_evidence
Create Date: 2026-09-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_evidence_sequence"
down_revision: str | None = "0003_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "acceptance_evidence_records",
        sa.Column("sequence_no", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("acceptance_evidence_records", "sequence_no")
