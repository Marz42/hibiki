"""m0 evidence records and decision payload

Revision ID: 0003_evidence
Revises: 0002_artifacts
Create Date: 2026-09-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_evidence"
down_revision: str | None = "0002_artifacts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("decisions", sa.Column("payload_json", sa.Text(), nullable=True))
    op.create_table(
        "acceptance_evidence_records",
        sa.Column("evidence_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("criterion_id", sa.String(64), nullable=False),
        sa.Column("artifact_hash", sa.String(64), nullable=False),
        sa.Column("work_unit_id", sa.String(64), nullable=True),
        sa.Column("run_id", sa.String(64), nullable=True),
        sa.Column("result_ref", sa.String(64), nullable=True),
        sa.Column("verdict", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "task_id",
            "run_id",
            "criterion_id",
            name="uq_acceptance_evidence_run_criterion",
        ),
    )
    op.create_index(
        "ix_acceptance_evidence_records_task_id",
        "acceptance_evidence_records",
        ["task_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_acceptance_evidence_records_task_id",
        table_name="acceptance_evidence_records",
    )
    op.drop_table("acceptance_evidence_records")
    op.drop_column("decisions", "payload_json")
