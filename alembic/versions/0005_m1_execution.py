"""m1 execution records

Revision ID: 0005_m1_execution
Revises: 0004_evidence_sequence
Create Date: 2026-09-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0005_m1_execution"
down_revision: str | None = "0004_evidence_sequence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "run_inputs",
        sa.Column("run_id", sa.String(64), sa.ForeignKey("agent_runs.run_id"), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("workspace_id", sa.String(64), nullable=True),
        sa.Column("workspace_path", sa.String(1024), nullable=True),
        sa.Column("profile_id", sa.String(64), nullable=False),
        sa.Column("profile_version", sa.Integer(), nullable=False),
        sa.Column("granted_tools_json", sa.Text(), nullable=False),
        sa.Column("permission_ceiling_json", sa.Text(), nullable=False),
        sa.Column("context_manifest_id", sa.String(64), nullable=True),
        sa.Column("spec_json", sa.Text(), nullable=False),
        sa.Column("spec_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_run_inputs_task_id", "run_inputs", ["task_id"])

    op.create_table(
        "tool_invocations",
        sa.Column("invocation_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("run_id", sa.String(64), sa.ForeignKey("agent_runs.run_id"), nullable=False),
        sa.Column("work_unit_id", sa.String(64), nullable=True),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("tool_name", sa.String(64), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=False),
        sa.Column("parameters_hash", sa.String(64), nullable=False),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("deny_reason", sa.String(256), nullable=True),
        sa.Column("grant_epoch", sa.Integer(), nullable=False),
        sa.Column("fencing_epoch", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=True),
        sa.Column("result_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("task_id", "run_id", "sequence_no", name="uq_tool_invocation_seq"),
    )
    op.create_index("ix_tool_invocations_task_id", "tool_invocations", ["task_id"])
    op.create_index("ix_tool_invocations_run_id", "tool_invocations", ["run_id"])

    op.create_table(
        "context_appends",
        sa.Column("append_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("run_id", sa.String(64), sa.ForeignKey("agent_runs.run_id"), nullable=False),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("authorized_ref", sa.String(512), nullable=False),
        sa.Column("version", sa.String(64), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("grant_ref", sa.String(128), nullable=True),
        sa.Column("materialized_hash", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "run_id", "sequence_no", name="uq_context_append_seq"),
    )
    op.create_index("ix_context_appends_task_id", "context_appends", ["task_id"])
    op.create_index("ix_context_appends_run_id", "context_appends", ["run_id"])

    # The M1 schema generation records its own version string. M0 wrote "m0" in 0001;
    # this migration is the point where the frozen M0 schema becomes the M1 schema.
    op.execute("UPDATE schema_meta SET value = 'm1' WHERE key = 'schema_version'")


def downgrade() -> None:
    op.execute("UPDATE schema_meta SET value = 'm0' WHERE key = 'schema_version'")
    op.drop_index("ix_context_appends_run_id", table_name="context_appends")
    op.drop_index("ix_context_appends_task_id", table_name="context_appends")
    op.drop_table("context_appends")
    op.drop_index("ix_tool_invocations_run_id", table_name="tool_invocations")
    op.drop_index("ix_tool_invocations_task_id", table_name="tool_invocations")
    op.drop_table("tool_invocations")
    op.drop_index("ix_run_inputs_task_id", table_name="run_inputs")
    op.drop_table("run_inputs")
