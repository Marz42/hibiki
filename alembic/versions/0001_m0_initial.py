"""m0 initial schema

Revision ID: 0001_m0
Revises:
Create Date: 2026-09-07
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_m0"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "schema_meta",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
    )
    op.execute("INSERT INTO schema_meta (key, value) VALUES ('schema_version', 'm0')")

    op.create_table(
        "tasks",
        sa.Column("task_id", sa.String(64), primary_key=True),
        sa.Column("principal_id", sa.String(64), nullable=False),
        sa.Column("intent_ref", sa.Text(), nullable=True),
        sa.Column("title", sa.String(256), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("state_reason", sa.String(64), nullable=True),
        sa.Column("state_revision", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("contract_version", sa.Integer(), nullable=True),
        sa.Column("plan_version", sa.Integer(), nullable=True),
        sa.Column("active_planner_session_id", sa.String(64), nullable=True),
        sa.Column("resume_point", sa.Text(), nullable=True),
        sa.Column("final_result_refs", sa.Text(), nullable=True),
        sa.Column("cancel_intent", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("pause_intent", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("revoke_epoch", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("model_calls_used", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("model_call_limit", sa.Integer(), nullable=False, server_default="200"),
        sa.Column("create_idempotency_key", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("principal_id", "create_idempotency_key", name="uq_tasks_principal_create_key"),
    )
    op.create_index("ix_tasks_principal_id", "tasks", ["principal_id"])
    op.create_index("ix_tasks_state", "tasks", ["state"])

    op.create_table(
        "contracts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("supersedes_version", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("approved_by", sa.String(64), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "contract_version", name="uq_contracts_task_version"),
    )
    op.create_index("ix_contracts_task_id", "contracts", ["task_id"])
    op.create_index("ix_contracts_status", "contracts", ["status"])

    op.create_table(
        "active_contract_markers",
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), primary_key=True),
        sa.Column("contract_version", sa.Integer(), nullable=False),
    )

    op.create_table(
        "explanations",
        sa.Column("explanation_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "decisions",
        sa.Column("decision_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("decision_kind", sa.String(64), nullable=False),
        sa.Column("target_ref", sa.String(128), nullable=False),
        sa.Column("target_version", sa.Integer(), nullable=False),
        sa.Column("target_hash", sa.String(64), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=True),
        sa.Column("explanation_ref", sa.String(64), nullable=True),
        sa.Column("explanation_hash", sa.String(64), nullable=True),
        sa.Column("parameters_hash", sa.String(64), nullable=True),
        sa.Column("result_snapshot_ref", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("choice", sa.String(32), nullable=True),
        sa.Column("decided_by_actor", sa.String(64), nullable=True),
        sa.Column("decided_by_principal", sa.String(64), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("gate_lifecycle", sa.String(32), nullable=False, server_default="OPEN"),
    )
    op.create_index("ix_decisions_task_id", "decisions", ["task_id"])
    op.create_index("ix_decisions_status", "decisions", ["status"])

    op.create_table(
        "gates",
        sa.Column("gate_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("decision_id", sa.String(64), sa.ForeignKey("decisions.decision_id"), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("lifecycle", sa.String(32), nullable=False, server_default="OPEN"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_gates_task_id", "gates", ["task_id"])

    op.create_table(
        "plans",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("plan_version", sa.Integer(), nullable=False),
        sa.Column("base_plan_version", sa.Integer(), nullable=True),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("nodes_json", sa.Text(), nullable=False),
        sa.Column("edges_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "plan_version", name="uq_plans_task_version"),
    )
    op.create_index("ix_plans_task_id", "plans", ["task_id"])
    op.create_index("ix_plans_status", "plans", ["status"])

    op.create_table(
        "active_plan_markers",
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), primary_key=True),
        sa.Column("plan_version", sa.Integer(), nullable=False),
    )

    op.create_table(
        "work_unit_specs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("work_unit_id", sa.String(64), nullable=False),
        sa.Column("spec_version", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("objective", sa.Text(), nullable=False),
        sa.Column("work_type", sa.String(32), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("work_unit_id", "spec_version", name="uq_wuspec_id_version"),
    )
    op.create_index("ix_work_unit_specs_work_unit_id", "work_unit_specs", ["work_unit_id"])
    op.create_index("ix_work_unit_specs_task_id", "work_unit_specs", ["task_id"])

    op.create_table(
        "work_unit_executions",
        sa.Column("work_unit_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("spec_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("active_run_id", sa.String(64), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("selected_result_ref", sa.String(64), nullable=True),
        sa.Column("blocked_reason", sa.String(128), nullable=True),
        sa.Column("selected_verdict", sa.String(16), nullable=True),
        sa.Column("verified_artifact_hash", sa.String(64), nullable=True),
    )
    op.create_index("ix_work_unit_executions_task_id", "work_unit_executions", ["task_id"])
    op.create_index("ix_work_unit_executions_status", "work_unit_executions", ["status"])

    op.create_table(
        "planner_sessions",
        sa.Column("planner_session_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("active_run_id", sa.String(64), nullable=True),
        sa.Column("checkpoint_ref", sa.String(64), nullable=True),
        sa.Column("checkpoint_version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_consumed_message_seq", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(32), nullable=False, server_default="ACTIVE"),
    )
    op.create_index("ix_planner_sessions_task_id", "planner_sessions", ["task_id"])

    op.create_table(
        "agent_runs",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("assignment_kind", sa.String(16), nullable=False),
        sa.Column("planner_session_id", sa.String(64), nullable=True),
        sa.Column("work_unit_id", sa.String(64), nullable=True),
        sa.Column("work_unit_spec_version", sa.Integer(), nullable=True),
        sa.Column("attempt_no", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("agent_instance_id", sa.String(64), nullable=True),
        sa.Column("profile_id", sa.String(64), nullable=False, server_default="fake"),
        sa.Column("profile_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("plan_version", sa.Integer(), nullable=True),
        sa.Column("context_manifest_id", sa.String(64), nullable=True),
        sa.Column("workspace_id", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("terminal_reason", sa.String(128), nullable=True),
        sa.Column("fencing_epoch", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("grant_epoch", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resumed_from_run_id", sa.String(64), nullable=True),
        sa.Column("result_ref", sa.String(64), nullable=True),
        sa.Column("result_json", sa.Text(), nullable=True),
        sa.Column("error_class", sa.String(64), nullable=True),
        sa.Column("late_arrival", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index("ix_agent_runs_task_id", "agent_runs", ["task_id"])
    op.create_index("ix_agent_runs_work_unit_id", "agent_runs", ["work_unit_id"])
    op.create_index("ix_agent_runs_status", "agent_runs", ["status"])

    op.create_table(
        "active_execute_run_markers",
        sa.Column("work_unit_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), sa.ForeignKey("agent_runs.run_id"), nullable=False),
    )

    op.create_table(
        "active_plan_run_markers",
        sa.Column("planner_session_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), sa.ForeignKey("agent_runs.run_id"), nullable=False),
    )

    op.create_table(
        "workspaces",
        sa.Column("workspace_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("work_unit_id", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False, server_default="local"),
        sa.Column("state", sa.String(32), nullable=False, server_default="READY"),
        sa.Column("owner_run_id", sa.String(64), nullable=True),
        sa.Column("fencing_epoch", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("writer_alive", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index("ix_workspaces_task_id", "workspaces", ["task_id"])
    op.create_index("ix_workspaces_work_unit_id", "workspaces", ["work_unit_id"])

    op.create_table(
        "context_manifests",
        sa.Column("context_manifest_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("plan_version", sa.Integer(), nullable=True),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("run_id", name="uq_context_manifests_run_id"),
    )

    op.create_table(
        "side_effects",
        sa.Column("effect_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("logical_action_key", sa.String(128), nullable=False),
        sa.Column("origin_work_unit_id", sa.String(64), nullable=True),
        sa.Column("request_run_id", sa.String(64), nullable=True),
        sa.Column("action_type", sa.String(64), nullable=False),
        sa.Column("target_ref", sa.String(256), nullable=False),
        sa.Column("parameters_hash", sa.String(64), nullable=False),
        sa.Column("action_digest", sa.String(64), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("authorization_ref", sa.String(64), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_idempotency_key", sa.String(128), nullable=False),
        sa.Column("provider_operation_id", sa.String(128), nullable=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("dispatch_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("receipt_json", sa.Text(), nullable=True),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("supports_idempotent_query", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "logical_action_key", name="uq_side_effects_task_logical_key"),
    )
    op.create_index("ix_side_effects_task_id", "side_effects", ["task_id"])
    op.create_index("ix_side_effects_status", "side_effects", ["state"])

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("actor_id", sa.String(64), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "sequence_no", name="uq_events_task_seq"),
    )
    op.create_index("ix_events_task_id", "events", ["task_id"])

    op.create_table(
        "inbox",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("actor_id", sa.String(64), nullable=False),
        sa.Column("message_id", sa.String(128), nullable=False),
        sa.Column("principal_id", sa.String(64), nullable=False),
        sa.Column("task_id", sa.String(64), nullable=True),
        sa.Column("operation_type", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("result_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("actor_id", "message_id", name="uq_inbox_actor_message"),
        sa.UniqueConstraint(
            "principal_id",
            "task_id",
            "operation_type",
            "idempotency_key",
            name="uq_inbox_idempotency",
        ),
    )

    op.create_table(
        "outbox",
        sa.Column("outbox_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("command_type", sa.String(64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("revoke_epoch", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_outbox_task_id", "outbox", ["task_id"])
    op.create_index("ix_outbox_status", "outbox", ["status"])

    op.create_table(
        "idempotency_keys",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("principal_id", sa.String(64), nullable=False),
        sa.Column("task_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("operation_type", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("result_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "principal_id",
            "task_id",
            "operation_type",
            "idempotency_key",
            name="uq_idempotency_domain",
        ),
    )

    op.create_table(
        "result_snapshots",
        sa.Column("result_snapshot_id", sa.String(64), primary_key=True),
        sa.Column("task_id", sa.String(64), sa.ForeignKey("tasks.task_id"), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("plan_version", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "agent_profiles",
        sa.Column("profile_id", sa.String(64), primary_key=True),
        sa.Column("profile_version", sa.Integer(), primary_key=True),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    for table in [
        "agent_profiles",
        "result_snapshots",
        "idempotency_keys",
        "outbox",
        "inbox",
        "events",
        "side_effects",
        "context_manifests",
        "workspaces",
        "active_plan_run_markers",
        "active_execute_run_markers",
        "agent_runs",
        "planner_sessions",
        "work_unit_executions",
        "work_unit_specs",
        "active_plan_markers",
        "plans",
        "gates",
        "decisions",
        "explanations",
        "active_contract_markers",
        "contracts",
        "tasks",
        "schema_meta",
    ]:
        op.drop_table(table)
