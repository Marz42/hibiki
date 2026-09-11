from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def utcnow() -> datetime:
    return datetime.now(UTC)


class SchemaMeta(Base):
    __tablename__ = "schema_meta"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)


class TaskRow(Base):
    __tablename__ = "tasks"

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    principal_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    intent_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    state_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    contract_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_planner_session_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resume_point: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_result_refs: Mapped[str | None] = mapped_column(Text, nullable=True)
    cancel_intent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    pause_intent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    revoke_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    model_calls_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    model_call_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=200)
    create_idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "principal_id",
            "create_idempotency_key",
            name="uq_tasks_principal_create_key",
        ),
    )


class ContractRow(Base):
    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    supersedes_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    approved_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("task_id", "contract_version", name="uq_contracts_task_version"),
    )


class ActiveContractMarker(Base):
    """Enforces at most one ACTIVE contract per task via unique (task_id)."""

    __tablename__ = "active_contract_markers"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), primary_key=True)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)


class ExplanationRow(Base):
    __tablename__ = "explanations"

    explanation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DecisionRow(Base):
    __tablename__ = "decisions"

    decision_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    decision_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    target_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    target_version: Mapped[int] = mapped_column(Integer, nullable=False)
    target_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    contract_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    explanation_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    explanation_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parameters_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_snapshot_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    choice: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decided_by_actor: Mapped[str | None] = mapped_column(String(64), nullable=True)
    decided_by_principal: Mapped[str | None] = mapped_column(String(64), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    gate_lifecycle: Mapped[str] = mapped_column(String(32), nullable=False, default="OPEN")
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class GateRow(Base):
    __tablename__ = "gates"

    gate_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    decision_id: Mapped[str] = mapped_column(ForeignKey("decisions.decision_id"), nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    lifecycle: Mapped[str] = mapped_column(String(32), nullable=False, default="OPEN")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PlanRow(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    plan_version: Mapped[int] = mapped_column(Integer, nullable=False)
    base_plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    nodes_json: Mapped[str] = mapped_column(Text, nullable=False)
    edges_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("task_id", "plan_version", name="uq_plans_task_version"),
    )


class ActivePlanMarker(Base):
    __tablename__ = "active_plan_markers"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), primary_key=True)
    plan_version: Mapped[int] = mapped_column(Integer, nullable=False)


class WorkUnitSpecRow(Base):
    __tablename__ = "work_unit_specs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    work_unit_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    spec_version: Mapped[int] = mapped_column(Integer, nullable=False)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    objective: Mapped[str] = mapped_column(Text, nullable=False)
    work_type: Mapped[str] = mapped_column(String(32), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("work_unit_id", "spec_version", name="uq_wuspec_id_version"),
    )


class WorkUnitExecutionRow(Base):
    __tablename__ = "work_unit_executions"

    work_unit_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    spec_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    active_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    selected_result_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    blocked_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    selected_verdict: Mapped[str | None] = mapped_column(String(16), nullable=True)
    verified_artifact_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)


class PlannerSessionRow(Base):
    __tablename__ = "planner_sessions"

    planner_session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    active_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checkpoint_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checkpoint_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_consumed_message_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ACTIVE")


class AgentRunRow(Base):
    __tablename__ = "agent_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    assignment_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    planner_session_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    work_unit_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    work_unit_spec_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    agent_instance_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    profile_id: Mapped[str] = mapped_column(String(64), nullable=False, default="fake")
    profile_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    context_manifest_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    workspace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    terminal_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fencing_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    grant_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resumed_from_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    late_arrival: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class ActiveExecuteRunMarker(Base):
    """At most one non-terminal EXECUTE run per work unit."""

    __tablename__ = "active_execute_run_markers"

    work_unit_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("agent_runs.run_id"), nullable=False)


class ActivePlanRunMarker(Base):
    __tablename__ = "active_plan_run_markers"

    planner_session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("agent_runs.run_id"), nullable=False)


class WorkspaceRow(Base):
    __tablename__ = "workspaces"

    workspace_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    work_unit_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="local")
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="READY")
    owner_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fencing_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    writer_alive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class ContextManifestRow(Base):
    __tablename__ = "context_manifests"

    context_manifest_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SideEffectRow(Base):
    __tablename__ = "side_effects"

    effect_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    logical_action_key: Mapped[str] = mapped_column(String(128), nullable=False)
    origin_work_unit_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    request_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    parameters_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    action_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    authorization_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider_operation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    dispatch_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    supports_idempotent_query: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "logical_action_key",
            name="uq_side_effects_task_logical_key",
        ),
    )


class EventRow(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("task_id", "sequence_no", name="uq_events_task_seq"),
    )


class InboxRow(Base):
    __tablename__ = "inbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor_id: Mapped[str] = mapped_column(String(64), nullable=False)
    message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    principal_id: Mapped[str] = mapped_column(String(64), nullable=False)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    result_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("actor_id", "message_id", name="uq_inbox_actor_message"),
        UniqueConstraint(
            "principal_id",
            "task_id",
            "operation_type",
            "idempotency_key",
            name="uq_inbox_idempotency",
        ),
    )


class OutboxRow(Base):
    __tablename__ = "outbox"

    outbox_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    command_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING", index=True)
    revoke_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class IdempotencyRow(Base):
    """Business idempotency for create_task where task_id may not yet exist."""

    __tablename__ = "idempotency_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    principal_id: Mapped[str] = mapped_column(String(64), nullable=False)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    result_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "principal_id",
            "task_id",
            "operation_type",
            "idempotency_key",
            name="uq_idempotency_domain",
        ),
    )


class ResultSnapshotRow(Base):
    __tablename__ = "result_snapshots"

    result_snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArtifactRow(Base):
    """Registered delivery content identity — not a verification verdict (§20.1)."""

    __tablename__ = "artifacts"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), primary_key=True)
    artifact_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    work_unit_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Legacy column; acceptance must use AcceptanceEvidenceRow, not this field.
    verdict: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AcceptanceEvidenceRow(Base):
    """Per-criterion verification evidence bound to a Result / Artifact hash."""

    __tablename__ = "acceptance_evidence_records"

    evidence_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.task_id"), nullable=False, index=True)
    criterion_id: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    work_unit_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    verdict: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "run_id",
            "criterion_id",
            name="uq_acceptance_evidence_run_criterion",
        ),
    )


class AgentProfileRow(Base):
    __tablename__ = "agent_profiles"

    profile_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    profile_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def create_sqlite_engine(url: str = "sqlite:///hibiki.db") -> Engine:
    engine = create_engine(url, connect_args={"check_same_thread": False}, future=True)

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _connection_record):  # type: ignore[no-untyped-def]
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()

    return engine


def make_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def ensure_schema_version(engine: Engine, expected: str = "m0") -> None:
    with engine.connect() as conn:
        try:
            row = conn.execute(
                text("SELECT value FROM schema_meta WHERE key='schema_version'")
            ).fetchone()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "schema version unknown; refuse to start. Run alembic upgrade head."
            ) from exc
        if row is None or row[0] != expected:
            raise RuntimeError(
                f"schema version mismatch: expected {expected!r}, got {None if row is None else row[0]!r}"
            )
