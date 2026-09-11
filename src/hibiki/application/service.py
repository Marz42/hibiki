from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hibiki.domain.defaults import DEFAULTS
from hibiki.domain.enums import (
    AgentRunStatus,
    AssignmentKind,
    ContractStatus,
    DecisionKind,
    DecisionStatus,
    DependencyPredicate,
    GateLifecycle,
    OutboxStatus,
    PlanStatus,
    SideEffectState,
    TaskState,
    Verdict,
    WaitingReason,
    WorkspaceState,
    WorkUnitStatus,
)
from hibiki.domain.errors import (
    AuthorizationError,
    ConflictError,
    DomainError,
    IdempotencyConflictError,
    NotFoundError,
    PreconditionError,
)
from hibiki.domain.guards import (
    guard_dispatch,
    guard_human_decision,
    guard_operation,
    guard_side_effect_send,
)
from hibiki.domain.hashing import canonical_json, content_hash, payload_hash
from hibiki.domain.plan import PlanEdge, PlanNode, validate_dag
from hibiki.domain.ports import AgentAdapter, Clock, ExternalAdapter
from hibiki.domain.transitions import (
    is_terminal_run,
    is_terminal_task,
    is_terminal_work_unit,
    transition_run,
    transition_side_effect,
    transition_task,
    transition_work_unit,
)
from hibiki.domain.types import AuthContext, CommandResult
from hibiki.persistence.models import (
    AcceptanceEvidenceRow,
    ActiveContractMarker,
    ActiveExecuteRunMarker,
    ActivePlanMarker,
    AgentProfileRow,
    AgentRunRow,
    ArtifactRow,
    ContextManifestRow,
    ContractRow,
    DecisionRow,
    EventRow,
    ExplanationRow,
    GateRow,
    IdempotencyRow,
    InboxRow,
    OutboxRow,
    PlannerSessionRow,
    PlanRow,
    ResultSnapshotRow,
    SideEffectRow,
    TaskRow,
    WorkspaceRow,
    WorkUnitExecutionRow,
    WorkUnitSpecRow,
)
from hibiki.persistence.session import SerialSessionExecutor
from hibiki.runtime.clock import as_utc_naive, new_id


class ApplicationService:
    """Single application entry — all writes via serial transactions."""

    def __init__(
        self,
        executor: SerialSessionExecutor,
        clock: Clock,
        agent_adapter: AgentAdapter,
        external_adapter: ExternalAdapter,
        *,
        dispatch_enabled: bool = True,
    ) -> None:
        self.executor = executor
        self.clock = clock
        self.agent_adapter = agent_adapter
        self.external_adapter = external_adapter
        self.dispatch_enabled = dispatch_enabled
        self._wake_requested = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(
        self,
        operation: str,
        auth: AuthContext,
        payload: dict[str, Any],
        *,
        message_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> CommandResult:
        try:
            guard_operation(auth, operation)
        except AuthorizationError as exc:
            return CommandResult.failure(exc.code, exc.message)

        # Strip client-supplied identity fields — never authoritative
        clean = {
            k: v
            for k, v in payload.items()
            if k not in {"principal_id", "actor_id", "actor_type", "auth_context_id"}
        }
        mid = message_id or new_id("msg")
        ikey = idempotency_key or mid
        ph = payload_hash({"operation": operation, **clean})

        try:
            result = self.executor.run(
                lambda s: self._execute_in_tx(s, operation, auth, clean, mid, ikey, ph)
            )
        except DomainError as exc:
            return CommandResult.failure(exc.code, exc.message)
        except RuntimeError as exc:
            if str(exc) in {"injected_crash_before_commit", "injected_crash_after_commit"}:
                raise
            return CommandResult.failure("runtime_error", str(exc))

        if self._wake_requested and self.dispatch_enabled:
            self._wake_requested = False
            self.drain_outbox()
        return result

    def drain_outbox(self) -> int:
        """Process pending outbox commands outside DB transaction for I/O."""
        processed = 0
        while True:
            item = self.executor.run(self._claim_one_outbox)
            if item is None:
                break
            self._dispatch_outbox_item(item)
            processed += 1
        return processed

    def reconcile(self) -> dict[str, Any]:
        """Startup reconciliation — no auto-dispatch for PAUSED/WAITING_HUMAN."""

        def _recon(session: Session) -> dict[str, Any]:
            notes: list[str] = []
            now = self.clock.now()
            # Recover expired IN_FLIGHT outbox without blind side-effect replay
            for row in session.scalars(
                select(OutboxRow).where(OutboxRow.status == OutboxStatus.IN_FLIGHT)
            ).all():
                leased = row.leased_until
                if leased is not None and as_utc_naive(leased) > now:
                    continue
                if row.command_type == "side_effect.dispatch":
                    payload = json.loads(row.payload_json)
                    effect = session.get(SideEffectRow, payload.get("effect_id"))
                    if effect and effect.state in {
                        SideEffectState.DISPATCHING,
                        SideEffectState.UNKNOWN,
                    }:
                        effect.state = SideEffectState.UNKNOWN
                        effect.last_error = effect.last_error or "outbox_inflight_timeout"
                        task = session.get(TaskRow, effect.task_id)
                        if task and not is_terminal_task(TaskState(task.state)):
                            if TaskState(task.state) != TaskState.WAITING_HUMAN:
                                self._set_task_state(
                                    session,
                                    task,
                                    "blocking_gate.opened",
                                    reason=WaitingReason.EXECUTION_UNCERTAIN,
                                )
                    row.status = OutboxStatus.ACKED
                    row.acked_at = now
                    notes.append(f"outbox_unknown:{row.outbox_id}")
                elif row.command_type == "agent.start":
                    notes.append(
                        self._reclaim_or_fence_agent_start(
                            session, row, reason="outbox_inflight_timeout"
                        )
                    )
                else:
                    row.status = OutboxStatus.PENDING
                    row.leased_until = None
                    notes.append(f"outbox_reclaim:{row.outbox_id}")

            tasks = session.scalars(select(TaskRow)).all()
            for task in tasks:
                if task.state in {TaskState.PAUSED, TaskState.WAITING_HUMAN, TaskState.CANCELLING}:
                    notes.append(f"keep:{task.task_id}:{task.state}")
                    continue
                runs = session.scalars(
                    select(AgentRunRow).where(
                        AgentRunRow.task_id == task.task_id,
                        AgentRunRow.status.in_(
                            [AgentRunStatus.CREATED, AgentRunStatus.RUNNING]
                        ),
                    )
                ).all()
                for run in runs:
                    insp = self.agent_adapter.inspect(run.run_id)
                    if run.status == AgentRunStatus.RUNNING and not insp.get("alive"):
                        run.status = AgentRunStatus.LOST
                        run.terminal_reason = "process_missing"
                        run.finished_at = self.clock.now()
                        self._clear_run_occupancy(session, run, release_workspace=True)
                        self._recover_work_unit_after_lost_run(session, run)
                        self._append_event(
                            session,
                            task.task_id,
                            "run.lost",
                            auth_actor=None,
                            payload={"run_id": run.run_id},
                        )
                        notes.append(f"lost:{run.run_id}")
                    if run.status == AgentRunStatus.RUNNING:
                        lease = run.lease_expires_at
                        if (
                            lease is not None
                            and as_utc_naive(lease) < self.clock.now()
                            and insp.get("alive")
                        ):
                            if run.workspace_id:
                                ws = session.get(WorkspaceRow, run.workspace_id)
                                if ws:
                                    ws.state = WorkspaceState.QUARANTINED
                                    notes.append(f"quarantine:{ws.workspace_id}")
            return {"notes": notes}

        return self.executor.run(_recon)

    # ------------------------------------------------------------------
    # Transactional command routing
    # ------------------------------------------------------------------

    def _execute_in_tx(
        self,
        session: Session,
        operation: str,
        auth: AuthContext,
        payload: dict[str, Any],
        message_id: str,
        idempotency_key: str,
        ph: str,
    ) -> CommandResult:
        task_id = str(payload.get("task_id") or "")
        # §12.2 Inbox: actor_id + message_id dedup (separate from business idempotency)
        inbox_hit = self._check_inbox(session, auth, message_id, ph)
        if inbox_hit is not None:
            return inbox_hit

        # create_task idempotency must not depend on generated task_id
        idem_task_id = "" if operation == "create_task" else task_id
        replay = self._check_idempotency(
            session, auth, idem_task_id, operation, idempotency_key, ph, message_id
        )
        if replay is not None:
            self._store_inbox(
                session, auth, task_id or "", operation, message_id, idempotency_key, ph, replay
            )
            return replay

        handlers = {
            "create_task": self._create_task,
            "submit_contract": self._submit_contract,
            "approve_contract": self._approve_contract,
            "resolve_decision": self._resolve_decision,
            "activate_minimal_plan": self._activate_minimal_plan,
            "activate_plan": self._activate_plan,
            "dispatch_ready_runs": self._dispatch_ready_runs,
            "submit_result": self._submit_result,
            "pause_task": self._pause_task,
            "resume_task": self._resume_task,
            "cancel_task": self._cancel_task,
            "runtime_quiescent": self._runtime_quiescent,
            "cancellation_settled": self._cancellation_settled,
            "accept_result": self._accept_result,
            "prepare_acceptance": self._prepare_acceptance,
            "propose_side_effect": self._propose_side_effect,
            "approve_side_effect": self._approve_side_effect,
            "dispatch_side_effect": self._dispatch_side_effect,
            "reconcile_side_effect": self._reconcile_side_effect,
            "confirm_run_exit": self._confirm_run_exit,
            "open_blocking_gate": self._open_blocking_gate,
            "record_model_usage": self._record_model_usage,
            "set_writer_alive": self._set_writer_alive,
            "heartbeat": self._heartbeat,
            "seed_profile": self._seed_profile,
            "replace_planner_generation": self._replace_planner_generation,
            "submit_plan_proposal": self._submit_plan_proposal,
            "apply_contract_delta": self._apply_contract_delta,
        }
        handler = handlers.get(operation)
        if handler is None:
            raise PreconditionError(f"unknown operation {operation}", code="unknown_operation")

        result = handler(session, auth, payload)
        store_task_id = "" if operation == "create_task" else (
            task_id if task_id else str(result.data.get("task_id") or "")
        )
        self._store_idempotency(
            session,
            auth,
            store_task_id,
            operation,
            idempotency_key,
            ph,
            message_id,
            result,
        )
        self._store_inbox(
            session, auth, store_task_id, operation, message_id, idempotency_key, ph, result
        )
        if result.ok:
            self._wake_requested = True
            self.executor.on_after_commit(lambda: None)
        return result

    # ------------------------------------------------------------------
    # Idempotency + Inbox
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_command_result(result: CommandResult) -> dict[str, Any]:
        return {
            "ok": result.ok,
            "data": result.data,
            "error_code": result.error_code,
            "error_message": result.error_message,
        }

    @staticmethod
    def _deserialize_command_result(
        stored: dict[str, Any], *, replayed: bool
    ) -> CommandResult:
        # Backward-compatible: older rows stored only success data, or failure envelope
        if "ok" in stored:
            return CommandResult(
                ok=bool(stored["ok"]),
                data=stored.get("data") or {},
                error_code=stored.get("error_code"),
                error_message=stored.get("error_message"),
                replayed=replayed,
            )
        if stored.get("ok") is False or (
            "error_code" in stored and "data" not in stored
        ):
            return CommandResult.failure(
                stored.get("error_code") or "unknown",
                stored.get("error_message") or "replayed failure",
                data={k: v for k, v in stored.items() if k not in {"ok", "error_code", "error_message"}},
            )
        return CommandResult.success(stored, replayed=replayed)

    def _check_inbox(
        self,
        session: Session,
        auth: AuthContext,
        message_id: str,
        ph: str,
    ) -> CommandResult | None:
        row = session.scalars(
            select(InboxRow).where(
                InboxRow.actor_id == auth.actor_id,
                InboxRow.message_id == message_id,
            )
        ).first()
        if row is None:
            return None
        if row.payload_hash != ph:
            raise IdempotencyConflictError(
                "same message_id with different payload",
                code="inbox_conflict",
            )
        stored = json.loads(row.result_json)
        return self._deserialize_command_result(stored, replayed=True)

    def _store_inbox(
        self,
        session: Session,
        auth: AuthContext,
        task_id: str,
        operation: str,
        message_id: str,
        idempotency_key: str,
        ph: str,
        result: CommandResult,
    ) -> None:
        existing = session.scalars(
            select(InboxRow).where(
                InboxRow.actor_id == auth.actor_id,
                InboxRow.message_id == message_id,
            )
        ).first()
        if existing is not None:
            return
        session.add(
            InboxRow(
                actor_id=auth.actor_id,
                message_id=message_id,
                principal_id=auth.principal_id,
                task_id=task_id or None,
                operation_type=operation,
                # Inbox uniqueness is message-scoped; do not reuse business ikey here
                # (business idempotency lives in idempotency_keys).
                idempotency_key=message_id,
                payload_hash=ph,
                result_json=canonical_json(self._serialize_command_result(result)),
                created_at=self.clock.now(),
            )
        )
    def _check_idempotency(
        self,
        session: Session,
        auth: AuthContext,
        task_id: str,
        operation: str,
        idempotency_key: str,
        ph: str,
        message_id: str,
    ) -> CommandResult | None:
        tid = task_id or ""
        row = session.scalars(
            select(IdempotencyRow).where(
                IdempotencyRow.principal_id == auth.principal_id,
                IdempotencyRow.task_id == tid,
                IdempotencyRow.operation_type == operation,
                IdempotencyRow.idempotency_key == idempotency_key,
            )
        ).first()
        if row is None:
            return None
        if row.payload_hash != ph:
            raise IdempotencyConflictError(
                "same idempotency key with different payload",
                code="idempotency_conflict",
            )
        data = json.loads(row.result_json)
        return CommandResult.success(data, replayed=True)

    def _store_idempotency(
        self,
        session: Session,
        auth: AuthContext,
        task_id: str,
        operation: str,
        idempotency_key: str,
        ph: str,
        message_id: str,
        result: CommandResult,
    ) -> None:
        if not result.ok:
            return
        now = self.clock.now()
        session.add(
            IdempotencyRow(
                principal_id=auth.principal_id,
                task_id=task_id or "",
                operation_type=operation,
                idempotency_key=idempotency_key,
                payload_hash=ph,
                result_json=canonical_json(result.data),
                created_at=now,
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_task(self, session: Session, task_id: str) -> TaskRow:
        task = session.get(TaskRow, task_id)
        if task is None:
            raise NotFoundError(f"task {task_id} not found", code="task_not_found")
        return task

    def _next_event_seq(self, session: Session, task_id: str) -> int:
        current = session.scalar(
            select(func.max(EventRow.sequence_no)).where(EventRow.task_id == task_id)
        )
        return int(current or 0) + 1

    def _append_event(
        self,
        session: Session,
        task_id: str,
        event_type: str,
        *,
        auth_actor: str | None,
        payload: dict[str, Any],
    ) -> None:
        session.flush()
        now = self.clock.now()
        session.add(
            EventRow(
                task_id=task_id,
                sequence_no=self._next_event_seq(session, task_id),
                event_type=event_type,
                actor_id=auth_actor,
                payload_json=canonical_json(payload),
                occurred_at=now,
                received_at=now,
            )
        )

    def _set_task_state(
        self,
        session: Session,
        task: TaskRow,
        event: str,
        *,
        reason: str | None = None,
    ) -> None:
        # Never bypass the transition table — terminal states cannot reopen.
        new_state = transition_task(TaskState(task.state), event)
        task.state = new_state
        task.state_reason = reason
        task.state_revision += 1
        task.updated_at = self.clock.now()

    def _recompute_runnable_state(self, session: Session, task: TaskRow) -> None:
        """After a gate resolves, leave WAITING_HUMAN only if other gates remain."""
        session.flush()
        if TaskState(task.state) != TaskState.WAITING_HUMAN:
            return
        if self._has_blocking_gate(session, task.task_id):
            return
        if task.cancel_intent or is_terminal_task(TaskState(task.state)):
            return
        # Clear gate-blocked work units so redisatch can create a new Run (§7.3)
        for wu in session.scalars(
            select(WorkUnitExecutionRow).where(
                WorkUnitExecutionRow.task_id == task.task_id,
                WorkUnitExecutionRow.status == WorkUnitStatus.BLOCKED,
            )
        ):
            if wu.blocked_reason in {"paused", "attempts_exhausted"}:
                continue
            wu.status = transition_work_unit(WorkUnitStatus.BLOCKED, "block.cleared")
            wu.blocked_reason = None
            wu.active_run_id = None
        self._set_task_state(session, task, "decision.resolved")

    def _clear_run_occupancy(
        self,
        session: Session,
        run: AgentRunRow,
        *,
        release_workspace: bool = True,
    ) -> None:
        if run.work_unit_id:
            marker = session.get(ActiveExecuteRunMarker, run.work_unit_id)
            if marker and marker.run_id == run.run_id:
                session.delete(marker)
            wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
            if wu and wu.active_run_id == run.run_id:
                wu.active_run_id = None
        if release_workspace and run.workspace_id:
            ws = session.get(WorkspaceRow, run.workspace_id)
            if ws and ws.owner_run_id == run.run_id:
                ws.writer_alive = False
                if ws.state != WorkspaceState.QUARANTINED:
                    ws.state = WorkspaceState.READY
                    ws.owner_run_id = None

    def _adapter_may_be_alive(self, run_id: str | None) -> bool:
        if not run_id:
            return False
        insp = self.agent_adapter.inspect(run_id)
        return bool(insp.get("alive") or insp.get("writer_alive"))

    def _keep_start_occupancy(self, session: Session, run: AgentRunRow) -> None:
        """Start may have reached the Adapter — never free Workspace yet."""
        if not run.workspace_id:
            return
        ws = session.get(WorkspaceRow, run.workspace_id)
        if ws and ws.owner_run_id == run.run_id:
            ws.writer_alive = True

    def _fence_claimed_agent_start(
        self,
        session: Session,
        row: OutboxRow,
        run: AgentRunRow | None,
        *,
        reason: str,
    ) -> str:
        """Claimed start is uncertain: DEAD outbox, keep occupancy, enqueue stop."""
        row.status = OutboxStatus.DEAD
        row.leased_until = None
        task = session.get(TaskRow, row.task_id) if row.task_id else None
        if task is not None:
            row.revoke_epoch = task.revoke_epoch
        if run is not None:
            self._keep_start_occupancy(session, run)
            if task is not None:
                self._enqueue_stops_for_runs(
                    session, task, [run.run_id], reason=reason
                )
        return f"outbox_fence_start:{row.outbox_id}"

    def _reclaim_or_fence_agent_start(
        self,
        session: Session,
        row: OutboxRow,
        *,
        reason: str,
    ) -> str:
        """Distinguish never-sent reclaim from claimed-start verification.

        PENDING reclaim is allowed only when the adapter confirms the executor
        is not alive. Otherwise preserve the fact that start may have happened.
        """
        payload = json.loads(row.payload_json)
        run_id = payload.get("run_id")
        run = session.get(AgentRunRow, run_id) if run_id else None
        if self._adapter_may_be_alive(run_id):
            return self._fence_claimed_agent_start(
                session, row, run, reason=reason
            )
        row.status = OutboxStatus.PENDING
        row.leased_until = None
        return f"outbox_reclaim_start:{row.outbox_id}"

    def _recover_work_unit_after_lost_run(
        self, session: Session, run: AgentRunRow
    ) -> None:
        """After LOST confirmation, restore a runnable or explicitly blocked Work Unit."""
        if not run.work_unit_id:
            return
        wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
        if wu is None or wu.status != WorkUnitStatus.RUNNING:
            return
        now = self.clock.now()
        if wu.attempt_count >= DEFAULTS.max_work_unit_attempts:
            wu.status = transition_work_unit(WorkUnitStatus.RUNNING, "run.blocked")
            wu.blocked_reason = "attempts_exhausted"
            wu.next_retry_at = None
            task = session.get(TaskRow, run.task_id)
            if task and not is_terminal_task(TaskState(task.state)):
                if TaskState(task.state) != TaskState.WAITING_HUMAN:
                    self._set_task_state(
                        session,
                        task,
                        "blocking_gate.opened",
                        reason=WaitingReason.EXECUTION_BLOCKED,
                    )
            return
        wu.status = transition_work_unit(WorkUnitStatus.RUNNING, "run.retryable_failed")
        backoff_idx = min(max(wu.attempt_count - 1, 0), len(DEFAULTS.retry_backoff_seconds) - 1)
        wu.next_retry_at = now + timedelta(seconds=DEFAULTS.retry_backoff_seconds[backoff_idx])
        wu.blocked_reason = None

    def _task_has_live_writer(self, session: Session, task_id: str) -> bool:
        """True if any adapter instance or workspace owner still indicates a live writer."""
        for run in session.scalars(
            select(AgentRunRow).where(AgentRunRow.task_id == task_id)
        ):
            if run.status in {AgentRunStatus.CREATED, AgentRunStatus.RUNNING}:
                insp = self.agent_adapter.inspect(run.run_id)
                if insp.get("alive") or insp.get("writer_alive"):
                    return True
            if run.workspace_id:
                ws = session.get(WorkspaceRow, run.workspace_id)
                if ws and ws.owner_run_id == run.run_id:
                    insp = self.agent_adapter.inspect(run.run_id)
                    if insp.get("alive") or insp.get("writer_alive") or ws.writer_alive:
                        return True
        return False

    def _assert_no_live_writers(self, session: Session, task_id: str) -> None:
        if self._task_has_live_writer(session, task_id):
            raise PreconditionError(
                "executor still alive or workspace writer not released",
                code="writer_alive",
            )

    def _guard_side_effect_eligibility(
        self,
        session: Session,
        task: TaskRow,
        effect: SideEffectRow,
        *,
        allowed_states: frozenset[SideEffectState],
    ) -> None:
        contract = self._active_contract(session, task.task_id)
        expired = bool(
            effect.expires_at and as_utc_naive(effect.expires_at) < self.clock.now()
        )
        guard_side_effect_send(
            task_state=TaskState(task.state),
            cancel_intent=bool(task.cancel_intent),
            pause_intent=bool(task.pause_intent),
            has_blocking_gate=self._has_blocking_gate(session, task.task_id),
            has_active_contract=contract is not None,
            effect_contract_version=effect.contract_version,
            active_contract_version=contract.contract_version if contract else None,
            approval_expired=expired,
            effect_state=SideEffectState(effect.state),
            allowed_states=allowed_states,
        )

    def _other_blocking_gates(
        self, session: Session, task_id: str, *, exclude_decision_id: str | None = None
    ) -> list[GateRow]:
        session.flush()
        gates = session.scalars(
            select(GateRow).where(
                GateRow.task_id == task_id,
                GateRow.lifecycle.in_(
                    [GateLifecycle.OPEN, GateLifecycle.APPROVED_PENDING_APPLY]
                ),
            )
        ).all()
        if exclude_decision_id is None:
            return list(gates)
        return [g for g in gates if g.decision_id != exclude_decision_id]

    def _count_active_runs(
        self, session: Session, *, task_id: str | None = None
    ) -> int:
        q = select(func.count()).select_from(AgentRunRow).where(
            AgentRunRow.status.in_([AgentRunStatus.CREATED, AgentRunStatus.RUNNING])
        )
        if task_id:
            q = q.where(AgentRunRow.task_id == task_id)
        return int(session.scalar(q) or 0)

    def _supersede_pending_acceptance(self, session: Session, task_id: str) -> None:
        now = self.clock.now()
        for dec in session.scalars(
            select(DecisionRow).where(
                DecisionRow.task_id == task_id,
                DecisionRow.decision_kind == DecisionKind.FINAL_ACCEPTANCE,
                DecisionRow.status == DecisionStatus.PENDING,
            )
        ):
            dec.status = DecisionStatus.SUPERSEDED
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == dec.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now

    def _has_blocking_gate(self, session: Session, task_id: str) -> bool:
        session.flush()
        gates = session.scalars(
            select(GateRow).where(
                GateRow.task_id == task_id,
                GateRow.lifecycle.in_(
                    [GateLifecycle.OPEN, GateLifecycle.APPROVED_PENDING_APPLY]
                ),
            )
        ).all()
        return len(gates) > 0

    def _active_contract(self, session: Session, task_id: str) -> ContractRow | None:
        marker = session.get(ActiveContractMarker, task_id)
        if marker is None:
            return None
        return session.scalars(
            select(ContractRow).where(
                ContractRow.task_id == task_id,
                ContractRow.contract_version == marker.contract_version,
            )
        ).one()

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def _create_task(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        now = self.clock.now()
        task_id = payload.get("task_id") or new_id("task")
        title = payload.get("title") or "untitled"
        intent = payload.get("intent") or ""
        task = TaskRow(
            task_id=task_id,
            principal_id=auth.principal_id,
            intent_ref=intent,
            title=title,
            state=TaskState.NEW,
            state_revision=0,
            create_idempotency_key=payload.get("_create_key"),
            created_at=now,
            updated_at=now,
            model_call_limit=int(payload.get("model_call_limit") or DEFAULTS.max_task_model_calls),
        )
        session.add(task)
        # default fake profile
        if session.get(AgentProfileRow, ("fake", 1)) is None:
            session.add(
                AgentProfileRow(
                    profile_id="fake",
                    profile_version=1,
                    content_json=canonical_json({"instructions": "fake worker"}),
                    created_at=now,
                )
            )
        self._append_event(
            session,
            task_id,
            "task.created",
            auth_actor=auth.actor_id,
            payload={"title": title},
        )
        return CommandResult.success({"task_id": task_id, "state": TaskState.NEW})

    def _submit_contract(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        if task.principal_id != auth.principal_id:
            raise AuthorizationError("task principal mismatch", code="authorization_denied")
        content = payload.get("contract") or {
            "objective": payload.get("objective") or task.title,
            "in_scope": payload.get("in_scope") or [],
            "out_of_scope": payload.get("out_of_scope") or [],
            "constraints": payload.get("constraints") or [],
            "assumptions": payload.get("assumptions") or [],
            "deliverables": payload.get("deliverables")
            or [{"deliverable_id": "d1", "description": "result", "expected_kind": "text"}],
            "acceptance_criteria": payload.get("acceptance_criteria")
            or [
                {
                    "criterion_id": "c1",
                    "statement": "done",
                    "evidence_kind": "artifact",
                    "required": True,
                }
            ],
            "allowed_side_effects": payload.get("allowed_side_effects") or [],
            "permission_ceiling": payload.get("permission_ceiling") or {"tools": ["read"]},
            "resource_limits": payload.get("resource_limits") or {},
            "human_gates": payload.get("human_gates") or [],
        }
        max_ver = session.scalar(
            select(func.max(ContractRow.contract_version)).where(
                ContractRow.task_id == task.task_id
            )
        )
        version = int(max_ver or 0) + 1
        # supersede prior drafts / pending approvals
        prior = session.scalars(
            select(ContractRow).where(
                ContractRow.task_id == task.task_id,
                ContractRow.status.in_([ContractStatus.DRAFT, ContractStatus.PENDING_APPROVAL]),
            )
        ).all()
        for p in prior:
            p.status = ContractStatus.SUPERSEDED
        # supersede prior pending contract-approval decisions/gates
        for dec in session.scalars(
            select(DecisionRow).where(
                DecisionRow.task_id == task.task_id,
                DecisionRow.decision_kind == DecisionKind.CONTRACT_APPROVAL,
                DecisionRow.status == DecisionStatus.PENDING,
            )
        ):
            dec.status = DecisionStatus.SUPERSEDED
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == dec.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = self.clock.now()

        ch = content_hash(content)
        now = self.clock.now()
        session.add(
            ContractRow(
                task_id=task.task_id,
                contract_version=version,
                supersedes_version=task.contract_version,
                content_hash=ch,
                content_json=canonical_json(content),
                status=ContractStatus.PENDING_APPROVAL,
                created_at=now,
            )
        )
        expl = {
            "objective": content["objective"],
            "deliverables": content["deliverables"],
            "acceptance_criteria": content["acceptance_criteria"],
            "permission_ceiling": content["permission_ceiling"],
            "resource_limits": content.get("resource_limits") or {},
            "allowed_side_effects": content.get("allowed_side_effects") or [],
        }
        explanation_id = new_id("expl")
        eh = content_hash(expl)
        session.add(
            ExplanationRow(
                explanation_id=explanation_id,
                task_id=task.task_id,
                contract_version=version,
                content_hash=eh,
                content_json=canonical_json(expl),
                created_at=now,
            )
        )
        decision_id = new_id("dec")
        expires = now + timedelta(hours=DEFAULTS.decision_ttl_hours)
        session.add(
            DecisionRow(
                decision_id=decision_id,
                task_id=task.task_id,
                decision_kind=DecisionKind.CONTRACT_APPROVAL,
                target_ref=f"contract:{task.task_id}",
                target_version=version,
                target_hash=ch,
                contract_version=version,
                explanation_ref=explanation_id,
                explanation_hash=eh,
                status=DecisionStatus.PENDING,
                created_at=now,
                expires_at=expires,
                gate_lifecycle=GateLifecycle.OPEN,
            )
        )
        gate_id = new_id("gate")
        session.add(
            GateRow(
                gate_id=gate_id,
                task_id=task.task_id,
                decision_id=decision_id,
                reason=WaitingReason.CONTRACT_APPROVAL,
                lifecycle=GateLifecycle.OPEN,
                created_at=now,
            )
        )
        self._set_task_state(
            session,
            task,
            "contract.submitted",
            reason=WaitingReason.CONTRACT_APPROVAL,
        )
        self._append_event(
            session,
            task.task_id,
            "contract.submitted",
            auth_actor=auth.actor_id,
            payload={"contract_version": version, "decision_id": decision_id},
        )
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "contract_version": version,
                "content_hash": ch,
                "decision_id": decision_id,
                "explanation_id": explanation_id,
                "explanation_hash": eh,
                "state": task.state,
            }
        )

    def _approve_contract(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        return self._resolve_decision(
            session,
            auth,
            {
                "decision_id": payload["decision_id"],
                "choice": payload.get("choice") or "APPROVE",
                "expected_target_version": payload.get("expected_target_version"),
                "expected_target_hash": payload.get("expected_target_hash"),
            },
        )

    def _resolve_decision(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        decision = session.get(DecisionRow, payload["decision_id"])
        if decision is None:
            raise NotFoundError("decision not found", code="decision_not_found")

        # Formal Decision kinds always require Human (alias-proof)
        guard_human_decision(auth, decision.decision_kind)

        task = self._get_task(session, decision.task_id)
        if task.principal_id != auth.principal_id:
            raise AuthorizationError(
                "decision principal mismatch", code="authorization_denied"
            )

        # Idempotent replay of same decision
        if decision.status in {DecisionStatus.APPROVED, DecisionStatus.REJECTED}:
            return CommandResult.success(
                {
                    "decision_id": decision.decision_id,
                    "status": decision.status,
                    "choice": decision.choice,
                    "task_id": decision.task_id,
                    "replayed": True,
                },
                replayed=True,
            )

        if decision.status != DecisionStatus.PENDING:
            raise PreconditionError(
                f"decision status {decision.status}", code="decision_not_pending"
            )

        now = self.clock.now()
        expires_at = decision.expires_at
        if expires_at is not None and as_utc_naive(expires_at) < now:
            decision.status = DecisionStatus.EXPIRED
            raise PreconditionError("decision expired", code="decision_expired")

        expected_version = payload.get("expected_target_version")
        expected_hash = payload.get("expected_target_hash")
        if expected_version is not None and int(expected_version) != decision.target_version:
            raise ConflictError("target version mismatch", code="target_version_conflict")
        if expected_hash is not None and expected_hash != decision.target_hash:
            raise ConflictError("target hash mismatch", code="target_hash_conflict")

        # If contract content changed, reject old approval binding
        if decision.decision_kind == DecisionKind.CONTRACT_APPROVAL:
            contract = session.scalars(
                select(ContractRow).where(
                    ContractRow.task_id == decision.task_id,
                    ContractRow.contract_version == decision.target_version,
                )
            ).one()
            if contract.content_hash != decision.target_hash:
                raise ConflictError("contract hash changed", code="target_hash_conflict")

        choice = (payload.get("choice") or "APPROVE").upper()
        decision.choice = choice
        decision.decided_by_actor = auth.actor_id
        decision.decided_by_principal = auth.principal_id
        decision.decided_at = now

        if choice == "APPROVE":
            decision.status = DecisionStatus.APPROVED
            self._apply_approved_decision(session, task, decision)
        else:
            decision.status = DecisionStatus.REJECTED
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now
            self._recompute_runnable_state(session, task)

        self._append_event(
            session,
            task.task_id,
            "decision.resolved",
            auth_actor=auth.actor_id,
            payload={
                "decision_id": decision.decision_id,
                "status": decision.status,
                "choice": choice,
            },
        )
        return CommandResult.success(
            {
                "decision_id": decision.decision_id,
                "status": decision.status,
                "choice": choice,
                "task_id": task.task_id,
                "task_state": task.state,
            }
        )

    def _apply_approved_decision(
        self, session: Session, task: TaskRow, decision: DecisionRow
    ) -> None:
        now = self.clock.now()
        kind = DecisionKind(decision.decision_kind)
        if kind == DecisionKind.CONTRACT_APPROVAL:
            contract = session.scalars(
                select(ContractRow).where(
                    ContractRow.task_id == task.task_id,
                    ContractRow.contract_version == decision.target_version,
                )
            ).one()
            # supersede previous active
            prev = session.get(ActiveContractMarker, task.task_id)
            if prev:
                old = session.scalars(
                    select(ContractRow).where(
                        ContractRow.task_id == task.task_id,
                        ContractRow.contract_version == prev.contract_version,
                    )
                ).first()
                if old:
                    old.status = ContractStatus.SUPERSEDED
                session.delete(prev)
            contract.status = ContractStatus.ACTIVE
            contract.approved_by = decision.decided_by_actor
            contract.approved_at = now
            session.add(
                ActiveContractMarker(
                    task_id=task.task_id, contract_version=contract.contract_version
                )
            )
            task.contract_version = contract.contract_version
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now
            decision.gate_lifecycle = GateLifecycle.RESOLVED
            # Sync Contract resource_limits into scheduling budget
            limits = json.loads(contract.content_json).get("resource_limits") or {}
            if "model_call_limit" in limits:
                task.model_call_limit = int(limits["model_call_limit"])
            simple = bool(json.loads(contract.content_json).get("simple", True))
            self._set_task_state(
                session,
                task,
                "contract.approved_simple" if simple else "contract.approved",
            )
        elif kind == DecisionKind.FINAL_ACCEPTANCE:
            snap = session.get(ResultSnapshotRow, decision.result_snapshot_ref)
            if snap is None or snap.content_hash != decision.target_hash:
                raise ConflictError("result snapshot mismatch", code="target_hash_conflict")
            # Snapshot must still match current Contract/Plan
            if snap.contract_version != task.contract_version:
                raise ConflictError(
                    "acceptance snapshot contract stale", code="snapshot_stale"
                )
            if snap.plan_version != task.plan_version:
                raise ConflictError(
                    "acceptance snapshot plan stale", code="snapshot_stale"
                )
            # Current-plan work units must be DONE; historical removed nodes are CANCELLED
            plan_marker = session.get(ActivePlanMarker, task.task_id)
            current_ids: set[str] = set()
            if plan_marker is not None:
                plan = session.scalars(
                    select(PlanRow).where(
                        PlanRow.task_id == task.task_id,
                        PlanRow.plan_version == plan_marker.plan_version,
                    )
                ).first()
                if plan is not None:
                    current_ids = {n["work_unit_id"] for n in json.loads(plan.nodes_json)}
            pending_wu = session.scalars(
                select(WorkUnitExecutionRow).where(
                    WorkUnitExecutionRow.task_id == task.task_id,
                    WorkUnitExecutionRow.status.in_(
                        [
                            WorkUnitStatus.PENDING,
                            WorkUnitStatus.RUNNING,
                            WorkUnitStatus.BLOCKED,
                        ]
                    ),
                )
            ).all()
            blockers = [
                wu
                for wu in pending_wu
                if not current_ids or wu.work_unit_id in current_ids
            ]
            if blockers:
                raise PreconditionError(
                    "incomplete work units block acceptance",
                    code="incomplete_work",
                )
            # Other open gates (clarification, side-effect, resource, …) block completion
            other_gates = self._other_blocking_gates(
                session, task.task_id, exclude_decision_id=decision.decision_id
            )
            if other_gates:
                raise PreconditionError(
                    "other blocking gates remain",
                    code="blocking_gate_open",
                )
            self._assert_no_live_writers(session, task.task_id)
            # Re-validate required criterion evidence on the frozen snapshot
            snap_content = json.loads(snap.content_json)
            contract = self._active_contract(session, task.task_id)
            if contract is None:
                raise PreconditionError("no active contract", code="no_active_contract")
            content = json.loads(contract.content_json)
            required = [
                c
                for c in (content.get("acceptance_criteria") or [])
                if c.get("required", True)
            ]
            self._assert_acceptance_evidence(
                required, snap_content.get("evidence") or []
            )
            # no active runs
            active = session.scalars(
                select(AgentRunRow).where(
                    AgentRunRow.task_id == task.task_id,
                    AgentRunRow.status.in_([AgentRunStatus.CREATED, AgentRunStatus.RUNNING]),
                )
            ).all()
            if active:
                raise PreconditionError("active runs present", code="active_runs")
            # unresolved UNKNOWN side effects block
            unknown = session.scalars(
                select(SideEffectRow).where(
                    SideEffectRow.task_id == task.task_id,
                    SideEffectRow.state == SideEffectState.UNKNOWN,
                )
            ).all()
            if unknown:
                raise PreconditionError(
                    "unresolved UNKNOWN side effects", code="unknown_side_effect"
                )
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now
            task.final_result_refs = decision.result_snapshot_ref
            self._set_task_state(session, task, "final.accepted")
        elif kind == DecisionKind.SIDE_EFFECT_APPROVAL:
            effect = session.get(SideEffectRow, decision.target_ref)
            if effect is None:
                raise NotFoundError("side effect not found")
            if effect.action_digest != decision.target_hash:
                raise ConflictError(
                    "side effect digest mismatch — parameters/target changed",
                    code="target_hash_conflict",
                )
            effect.state = transition_side_effect(
                SideEffectState(effect.state), "approve"
            )
            effect.authorization_ref = decision.decision_id
            effect.expires_at = now + timedelta(
                minutes=DEFAULTS.side_effect_approval_ttl_minutes
            )
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now
            self._recompute_runnable_state(session, task)
        elif kind == DecisionKind.CONTRACT_DELTA:
            # Approve records immutable Decision; Gate stays APPROVED_PENDING_APPLY
            # until apply_contract_delta activates the new Contract (§6.3).
            if not decision.payload_json:
                raise PreconditionError(
                    "contract delta payload missing", code="delta_payload_missing"
                )
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.APPROVED_PENDING_APPLY
            decision.gate_lifecycle = GateLifecycle.APPROVED_PENDING_APPLY
            # Keep Task in WAITING_HUMAN — do not pretend authorization is applied
            if TaskState(task.state) != TaskState.WAITING_HUMAN:
                self._set_task_state(
                    session,
                    task,
                    "blocking_gate.opened",
                    reason=WaitingReason.CONTRACT_DELTA,
                )
            else:
                task.state_reason = WaitingReason.CONTRACT_DELTA
        else:
            for gate in session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            ):
                gate.lifecycle = GateLifecycle.RESOLVED
                gate.resolved_at = now
            self._recompute_runnable_state(session, task)

    def _activate_minimal_plan(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        contract = self._active_contract(session, task.task_id)
        if contract is None:
            raise PreconditionError("no ACTIVE contract", code="no_active_contract")
        work_unit_id = payload.get("work_unit_id") or new_id("wu")
        nodes = [{"work_unit_id": work_unit_id, "spec_version": 1, "work_type": "EXECUTE"}]
        edges: list[dict[str, Any]] = []
        return self._activate_plan(
            session,
            auth,
            {
                "task_id": task.task_id,
                "nodes": nodes,
                "edges": edges,
                "expected_contract_version": contract.contract_version,
            },
        )

    def _activate_plan(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        contract = self._active_contract(session, task.task_id)
        if contract is None:
            raise PreconditionError("no ACTIVE contract", code="no_active_contract")
        expected_cv = payload.get("expected_contract_version")
        if expected_cv is not None and int(expected_cv) != contract.contract_version:
            raise ConflictError("contract version conflict", code="version_conflict")
        expected_pv = payload.get("expected_plan_version")
        if expected_pv is not None and task.plan_version is not None:
            if int(expected_pv) != task.plan_version:
                raise ConflictError("plan version conflict", code="version_conflict")

        raw_nodes = payload["nodes"]
        raw_edges = payload.get("edges") or []
        nodes = [
            PlanNode(
                work_unit_id=n["work_unit_id"],
                spec_version=int(n.get("spec_version") or 1),
                work_type=n.get("work_type") or "EXECUTE",
            )
            for n in raw_nodes
        ]
        edges = [
            PlanEdge(
                from_work_unit_id=e["from_work_unit_id"],
                to_work_unit_id=e["to_work_unit_id"],
                predicate=DependencyPredicate(e.get("predicate") or "DONE"),
                artifact_hash=e.get("artifact_hash"),
            )
            for e in raw_edges
        ]
        validate_dag(task_id=task.task_id, nodes=nodes, edges=edges)

        now = self.clock.now()
        plan_version = (task.plan_version or 0) + 1
        plan_content = {"nodes": raw_nodes, "edges": raw_edges}
        ph = content_hash(plan_content)

        # supersede previous active plan
        prev = session.get(ActivePlanMarker, task.task_id)
        if prev:
            old = session.scalars(
                select(PlanRow).where(
                    PlanRow.task_id == task.task_id,
                    PlanRow.plan_version == prev.plan_version,
                )
            ).first()
            if old:
                old.status = PlanStatus.SUPERSEDED
            session.delete(prev)

        session.add(
            PlanRow(
                task_id=task.task_id,
                plan_version=plan_version,
                base_plan_version=task.plan_version,
                contract_version=contract.contract_version,
                content_hash=ph,
                nodes_json=canonical_json(raw_nodes),
                edges_json=canonical_json(raw_edges),
                status=PlanStatus.ACTIVE,
                created_at=now,
            )
        )
        session.add(ActivePlanMarker(task_id=task.task_id, plan_version=plan_version))
        task.plan_version = plan_version
        # Plan change invalidates pending final-acceptance snapshots
        self._supersede_pending_acceptance(session, task.task_id)

        for n in nodes:
            # Task isolation: reject Work Units owned by another Task
            any_spec = session.scalars(
                select(WorkUnitSpecRow).where(WorkUnitSpecRow.work_unit_id == n.work_unit_id)
            ).first()
            if any_spec is not None and any_spec.task_id != task.task_id:
                raise PreconditionError(
                    f"work unit {n.work_unit_id} belongs to another task",
                    code="cross_task_work_unit",
                )
            exec_existing = session.get(WorkUnitExecutionRow, n.work_unit_id)
            if exec_existing is not None and exec_existing.task_id != task.task_id:
                raise PreconditionError(
                    f"work unit execution {n.work_unit_id} belongs to another task",
                    code="cross_task_work_unit",
                )

            spec = {
                "objective": payload.get("objective") or task.title,
                "work_type": n.work_type,
                "context_policy": "FRESH",
                "input_refs": [],
                "expected_outputs": [],
                "acceptance_criteria": [],
                "required_capabilities": [],
                "requested_permissions": [],
                "workspace_policy": "dedicated",
            }
            sh = content_hash(spec)
            existing = session.scalars(
                select(WorkUnitSpecRow).where(
                    WorkUnitSpecRow.work_unit_id == n.work_unit_id,
                    WorkUnitSpecRow.spec_version == n.spec_version,
                )
            ).first()
            if existing is None:
                session.add(
                    WorkUnitSpecRow(
                        work_unit_id=n.work_unit_id,
                        spec_version=n.spec_version,
                        task_id=task.task_id,
                        objective=spec["objective"],
                        work_type=n.work_type,
                        content_hash=sh,
                        content_json=canonical_json(spec),
                        created_at=now,
                    )
                )
            elif existing.task_id != task.task_id:
                raise PreconditionError(
                    "work unit spec task mismatch", code="cross_task_work_unit"
                )

            if exec_existing is None:
                session.add(
                    WorkUnitExecutionRow(
                        work_unit_id=n.work_unit_id,
                        task_id=task.task_id,
                        spec_version=n.spec_version,
                        status=WorkUnitStatus.PENDING,
                    )
                )
            else:
                # §6.4 / §8.1: terminal Work Units never revive; definition change
                # requires a new work_unit_id rather than mutating Execution in place.
                if is_terminal_work_unit(WorkUnitStatus(exec_existing.status)):
                    if exec_existing.spec_version != n.spec_version:
                        raise PreconditionError(
                            f"work unit {n.work_unit_id} is terminal at spec "
                            f"v{exec_existing.spec_version}; definition change needs a new work_unit_id",
                            code="work_unit_spec_immutable",
                        )
                    # Same spec + DONE/FAILED/CANCELLED: keep historical fact
                elif exec_existing.spec_version != n.spec_version:
                    if (
                        exec_existing.status == WorkUnitStatus.PENDING
                        and not exec_existing.active_run_id
                        and int(exec_existing.attempt_count or 0) == 0
                    ):
                        # Never started — allow rebinding before first Run
                        exec_existing.spec_version = n.spec_version
                    else:
                        raise PreconditionError(
                            f"work unit {n.work_unit_id} already bound to spec "
                            f"v{exec_existing.spec_version} with progress; "
                            "definition/input change requires a new work_unit_id",
                            code="work_unit_spec_immutable",
                        )

            ws_id = f"ws_{n.work_unit_id}"
            ws = session.get(WorkspaceRow, ws_id)
            if ws is None:
                session.add(
                    WorkspaceRow(
                        workspace_id=ws_id,
                        task_id=task.task_id,
                        work_unit_id=n.work_unit_id,
                        state=WorkspaceState.READY,
                        fencing_epoch=0,
                    )
                )
            elif ws.task_id != task.task_id:
                raise PreconditionError(
                    "workspace belongs to another task", code="cross_task_workspace"
                )

        # Plan removal: tear down Work Units no longer in the active node set (§6)
        keep_ids = {n.work_unit_id for n in nodes}
        for orphan in session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task.task_id)
        ):
            if orphan.work_unit_id in keep_ids:
                continue
            if is_terminal_work_unit(WorkUnitStatus(orphan.status)):
                continue
            if orphan.status == WorkUnitStatus.PENDING:
                orphan.status = WorkUnitStatus.CANCELLED
                orphan.blocked_reason = "removed_from_plan"
                orphan.active_run_id = None
                continue
            # RUNNING / BLOCKED: stop live runs and cancel the unit
            if orphan.active_run_id:
                run = session.get(AgentRunRow, orphan.active_run_id)
                if run and run.status in {
                    AgentRunStatus.CREATED,
                    AgentRunStatus.RUNNING,
                }:
                    self._enqueue_stops_for_runs(
                        session, task, [run.run_id], reason="removed_from_plan"
                    )
                    if run.status == AgentRunStatus.CREATED:
                        # Prefer cancelling unsent; IN_FLIGHT handled by invalidate/stop
                        pending_start = session.scalars(
                            select(OutboxRow).where(
                                OutboxRow.task_id == task.task_id,
                                OutboxRow.command_type == "agent.start",
                                OutboxRow.status == OutboxStatus.PENDING,
                            )
                        ).all()
                        for ob in pending_start:
                            pl = json.loads(ob.payload_json)
                            if pl.get("run_id") == run.run_id:
                                ob.status = OutboxStatus.DEAD
                                run.status = AgentRunStatus.CANCELLED
                                run.finished_at = now
                                run.terminal_reason = "removed_from_plan"
                                self._clear_run_occupancy(
                                    session, run, release_workspace=True
                                )
            orphan.status = WorkUnitStatus.CANCELLED
            orphan.blocked_reason = "removed_from_plan"
            orphan.active_run_id = None
            marker = session.get(ActiveExecuteRunMarker, orphan.work_unit_id)
            if marker:
                session.delete(marker)

        if task.state in {TaskState.PLANNING, TaskState.EXECUTING, TaskState.VERIFYING}:
            if task.state != TaskState.EXECUTING:
                self._set_task_state(session, task, "plan.activated")

        self._append_event(
            session,
            task.task_id,
            "plan.activated",
            auth_actor=auth.actor_id,
            payload={"plan_version": plan_version},
        )
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "plan_version": plan_version,
                "nodes": [n.work_unit_id for n in nodes],
                "state": task.state,
            }
        )

    def _dispatch_ready_runs(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        try:
            guard_dispatch(
                task_state=TaskState(task.state),
                has_active_contract=self._active_contract(session, task.task_id) is not None,
                has_blocking_gate=self._has_blocking_gate(session, task.task_id),
                cancel_intent=bool(task.cancel_intent),
                pause_intent=bool(task.pause_intent),
            )
        except PreconditionError as exc:
            return CommandResult.failure(exc.code, exc.message)

        if task.model_calls_used >= task.model_call_limit:
            self._open_resource_gate(session, task)
            return CommandResult.failure("resource_limit", "model call budget exhausted")

        plan_marker = session.get(ActivePlanMarker, task.task_id)
        if plan_marker is None:
            return CommandResult.failure("no_active_plan", "no ACTIVE plan")

        plan = session.scalars(
            select(PlanRow).where(
                PlanRow.task_id == task.task_id,
                PlanRow.plan_version == plan_marker.plan_version,
            )
        ).one()
        nodes = json.loads(plan.nodes_json)
        edges = json.loads(plan.edges_json)
        created: list[str] = []
        node_by_id = {n["work_unit_id"]: n for n in nodes}

        global_active = self._count_active_runs(session)
        task_active = self._count_active_runs(session, task_id=task.task_id)

        for node in nodes:
            if global_active >= DEFAULTS.global_run_concurrency:
                break
            if task_active >= DEFAULTS.per_task_run_concurrency:
                break

            wu_id = node["work_unit_id"]
            plan_spec_version = int(node.get("spec_version") or 1)
            wu = session.get(WorkUnitExecutionRow, wu_id)
            if wu is None or wu.status != WorkUnitStatus.PENDING:
                continue
            if wu.task_id != task.task_id:
                continue
            # Bind to Plan's fixed Spec version
            if wu.spec_version != plan_spec_version:
                wu.spec_version = plan_spec_version
            if wu.next_retry_at and as_utc_naive(wu.next_retry_at) > self.clock.now():
                continue
            if not self._deps_satisfied(session, wu_id, edges):
                continue
            # workspace writer check
            ws = session.get(WorkspaceRow, f"ws_{wu_id}")
            if ws and ws.state == WorkspaceState.QUARANTINED:
                continue
            if ws and ws.writer_alive and ws.owner_run_id:
                continue
            # stale marker without active run must not block forever — but also
            # must not allow double writers; clear orphan markers for non-active runs
            marker = session.get(ActiveExecuteRunMarker, wu_id)
            if marker is not None:
                marked_run = session.get(AgentRunRow, marker.run_id)
                if marked_run is None or marked_run.status not in {
                    AgentRunStatus.CREATED,
                    AgentRunStatus.RUNNING,
                }:
                    session.delete(marker)
                else:
                    continue

            run_id = new_id("run")
            now = self.clock.now()
            attempt = wu.attempt_count + 1
            if attempt > DEFAULTS.max_work_unit_attempts:
                wu.status = WorkUnitStatus.BLOCKED
                wu.blocked_reason = "attempts_exhausted"
                continue

            fencing = (ws.fencing_epoch + 1) if ws else 1
            manifest_id = new_id("ctx")
            manifest = {
                "context_manifest_id": manifest_id,
                "task_id": task.task_id,
                "run_id": run_id,
                "contract_version": task.contract_version,
                "plan_version": task.plan_version,
                "context_policy": "FRESH",
                "mandatory_refs": [],
                "optional_refs": [],
            }
            mh = content_hash(manifest)
            session.add(
                ContextManifestRow(
                    context_manifest_id=manifest_id,
                    task_id=task.task_id,
                    run_id=run_id,
                    contract_version=int(task.contract_version or 0),
                    plan_version=task.plan_version,
                    content_json=canonical_json(manifest),
                    manifest_hash=mh,
                    created_at=now,
                )
            )
            profile_version = int(payload.get("profile_version") or 1)
            run = AgentRunRow(
                run_id=run_id,
                task_id=task.task_id,
                assignment_kind=AssignmentKind.EXECUTE,
                work_unit_id=wu_id,
                work_unit_spec_version=plan_spec_version,
                attempt_no=attempt,
                profile_id="fake",
                profile_version=profile_version,
                contract_version=int(task.contract_version or 0),
                plan_version=task.plan_version,
                context_manifest_id=manifest_id,
                workspace_id=ws.workspace_id if ws else None,
                status=AgentRunStatus.CREATED,
                fencing_epoch=fencing,
                grant_epoch=task.revoke_epoch,
                lease_expires_at=now + timedelta(seconds=DEFAULTS.lease_seconds),
            )
            session.add(run)
            session.flush()
            session.add(ActiveExecuteRunMarker(work_unit_id=wu_id, run_id=run_id))
            wu.status = transition_work_unit(WorkUnitStatus(wu.status), "run.started")
            wu.active_run_id = run_id
            wu.attempt_count = attempt
            wu.spec_version = plan_spec_version
            if ws:
                ws.state = WorkspaceState.LOCKED
                ws.owner_run_id = run_id
                ws.fencing_epoch = fencing
                ws.writer_alive = True

            outbox_id = new_id("ob")
            session.add(
                OutboxRow(
                    outbox_id=outbox_id,
                    task_id=task.task_id,
                    command_type="agent.start",
                    payload_json=canonical_json(
                        {
                            "run_id": run_id,
                            "task_id": task.task_id,
                            "assignment_kind": AssignmentKind.EXECUTE,
                            "fencing_epoch": fencing,
                            "revoke_epoch": task.revoke_epoch,
                        }
                    ),
                    status=OutboxStatus.PENDING,
                    revoke_epoch=task.revoke_epoch,
                    created_at=now,
                )
            )
            self._append_event(
                session,
                task.task_id,
                "run.created",
                auth_actor=auth.actor_id,
                payload={"run_id": run_id, "work_unit_id": wu_id},
            )
            created.append(run_id)
            global_active += 1
            task_active += 1

        _ = node_by_id
        return CommandResult.success({"task_id": task.task_id, "created_runs": created})

    def _deps_satisfied(
        self, session: Session, wu_id: str, edges: list[dict[str, Any]]
    ) -> bool:
        incoming = [e for e in edges if e["to_work_unit_id"] == wu_id]
        for e in incoming:
            dep = session.get(WorkUnitExecutionRow, e["from_work_unit_id"])
            if dep is None:
                return False
            pred = e.get("predicate") or "DONE"
            if pred == DependencyPredicate.DONE:
                if dep.status != WorkUnitStatus.DONE:
                    return False
            elif pred == DependencyPredicate.VERDICT_PASS:
                if dep.status != WorkUnitStatus.DONE or dep.selected_verdict != Verdict.PASS:
                    return False
                needed = e.get("artifact_hash")
                if needed and dep.verified_artifact_hash != needed:
                    return False
            else:
                return False
        return True

    def _assert_run_write_binding(
        self,
        session: Session,
        auth: AuthContext,
        run: AgentRunRow,
        payload: dict[str, Any],
        *,
        require_fencing: bool = True,
    ) -> None:
        """Bind Internal run writes to principal / task / run / fencing epoch."""
        task = self._get_task(session, run.task_id)
        if auth.principal_id != task.principal_id:
            raise AuthorizationError(
                "run principal mismatch", code="authorization_denied"
            )
        if require_fencing:
            if "fencing_epoch" not in payload:
                raise ConflictError(
                    "fencing_epoch required", code="fencing_required"
                )
            if int(payload["fencing_epoch"]) != int(run.fencing_epoch):
                raise ConflictError(
                    "fencing epoch mismatch", code="fencing_conflict"
                )
        payload_task_id = payload.get("task_id")
        if payload_task_id is not None and str(payload_task_id) != run.task_id:
            raise AuthorizationError(
                "run task mismatch", code="authorization_denied"
            )
        payload_run_id = payload.get("run_id")
        if payload_run_id is not None and str(payload_run_id) != run.run_id:
            raise AuthorizationError(
                "run id mismatch", code="authorization_denied"
            )

    def _submit_result(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        run = session.get(AgentRunRow, payload["run_id"])
        if run is None:
            raise NotFoundError("run not found", code="run_not_found")
        self._assert_run_write_binding(session, auth, run, payload)
        task = self._get_task(session, run.task_id)

        # fencing / late arrival
        if run.work_unit_id:
            wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
            if wu and wu.active_run_id and wu.active_run_id != run.run_id:
                run.late_arrival = True
                run.result_json = canonical_json(payload.get("result") or {})
                self._append_event(
                    session,
                    task.task_id,
                    "run.late_result",
                    auth_actor=auth.actor_id,
                    payload={"run_id": run.run_id, "active_run_id": wu.active_run_id},
                )
                return CommandResult.success(
                    {
                        "run_id": run.run_id,
                        "late_arrival": True,
                        "accepted_as_history": True,
                    }
                )
            marker = session.get(ActiveExecuteRunMarker, run.work_unit_id)
            if marker and marker.run_id != run.run_id:
                run.late_arrival = True
                return CommandResult.success(
                    {"run_id": run.run_id, "late_arrival": True, "accepted_as_history": True}
                )

        if is_terminal_run(AgentRunStatus(run.status)):
            # already terminal — do not revive
            raise PreconditionError("run already terminal", code="run_terminal")

        result = payload.get("result") or {}
        outcome = result.get("outcome") or "COMPLETED"
        now = self.clock.now()

        # Fixed assignment: result may complete the Work Unit only if the Run's
        # frozen spec still matches the current Execution binding (§6 / §8.1).
        assignment_current = True
        if run.work_unit_id:
            wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
            if wu is None:
                assignment_current = False
            elif run.work_unit_spec_version is not None and int(
                run.work_unit_spec_version
            ) != int(wu.spec_version):
                assignment_current = False
            elif wu.active_run_id and wu.active_run_id != run.run_id:
                assignment_current = False
            else:
                plan_marker = session.get(ActivePlanMarker, task.task_id)
                if plan_marker is not None:
                    plan = session.scalars(
                        select(PlanRow).where(
                            PlanRow.task_id == task.task_id,
                            PlanRow.plan_version == plan_marker.plan_version,
                        )
                    ).first()
                    if plan is not None:
                        nodes = json.loads(plan.nodes_json)
                        node = next(
                            (n for n in nodes if n["work_unit_id"] == run.work_unit_id),
                            None,
                        )
                        if node is None:
                            assignment_current = False
                        elif run.work_unit_spec_version is not None and int(
                            node.get("spec_version") or 1
                        ) != int(run.work_unit_spec_version):
                            assignment_current = False

        if run.work_unit_id and not assignment_current:
            run.late_arrival = True
            run.result_json = canonical_json(result)
            run.result_ref = payload.get("result_ref") or new_id("res")
            if not is_terminal_run(AgentRunStatus(run.status)):
                run.status = transition_run(AgentRunStatus(run.status), "succeed")
                run.finished_at = now
            # Free occupancy without completing a superseded Work Unit definition
            marker = session.get(ActiveExecuteRunMarker, run.work_unit_id)
            if marker and marker.run_id == run.run_id:
                session.delete(marker)
            wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
            if wu and wu.active_run_id == run.run_id:
                wu.active_run_id = None
            self._register_result_artifacts(
                session,
                task.task_id,
                run,
                result,
                apply_to_work_unit=False,
            )
            self._append_event(
                session,
                task.task_id,
                "run.late_result",
                auth_actor=auth.actor_id,
                payload={
                    "run_id": run.run_id,
                    "reason": "assignment_superseded",
                    "work_unit_spec_version": run.work_unit_spec_version,
                },
            )
            return CommandResult.success(
                {
                    "run_id": run.run_id,
                    "late_arrival": True,
                    "accepted_as_history": True,
                    "assignment_current": False,
                }
            )

        run.result_json = canonical_json(result)
        run.result_ref = payload.get("result_ref") or new_id("res")
        run.status = transition_run(AgentRunStatus(run.status), "succeed")
        run.finished_at = now

        if run.work_unit_id:
            wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
            assert wu is not None
            if outcome == "BLOCKED":
                wu.status = transition_work_unit(WorkUnitStatus.RUNNING, "run.blocked")
                wu.blocked_reason = ",".join(result.get("blockers") or ["blocked"])
            else:
                wu.status = transition_work_unit(WorkUnitStatus.RUNNING, "run.completed")
                wu.selected_result_ref = run.result_ref
                wu.selected_verdict = result.get("verdict")
                refs = result.get("verified_artifact_refs") or result.get("artifact_refs") or []
                if refs:
                    wu.verified_artifact_hash = refs[0] if isinstance(refs[0], str) else refs[0].get(
                        "hash"
                    )
            wu.active_run_id = None
            marker = session.get(ActiveExecuteRunMarker, run.work_unit_id)
            if marker:
                session.delete(marker)
            self._register_result_artifacts(
                session,
                task.task_id,
                run,
                result,
                apply_to_work_unit=True,
            )
            # Result completion ≠ executor exit: keep Workspace ownership until
            # confirm_run_exit / stop acknowledgment (§8.2).

        self._append_event(
            session,
            task.task_id,
            "work.result.submitted",
            auth_actor=auth.actor_id,
            payload={"run_id": run.run_id, "outcome": outcome, "verdict": result.get("verdict")},
        )
        return CommandResult.success(
            {
                "run_id": run.run_id,
                "status": run.status,
                "work_unit_id": run.work_unit_id,
                "verdict": result.get("verdict"),
                "workspace_released": False,
            }
        )

    def _register_result_artifacts(
        self,
        session: Session,
        task_id: str,
        run: AgentRunRow,
        result: dict[str, Any],
        *,
        apply_to_work_unit: bool,
    ) -> None:
        """Register content hashes once; store criterion verdicts separately."""
        now = self.clock.now()
        hashes: list[str] = []
        refs = result.get("verified_artifact_refs") or result.get("artifact_refs") or []
        for ref in refs:
            artifact_hash = ref if isinstance(ref, str) else (ref or {}).get("hash")
            if artifact_hash:
                hashes.append(str(artifact_hash))
        evidence_items = list(result.get("acceptance_evidence") or [])
        for ev in evidence_items:
            artifact_hash = ev.get("artifact_hash") or ev.get("artifact_ref")
            if artifact_hash:
                hashes.append(str(artifact_hash))
        # Dedupe within this Result before insert (autoflush=False)
        seen: set[str] = set()
        for artifact_hash in hashes:
            if artifact_hash in seen:
                continue
            seen.add(artifact_hash)
            existing = session.get(
                ArtifactRow, {"task_id": task_id, "artifact_hash": artifact_hash}
            )
            if existing is None:
                session.add(
                    ArtifactRow(
                        task_id=task_id,
                        artifact_hash=artifact_hash,
                        work_unit_id=run.work_unit_id,
                        run_id=run.run_id,
                        result_ref=run.result_ref,
                        verdict=None,
                        created_at=now,
                    )
                )
        session.flush()
        if not apply_to_work_unit:
            return
        for ev in evidence_items:
            cid = ev.get("criterion_id")
            artifact_hash = ev.get("artifact_hash") or ev.get("artifact_ref")
            verdict = ev.get("verdict")
            if not cid or not artifact_hash or not verdict:
                continue
            self._resolve_registered_artifact(session, task_id, str(artifact_hash))
            existing_ev = session.scalars(
                select(AcceptanceEvidenceRow).where(
                    AcceptanceEvidenceRow.task_id == task_id,
                    AcceptanceEvidenceRow.run_id == run.run_id,
                    AcceptanceEvidenceRow.criterion_id == str(cid),
                )
            ).first()
            if existing_ev is None:
                session.add(
                    AcceptanceEvidenceRow(
                        evidence_id=new_id("aev"),
                        task_id=task_id,
                        criterion_id=str(cid),
                        artifact_hash=str(artifact_hash),
                        work_unit_id=run.work_unit_id,
                        run_id=run.run_id,
                        result_ref=run.result_ref,
                        verdict=str(verdict),
                        sequence_no=self._next_event_seq(session, task_id),
                        created_at=now,
                    )
                )

    def _pause_task(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        if task.state == TaskState.CANCELLING:
            raise PreconditionError("cannot pause while cancelling", code="cancelling")
        task.pause_intent = True
        self._set_task_state(session, task, "pause.requested")
        self._invalidate_pending_outbox(session, task)
        self._enqueue_stops(session, task, reason="pause")
        self._append_event(
            session, task.task_id, "pause.requested", auth_actor=auth.actor_id, payload={}
        )
        return CommandResult.success({"task_id": task.task_id, "state": task.state})

    def _runtime_quiescent(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        if task.state != TaskState.PAUSING:
            raise PreconditionError("not pausing", code="invalid_state")
        # Refuse PAUSED while any writer is still alive (including SUCCEEDED runs
        # that submitted results but have not confirmed exit).
        if self._task_has_live_writer(session, task.task_id):
            for run in session.scalars(
                select(AgentRunRow).where(AgentRunRow.task_id == task.task_id)
            ):
                if not run.workspace_id:
                    continue
                insp = self.agent_adapter.inspect(run.run_id)
                if insp.get("alive") or insp.get("writer_alive"):
                    ws = session.get(WorkspaceRow, run.workspace_id)
                    if ws:
                        ws.state = WorkspaceState.QUARANTINED
                        ws.writer_alive = True
            raise PreconditionError(
                "writer still alive; cannot mark PAUSED",
                code="writer_alive",
            )
        for wu in session.scalars(
            select(WorkUnitExecutionRow).where(
                WorkUnitExecutionRow.task_id == task.task_id,
                WorkUnitExecutionRow.status == WorkUnitStatus.RUNNING,
            )
        ):
            wu.status = WorkUnitStatus.BLOCKED
            wu.blocked_reason = "paused"
            wu.active_run_id = None
        for run in session.scalars(
            select(AgentRunRow).where(
                AgentRunRow.task_id == task.task_id,
                AgentRunRow.status.in_([AgentRunStatus.CREATED, AgentRunStatus.RUNNING]),
            )
        ):
            run.status = AgentRunStatus.CANCELLED
            run.finished_at = self.clock.now()
            run.terminal_reason = "paused"
            self._clear_run_occupancy(session, run)
        task.pause_intent = False
        self._set_task_state(session, task, "runtime.quiescent")
        self._append_event(
            session, task.task_id, "task.paused", auth_actor=auth.actor_id, payload={}
        )
        return CommandResult.success({"task_id": task.task_id, "state": task.state})

    def _resume_task(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        if task.state != TaskState.PAUSED:
            raise PreconditionError("not paused", code="invalid_state")
        if self._has_blocking_gate(session, task.task_id):
            self._set_task_state(
                session,
                task,
                "resume.requested",
                reason=task.state_reason or WaitingReason.CONTRACT_APPROVAL,
            )
        else:
            self._set_task_state(session, task, "resume.requested_runnable")
            for wu in session.scalars(
                select(WorkUnitExecutionRow).where(
                    WorkUnitExecutionRow.task_id == task.task_id,
                    WorkUnitExecutionRow.status == WorkUnitStatus.BLOCKED,
                    WorkUnitExecutionRow.blocked_reason == "paused",
                )
            ):
                wu.status = WorkUnitStatus.PENDING
                wu.blocked_reason = None
        self._append_event(
            session, task.task_id, "task.resumed", auth_actor=auth.actor_id, payload={}
        )
        return CommandResult.success({"task_id": task.task_id, "state": task.state})

    def _cancel_task(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        task.cancel_intent = True
        task.revoke_epoch += 1
        self._set_task_state(session, task, "cancel.requested")
        self._invalidate_pending_outbox(session, task)
        # cancel pending approvals not yet dispatched
        for effect in session.scalars(
            select(SideEffectRow).where(
                SideEffectRow.task_id == task.task_id,
                SideEffectRow.state.in_(
                    [
                        SideEffectState.PROPOSED,
                        SideEffectState.WAITING_APPROVAL,
                        SideEffectState.AUTHORIZED,
                        SideEffectState.DISPATCHING,
                    ]
                ),
            )
        ):
            # DISPATCHING with unsent outbox is cancelled; already-sent stays UNKNOWN via invalidate
            if effect.state == SideEffectState.DISPATCHING:
                pending = session.scalars(
                    select(OutboxRow).where(
                        OutboxRow.task_id == task.task_id,
                        OutboxRow.command_type == "side_effect.dispatch",
                        OutboxRow.status == OutboxStatus.PENDING,
                    )
                ).all()
                still_pending = any(
                    json.loads(ob.payload_json).get("effect_id") == effect.effect_id
                    for ob in pending
                )
                if still_pending:
                    effect.state = SideEffectState.CANCELLED
                # else leave DISPATCHING for invalidate/reconcile → UNKNOWN
            else:
                effect.state = SideEffectState.CANCELLED
        for wu in session.scalars(
            select(WorkUnitExecutionRow).where(
                WorkUnitExecutionRow.task_id == task.task_id,
                WorkUnitExecutionRow.status.in_(
                    [WorkUnitStatus.PENDING, WorkUnitStatus.BLOCKED]
                ),
            )
        ):
            wu.status = WorkUnitStatus.CANCELLED
        self._enqueue_stops(session, task, reason="cancel")
        self._append_event(
            session, task.task_id, "cancel.requested", auth_actor=auth.actor_id, payload={}
        )
        return CommandResult.success(
            {"task_id": task.task_id, "state": task.state, "revoke_epoch": task.revoke_epoch}
        )

    def _cancellation_settled(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        if task.state != TaskState.CANCELLING:
            raise PreconditionError("not cancelling", code="invalid_state")
        # UNKNOWN may remain if human accepted uncertainty
        accept_unknown = bool(payload.get("accept_unknown"))
        unknowns = session.scalars(
            select(SideEffectRow).where(
                SideEffectRow.task_id == task.task_id,
                SideEffectRow.state.in_(
                    [SideEffectState.UNKNOWN, SideEffectState.DISPATCHING]
                ),
            )
        ).all()
        if unknowns and not accept_unknown:
            raise PreconditionError(
                "unresolved external effects", code="unresolved_external"
            )
        self._assert_no_live_writers(session, task.task_id)
        for run in session.scalars(
            select(AgentRunRow).where(
                AgentRunRow.task_id == task.task_id,
                AgentRunRow.status.in_([AgentRunStatus.CREATED, AgentRunStatus.RUNNING]),
            )
        ):
            run.status = AgentRunStatus.CANCELLED
            run.finished_at = self.clock.now()
        self._set_task_state(
            session, task, "cancellation.settled")
        self._append_event(
            session, task.task_id, "task.aborted", auth_actor=auth.actor_id, payload={}
        )
        return CommandResult.success({"task_id": task.task_id, "state": task.state})

    def _invalidate_pending_outbox(self, session: Session, task: TaskRow) -> None:
        """Revoke unsent starts and unsent external dispatches on pause/cancel/gate.

        PENDING agent.start + adapter not alive: confirmed unsent — CANCEL CREATED
        and free Workspace.
        Claimed / adapter-may-be-alive agent.start: keep occupancy, enqueue stop;
        never release writer solely because Run is CREATED.
        """
        inflight_start_runs: list[str] = []
        for ob in session.scalars(
            select(OutboxRow).where(
                OutboxRow.task_id == task.task_id,
                OutboxRow.status.in_([OutboxStatus.PENDING, OutboxStatus.IN_FLIGHT]),
                OutboxRow.command_type.in_(["agent.start", "side_effect.dispatch"]),
            )
        ):
            # IN_FLIGHT side effects may already be sent — leave for reconcile as UNKNOWN
            if (
                ob.status == OutboxStatus.IN_FLIGHT
                and ob.command_type == "side_effect.dispatch"
            ):
                payload = json.loads(ob.payload_json)
                effect = session.get(SideEffectRow, payload.get("effect_id"))
                if effect and effect.state == SideEffectState.DISPATCHING:
                    effect.state = SideEffectState.UNKNOWN
                ob.status = OutboxStatus.ACKED
                ob.acked_at = self.clock.now()
                continue

            if ob.command_type == "agent.start":
                payload = json.loads(ob.payload_json)
                run_id = payload.get("run_id")
                run = session.get(AgentRunRow, run_id) if run_id else None
                # IN_FLIGHT or adapter-alive PENDING: start may already have happened
                if ob.status == OutboxStatus.IN_FLIGHT or self._adapter_may_be_alive(
                    run_id
                ):
                    ob.status = OutboxStatus.DEAD
                    ob.revoke_epoch = task.revoke_epoch
                    if run_id:
                        inflight_start_runs.append(run_id)
                        if run is not None:
                            self._keep_start_occupancy(session, run)
                    continue

                # Confirmed unsent — free occupancy for later redisatch
                ob.status = OutboxStatus.DEAD
                ob.revoke_epoch = task.revoke_epoch
                if run is not None and run.status == AgentRunStatus.CREATED:
                    run.status = AgentRunStatus.CANCELLED
                    run.finished_at = self.clock.now()
                    run.terminal_reason = "start_revoked"
                    self._clear_run_occupancy(session, run, release_workspace=True)
                    if run.work_unit_id:
                        wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
                        if wu is not None and wu.status == WorkUnitStatus.RUNNING:
                            wu.status = WorkUnitStatus.PENDING
                            wu.blocked_reason = None
                            wu.active_run_id = None
                continue

            ob.status = OutboxStatus.DEAD
            ob.revoke_epoch = task.revoke_epoch
            if ob.command_type == "side_effect.dispatch":
                payload = json.loads(ob.payload_json)
                effect = session.get(SideEffectRow, payload.get("effect_id"))
                if effect and effect.state == SideEffectState.DISPATCHING:
                    if task.cancel_intent:
                        effect.state = SideEffectState.CANCELLED
                    else:
                        effect.state = SideEffectState.AUTHORIZED

        if inflight_start_runs:
            self._enqueue_stops_for_runs(
                session, task, inflight_start_runs, reason="start_inflight_revoked"
            )

    def _invalidate_pending_starts(self, session: Session, task: TaskRow) -> None:
        self._invalidate_pending_outbox(session, task)

    def _runs_requiring_stop(self, session: Session, task_id: str) -> list[AgentRunRow]:
        """Stop targets: non-terminal runs and any Workspace owner still alive."""
        selected: dict[str, AgentRunRow] = {}
        for run in session.scalars(
            select(AgentRunRow).where(AgentRunRow.task_id == task_id)
        ):
            if run.status in {AgentRunStatus.CREATED, AgentRunStatus.RUNNING}:
                selected[run.run_id] = run
                continue
            if not run.workspace_id:
                continue
            ws = session.get(WorkspaceRow, run.workspace_id)
            if ws is None or ws.owner_run_id != run.run_id:
                continue
            insp = self.agent_adapter.inspect(run.run_id)
            if insp.get("alive") or insp.get("writer_alive") or ws.writer_alive:
                selected[run.run_id] = run
        return list(selected.values())

    def _enqueue_stops_for_runs(
        self,
        session: Session,
        task: TaskRow,
        run_ids: list[str],
        *,
        reason: str,
    ) -> None:
        now = self.clock.now()
        pending_stops = {
            json.loads(ob.payload_json).get("run_id")
            for ob in session.scalars(
                select(OutboxRow).where(
                    OutboxRow.task_id == task.task_id,
                    OutboxRow.command_type == "agent.stop",
                    OutboxRow.status.in_([OutboxStatus.PENDING, OutboxStatus.IN_FLIGHT]),
                )
            )
        }
        for run_id in run_ids:
            if run_id in pending_stops:
                continue
            session.add(
                OutboxRow(
                    outbox_id=new_id("ob"),
                    task_id=task.task_id,
                    command_type="agent.stop",
                    payload_json=canonical_json({"run_id": run_id, "reason": reason}),
                    status=OutboxStatus.PENDING,
                    revoke_epoch=task.revoke_epoch,
                    created_at=now,
                )
            )

    def _enqueue_stops(self, session: Session, task: TaskRow, *, reason: str) -> None:
        runs = self._runs_requiring_stop(session, task.task_id)
        self._enqueue_stops_for_runs(
            session, task, [r.run_id for r in runs], reason=reason
        )

    def _assert_acceptance_evidence(
        self, required: list[dict[str, Any]], evidence: list[dict[str, Any]]
    ) -> None:
        by_criterion: dict[str, dict[str, Any]] = {}
        for item in evidence:
            cid = item.get("criterion_id")
            if not cid:
                continue
            by_criterion[str(cid)] = item
        for crit in required:
            cid = str(crit["criterion_id"])
            item = by_criterion.get(cid)
            if item is None:
                raise PreconditionError(
                    f"missing evidence for criterion {cid}",
                    code="missing_evidence",
                )
            artifact_hash = item.get("artifact_hash") or item.get("artifact_ref")
            if not artifact_hash:
                raise PreconditionError(
                    f"criterion {cid} lacks fixed artifact hash",
                    code="missing_evidence",
                )
            verdict = item.get("verdict")
            if verdict in {Verdict.FAIL, "FAIL"}:
                raise PreconditionError(
                    f"criterion {cid} evidence verdict is FAIL",
                    code="evidence_failed",
                )
            if verdict not in {Verdict.PASS, "PASS"}:
                raise PreconditionError(
                    f"criterion {cid} evidence verdict not PASS",
                    code="missing_evidence",
                )

    def _resolve_registered_artifact(
        self, session: Session, task_id: str, artifact_hash: str
    ) -> ArtifactRow:
        row = session.get(
            ArtifactRow, {"task_id": task_id, "artifact_hash": artifact_hash}
        )
        if row is None:
            raise PreconditionError(
                f"artifact {artifact_hash} is not registered for this task",
                code="unregistered_artifact",
            )
        return row

    def _current_delivery_hashes(
        self, session: Session, nodes: list[dict[str, Any]]
    ) -> set[str]:
        """Hashes currently delivered by DONE work units on the active plan."""
        hashes: set[str] = set()
        for node in nodes:
            wu = session.get(WorkUnitExecutionRow, node["work_unit_id"])
            if wu is None or wu.status != WorkUnitStatus.DONE:
                continue
            if wu.verified_artifact_hash:
                hashes.add(str(wu.verified_artifact_hash))
        return hashes

    def _latest_criterion_evidence(
        self,
        session: Session,
        task_id: str,
        criterion_id: str,
        wu_ids: set[str],
        delivery_hashes: set[str],
    ) -> AcceptanceEvidenceRow | None:
        """Select evidence by criterion + current delivery hash; order by Core seq."""
        rows = session.scalars(
            select(AcceptanceEvidenceRow)
            .where(
                AcceptanceEvidenceRow.task_id == task_id,
                AcceptanceEvidenceRow.criterion_id == criterion_id,
            )
            .order_by(
                AcceptanceEvidenceRow.sequence_no.desc(),
                AcceptanceEvidenceRow.created_at.desc(),
            )
        ).all()
        for row in rows:
            if row.work_unit_id and row.work_unit_id not in wu_ids:
                continue
            if delivery_hashes and row.artifact_hash not in delivery_hashes:
                continue
            return row
        return None

    def _evidence_item_from_row(
        self, criterion_id: str, ev_row: AcceptanceEvidenceRow
    ) -> dict[str, Any]:
        return {
            "criterion_id": criterion_id,
            "work_unit_id": ev_row.work_unit_id,
            "result_ref": ev_row.result_ref,
            "artifact_hash": ev_row.artifact_hash,
            "verdict": ev_row.verdict,
            "run_id": ev_row.run_id,
        }

    def _build_acceptance_evidence(
        self,
        session: Session,
        task_id: str,
        nodes: list[dict[str, Any]],
        required: list[dict[str, Any]],
        extra_evidence: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Map criteria via current delivery + verification; extras cannot override."""
        current_wu_ids = {n["work_unit_id"] for n in nodes}
        delivery_hashes = self._current_delivery_hashes(session, nodes)

        default_by_cid: dict[str, dict[str, Any]] = {}
        for crit in required:
            cid = str(crit["criterion_id"])
            ev_row = self._latest_criterion_evidence(
                session, task_id, cid, current_wu_ids, delivery_hashes
            )
            if ev_row is not None:
                default_by_cid[cid] = self._evidence_item_from_row(cid, ev_row)
                continue
            # Fallback: current-plan DONE WU with explicit PASS + registered hash
            for node in nodes:
                wu = session.get(WorkUnitExecutionRow, node["work_unit_id"])
                if wu is None or wu.status != WorkUnitStatus.DONE:
                    continue
                if wu.selected_verdict not in {Verdict.PASS, "PASS"}:
                    continue
                if not wu.verified_artifact_hash:
                    continue
                if delivery_hashes and wu.verified_artifact_hash not in delivery_hashes:
                    continue
                self._resolve_registered_artifact(
                    session, task_id, wu.verified_artifact_hash
                )
                default_by_cid[cid] = {
                    "criterion_id": cid,
                    "work_unit_id": wu.work_unit_id,
                    "result_ref": wu.selected_result_ref,
                    "artifact_hash": wu.verified_artifact_hash,
                    "verdict": wu.selected_verdict,
                }
                break

        for item in extra_evidence:
            cid = item.get("criterion_id")
            artifact_hash = item.get("artifact_hash") or item.get("artifact_ref")
            if not cid or not artifact_hash:
                raise PreconditionError(
                    "extra evidence requires criterion_id and artifact_hash",
                    code="missing_evidence",
                )
            cid_s = str(cid)
            artifact_s = str(artifact_hash)
            self._resolve_registered_artifact(session, task_id, artifact_s)
            default = default_by_cid.get(cid_s)
            if default is None or default.get("artifact_hash") != artifact_s:
                raise PreconditionError(
                    f"artifact {artifact_s} is not the current delivery for criterion {cid_s}",
                    code="evidence_not_current",
                )
            # Currency check: registered evidence for this criterion/hash must apply
            ev_row = session.scalars(
                select(AcceptanceEvidenceRow)
                .where(
                    AcceptanceEvidenceRow.task_id == task_id,
                    AcceptanceEvidenceRow.criterion_id == cid_s,
                    AcceptanceEvidenceRow.artifact_hash == artifact_s,
                )
                .order_by(
                    AcceptanceEvidenceRow.sequence_no.desc(),
                    AcceptanceEvidenceRow.created_at.desc(),
                )
            ).first()
            if ev_row is None:
                raise PreconditionError(
                    f"no verification evidence for criterion {cid_s} / {artifact_s}",
                    code="missing_evidence",
                )
            if ev_row.work_unit_id and ev_row.work_unit_id not in current_wu_ids:
                raise PreconditionError(
                    f"artifact {artifact_s} is not from the current plan deliverables",
                    code="evidence_not_current",
                )
            if delivery_hashes and artifact_s not in delivery_hashes:
                raise PreconditionError(
                    f"artifact {artifact_s} is not the current delivery for criterion {cid_s}",
                    code="evidence_not_current",
                )

        return [default_by_cid[str(c["criterion_id"])] for c in required if str(c["criterion_id"]) in default_by_cid]

    def _prepare_acceptance(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        contract = self._active_contract(session, task.task_id)
        if contract is None:
            raise PreconditionError("no active contract", code="no_active_contract")
        unknown = session.scalars(
            select(SideEffectRow).where(
                SideEffectRow.task_id == task.task_id,
                SideEffectRow.state == SideEffectState.UNKNOWN,
            )
        ).all()
        if unknown:
            raise PreconditionError("UNKNOWN side effects", code="unknown_side_effect")

        # Other blocking gates must be resolved before final acceptance
        if self._other_blocking_gates(session, task.task_id):
            raise PreconditionError(
                "other blocking gates remain",
                code="blocking_gate_open",
            )
        self._assert_no_live_writers(session, task.task_id)

        # All current-plan work units must be DONE
        plan_marker = session.get(ActivePlanMarker, task.task_id)
        if plan_marker is None:
            raise PreconditionError("no active plan", code="no_active_plan")
        plan = session.scalars(
            select(PlanRow).where(
                PlanRow.task_id == task.task_id,
                PlanRow.plan_version == plan_marker.plan_version,
            )
        ).one()
        nodes = json.loads(plan.nodes_json)
        deliverables = []
        for node in nodes:
            wu = session.get(WorkUnitExecutionRow, node["work_unit_id"])
            if wu is None or wu.status != WorkUnitStatus.DONE:
                raise PreconditionError(
                    f"work unit {node['work_unit_id']} not DONE",
                    code="incomplete_work",
                )
            if not wu.selected_result_ref:
                raise PreconditionError(
                    f"work unit {node['work_unit_id']} missing result",
                    code="missing_evidence",
                )
            deliverables.append(
                {
                    "work_unit_id": wu.work_unit_id,
                    "result_ref": wu.selected_result_ref,
                    "verdict": wu.selected_verdict,
                }
            )

        content = json.loads(contract.content_json)
        required = [
            c
            for c in (content.get("acceptance_criteria") or [])
            if c.get("required", True)
        ]
        evidence = self._build_acceptance_evidence(
            session, task.task_id, nodes, required, payload.get("evidence") or []
        )
        self._assert_acceptance_evidence(required, evidence)

        self._supersede_pending_acceptance(session, task.task_id)

        snap_content = {
            "contract_version": contract.contract_version,
            "plan_version": task.plan_version,
            "deliverables": deliverables,
            "evidence": evidence,
            "open_items": payload.get("open_items") or [],
        }
        sh = content_hash(snap_content)
        snap_id = new_id("snap")
        now = self.clock.now()
        session.add(
            ResultSnapshotRow(
                result_snapshot_id=snap_id,
                task_id=task.task_id,
                contract_version=contract.contract_version,
                plan_version=task.plan_version,
                content_hash=sh,
                content_json=canonical_json(snap_content),
                created_at=now,
            )
        )
        decision_id = new_id("dec")
        session.add(
            DecisionRow(
                decision_id=decision_id,
                task_id=task.task_id,
                decision_kind=DecisionKind.FINAL_ACCEPTANCE,
                target_ref=snap_id,
                target_version=1,
                target_hash=sh,
                contract_version=contract.contract_version,
                result_snapshot_ref=snap_id,
                status=DecisionStatus.PENDING,
                created_at=now,
                expires_at=now + timedelta(hours=DEFAULTS.decision_ttl_hours),
                gate_lifecycle=GateLifecycle.OPEN,
            )
        )
        session.add(
            GateRow(
                gate_id=new_id("gate"),
                task_id=task.task_id,
                decision_id=decision_id,
                reason=WaitingReason.FINAL_ACCEPTANCE,
                lifecycle=GateLifecycle.OPEN,
                created_at=now,
            )
        )
        if TaskState(task.state) in {
            TaskState.EXECUTING,
            TaskState.VERIFYING,
            TaskState.PLANNING,
        }:
            self._set_task_state(
                session,
                task,
                "acceptance.ready",
                reason=WaitingReason.FINAL_ACCEPTANCE,
            )
        elif TaskState(task.state) != TaskState.WAITING_HUMAN:
            raise PreconditionError(
                f"cannot prepare acceptance from {task.state}", code="invalid_state"
            )
        else:
            task.state_reason = WaitingReason.FINAL_ACCEPTANCE
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "decision_id": decision_id,
                "result_snapshot_id": snap_id,
                "content_hash": sh,
            }
        )

    def _accept_result(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        return self._resolve_decision(
            session,
            auth,
            {
                "decision_id": payload["decision_id"],
                "choice": "APPROVE",
                "expected_target_hash": payload.get("expected_target_hash"),
            },
        )

    def _propose_side_effect(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        contract = self._active_contract(session, task.task_id)
        if contract is None:
            raise PreconditionError("no active contract", code="no_active_contract")
        logical_key = payload["logical_action_key"]
        existing = session.scalars(
            select(SideEffectRow).where(
                SideEffectRow.task_id == task.task_id,
                SideEffectRow.logical_action_key == logical_key,
            )
        ).first()
        params = payload.get("parameters") or {}
        target_ref = payload["target_ref"]
        action_type = payload.get("action_type") or "external.write"
        ph = content_hash(params)
        digest = content_hash(
            {"action_type": action_type, "target_ref": target_ref, "parameters": params}
        )
        if existing is not None:
            if existing.action_digest != digest:
                raise ConflictError(
                    "logical_action_key exists with different parameters/target",
                    code="side_effect_digest_conflict",
                )
            return CommandResult.success(
                {
                    "effect_id": existing.effect_id,
                    "state": existing.state,
                    "reused": True,
                    "task_id": task.task_id,
                    "action_digest": existing.action_digest,
                },
                replayed=True,
            )
        effect_id = new_id("eff")
        now = self.clock.now()
        preauthorized = logical_key in (
            json.loads(contract.content_json).get("allowed_side_effects") or []
        )
        state = SideEffectState.AUTHORIZED if preauthorized else SideEffectState.PROPOSED
        effect = SideEffectRow(
            effect_id=effect_id,
            task_id=task.task_id,
            logical_action_key=logical_key,
            origin_work_unit_id=payload.get("work_unit_id"),
            request_run_id=payload.get("run_id"),
            action_type=action_type,
            target_ref=target_ref,
            parameters_hash=ph,
            action_digest=digest,
            parameters_json=canonical_json(params),
            contract_version=contract.contract_version,
            external_idempotency_key=f"ext:{task.task_id}:{logical_key}",
            state=state,
            supports_idempotent_query=bool(
                payload.get("supports_idempotent_query", True)
            ),
            created_at=now,
        )
        session.add(effect)
        decision_id = None
        if not preauthorized:
            decision_id = new_id("dec")
            session.add(
                DecisionRow(
                    decision_id=decision_id,
                    task_id=task.task_id,
                    decision_kind=DecisionKind.SIDE_EFFECT_APPROVAL,
                    target_ref=effect_id,
                    target_version=1,
                    target_hash=digest,
                    contract_version=contract.contract_version,
                    parameters_hash=ph,
                    status=DecisionStatus.PENDING,
                    created_at=now,
                    expires_at=now
                    + timedelta(minutes=DEFAULTS.side_effect_approval_ttl_minutes),
                    gate_lifecycle=GateLifecycle.OPEN,
                )
            )
            session.add(
                GateRow(
                    gate_id=new_id("gate"),
                    task_id=task.task_id,
                    decision_id=decision_id,
                    reason=WaitingReason.SIDE_EFFECT_APPROVAL,
                    lifecycle=GateLifecycle.OPEN,
                    created_at=now,
                )
            )
            effect.state = SideEffectState.WAITING_APPROVAL
            self._set_task_state(
                session,
                task,
                "blocking_gate.opened",
                reason=WaitingReason.SIDE_EFFECT_APPROVAL,
            )
        self._append_event(
            session,
            task.task_id,
            "side_effect.proposed",
            auth_actor=auth.actor_id,
            payload={"effect_id": effect_id, "state": effect.state},
        )
        return CommandResult.success(
            {
                "effect_id": effect_id,
                "state": effect.state,
                "decision_id": decision_id,
                "action_digest": digest,
                "task_id": task.task_id,
            }
        )

    def _approve_side_effect(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        return self._resolve_decision(
            session,
            auth,
            {
                "decision_id": payload["decision_id"],
                "choice": "APPROVE",
                "expected_target_hash": payload.get("expected_target_hash"),
            },
        )

    def _dispatch_side_effect(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        effect = session.get(SideEffectRow, payload["effect_id"])
        if effect is None:
            raise NotFoundError("effect not found")
        task = self._get_task(session, effect.task_id)
        self._guard_side_effect_eligibility(
            session,
            task,
            effect,
            allowed_states=frozenset({SideEffectState.AUTHORIZED}),
        )
        # claim
        effect.state = transition_side_effect(SideEffectState(effect.state), "claim_dispatch")
        effect.dispatch_attempts += 1
        self._append_event(
            session,
            task.task_id,
            "side_effect.dispatching",
            auth_actor=auth.actor_id,
            payload={"effect_id": effect.effect_id},
        )
        # Actual I/O happens after commit via outbox
        now = self.clock.now()
        session.add(
            OutboxRow(
                outbox_id=new_id("ob"),
                task_id=task.task_id,
                command_type="side_effect.dispatch",
                payload_json=canonical_json(
                    {
                        "effect_id": effect.effect_id,
                        "external_idempotency_key": effect.external_idempotency_key,
                        "parameters": json.loads(effect.parameters_json),
                        "target_ref": effect.target_ref,
                        "action_type": effect.action_type,
                        "supports_idempotent_query": effect.supports_idempotent_query,
                    }
                ),
                status=OutboxStatus.PENDING,
                revoke_epoch=task.revoke_epoch,
                created_at=now,
            )
        )
        return CommandResult.success(
            {"effect_id": effect.effect_id, "state": effect.state, "task_id": task.task_id}
        )

    def _reconcile_side_effect(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        effect = session.get(SideEffectRow, payload["effect_id"])
        if effect is None:
            raise NotFoundError("effect not found")
        if effect.state not in {SideEffectState.DISPATCHING, SideEffectState.UNKNOWN}:
            return CommandResult.success(
                {"effect_id": effect.effect_id, "state": effect.state}
            )
        if not effect.supports_idempotent_query:
            # Cannot auto-retry; stay UNKNOWN
            if effect.state == SideEffectState.DISPATCHING:
                effect.state = SideEffectState.UNKNOWN
            task = self._get_task(session, effect.task_id)
            if task.state not in {TaskState.WAITING_HUMAN, TaskState.CANCELLING, TaskState.ABORTED}:
                self._set_task_state(
                    session,
                    task,
                    "blocking_gate.opened",
                    reason=WaitingReason.EXECUTION_UNCERTAIN,
                )
            return CommandResult.success(
                {
                    "effect_id": effect.effect_id,
                    "state": effect.state,
                    "auto_retry": False,
                }
            )
        q = self.external_adapter.query(
            effect.effect_id, effect.external_idempotency_key
        )
        if q and q.get("status") == "succeeded":
            effect.state = SideEffectState.SUCCEEDED
            effect.receipt_json = canonical_json(q)
            effect.provider_operation_id = q.get("provider_operation_id")
            return CommandResult.success(
                {"effect_id": effect.effect_id, "state": effect.state, "reconciled": True}
            )
        if q and q.get("status") == "failed":
            effect.state = SideEffectState.FAILED_CONFIRMED
            return CommandResult.success(
                {"effect_id": effect.effect_id, "state": effect.state}
            )
        effect.state = SideEffectState.UNKNOWN
        return CommandResult.success(
            {"effect_id": effect.effect_id, "state": effect.state, "auto_retry": False}
        )

    def _open_blocking_gate(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        now = self.clock.now()
        decision_id = new_id("dec")
        reason = payload.get("reason") or WaitingReason.EXECUTION_BLOCKED
        session.add(
            DecisionRow(
                decision_id=decision_id,
                task_id=task.task_id,
                decision_kind=DecisionKind.CLARIFICATION,
                target_ref=payload.get("target_ref") or "gate",
                target_version=1,
                target_hash=content_hash(payload),
                status=DecisionStatus.PENDING,
                created_at=now,
                expires_at=now + timedelta(hours=DEFAULTS.decision_ttl_hours),
                gate_lifecycle=GateLifecycle.OPEN,
            )
        )
        session.add(
            GateRow(
                gate_id=new_id("gate"),
                task_id=task.task_id,
                decision_id=decision_id,
                reason=reason,
                lifecycle=GateLifecycle.OPEN,
                created_at=now,
            )
        )
        if run_id := payload.get("run_id"):
            run = session.get(AgentRunRow, run_id)
            if run and run.work_unit_id:
                wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
                if wu and wu.status == WorkUnitStatus.RUNNING:
                    wu.status = WorkUnitStatus.BLOCKED
                    wu.blocked_reason = reason
                    wu.active_run_id = None
                marker = session.get(ActiveExecuteRunMarker, run.work_unit_id)
                if marker and marker.run_id == run.run_id:
                    session.delete(marker)
                if run.status == AgentRunStatus.RUNNING:
                    run.status = AgentRunStatus.SUCCEEDED
                    run.result_json = canonical_json(
                        {"outcome": "BLOCKED", "blockers": [reason]}
                    )
                    run.finished_at = now
                # Keep Workspace until exit confirm; enqueue stop for live writer
                self._enqueue_stops_for_runs(
                    session, task, [run.run_id], reason="blocking_gate"
                )
        self._set_task_state(
            session,
            task,
            "blocking_gate.opened",
            reason=reason,
        )
        self._invalidate_pending_outbox(session, task)
        return CommandResult.success(
            {"task_id": task.task_id, "decision_id": decision_id, "state": task.state}
        )

    def _open_resource_gate(self, session: Session, task: TaskRow) -> None:
        now = self.clock.now()
        decision_id = new_id("dec")
        session.add(
            DecisionRow(
                decision_id=decision_id,
                task_id=task.task_id,
                decision_kind=DecisionKind.RESOURCE_LIMIT,
                target_ref="budget",
                target_version=1,
                target_hash=content_hash({"limit": task.model_call_limit}),
                status=DecisionStatus.PENDING,
                created_at=now,
                expires_at=now + timedelta(hours=DEFAULTS.decision_ttl_hours),
                gate_lifecycle=GateLifecycle.OPEN,
            )
        )
        session.add(
            GateRow(
                gate_id=new_id("gate"),
                task_id=task.task_id,
                decision_id=decision_id,
                reason=WaitingReason.RESOURCE_LIMIT,
                lifecycle=GateLifecycle.OPEN,
                created_at=now,
            )
        )
        self._set_task_state(
            session,
            task,
            "blocking_gate.opened",
            reason=WaitingReason.RESOURCE_LIMIT,
        )

    def _record_model_usage(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        task.model_calls_used += int(payload.get("calls") or 1)
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "model_calls_used": task.model_calls_used,
                "limit": task.model_call_limit,
            }
        )

    def _confirm_run_exit(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        """Confirm executor exit and release Workspace — separate from Result (§8.2)."""
        run = session.get(AgentRunRow, payload["run_id"])
        if run is None:
            raise NotFoundError("run not found", code="run_not_found")
        insp = self.agent_adapter.inspect(run.run_id)
        if insp.get("alive") or insp.get("writer_alive"):
            # Attempt cooperative stop then re-inspect
            stop = self.agent_adapter.stop(run.run_id, payload.get("reason") or "confirm_exit")
            insp = self.agent_adapter.inspect(run.run_id)
            if stop.get("alive") or insp.get("alive") or insp.get("writer_alive"):
                if run.workspace_id:
                    ws = session.get(WorkspaceRow, run.workspace_id)
                    if ws:
                        ws.state = WorkspaceState.QUARANTINED
                        ws.writer_alive = True
                raise PreconditionError(
                    "executor still alive after stop",
                    code="writer_alive",
                )
        if run.workspace_id:
            ws = session.get(WorkspaceRow, run.workspace_id)
            if ws and ws.owner_run_id == run.run_id:
                ws.writer_alive = False
                if ws.state != WorkspaceState.QUARANTINED:
                    ws.state = WorkspaceState.READY
                    ws.owner_run_id = None
        self._append_event(
            session,
            run.task_id,
            "run.exit_confirmed",
            auth_actor=auth.actor_id,
            payload={"run_id": run.run_id},
        )
        return CommandResult.success(
            {
                "run_id": run.run_id,
                "workspace_released": True,
                "alive": False,
            }
        )

    def _set_writer_alive(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        ws = session.get(WorkspaceRow, payload["workspace_id"])
        if ws is None:
            raise NotFoundError("workspace not found")
        run_id = payload.get("run_id") or ws.owner_run_id
        if not run_id:
            raise PreconditionError(
                "workspace has no owner run", code="workspace_unbound"
            )
        run = session.get(AgentRunRow, run_id)
        if run is None:
            raise NotFoundError("run not found", code="run_not_found")
        if ws.owner_run_id and ws.owner_run_id != run.run_id:
            raise AuthorizationError(
                "workspace owner mismatch", code="authorization_denied"
            )
        bind_payload = dict(payload)
        bind_payload.setdefault("run_id", run.run_id)
        self._assert_run_write_binding(session, auth, run, bind_payload)
        ws.writer_alive = bool(payload.get("alive", True))
        if payload.get("quarantine"):
            ws.state = WorkspaceState.QUARANTINED
        return CommandResult.success(
            {
                "workspace_id": ws.workspace_id,
                "writer_alive": ws.writer_alive,
                "state": ws.state,
            }
        )

    def _heartbeat(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        run = session.get(AgentRunRow, payload["run_id"])
        if run is None:
            raise NotFoundError("run not found", code="run_not_found")
        self._assert_run_write_binding(session, auth, run, payload)
        now = self.clock.now()
        run.last_heartbeat_at = now
        run.lease_expires_at = now + timedelta(seconds=DEFAULTS.lease_seconds)
        return CommandResult.success({"run_id": run.run_id, "lease_expires_at": run.lease_expires_at.isoformat()})

    def _seed_profile(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        now = self.clock.now()
        session.add(
            AgentProfileRow(
                profile_id=payload["profile_id"],
                profile_version=int(payload["profile_version"]),
                content_json=canonical_json(payload.get("content") or {}),
                created_at=now,
            )
        )
        return CommandResult.success(
            {
                "profile_id": payload["profile_id"],
                "profile_version": int(payload["profile_version"]),
            }
        )

    def _replace_planner_generation(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        now = self.clock.now()
        session_id = task.active_planner_session_id or new_id("ps")
        existing = session.get(PlannerSessionRow, session_id)
        if existing is None:
            existing = PlannerSessionRow(
                planner_session_id=session_id,
                task_id=task.task_id,
                generation=1,
                status="ACTIVE",
            )
            session.add(existing)
            task.active_planner_session_id = session_id
            generation = 1
        else:
            existing.generation = int(existing.generation or 0) + 1
            generation = existing.generation
        existing.checkpoint_version = int(existing.checkpoint_version or 0) + 1
        self._append_event(
            session,
            task.task_id,
            "planner.generation_bumped",
            auth_actor=auth.actor_id,
            payload={"generation": generation},
        )
        _ = now
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "planner_session_id": session_id,
                "generation": generation,
            }
        )

    def _submit_plan_proposal(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        task = self._get_task(session, payload["task_id"])
        session_id = task.active_planner_session_id
        if session_id is None:
            raise PreconditionError("no planner session", code="no_planner_session")
        ps = session.get(PlannerSessionRow, session_id)
        assert ps is not None
        gen = int(payload.get("generation") or 0)
        if gen != ps.generation:
            self._append_event(
                session,
                task.task_id,
                "plan.proposal_rejected_stale_generation",
                auth_actor=auth.actor_id,
                payload={"got": gen, "expected": ps.generation},
            )
            return CommandResult.failure(
                "stale_generation",
                "stale planner generation",
                data={"got": gen, "expected": ps.generation, "task_id": task.task_id},
            )
        # Check if proposal tries to change authorization fields
        if payload.get("changes_authorization"):
            # Must go to contract delta — persist immutable baseline + target
            active = self._active_contract(session, task.task_id)
            if active is None:
                raise PreconditionError("no active contract", code="no_active_contract")
            now = self.clock.now()
            decision_id = new_id("dec")
            delta = payload.get("delta") or {}
            baseline_version = int(active.contract_version)
            baseline_hash = active.content_hash
            merged = self._merge_contract_content(
                json.loads(active.content_json), delta
            )
            approved_target_hash = content_hash(merged)
            target_version = baseline_version + 1
            envelope = {
                "delta": delta,
                "baseline_contract_version": baseline_version,
                "baseline_contract_hash": baseline_hash,
                "approved_merged_hash": approved_target_hash,
            }
            session.add(
                DecisionRow(
                    decision_id=decision_id,
                    task_id=task.task_id,
                    decision_kind=DecisionKind.CONTRACT_DELTA,
                    target_ref="delta",
                    target_version=target_version,
                    target_hash=approved_target_hash,
                    contract_version=baseline_version,
                    payload_json=canonical_json(envelope),
                    status=DecisionStatus.PENDING,
                    created_at=now,
                    expires_at=now + timedelta(hours=DEFAULTS.decision_ttl_hours),
                    gate_lifecycle=GateLifecycle.OPEN,
                )
            )
            session.add(
                GateRow(
                    gate_id=new_id("gate"),
                    task_id=task.task_id,
                    decision_id=decision_id,
                    reason=WaitingReason.CONTRACT_DELTA,
                    lifecycle=GateLifecycle.OPEN,
                    created_at=now,
                )
            )
            self._set_task_state(
                session,
                task,
                "blocking_gate.opened",
                reason=WaitingReason.CONTRACT_DELTA,
            )
            return CommandResult.success(
                {
                    "task_id": task.task_id,
                    "requires_contract_delta": True,
                    "decision_id": decision_id,
                }
            )
        return self._activate_plan(session, auth, payload)

    def _merge_contract_content(
        self, base: dict[str, Any], delta: dict[str, Any]
    ) -> dict[str, Any]:
        merged = dict(base)
        if "resource_limits" in delta:
            limits = dict(merged.get("resource_limits") or {})
            limits.update(delta["resource_limits"] or {})
            merged["resource_limits"] = limits
        for key in (
            "objective",
            "in_scope",
            "out_of_scope",
            "constraints",
            "assumptions",
            "deliverables",
            "acceptance_criteria",
            "allowed_side_effects",
            "permission_ceiling",
            "human_gates",
        ):
            if key in delta:
                merged[key] = delta[key]
        return merged

    def _parse_contract_delta_payload(
        self, decision: DecisionRow
    ) -> tuple[dict[str, Any], int | None, str | None]:
        raw = json.loads(decision.payload_json or "{}")
        if isinstance(raw, dict) and "delta" in raw and isinstance(raw["delta"], dict):
            baseline_v = raw.get("baseline_contract_version", decision.contract_version)
            baseline_h = raw.get("baseline_contract_hash")
            return (
                raw["delta"],
                int(baseline_v) if baseline_v is not None else None,
                str(baseline_h) if baseline_h else None,
            )
        # Legacy: payload was the delta object alone
        return raw, decision.contract_version, None

    def _invalidate_stale_contract_delta(
        self,
        session: Session,
        task: TaskRow,
        decision: DecisionRow,
        gates: list[GateRow],
        *,
        auth_actor: str | None,
        code: str,
    ) -> None:
        """Keep APPROVED history; close apply gate and emit domain event."""
        now = self.clock.now()
        for gate in gates:
            gate.lifecycle = GateLifecycle.RESOLVED
            gate.resolved_at = now
        decision.gate_lifecycle = GateLifecycle.RESOLVED
        self._append_event(
            session,
            task.task_id,
            "contract.delta_apply_rejected_stale",
            auth_actor=auth_actor,
            payload={
                "decision_id": decision.decision_id,
                "code": code,
                "target_version": decision.target_version,
                "current_contract_version": task.contract_version,
            },
        )
        self._recompute_runnable_state(session, task)

    def _apply_contract_delta(
        self, session: Session, auth: AuthContext, payload: dict[str, Any]
    ) -> CommandResult:
        """Activate an approved Contract Delta after writers are drained (§6.3)."""
        decision = session.get(DecisionRow, payload["decision_id"])
        if decision is None:
            raise NotFoundError("decision not found", code="decision_not_found")
        if decision.decision_kind != DecisionKind.CONTRACT_DELTA:
            raise PreconditionError("not a contract delta", code="invalid_decision_kind")
        if decision.status != DecisionStatus.APPROVED:
            raise PreconditionError(
                "delta decision not approved", code="decision_not_approved"
            )
        task = self._get_task(session, decision.task_id)
        if task.principal_id != auth.principal_id:
            raise AuthorizationError(
                "decision principal mismatch", code="authorization_denied"
            )
        gates = list(
            session.scalars(
                select(GateRow).where(GateRow.decision_id == decision.decision_id)
            )
        )
        if not any(g.lifecycle == GateLifecycle.APPROVED_PENDING_APPLY for g in gates):
            raise PreconditionError(
                "delta gate not awaiting apply", code="delta_not_pending_apply"
            )
        self._assert_no_live_writers(session, task.task_id)
        if not decision.payload_json:
            raise PreconditionError(
                "contract delta payload missing", code="delta_payload_missing"
            )
        delta, baseline_v, baseline_h = self._parse_contract_delta_payload(decision)
        active = self._active_contract(session, task.task_id)
        if active is None:
            raise PreconditionError("no active contract", code="no_active_contract")

        # CAS: baseline must still be the active contract; never renumber target
        if baseline_v is not None and (
            int(task.contract_version or 0) != int(baseline_v)
            or int(active.contract_version) != int(baseline_v)
        ):
            self._invalidate_stale_contract_delta(
                session,
                task,
                decision,
                gates,
                auth_actor=auth.actor_id,
                code="baseline_stale",
            )
            return CommandResult.failure(
                "baseline_stale",
                "baseline contract superseded",
                data={"decision_id": decision.decision_id},
            )
        if baseline_h and active.content_hash != baseline_h:
            self._invalidate_stale_contract_delta(
                session,
                task,
                decision,
                gates,
                auth_actor=auth.actor_id,
                code="baseline_stale",
            )
            return CommandResult.failure(
                "baseline_stale",
                "baseline contract content changed",
                data={"decision_id": decision.decision_id},
            )

        version = int(decision.target_version)
        existing = session.scalars(
            select(ContractRow).where(
                ContractRow.task_id == task.task_id,
                ContractRow.contract_version == version,
            )
        ).first()
        if existing is not None:
            self._invalidate_stale_contract_delta(
                session,
                task,
                decision,
                gates,
                auth_actor=auth.actor_id,
                code="target_version_taken",
            )
            return CommandResult.failure(
                "target_version_taken",
                "target contract version already exists",
                data={"decision_id": decision.decision_id, "target_version": version},
            )

        if baseline_v is not None:
            base_row = session.scalars(
                select(ContractRow).where(
                    ContractRow.task_id == task.task_id,
                    ContractRow.contract_version == int(baseline_v),
                )
            ).one()
            base = json.loads(base_row.content_json)
        else:
            base = json.loads(active.content_json)
        merged = self._merge_contract_content(base, delta)
        now = self.clock.now()
        ch = content_hash(merged)
        if ch != decision.target_hash:
            # Legacy decisions hashed the delta object; reject only when envelope
            # promised a merged hash (approved_merged_hash path).
            if baseline_h is not None:
                raise ConflictError(
                    "merged contract hash mismatch", code="target_hash_conflict"
                )
        # Supersede previous active
        prev = session.get(ActiveContractMarker, task.task_id)
        if prev:
            old = session.scalars(
                select(ContractRow).where(
                    ContractRow.task_id == task.task_id,
                    ContractRow.contract_version == prev.contract_version,
                )
            ).first()
            if old:
                old.status = ContractStatus.SUPERSEDED
            session.delete(prev)
        session.add(
            ContractRow(
                task_id=task.task_id,
                contract_version=version,
                supersedes_version=task.contract_version,
                content_hash=ch,
                content_json=canonical_json(merged),
                status=ContractStatus.ACTIVE,
                created_at=now,
                approved_by=decision.decided_by_actor,
                approved_at=now,
            )
        )
        session.add(ActiveContractMarker(task_id=task.task_id, contract_version=version))
        task.contract_version = version
        limits = merged.get("resource_limits") or {}
        if "model_call_limit" in limits:
            task.model_call_limit = int(limits["model_call_limit"])
        for gate in gates:
            gate.lifecycle = GateLifecycle.RESOLVED
            gate.resolved_at = now
        decision.gate_lifecycle = GateLifecycle.RESOLVED
        self._recompute_runnable_state(session, task)
        self._append_event(
            session,
            task.task_id,
            "contract.delta_applied",
            auth_actor=auth.actor_id,
            payload={"decision_id": decision.decision_id, "contract_version": version},
        )
        return CommandResult.success(
            {
                "task_id": task.task_id,
                "contract_version": version,
                "decision_id": decision.decision_id,
                "state": task.state,
                "model_call_limit": task.model_call_limit,
            }
        )

    # ------------------------------------------------------------------
    # Outbox
    # ------------------------------------------------------------------

    def _claim_one_outbox(self, session: Session) -> dict[str, Any] | None:
        row = session.scalars(
            select(OutboxRow)
            .where(OutboxRow.status == OutboxStatus.PENDING)
            .order_by(OutboxRow.created_at)
        ).first()
        if row is None:
            return None
        # Honor backoff lease on PENDING rows (agent.start retry)
        if row.leased_until is not None and as_utc_naive(row.leased_until) > self.clock.now():
            # Look for another claimable row
            rows = session.scalars(
                select(OutboxRow)
                .where(OutboxRow.status == OutboxStatus.PENDING)
                .order_by(OutboxRow.created_at)
            ).all()
            row = None
            for candidate in rows:
                if (
                    candidate.leased_until is None
                    or as_utc_naive(candidate.leased_until) <= self.clock.now()
                ):
                    row = candidate
                    break
            if row is None:
                return None
        task = session.get(TaskRow, row.task_id)
        if task and row.command_type == "agent.start":
            if row.revoke_epoch < task.revoke_epoch or task.cancel_intent or task.pause_intent:
                row.status = OutboxStatus.DEAD
                return None
            if TaskState(task.state) not in {
                TaskState.PLANNING,
                TaskState.EXECUTING,
                TaskState.VERIFYING,
            }:
                row.status = OutboxStatus.DEAD
                return None
        if task and row.command_type == "side_effect.dispatch":
            payload = json.loads(row.payload_json)
            effect = session.get(SideEffectRow, payload.get("effect_id"))
            if effect is None or task is None:
                row.status = OutboxStatus.DEAD
                return None
            try:
                self._guard_side_effect_eligibility(
                    session,
                    task,
                    effect,
                    allowed_states=frozenset(
                        {SideEffectState.DISPATCHING, SideEffectState.AUTHORIZED}
                    ),
                )
            except PreconditionError:
                row.status = OutboxStatus.DEAD
                if effect.state == SideEffectState.DISPATCHING:
                    if task.cancel_intent or TaskState(task.state) == TaskState.CANCELLING:
                        effect.state = SideEffectState.CANCELLED
                    else:
                        effect.state = SideEffectState.AUTHORIZED
                elif (
                    effect.state == SideEffectState.AUTHORIZED
                    and (task.cancel_intent or TaskState(task.state) == TaskState.CANCELLING)
                ):
                    effect.state = SideEffectState.CANCELLED
                return None
            if row.revoke_epoch < task.revoke_epoch:
                row.status = OutboxStatus.DEAD
                if effect.state == SideEffectState.DISPATCHING:
                    effect.state = (
                        SideEffectState.CANCELLED
                        if task.cancel_intent
                        else SideEffectState.AUTHORIZED
                    )
                return None
            if effect.state == SideEffectState.AUTHORIZED:
                effect.state = SideEffectState.DISPATCHING
        row.status = OutboxStatus.IN_FLIGHT
        row.leased_until = self.clock.now() + timedelta(seconds=30)
        return {
            "outbox_id": row.outbox_id,
            "task_id": row.task_id,
            "command_type": row.command_type,
            "payload": json.loads(row.payload_json),
        }

    def _dispatch_outbox_item(self, item: dict[str, Any]) -> None:
        ctype = item["command_type"]
        payload = item["payload"]
        try:
            if ctype == "agent.start":
                self.agent_adapter.start(payload)
                self.executor.run(
                    lambda s: self._ack_outbox_and_mark_running(
                        s, item["outbox_id"], payload
                    )
                )
            elif ctype == "agent.stop":
                result = self.agent_adapter.stop(payload["run_id"], payload.get("reason") or "stop")
                self.executor.run(
                    lambda s: self._ack_stop(s, item["outbox_id"], payload, result)
                )
            elif ctype == "side_effect.dispatch":
                self._do_external_dispatch(item)
            else:
                self.executor.run(lambda s: self._ack_outbox(s, item["outbox_id"], dead=True))
        except Exception as exc:  # noqa: BLE001
            err = str(exc)
            self.executor.run(lambda s: self._fail_outbox(s, item["outbox_id"], err))

    def _fail_outbox(self, session: Session, outbox_id: str, error: str) -> None:
        row = session.get(OutboxRow, outbox_id)
        if row is None:
            return
        if row.command_type == "side_effect.dispatch":
            # UNKNOWN must not return to PENDING — no blind replay
            payload = json.loads(row.payload_json)
            effect = session.get(SideEffectRow, payload["effect_id"])
            if effect and effect.state in {
                SideEffectState.DISPATCHING,
                SideEffectState.AUTHORIZED,
            }:
                effect.state = SideEffectState.UNKNOWN
                effect.last_error = error
                task = session.get(TaskRow, effect.task_id)
                if task and not is_terminal_task(TaskState(task.state)):
                    if TaskState(task.state) != TaskState.WAITING_HUMAN:
                        self._set_task_state(
                            session,
                            task,
                            "blocking_gate.opened",
                            reason=WaitingReason.EXECUTION_UNCERTAIN,
                        )
            row.status = OutboxStatus.ACKED
            row.acked_at = self.clock.now()
            return
        if row.command_type == "agent.start":
            if row.status == OutboxStatus.DEAD:
                return
            # Adapter may already have started even if ACK/tx failed — inspect before
            # erasing the claim into a "never sent" PENDING reclaim.
            note = self._reclaim_or_fence_agent_start(
                session, row, reason="start_dispatch_failed"
            )
            if note.startswith("outbox_reclaim_start:"):
                row.leased_until = self.clock.now() + timedelta(
                    seconds=DEFAULTS.retry_backoff_seconds[0]
                )
            return
        # other outbox types: bounded backoff via PENDING
        if row.status == OutboxStatus.DEAD:
            return
        row.status = OutboxStatus.PENDING
        row.leased_until = self.clock.now() + timedelta(seconds=DEFAULTS.retry_backoff_seconds[0])

    def _ack_outbox_and_mark_running(
        self, session: Session, outbox_id: str, payload: dict[str, Any]
    ) -> None:
        row = session.get(OutboxRow, outbox_id)
        run = session.get(AgentRunRow, payload["run_id"])
        task = session.get(TaskRow, run.task_id) if run else None

        # Fence: revoked / DEAD outbox or control freeze must not promote Run.
        revoked = (
            row is None
            or row.status == OutboxStatus.DEAD
            or (
                task is not None
                and (
                    task.cancel_intent
                    or task.pause_intent
                    or TaskState(task.state)
                    in {
                        TaskState.WAITING_HUMAN,
                        TaskState.PAUSING,
                        TaskState.PAUSED,
                        TaskState.CANCELLING,
                    }
                    or self._has_blocking_gate(session, task.task_id)
                )
            )
            or (run is not None and run.status == AgentRunStatus.CANCELLED)
        )
        if revoked:
            if row is not None and row.status == OutboxStatus.IN_FLIGHT:
                row.status = OutboxStatus.DEAD
                if task is not None:
                    row.revoke_epoch = task.revoke_epoch
            # Adapter already started — keep occupancy and ensure stop
            if run is not None and task is not None:
                self._keep_start_occupancy(session, run)
                self._enqueue_stops_for_runs(
                    session, task, [run.run_id], reason="start_ack_fenced"
                )
            return

        if row:
            row.status = OutboxStatus.ACKED
            row.acked_at = self.clock.now()
        if run and run.status == AgentRunStatus.CREATED:
            run.status = AgentRunStatus.RUNNING
            run.started_at = self.clock.now()
            run.agent_instance_id = f"fake:{run.run_id}"

    def _ack_stop(
        self,
        session: Session,
        outbox_id: str,
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        row = session.get(OutboxRow, outbox_id)
        if row:
            row.status = OutboxStatus.ACKED
            row.acked_at = self.clock.now()
        run = session.get(AgentRunRow, payload["run_id"])
        if run is None:
            return
        if run.workspace_id:
            ws = session.get(WorkspaceRow, run.workspace_id)
            if ws:
                if result.get("alive"):
                    ws.state = WorkspaceState.QUARANTINED
                    ws.writer_alive = True
                else:
                    ws.writer_alive = False
                    if ws.owner_run_id == run.run_id and ws.state != WorkspaceState.QUARANTINED:
                        ws.state = WorkspaceState.READY
                        ws.owner_run_id = None
        # Fenced / revoked start that never became RUNNING: settle as CANCELLED
        if run.status == AgentRunStatus.CREATED and not result.get("alive"):
            run.status = AgentRunStatus.CANCELLED
            run.finished_at = self.clock.now()
            run.terminal_reason = run.terminal_reason or payload.get("reason") or "start_revoked"
            self._clear_run_occupancy(session, run, release_workspace=True)
            if run.work_unit_id:
                wu = session.get(WorkUnitExecutionRow, run.work_unit_id)
                if wu is not None and wu.status == WorkUnitStatus.RUNNING:
                    # Gate/pause may still block redisatch; free unit for later PENDING
                    if wu.blocked_reason:
                        pass
                    else:
                        wu.status = WorkUnitStatus.PENDING
                    wu.active_run_id = None

    def _ack_outbox(self, session: Session, outbox_id: str, *, dead: bool = False) -> None:
        row = session.get(OutboxRow, outbox_id)
        if row:
            row.status = OutboxStatus.DEAD if dead else OutboxStatus.ACKED
            row.acked_at = self.clock.now()

    def _do_external_dispatch(self, item: dict[str, Any]) -> None:
        payload = item["payload"]

        # Final pre-send gate: cancel / pause / gate / contract / non-DISPATCHING
        def _precheck(session: Session) -> bool:
            row = session.get(OutboxRow, item["outbox_id"])
            effect = session.get(SideEffectRow, payload["effect_id"])
            task = session.get(TaskRow, item["task_id"]) if item.get("task_id") else None
            if row is None or effect is None:
                return False
            if task is None:
                task = session.get(TaskRow, effect.task_id)
            if task is None or row.status == OutboxStatus.DEAD:
                if row:
                    row.status = OutboxStatus.DEAD
                return False
            try:
                self._guard_side_effect_eligibility(
                    session,
                    task,
                    effect,
                    allowed_states=frozenset({SideEffectState.DISPATCHING}),
                )
            except PreconditionError:
                row.status = OutboxStatus.DEAD
                if effect.state == SideEffectState.DISPATCHING and (
                    task.cancel_intent or TaskState(task.state) == TaskState.CANCELLING
                ):
                    # Unsent cancel path — only if we never called adapter
                    effect.state = SideEffectState.CANCELLED
                elif effect.state == SideEffectState.DISPATCHING:
                    effect.state = SideEffectState.AUTHORIZED
                return False
            return True

        if not self.executor.run(_precheck):
            return

        try:
            receipt = self.external_adapter.dispatch(
                {
                    "effect_id": payload["effect_id"],
                    "external_idempotency_key": payload["external_idempotency_key"],
                    "parameters": payload.get("parameters"),
                    "target_ref": payload.get("target_ref"),
                    "action_type": payload.get("action_type"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            self.executor.run(lambda s: self._fail_outbox(s, item["outbox_id"], str(exc)))
            return

        def _apply(session: Session) -> None:
            effect = session.get(SideEffectRow, payload["effect_id"])
            row = session.get(OutboxRow, item["outbox_id"])
            if row:
                row.status = OutboxStatus.ACKED
                row.acked_at = self.clock.now()
            if effect is None:
                return
            status = receipt.get("status")
            if status == "succeeded":
                effect.state = SideEffectState.SUCCEEDED
                effect.receipt_json = canonical_json(receipt)
                effect.provider_operation_id = receipt.get("provider_operation_id")
            elif status == "failed":
                effect.state = SideEffectState.FAILED_CONFIRMED
                effect.last_error = receipt.get("error")
            else:
                effect.state = SideEffectState.UNKNOWN
                task = session.get(TaskRow, effect.task_id)
                if task and not is_terminal_task(TaskState(task.state)):
                    if TaskState(task.state) != TaskState.WAITING_HUMAN:
                        self._set_task_state(
                            session,
                            task,
                            "blocking_gate.opened",
                            reason=WaitingReason.EXECUTION_UNCERTAIN,
                        )

        self.executor.run(_apply)

    # ------------------------------------------------------------------
    # Read helpers for tests/CLI
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> dict[str, Any]:
        def _read(session: Session) -> dict[str, Any]:
            task = self._get_task(session, task_id)
            return {
                "task_id": task.task_id,
                "state": task.state,
                "state_reason": task.state_reason,
                "contract_version": task.contract_version,
                "plan_version": task.plan_version,
                "state_revision": task.state_revision,
                "revoke_epoch": task.revoke_epoch,
                "model_calls_used": task.model_calls_used,
                "model_call_limit": task.model_call_limit,
            }

        return self.executor.run(_read)

    def list_runs(self, task_id: str) -> list[dict[str, Any]]:
        def _read(session: Session) -> list[dict[str, Any]]:
            rows = session.scalars(
                select(AgentRunRow).where(AgentRunRow.task_id == task_id)
            ).all()
            return [
                {
                    "run_id": r.run_id,
                    "status": r.status,
                    "assignment_kind": r.assignment_kind,
                    "work_unit_id": r.work_unit_id,
                    "profile_version": r.profile_version,
                    "fencing_epoch": r.fencing_epoch,
                    "late_arrival": r.late_arrival,
                    "result_json": r.result_json,
                }
                for r in rows
            ]

        return self.executor.run(_read)

    def list_events(self, task_id: str) -> list[dict[str, Any]]:
        def _read(session: Session) -> list[dict[str, Any]]:
            rows = session.scalars(
                select(EventRow)
                .where(EventRow.task_id == task_id)
                .order_by(EventRow.sequence_no)
            ).all()
            return [
                {
                    "sequence_no": e.sequence_no,
                    "event_type": e.event_type,
                    "payload": json.loads(e.payload_json),
                }
                for e in rows
            ]

        return self.executor.run(_read)

    def get_decision(self, decision_id: str) -> dict[str, Any]:
        def _read(session: Session) -> dict[str, Any]:
            d = session.get(DecisionRow, decision_id)
            if d is None:
                raise NotFoundError("decision not found")
            return {
                "decision_id": d.decision_id,
                "status": d.status,
                "choice": d.choice,
                "target_version": d.target_version,
                "target_hash": d.target_hash,
                "decision_kind": d.decision_kind,
                "task_id": d.task_id,
                "gate_lifecycle": d.gate_lifecycle,
            }

        return self.executor.run(_read)

    def get_side_effect(self, effect_id: str) -> dict[str, Any]:
        def _read(session: Session) -> dict[str, Any]:
            e = session.get(SideEffectRow, effect_id)
            if e is None:
                raise NotFoundError("effect not found")
            return {
                "effect_id": e.effect_id,
                "state": e.state,
                "logical_action_key": e.logical_action_key,
                "action_digest": e.action_digest,
                "dispatch_attempts": e.dispatch_attempts,
                "external_idempotency_key": e.external_idempotency_key,
            }

        return self.executor.run(_read)

    def get_workspace(self, workspace_id: str) -> dict[str, Any]:
        def _read(session: Session) -> dict[str, Any]:
            ws = session.get(WorkspaceRow, workspace_id)
            if ws is None:
                raise NotFoundError("workspace not found")
            return {
                "workspace_id": ws.workspace_id,
                "state": ws.state,
                "owner_run_id": ws.owner_run_id,
                "writer_alive": ws.writer_alive,
                "fencing_epoch": ws.fencing_epoch,
            }

        return self.executor.run(_read)

    def get_work_unit(self, work_unit_id: str) -> dict[str, Any]:
        def _read(session: Session) -> dict[str, Any]:
            wu = session.get(WorkUnitExecutionRow, work_unit_id)
            if wu is None:
                raise NotFoundError("work unit not found")
            return {
                "work_unit_id": wu.work_unit_id,
                "status": wu.status,
                "active_run_id": wu.active_run_id,
                "selected_verdict": wu.selected_verdict,
                "verified_artifact_hash": wu.verified_artifact_hash,
                "attempt_count": wu.attempt_count,
                "blocked_reason": wu.blocked_reason,
            }

        return self.executor.run(_read)

    def count_outbox(self, *, status: str | None = None, task_id: str | None = None) -> int:
        def _read(session: Session) -> int:
            q = select(func.count()).select_from(OutboxRow)
            if status:
                q = q.where(OutboxRow.status == status)
            if task_id:
                q = q.where(OutboxRow.task_id == task_id)
            return int(session.scalar(q) or 0)

        return self.executor.run(_read)

    def count_inbox(self) -> int:
        def _read(session: Session) -> int:
            return int(session.scalar(select(func.count()).select_from(InboxRow)) or 0)

        return self.executor.run(_read)
