"""Tool Broker: decide and record every tool request before execution (M1 Task C).

The broker is the execution boundary the Core never crosses itself.  Every request
is validated against the frozen :class:`~hibiki.persistence.models.RunInputRow`
(``grant_epoch``, granted tools, workspace) and the run's current lifecycle, then
written to ``tool_invocations`` **before** any filesystem effect.  Refusals are rows
too, so a denied request is auditable (SPEC §13.1/§13.2, INV-09).

The workspace is always taken from the Run's recorded ``workspace_path``; a caller
cannot name a different root.  Filesystem I/O happens outside the DB transaction,
through :class:`~hibiki.tools.paths.WorkspacePaths`, which refuses traversal and
symlinks at the kernel level.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from hibiki.domain.enums import ActorType, AgentRunStatus
from hibiki.domain.errors import AuthorizationError, ConflictError, NotFoundError
from hibiki.domain.execution import M1_TOOL_CATALOG
from hibiki.domain.hashing import canonical_json, content_hash
from hibiki.domain.types import AuthContext
from hibiki.persistence.models import (
    AgentRunRow,
    GateRow,
    RunInputRow,
    TaskRow,
    ToolInvocationRow,
)
from hibiki.runtime.clock import new_id
from hibiki.tools.paths import PathSafetyError, WorkspacePaths

#: Task control states in which no tool may run.
_FROZEN_TASK_STATES = ("WAITING_HUMAN", "PAUSING", "PAUSED", "CANCELLING")

#: Gate lifecycles that still block execution (same rule as the Core).
_BLOCKING_GATE_LIFECYCLES = ("OPEN", "APPROVED_PENDING_APPLY")

#: Default cap on a single ``fs.read`` payload.
DEFAULT_MAX_READ_BYTES = 256 * 1024


@dataclass(frozen=True)
class ToolRequest:
    run_id: str
    task_id: str
    work_unit_id: str | None
    tool_name: str
    parameters: dict
    grant_epoch: int
    fencing_epoch: int
    sequence_no: int


@dataclass(frozen=True)
class ToolDecision:
    allowed: bool
    reason: str | None = None
    #: Set on an ALLOW so the execute_* helpers can finish the same audited row.
    invocation_id: str | None = None


class ToolBroker:
    """Authorize, record and (for filesystem tools) perform worker tool requests."""

    def __init__(self, executor: Any, clock: Any, *, workspace_root: str | None = None) -> None:
        self.executor = executor
        self.clock = clock
        self.workspace_root = workspace_root
        self.max_read_bytes = DEFAULT_MAX_READ_BYTES

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------

    def authorize(self, auth: AuthContext, request: ToolRequest) -> ToolDecision:
        """Validate, claim the invocation atomically, and record the decision."""
        return self.executor.run(lambda session: self._authorize_in_tx(session, auth, request))

    def _authorize_in_tx(
        self, session: Session, auth: AuthContext, request: ToolRequest
    ) -> ToolDecision:
        run = session.get(AgentRunRow, request.run_id)
        if run is None:
            # Nothing valid to bind the audit row to; there is no run/task to record
            # against, so this one refusal is returned without a row.
            return ToolDecision(False, "run_not_found")
        run_input = session.get(RunInputRow, request.run_id)
        if run_input is None:
            return self._deny(session, run, request, "run_input_unknown")
        task = session.get(TaskRow, run.task_id)
        if task is None:
            return self._deny(session, run, request, "run_not_found")

        # 1/2. Exact Core binding rules for a run-bound Internal credential.
        if self._binding_error(auth, run, task, request) is not None:
            return self._deny(session, run, request, "authorization_denied")
        if run.status != AgentRunStatus.RUNNING:
            return self._deny(session, run, request, "run_not_running")

        # 3. The request must carry the run's current epochs.
        if int(request.fencing_epoch) != int(run.fencing_epoch):
            return self._deny(session, run, request, "fencing_conflict")
        if int(request.grant_epoch) != int(run.grant_epoch):
            return self._deny(session, run, request, "grant_conflict")

        # 4. Task control state.
        if self._task_frozen(session, task):
            return self._deny(session, run, request, "task_frozen")

        # 5. Closed catalog ∩ frozen grant.
        granted = json.loads(run_input.granted_tools_json or "[]")
        if request.tool_name not in M1_TOOL_CATALOG or request.tool_name not in granted:
            return self._deny(session, run, request, "tool_not_granted")

        if self._sequence_taken(session, run, request):
            return self._deny(session, run, request, "duplicate_sequence")

        invocation_id = new_id("tool")
        params_json, params_hash = self._parameters(request)
        session.add(
            ToolInvocationRow(
                invocation_id=invocation_id,
                task_id=run.task_id,
                run_id=run.run_id,
                work_unit_id=self._work_unit(request, run),
                sequence_no=request.sequence_no,
                tool_name=request.tool_name,
                parameters_json=params_json,
                parameters_hash=params_hash,
                decision="ALLOW",
                deny_reason=None,
                grant_epoch=int(request.grant_epoch),
                fencing_epoch=int(request.fencing_epoch),
                outcome=None,
                result_json=None,
                created_at=self.clock.now(),
                finished_at=None,
            )
        )
        return ToolDecision(True, None, invocation_id)

    def record_outcome(
        self,
        auth: AuthContext,
        invocation_id: str,
        *,
        outcome: str,
        result: dict | None = None,
    ) -> None:
        """Finish an invocation; never overwrite it with a different outcome."""
        if not isinstance(outcome, str) or not outcome:
            raise ValueError("outcome must be a non-empty string")

        def _tx(session: Session) -> None:
            row = session.get(ToolInvocationRow, invocation_id)
            if row is None:
                raise NotFoundError("tool invocation not found", code="invocation_not_found")
            run = session.get(AgentRunRow, row.run_id)
            if run is None:
                raise NotFoundError("run not found", code="run_not_found")
            task = session.get(TaskRow, run.task_id)
            if task is None:
                raise NotFoundError("task not found", code="task_not_found")
            # Re-check the binding: a stale invocation from another run must never
            # be finished by this credential.
            if self._binding_error(auth, run, task, None) is not None:
                raise AuthorizationError(
                    "authenticated runtime is not bound to this invocation's run",
                    code="authorization_denied",
                )
            if row.outcome is not None and row.outcome != outcome:
                raise ConflictError(
                    f"tool invocation {invocation_id} already finished with "
                    f"outcome {row.outcome!r}",
                    code="invocation_already_finished",
                )
            row.outcome = outcome
            if result is not None:
                row.result_json = canonical_json(result)
            if row.finished_at is None:
                row.finished_at = self.clock.now()

        self.executor.run(_tx)

    # ------------------------------------------------------------------
    # Filesystem tools
    # ------------------------------------------------------------------

    def execute_fs_read(self, auth: AuthContext, request: ToolRequest) -> dict:
        decision = self.authorize(auth, request)
        if not decision.allowed:
            return {"status": "denied", "reason": decision.reason}
        invocation_id = decision.invocation_id
        try:
            params = self._params(request)
            relative = self._require_str(params, "path")
            limit = int(params.get("max_bytes", self.max_read_bytes))
            if limit <= 0:
                raise ValueError("max_bytes must be a positive integer")
            workspace = self._workspace_for(request.run_id)
            with WorkspacePaths(workspace) as paths:
                with paths.open_for_read(relative) as handle:
                    data = handle.read(limit + 1)
            truncated = len(data) > limit
            if truncated:
                data = data[:limit]
            result = {
                "status": "ok",
                "path": relative,
                "content": data.decode("utf-8", errors="replace"),
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
                "truncated": truncated,
            }
        except PathSafetyError as exc:
            return self._refuse(auth, invocation_id, "path_escape", str(exc))
        except Exception as exc:  # noqa: BLE001 - any I/O failure is a recorded error
            return self._fail(auth, invocation_id, exc)
        self.record_outcome(auth, invocation_id, outcome="ok", result=result)
        return result

    def execute_fs_list(self, auth: AuthContext, request: ToolRequest) -> dict:
        decision = self.authorize(auth, request)
        if not decision.allowed:
            return {"status": "denied", "reason": decision.reason}
        invocation_id = decision.invocation_id
        try:
            params = self._params(request)
            relative = self._require_str(params, "path")
            workspace = self._workspace_for(request.run_id)
            with WorkspacePaths(workspace) as paths:
                entries = paths.list_dir(relative)
            result = {"status": "ok", "path": relative, "entries": entries}
        except PathSafetyError as exc:
            return self._refuse(auth, invocation_id, "path_escape", str(exc))
        except Exception as exc:  # noqa: BLE001
            return self._fail(auth, invocation_id, exc)
        self.record_outcome(auth, invocation_id, outcome="ok", result=result)
        return result

    def execute_fs_write(self, auth: AuthContext, request: ToolRequest) -> dict:
        decision = self.authorize(auth, request)
        if not decision.allowed:
            return {"status": "denied", "reason": decision.reason}
        invocation_id = decision.invocation_id
        try:
            params = self._params(request)
            relative = self._require_str(params, "path")
            content = params.get("content")
            if not isinstance(content, str):
                raise ValueError("parameter 'content' must be a string")
            data = content.encode("utf-8")
            workspace = self._workspace_for(request.run_id)
            with WorkspacePaths(workspace) as paths:
                digest = paths.atomic_write(relative, data)
            result = {"status": "ok", "path": relative, "sha256": digest, "bytes": len(data)}
        except PathSafetyError as exc:
            return self._refuse(auth, invocation_id, "path_escape", str(exc))
        except Exception as exc:  # noqa: BLE001
            return self._fail(auth, invocation_id, exc)
        self.record_outcome(auth, invocation_id, outcome="ok", result=result)
        return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _deny(
        self, session: Session, run: AgentRunRow, request: ToolRequest, reason: str
    ) -> ToolDecision:
        params_json, params_hash = self._parameters(request)
        now = self.clock.now()
        invocation_id = new_id("tool")
        session.add(
            ToolInvocationRow(
                invocation_id=invocation_id,
                task_id=run.task_id,
                run_id=run.run_id,
                work_unit_id=self._work_unit(request, run),
                sequence_no=request.sequence_no,
                tool_name=request.tool_name,
                parameters_json=params_json,
                parameters_hash=params_hash,
                decision="DENY",
                deny_reason=reason,
                grant_epoch=int(request.grant_epoch),
                fencing_epoch=int(request.fencing_epoch),
                outcome="denied",
                result_json=None,
                created_at=now,
                finished_at=now,
            )
        )
        return ToolDecision(False, reason, invocation_id)

    def _refuse(
        self, auth: AuthContext, invocation_id: str | None, reason: str, detail: str
    ) -> dict:
        if invocation_id is not None:
            self.record_outcome(
                auth,
                invocation_id,
                outcome="denied",
                result={"reason": reason, "error": detail},
            )
        return {"status": "denied", "reason": reason}

    def _fail(self, auth: AuthContext, invocation_id: str | None, exc: Exception) -> dict:
        if invocation_id is not None:
            self.record_outcome(
                auth,
                invocation_id,
                outcome="error",
                result={"error": str(exc), "error_type": type(exc).__name__},
            )
        return {"status": "error", "error": str(exc)}

    def _binding_error(
        self,
        auth: AuthContext,
        run: AgentRunRow,
        task: TaskRow,
        request: ToolRequest | None,
    ) -> str | None:
        """Return a denial code when the credential is not this Run's exact binding."""
        if auth.actor_type != ActorType.INTERNAL:
            return "authorization_denied"
        if auth.principal_id != task.principal_id:
            return "authorization_denied"
        if request is not None and request.task_id and request.task_id != run.task_id:
            return "authorization_denied"
        if request is not None and run.work_unit_id is not None:
            if request.work_unit_id not in (None, run.work_unit_id):
                return "authorization_denied"
        if (
            auth.bound_task_id != run.task_id
            or auth.bound_run_id != run.run_id
            or auth.bound_fencing_epoch != int(run.fencing_epoch)
            or auth.bound_grant_epoch != int(run.grant_epoch)
            or auth.actor_id != run.agent_instance_id
        ):
            return "authorization_denied"
        return None

    def _task_frozen(self, session: Session, task: TaskRow) -> bool:
        if str(task.state) in _FROZEN_TASK_STATES:
            return True
        if task.cancel_intent or task.pause_intent:
            return True
        gate = session.scalars(
            select(GateRow).where(
                GateRow.task_id == task.task_id,
                GateRow.lifecycle.in_(_BLOCKING_GATE_LIFECYCLES),
            )
        ).first()
        return gate is not None

    @staticmethod
    def _sequence_taken(session: Session, run: AgentRunRow, request: ToolRequest) -> bool:
        existing = session.scalars(
            select(ToolInvocationRow).where(
                ToolInvocationRow.task_id == run.task_id,
                ToolInvocationRow.run_id == run.run_id,
                ToolInvocationRow.sequence_no == request.sequence_no,
            )
        ).first()
        return existing is not None

    @staticmethod
    def _work_unit(request: ToolRequest, run: AgentRunRow) -> str | None:
        return request.work_unit_id if request.work_unit_id is not None else run.work_unit_id

    @staticmethod
    def _parameters(request: ToolRequest) -> tuple[str, str]:
        params = request.parameters if isinstance(request.parameters, dict) else {}
        return canonical_json(params), content_hash(params)

    @staticmethod
    def _params(request: ToolRequest) -> dict:
        if not isinstance(request.parameters, dict):
            raise TypeError("tool parameters must be a mapping")
        return request.parameters

    @staticmethod
    def _require_str(params: dict, key: str) -> str:
        value = params.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"parameter {key!r} must be a non-empty string")
        return value

    def _workspace_for(self, run_id: str) -> str:
        """The Run's recorded workspace; a caller-supplied path is never accepted."""

        def _read(session: Session) -> str | None:
            row = session.get(RunInputRow, run_id)
            return row.workspace_path if row is not None else None

        recorded = self.executor.run(_read)
        if not recorded:
            raise FileNotFoundError(f"run {run_id} has no recorded workspace path")
        resolved = os.path.realpath(recorded)
        if self.workspace_root is not None:
            root = os.path.realpath(self.workspace_root)
            if os.path.commonpath([root, resolved]) != root:
                raise PathSafetyError(
                    f"run workspace {recorded!r} is outside the configured workspace root"
                )
        return resolved
