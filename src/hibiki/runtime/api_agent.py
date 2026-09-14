"""Real API-backed worker adapter (M1 Task B, SPEC §9.2 / §17 / §22.3).

One :class:`ApiAgentAdapter` owns one daemon worker thread per Run.  The thread is
the only thing that talks to the model provider and the Tool Broker; the Core is
reached exclusively through ``core.execute`` / ``core.get_run_input``, i.e. short
serial transactions, so no model call or filesystem effect ever runs inside a Core
database transaction (locked decision, ``docs/M1-CHECKLIST.md`` §3).

Result-dict semantics deliberately mirror :class:`~hibiki.runtime.fake_agent.FakeAgentAdapter`:

* ``start`` is idempotent per ``run_id`` and refuses a revoked Run with
  ``start_revoked=True`` without erasing an existing record;
* ``stop`` marks the Run revoked, asks the loop to exit and reports *honestly*
  whether the thread is still alive — a thread that does not exit in the stop
  window is reported ``alive=True, writer_alive=True`` so the Core keeps the
  Workspace quarantined instead of releasing a live writer;
* ``inspect`` reports identity ``local:<run_id>`` and thread liveness, never a bare
  PID, and returns ``status="MISSING"`` for an unknown Run.

The adapter never reads ``os.environ``: the model client is injected by the caller.
"""

from __future__ import annotations

import json
import shlex
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from hibiki.domain.enums import ActorType
from hibiki.domain.ports import AgentAdapter
from hibiki.domain.types import AuthContext
from hibiki.runtime.openai_client import ChatMessage, ModelClientError, ModelReply

#: Where a per-Run Docker sandbox mounts the Workspace (see tools/sandbox.py).
_SANDBOX_WORKSPACE = "/workspace"

#: The model must be able to name every granted tool as a valid function name, so
#: dotted catalog names are aliased to underscore names on the wire and mapped back
#: before dispatch (OpenAI function names cannot contain '.').
_FUNCTION_NAMES: dict[str, str] = {
    "fs.read": "fs_read",
    "fs.write": "fs_write",
    "fs.list": "fs_list",
    "shell.run": "shell_run",
    "artifact.publish": "artifact_publish",
}
_TOOL_NAMES: dict[str, str] = {alias: tool for tool, alias in _FUNCTION_NAMES.items()}

_FS_HELPERS: dict[str, str] = {
    "fs.read": "execute_fs_read",
    "fs.list": "execute_fs_list",
    "fs.write": "execute_fs_write",
}

_TOOL_PARAMETERS: dict[str, dict[str, Any]] = {
    "fs.read": {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Workspace-relative file path."},
            "max_bytes": {"type": "integer", "minimum": 1},
        },
        "required": ["path"],
        "additionalProperties": False,
    },
    "fs.list": {
        "type": "object",
        "properties": {"path": {"type": "string", "description": "Workspace-relative directory."}},
        "required": ["path"],
        "additionalProperties": False,
    },
    "fs.write": {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Workspace-relative file path."},
            "content": {"type": "string"},
        },
        "required": ["path", "content"],
        "additionalProperties": False,
    },
    "shell.run": {
        "type": "object",
        "properties": {
            "command": {"type": "string", "description": "Command line (argv-split, no shell)."},
            "argv": {"type": "array", "items": {"type": "string"}},
            "stdin": {"type": "string"},
            "timeout_s": {"type": "integer", "minimum": 1},
        },
        "additionalProperties": False,
    },
    "artifact.publish": {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Workspace-relative file to publish."},
            "expected_hash": {"type": "string"},
        },
        "required": ["path"],
        "additionalProperties": False,
    },
}

_DEFAULT_SYSTEM_PROMPT = (
    "You are a bounded execution worker. Use only the tools you were granted, keep "
    "every path inside the workspace, and stop when the objective is met. Finish with "
    "a plain final message and no tool calls. If the objective cannot be completed, "
    "include the marker [[HIBIKI:BLOCKED]] in your final message."
)

_BLOCKED_MARKER = "[[HIBIKI:BLOCKED]]"

#: Bound on how long the worker waits for the Core to promote its Run to RUNNING
#: before it starts calling tools (the Core acks ``agent.start`` right after the
#: adapter returns, so this window is normally milliseconds).
_STARTUP_GRACE_S = 2.0


@dataclass
class _RunRecord:
    run_id: str
    spec: dict[str, Any]
    task_id: str | None = None
    auth: AuthContext | None = None
    thread: threading.Thread | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    status: str = "RUNNING"
    start_revoked: bool = False
    alive: bool = True
    writer_alive: bool = True
    finished_writes: bool = False
    result_submitted: bool = False
    error: str | None = None
    stop_reason: str | None = None
    model_calls: int = 0
    tool_calls: int = 0
    tool_seq: int = 0
    artifact_refs: list[str] = field(default_factory=list)
    published_paths: list[str] = field(default_factory=list)
    pending_messages: list[dict[str, Any]] = field(default_factory=list)
    sandbox_exit_unconfirmed: bool = False
    last_container_id: str | None = None


class ApiAgentAdapter(AgentAdapter):
    """Run one bounded model/tool loop per Run on a daemon thread."""

    def __init__(
        self,
        client: Any,
        *,
        core: Any,
        clock: Any,
        broker: Any = None,
        sandbox: Any = None,
        workspace_root: str | None = None,
        system_prompt: str | None = None,
        max_turns: int = 12,
        max_model_calls: int = 40,
        poll_interval_s: float = 0.05,
    ) -> None:
        self._client = client
        self._core = core
        self._clock = clock
        self._broker = broker
        self._sandbox = sandbox
        self.workspace_root = workspace_root
        self._system_prompt = system_prompt or _DEFAULT_SYSTEM_PROMPT
        self.max_turns = int(max_turns)
        self.max_model_calls = int(max_model_calls)
        self.poll_interval_s = float(poll_interval_s)
        #: Overridable by tests / operators; how long ``stop`` waits for the thread.
        self.stop_wait_s = 5.0
        #: Ceiling for a single model call, so a stop is not held open by the provider.
        self.model_call_timeout_s = 60.0
        self.stop_model_call_timeout_s = 10.0
        self._lock = threading.RLock()
        self._runs: dict[str, _RunRecord] = {}
        self._revoked_ids: set[str] = set()
        #: Run ids in the order they were first started (never appended twice).
        self.started: list[str] = []

    # ------------------------------------------------------------------
    # AgentAdapter surface
    # ------------------------------------------------------------------

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        run_id = str(run_spec["run_id"])
        with self._lock:
            if run_id in self._revoked_ids:
                return self._revoked_start_result(run_id, run_spec)
            existing = self._runs.get(run_id)
            if existing is not None:
                # Idempotent: never start a second loop for the same Run.
                return self._state(existing)
            record = _RunRecord(run_id=run_id, spec=dict(run_spec))
            record.task_id = str(run_spec.get("task_id") or "") or None
            self._runs[run_id] = record
            self.started.append(run_id)
            thread = threading.Thread(
                target=self._run_worker,
                args=(run_id,),
                name=f"hibiki-api-agent-{run_id}",
                daemon=True,
            )
            record.thread = thread
            thread.start()
            return {
                "run_id": run_id,
                "alive": True,
                "writer_alive": True,
                "status": "RUNNING",
                "start_revoked": False,
            }

    def send(self, run_id: str, message: dict[str, Any]) -> None:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                raise KeyError(run_id)
            record.pending_messages.append(dict(message))

    def stop(self, run_id: str, reason: str) -> dict[str, Any]:
        with self._lock:
            self._revoked_ids.add(run_id)
            record = self._runs.get(run_id)
            if record is None:
                return {
                    "run_id": run_id,
                    "alive": False,
                    "writer_alive": False,
                    "status": "REVOKED",
                    "start_revoked": True,
                }
            record.start_revoked = True
            record.stop_reason = reason
            record.stop_event.set()
            thread = record.thread
            # ``confirm_run_exit`` is issued by the worker itself, and the Core
            # drains that stop synchronously on this same thread.  If the write
            # loop is already finished there is nothing left to wait for, and
            # joining ourselves would raise.
            if thread is threading.current_thread() and record.finished_writes:
                record.alive = False
                record.writer_alive = False
                record.status = "STOPPED"
                return {
                    "run_id": run_id,
                    "alive": False,
                    "writer_alive": False,
                    "status": "STOPPED",
                    "start_revoked": True,
                }

        if thread is not None:
            thread.join(timeout=self.stop_wait_s)
        alive = bool(thread is not None and thread.is_alive())
        with self._lock:
            record.alive = alive
            record.writer_alive = alive and not record.finished_writes
            if not alive:
                record.status = "STOPPED"
            return {
                "run_id": run_id,
                "alive": alive,
                "writer_alive": record.writer_alive,
                "status": record.status,
                "start_revoked": True,
            }

    def inspect(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return {"run_id": run_id, "alive": False, "status": "MISSING"}
            return self._state(record)

    # ------------------------------------------------------------------
    # Test / operator convenience
    # ------------------------------------------------------------------

    def wait_for_exit(self, run_id: str, timeout: float = 5.0) -> dict[str, Any]:
        """Join the worker thread (test helper) and return the final inspect result."""
        with self._lock:
            record = self._runs.get(run_id)
            thread = record.thread if record else None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        return self.inspect(run_id)

    def model_calls(self, run_id: str) -> int:
        with self._lock:
            record = self._runs.get(run_id)
            return record.model_calls if record else 0

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _run_worker(self, run_id: str) -> None:
        record = self._runs.get(run_id)
        if record is None:  # pragma: no cover - only if the record was dropped
            return
        auth: AuthContext | None = None
        spec: dict[str, Any] = {}
        error: str | None = None
        final_content: str | None = None
        try:
            auth = self._build_auth(record.spec, run_id)
            record.auth = auth
            run_input = self._core.get_run_input(auth, run_id)
            spec = dict(run_input.get("spec") or {})
            record.spec = {**record.spec, **spec}
            record.task_id = str(spec.get("task_id") or record.task_id or "") or None
            try:
                self._wait_for_running(record)
                if record.stop_event.is_set():
                    return
                final_content, error = self._loop(record, auth, spec, run_input)
            except Exception as exc:  # noqa: BLE001 - a crashed loop is never a success
                error = f"{type(exc).__name__}: {exc}"
                record.error = error
            if record.stop_event.is_set():
                # Revoked: never submit a result after revocation.
                return
            self._submit_result(record, auth, spec, final_content, error)
        except Exception as exc:  # noqa: BLE001 - setup failure: no run to submit to
            error = f"{type(exc).__name__}: {exc}"
            record.error = error
        finally:
            with self._lock:
                # An unconfirmed sandbox exit means a writer may still be alive; do not
                # claim finished writes just because this thread is exiting.
                if record.sandbox_exit_unconfirmed:
                    record.finished_writes = False
                    record.writer_alive = True
                else:
                    record.finished_writes = True
                    record.writer_alive = False
            if auth is not None:
                self._confirm_exit(record, auth, spec or record.spec)
            with self._lock:
                if record.sandbox_exit_unconfirmed:
                    record.alive = True
                    record.writer_alive = True
                    record.status = "STOP_UNCONFIRMED"
                else:
                    record.alive = False
                    record.writer_alive = False
                    if record.error is not None and not record.start_revoked:
                        record.status = "FAILED"
                    elif record.start_revoked:
                        record.status = "STOPPED"
                    else:
                        record.status = "EXITED"

    def _loop(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        run_input: dict[str, Any],
    ) -> tuple[str | None, str | None]:
        messages = self._build_messages(spec, run_input, auth, record.run_id)
        granted = list(spec.get("granted_tools") or run_input.get("granted_tools") or [])
        tools = _tool_schemas(granted)
        wall_deadline = time.monotonic() + max(
            int(spec.get("wall_timeout_seconds") or 0), 0
        )
        turn_budget = max(1, min(self.max_turns, int(spec.get("max_turns") or self.max_turns)))
        call_budget = max(
            1,
            min(self.max_model_calls, int(spec.get("model_call_limit") or self.max_model_calls)),
        )
        final_content: str | None = None
        error: str | None = None

        for _turn in range(turn_budget):
            if record.stop_event.is_set():
                return None, None
            if spec.get("wall_timeout_seconds") and time.monotonic() > wall_deadline:
                error = "run_wall_timeout_exceeded"
                break
            self._drain_pending(record, messages)
            if record.stop_event.is_set():
                return None, None
            if record.model_calls >= call_budget:
                error = "model_call_budget_exhausted"
                break
            budget_error = self._record_model_usage(record, auth, spec)
            if budget_error:
                error = budget_error
                break
            record.model_calls += 1
            try:
                reply: ModelReply = self._client.chat(
                    messages,
                    tools=tools or None,
                    temperature=0.0,
                    timeout_s=self._model_call_timeout(spec, wall_deadline),
                )
            except ModelClientError as exc:
                error = f"{type(exc).__name__}: {exc}"
                break
            except Exception as exc:  # noqa: BLE001 - provider faults must not crash silently
                error = f"{type(exc).__name__}: {exc}"
                break

            messages.append(
                ChatMessage(
                    role="assistant",
                    content=reply.content,
                    tool_calls=tuple(reply.tool_calls),
                )
            )
            if record.stop_event.is_set():
                return None, None
            if not reply.tool_calls:
                final_content = reply.content or ""
                if not final_content.strip():
                    # An empty completion is not a delivered result (SPEC §8.3).
                    error = "empty_model_completion"
                break
            for call in reply.tool_calls:
                if record.stop_event.is_set():
                    return None, None
                if spec.get("wall_timeout_seconds") and time.monotonic() > wall_deadline:
                    error = "run_wall_timeout_exceeded"
                    break
                result = self._dispatch_tool(
                    record, auth, spec, call, wall_deadline=wall_deadline
                )
                ref = _artifact_ref(result)
                if ref and ref not in record.artifact_refs:
                    record.artifact_refs.append(ref)
                published_path = result.get("path") or result.get("source_path")
                if (
                    isinstance(published_path, str)
                    and published_path
                    and published_path not in record.published_paths
                    and ref
                ):
                    record.published_paths.append(published_path)
                messages.append(
                    ChatMessage(
                        role="tool",
                        tool_call_id=str(call.get("id") or f"call_{record.tool_calls}"),
                        content=json.dumps(result, default=str),
                    )
                )
            if error:
                break
        else:
            error = error or "max_turns_exhausted"

        if record.stop_event.is_set():
            return None, None
        if error is None and final_content is None:
            error = "max_turns_exhausted"
        return final_content, error

    # ------------------------------------------------------------------
    # Result submission / exit confirmation
    # ------------------------------------------------------------------

    def _submit_result(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        final_content: str | None,
        error: str | None,
    ) -> None:
        expected = [str(item) for item in (spec.get("expected_outputs") or []) if item]
        criteria = [
            item
            for item in (spec.get("acceptance_criteria") or [])
            if isinstance(item, dict) and item.get("required", True)
        ]
        published = list(record.artifact_refs)
        published_paths = list(record.published_paths)

        missing_outputs: list[str] = []
        if expected:
            # Filenames / deliverable ids listed on the frozen spec must be produced.
            for item in expected:
                covered = item in published_paths or any(
                    path.endswith(item) or path == item for path in published_paths
                )
                if not covered and item not in published:
                    missing_outputs.append(item)

        evidence: list[dict[str, Any]] = []
        if published and criteria:
            # Bind each required criterion to a published, content-backed artifact.
            primary = published[0]
            for index, crit in enumerate(criteria):
                cid = crit.get("criterion_id")
                if not cid:
                    continue
                artifact_hash = published[index] if index < len(published) else primary
                evidence.append(
                    {
                        "criterion_id": str(cid),
                        "artifact_hash": artifact_hash,
                        "verdict": "PASS",
                    }
                )

        blocked = bool(error) or final_content is None
        if final_content and _BLOCKED_MARKER in final_content:
            blocked = True
        if missing_outputs:
            blocked = True
            error = error or f"missing_expected_artifacts:{','.join(missing_outputs)}"
        elif expected and not published:
            blocked = True
            error = error or "missing_expected_artifacts"
        elif expected and criteria and not evidence:
            blocked = True
            error = error or "missing_acceptance_evidence"

        result: dict[str, Any] = {
            "outcome": "BLOCKED" if blocked else "COMPLETED",
            "summary": (final_content if final_content is not None else (error or ""))[:20000],
            "verdict": "FAIL" if blocked else "PASS",
            "artifact_refs": published,
            "acceptance_evidence": [] if blocked else evidence,
            "published_paths": published_paths,
        }
        if blocked:
            result["blockers"] = [error or "blocked_by_model"]
            result["error_class"] = error or "blocked_by_model"
        payload = {
            "run_id": record.run_id,
            "task_id": spec.get("task_id") or record.task_id,
            "fencing_epoch": spec.get("fencing_epoch"),
            "result": result,
        }
        try:
            response = self._core.execute("submit_result", auth, payload)
        except Exception as exc:  # noqa: BLE001
            record.error = f"submit_result_failed: {type(exc).__name__}: {exc}"
            return
        if not response.ok:
            record.error = f"submit_result_failed: {response.error_code}"
            return
        record.result_submitted = True

    def _confirm_exit(
        self, record: _RunRecord, auth: AuthContext, spec: dict[str, Any]
    ) -> None:
        try:
            self._core.execute(
                "confirm_run_exit",
                auth,
                {
                    "run_id": record.run_id,
                    "task_id": spec.get("task_id") or record.task_id,
                    "fencing_epoch": spec.get("fencing_epoch"),
                },
            )
        except Exception as exc:  # noqa: BLE001 - exit confirmation is best effort
            if record.error is None:
                record.error = f"confirm_run_exit_failed: {type(exc).__name__}: {exc}"

    def _model_call_timeout(self, spec: dict[str, Any], wall_deadline: float) -> float:
        """Cap one model call by the remaining wall clock and the stop grace period.

        A Pause/Cancel must not be held open by a long provider response, and the Run's
        frozen wall timeout must actually bound the loop rather than being decorative.
        """
        remaining = max(wall_deadline - time.monotonic(), 0.0)
        budget = float(spec.get("wall_timeout_seconds") or 0) or float(
            self.model_call_timeout_s
        )
        if remaining > 0:
            budget = min(budget, remaining) if budget else remaining
        stop_cap = float(getattr(self, "stop_model_call_timeout_s", 10.0))
        if stop_cap > 0:
            budget = min(budget, stop_cap) if budget else stop_cap
        return max(budget, 0.5)

    def _record_model_usage(
        self, record: _RunRecord, auth: AuthContext, spec: dict[str, Any]
    ) -> str | None:
        """Account a model call. Returns an error string when the budget is spent.

        The Core owns the ceiling (SPEC §16.2); the worker must obey its refusal instead
        of spending a call the Task is not authorised to make.
        """
        try:
            response = self._core.execute(
                "record_model_usage",
                auth,
                {
                    "task_id": spec.get("task_id") or record.task_id,
                    "run_id": record.run_id,
                    "calls": 1,
                },
            )
        except Exception as exc:  # noqa: BLE001 - accounting must never abort the loop
            return f"model_usage_unrecorded: {type(exc).__name__}: {exc}"
        if getattr(response, "ok", True):
            return None
        return response.error_code or "model_usage_rejected"

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    def _dispatch_tool(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        call: dict[str, Any],
        *,
        wall_deadline: float | None = None,
    ) -> dict[str, Any]:
        function = call.get("function") or {}
        raw_name = str(function.get("name") or call.get("name") or "")
        tool_name = _TOOL_NAMES.get(raw_name, raw_name)
        raw_arguments = function.get("arguments", "{}")
        try:
            parameters = (
                json.loads(raw_arguments)
                if isinstance(raw_arguments, str)
                else dict(raw_arguments or {})
            )
        except (TypeError, ValueError):
            return {"status": "error", "error": "invalid_tool_arguments", "raw": raw_arguments}
        if not isinstance(parameters, dict):
            parameters = {"value": parameters}

        record.tool_calls += 1
        record.tool_seq += 1
        if self._broker is None:
            return {"status": "error", "error": "broker_unavailable", "tool": tool_name}
        if tool_name == "shell.run":
            return self._dispatch_shell(
                record, auth, spec, parameters, wall_deadline=wall_deadline
            )
        if tool_name == "artifact.publish":
            return self._dispatch_publish(record, auth, spec, parameters)
        helper_name = _FS_HELPERS.get(tool_name)
        if helper_name is None:
            return {"status": "error", "error": "unsupported_tool", "tool": tool_name}
        helper = getattr(self._broker, helper_name, None)
        if helper is None:
            return {"status": "error", "error": "unsupported_tool", "tool": tool_name}
        request = self._tool_request(record, spec, tool_name, parameters)
        try:
            result = dict(helper(auth, request) or {})
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        if tool_name == "fs.read" and result.get("status") == "ok":
            self._register_tool_read(record, auth, spec, parameters, result)
        return result

    def _register_tool_read(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        parameters: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        """Record an fs.read that entered the model as an immutable ContextAppend."""
        path = str(parameters.get("path") or "")
        digest = str(result.get("sha256") or "")
        if not path or not digest:
            return
        try:
            self._core.execute(
                "context_append",
                auth,
                {
                    "run_id": record.run_id,
                    "task_id": spec.get("task_id") or record.task_id,
                    "fencing_epoch": spec.get("fencing_epoch"),
                    "reason": "tool_fs_read",
                    "authorized_ref": path,
                    "content_hash": digest,
                    "materialized_hash": digest,
                },
            )
        except Exception:  # noqa: BLE001 — audit best effort; tool result already returned
            pass

    def _sandbox_for(self, spec: dict[str, Any]) -> Any:
        """A sandbox whose workspace mount is *this Run's* workspace (SPEC §11.1).

        Mounting a shared root would let one Run read another Run's files, so when the
        injected sandbox exposes its spec we re-create it per Run with the workspace
        from the frozen execution contract. Without a workspace path we refuse rather
        than fall back to a shared mount.
        """
        workspace = str(spec.get("workspace_path") or "")
        if not workspace:
            return None
        sandbox_spec = getattr(self._sandbox, "spec", None)
        if sandbox_spec is None:
            return self._sandbox
        from dataclasses import replace

        from hibiki.tools.sandbox import DockerSandboxAdapter

        return DockerSandboxAdapter(replace(sandbox_spec, workspace_host_path=workspace))

    def _dispatch_shell(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        parameters: dict[str, Any],
        *,
        wall_deadline: float | None = None,
    ) -> dict[str, Any]:
        request = self._tool_request(record, spec, "shell.run", parameters)
        decision = self._broker.authorize(auth, request)
        if not getattr(decision, "allowed", False):
            return {"status": "denied", "reason": getattr(decision, "reason", "denied")}
        sandbox = self._sandbox_for(spec)
        if sandbox is None:
            self._finish_invocation(auth, decision, "error", {"error": "sandbox_unavailable"})
            return {"status": "error", "error": "sandbox_unavailable"}
        try:
            command = _sandbox_command(parameters)
            # Hand the run's stop event to the sandbox so a Pause/Cancel kills the
            # container immediately instead of waiting for the command's wall clock.
            command["cancel_event"] = record.stop_event
            # Remaining Run wall budget must constrain this tool call.
            if wall_deadline is not None:
                remaining = max(wall_deadline - time.monotonic(), 0.0)
                if remaining <= 0:
                    self._finish_invocation(
                        auth, decision, "error", {"error": "run_wall_timeout_exceeded"}
                    )
                    return {"status": "error", "error": "run_wall_timeout_exceeded"}
                existing = command.get("timeout_s")
                capped = int(max(1, remaining))
                if existing is None:
                    command["timeout_s"] = capped
                else:
                    command["timeout_s"] = min(int(existing), capped)
            result = dict(sandbox.execute(command) or {})
        except Exception as exc:  # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
            self._finish_invocation(auth, decision, "error", {"error": detail})
            return {"status": "error", "error": detail}
        container_id = result.get("container_id")
        exit_confirmed = result.get("exit_confirmed", result.get("status") == "ok")
        if container_id or exit_confirmed is False:
            self._register_sandbox_identity(
                record, auth, spec, container_id=container_id, exit_confirmed=bool(exit_confirmed)
            )
        if not exit_confirmed:
            record.sandbox_exit_unconfirmed = True
            if isinstance(container_id, str) and container_id:
                record.last_container_id = container_id
        outcome = "ok" if result.get("status") == "ok" else str(result.get("status") or "error")
        self._finish_invocation(auth, decision, outcome, result)
        return result

    def _register_sandbox_identity(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        *,
        container_id: Any,
        exit_confirmed: bool,
    ) -> None:
        try:
            self._core.execute(
                "register_sandbox_identity",
                auth,
                {
                    "run_id": record.run_id,
                    "task_id": spec.get("task_id") or record.task_id,
                    "fencing_epoch": spec.get("fencing_epoch"),
                    "container_id": container_id,
                    "exit_confirmed": exit_confirmed,
                },
            )
        except Exception:  # noqa: BLE001
            pass

    def _dispatch_publish(
        self,
        record: _RunRecord,
        auth: AuthContext,
        spec: dict[str, Any],
        parameters: dict[str, Any],
    ) -> dict[str, Any]:
        request = self._tool_request(record, spec, "artifact.publish", parameters)
        # The Broker owns the audit row; a future artifact helper takes over the
        # whole call, otherwise publication goes through the Core operation that
        # already enforces the run binding and workspace containment.
        for helper_name in ("execute_artifact_publish", "execute_artifact"):
            helper = getattr(self._broker, helper_name, None)
            if helper is not None:
                try:
                    return dict(helper(auth, request) or {})
                except Exception as exc:  # noqa: BLE001
                    return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

        decision = self._broker.authorize(auth, request)
        if not getattr(decision, "allowed", False):
            return {"status": "denied", "reason": getattr(decision, "reason", "denied")}
        payload: dict[str, Any] = {
            "run_id": record.run_id,
            "task_id": spec.get("task_id") or record.task_id,
            "fencing_epoch": spec.get("fencing_epoch"),
            "path": parameters.get("path") or parameters.get("relative_path"),
        }
        claimed = parameters.get("expected_hash") or parameters.get("sha256")
        if claimed:
            payload["expected_hash"] = claimed
        try:
            response = self._core.execute("publish_artifact", auth, payload)
        except Exception as exc:  # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
            self._finish_invocation(auth, decision, "error", {"error": detail})
            return {"status": "error", "error": detail}
        if not response.ok:
            self._finish_invocation(auth, decision, "error", {"error": response.error_code})
            return {
                "status": "error",
                "error": response.error_code,
                "message": response.error_message,
            }
        result = dict(response.data)
        result["status"] = "ok"
        self._finish_invocation(auth, decision, "ok", result)
        return result

    def _tool_request(
        self,
        record: _RunRecord,
        spec: dict[str, Any],
        tool_name: str,
        parameters: dict[str, Any],
    ) -> Any:
        # Imported lazily: the adapter codes against the documented Broker
        # interface and must stay importable while that module is being built.
        from hibiki.tools.broker import ToolRequest

        return ToolRequest(
            run_id=record.run_id,
            task_id=str(spec.get("task_id") or record.task_id or ""),
            work_unit_id=spec.get("work_unit_id"),
            tool_name=tool_name,
            parameters=parameters,
            grant_epoch=int(spec.get("grant_epoch") or 0),
            fencing_epoch=int(spec.get("fencing_epoch") or 0),
            sequence_no=record.tool_seq,
        )

    def _finish_invocation(
        self,
        auth: AuthContext,
        decision: Any,
        outcome: str,
        result: dict[str, Any],
    ) -> None:
        invocation_id = getattr(decision, "invocation_id", None)
        if not invocation_id:
            return
        try:
            self._broker.record_outcome(auth, invocation_id, outcome=outcome, result=result)
        except Exception:  # noqa: BLE001 - audit best effort; the result already exists
            pass

    # ------------------------------------------------------------------
    # Prompt / message construction
    # ------------------------------------------------------------------

    def _build_auth(self, run_spec: dict[str, Any], run_id: str) -> AuthContext:
        missing = [
            key
            for key in ("principal_id", "agent_instance_id", "task_id", "fencing_epoch", "grant_epoch")
            if run_spec.get(key) is None
        ]
        if missing:
            raise ValueError(f"run start payload missing {', '.join(missing)}")
        return AuthContext(
            principal_id=str(run_spec["principal_id"]),
            actor_id=str(run_spec["agent_instance_id"]),
            actor_type=ActorType.INTERNAL,
            auth_context_id=f"run:{run_id}",
            bound_task_id=str(run_spec["task_id"]),
            bound_run_id=run_id,
            bound_fencing_epoch=int(run_spec["fencing_epoch"]),
            bound_grant_epoch=int(run_spec["grant_epoch"]),
        )

    def _build_messages(
        self,
        spec: dict[str, Any],
        run_input: dict[str, Any],
        auth: Any = None,
        run_id: str = "",
    ) -> list[ChatMessage]:
        lines = [f"Task: {spec.get('task_id')}"]
        objective = spec.get("objective") or ""
        if objective:
            lines.append(f"Objective: {objective}")
        if spec.get("work_type"):
            lines.append(f"Work type: {spec['work_type']}")
        expected = list(spec.get("expected_outputs") or [])
        if expected:
            lines.append("Expected outputs: " + ", ".join(str(item) for item in expected))
        criteria = list(spec.get("acceptance_criteria") or [])
        if criteria:
            lines.append("Acceptance criteria: " + json.dumps(criteria, default=str))
        materialized = _materialized_mandatory_texts(self._core, auth, run_id or str(spec.get("run_id") or ""))
        if materialized:
            lines.append("Authorized context:\n" + "\n".join(materialized))
        appends = _context_append_texts(
            self._core, auth, str(spec.get("run_id") or run_id or ""), run_input.get("workspace_path")
        )
        if appends:
            lines.append("Appended context:\n" + "\n".join(appends))
        granted = list(spec.get("granted_tools") or run_input.get("granted_tools") or [])
        lines.append("Granted tools: " + (", ".join(granted) if granted else "(none)"))
        return [
            ChatMessage(role="system", content=self._system_prompt),
            ChatMessage(role="user", content="\n".join(lines)),
        ]

    def _drain_pending(self, record: _RunRecord, messages: list[ChatMessage]) -> None:
        with self._lock:
            pending = list(record.pending_messages)
            record.pending_messages.clear()
        for message in pending:
            if isinstance(message, str):
                messages.append(ChatMessage(role="user", content=message))
                continue
            role = str(message.get("role") or "user")
            content = message.get("content")
            messages.append(
                ChatMessage(
                    role=role,
                    content=None if content is None else str(content),
                    tool_call_id=message.get("tool_call_id"),
                    name=message.get("name"),
                )
            )

    # ------------------------------------------------------------------
    # Core lifecycle waits / state
    # ------------------------------------------------------------------

    def _wait_for_running(self, record: _RunRecord) -> None:
        """Wait for the Core to promote the Run from CREATED to RUNNING.

        Tools and results are only legal on a RUNNING Run, and the Core acks
        ``agent.start`` immediately after this adapter returns, so the worker
        waits briefly instead of racing the ack.
        """
        task_id = record.task_id
        if not task_id:
            return
        deadline = time.monotonic() + _STARTUP_GRACE_S
        terminal = {"SUCCEEDED", "FAILED", "CANCELLED", "LOST", "TIMED_OUT"}
        while time.monotonic() < deadline:
            if record.stop_event.is_set():
                return
            try:
                runs = self._core.list_runs(task_id)
            except Exception:  # noqa: BLE001 - a read failure must not kill the loop
                return
            status = next(
                (str(item.get("status")) for item in runs if item.get("run_id") == record.run_id),
                None,
            )
            if status == "RUNNING" or status in terminal:
                return
            time.sleep(self.poll_interval_s)

    def _state(self, record: _RunRecord) -> dict[str, Any]:
        thread_alive = bool(record.thread is not None and record.thread.is_alive())
        return {
            "run_id": record.run_id,
            "alive": thread_alive,
            "writer_alive": bool(thread_alive and record.writer_alive),
            "status": record.status,
            "identity": f"local:{record.run_id}",
            "start_revoked": bool(record.start_revoked),
        }

    def _revoked_start_result(
        self, run_id: str, run_spec: dict[str, Any]
    ) -> dict[str, Any]:
        """Forbid start without erasing an existing stubborn writer record."""
        existing = self._runs.get(run_id)
        if existing is not None:
            existing.start_revoked = True
            return self._state(existing)
        record = _RunRecord(
            run_id=run_id,
            spec=dict(run_spec),
            status="REVOKED",
            start_revoked=True,
            alive=False,
            writer_alive=False,
            finished_writes=True,
        )
        self._runs[run_id] = record
        return {
            "run_id": run_id,
            "alive": False,
            "writer_alive": False,
            "status": "REVOKED",
            "start_revoked": True,
        }


def _tool_schemas(granted: list[str]) -> list[dict]:
    schemas: list[dict] = []
    for tool in granted:
        function_name = _FUNCTION_NAMES.get(tool)
        parameters = _TOOL_PARAMETERS.get(tool)
        if function_name is None or parameters is None:
            continue
        schemas.append(
            {
                "type": "function",
                "function": {
                    "name": function_name,
                    "description": f"HIBIKI tool {tool}",
                    "parameters": parameters,
                },
            }
        )
    return schemas


def _sandbox_command(parameters: dict[str, Any]) -> dict[str, Any]:
    argv = parameters.get("argv")
    if isinstance(argv, list) and argv and all(isinstance(item, str) for item in argv):
        args = list(argv)
    else:
        command = parameters.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("shell.run requires a non-empty 'command' or 'argv'")
        args = shlex.split(command)
    if not args:
        raise ValueError("shell.run resolved to an empty argv")
    timeout_s = parameters.get("timeout_s")
    if not isinstance(timeout_s, int) or isinstance(timeout_s, bool) or timeout_s <= 0:
        timeout_s = None
    command_spec: dict[str, Any] = {"argv": args, "cwd": _SANDBOX_WORKSPACE}
    stdin = parameters.get("stdin")
    if isinstance(stdin, str):
        command_spec["stdin"] = stdin
    if timeout_s is not None:
        command_spec["timeout_s"] = timeout_s
    return command_spec


def _materialized_mandatory_texts(core: Any, auth: Any, run_id: str) -> list[str]:
    """Include Manifest-declared required inputs as concrete text, not only a hash."""
    if not run_id or auth is None:
        return []
    try:
        ctx = core.get_run_context(auth, run_id)
    except Exception:  # noqa: BLE001
        return []
    texts: list[str] = []
    if ctx.get("manifest_hash"):
        texts.append(f"[manifest {ctx.get('manifest_id')} sha256={ctx['manifest_hash']}]")
    for item in ctx.get("materialized") or []:
        body = item.get("text")
        if not body:
            texts.append(
                f"[{item.get('kind')}] {item.get('ref')} sha256={item.get('hash')} (unavailable)"
            )
            continue
        texts.append(
            f"[{item.get('kind')}] {item.get('ref')} sha256={item.get('hash')}\n{body}"
        )
    return texts


def _context_append_texts(
    core: Any, auth: Any, run_id: str, workspace_path: str | None
) -> list[str]:
    """Read the Run's admitted context so the worker prompt matches the audit record.

    Every ``ContextAppend`` is read through Core authorization: ``artifact://`` URIs
    must be Task-owned digests, and workspace files must still match the recorded
    hash — mutated files are refused rather than re-labeled under a stale digest.
    """
    del workspace_path  # reads go through Core; local path open is no longer used
    try:
        ctx = core.get_run_context(auth, run_id)
    except Exception:  # noqa: BLE001 — context is best effort, never fatal
        return []
    texts: list[str] = []
    for append in ctx.get("appends") or []:
        reason = append.get("reason") or "append"
        ref = str(append.get("authorized_ref") or "")
        try:
            materialize = getattr(core, "materialize_context_append", None)
            if materialize is None:
                texts.append(f"[{reason}] {ref} (unavailable)")
                continue
            loaded = materialize(auth, run_id, append)
        except Exception:  # noqa: BLE001
            texts.append(f"[{reason}] {ref} (unavailable)")
            continue
        if not loaded.get("ok"):
            texts.append(
                f"[{reason}] {ref} (unavailable:{loaded.get('error') or 'refused'})"
            )
            continue
        digest = loaded.get("materialized_hash") or append.get("materialized_hash") or ""
        texts.append(f"[{reason}] {ref} sha256={digest}\n{loaded.get('content')}")
    return texts


#: Result keys that mean "the Core registered this content as an Artifact". A plain
#: file digest from ``fs.write`` is NOT an artifact reference and must never be
#: submitted as one.
_ARTIFACT_REF_KEYS = ("artifact_hash", "artifact_ref")


def _artifact_ref(result: dict[str, Any]) -> str | None:
    if not isinstance(result, dict):
        return None
    if str(result.get("status") or "") not in {"ok", "published"}:
        return None
    for key in _ARTIFACT_REF_KEYS:
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
    return None
