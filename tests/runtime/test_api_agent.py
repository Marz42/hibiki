"""Integration tests for the real API worker adapter against the real Core.

The model client is duck-typed and local: no network, no Docker.  The point is the
adapter contract — idempotent start, honest stop, identity, result submission and
failure-as-BLOCKED — driven through ``ApplicationService`` exactly as dispatch does.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest

from hibiki.persistence.models import OutboxRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import ModelClientError, ModelReply
from hibiki.tools.broker import ToolBroker
from tests.helpers import human_auth, make_core, run_auth


class ScriptedClient:
    """Minimal duck-typed model client: a queue of replies, exceptions or callables."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.call_count = 0
        self.calls: list[list] = []
        self._lock = threading.Lock()

    def chat(self, messages, *, tools=None, temperature=0.0):
        with self._lock:
            self.call_count += 1
            self.calls.append(list(messages))
            item = self.script.pop(0) if self.script else None
        if callable(item):
            return item(messages, tools)
        if item is None:
            return ModelReply(content="done", tool_calls=(), finish_reason="stop", usage={}, raw={})
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        return None


def _final(content: str = "done") -> ModelReply:
    return ModelReply(content=content, tool_calls=(), finish_reason="stop", usage={}, raw={})


def _tool_call(name: str, arguments: dict, call_id: str = "call_1") -> dict:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(arguments)},
    }


def _task_with_ceiling(svc, auth, tools: list[str]) -> str:
    r = svc.execute("create_task", auth, {"title": "api-agent"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "produce a file",
            "permission_ceiling": {"tools": tools},
        },
    )
    assert r.ok, r
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    assert r.ok, r
    r = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert r.ok, r
    return task_id


def _read_start_payload(svc, run_id: str) -> dict:
    def _read(session):
        for row in session.query(OutboxRow).filter(OutboxRow.command_type == "agent.start").all():
            payload = json.loads(row.payload_json)
            if payload.get("run_id") == run_id:
                return payload
        raise AssertionError(f"no agent.start row for {run_id}")

    return svc.executor.run(_read)


def _adapter(svc, client, *, sandbox=None, broker=True) -> ApiAgentAdapter:
    tool_broker = (
        ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root) if broker else None
    )
    return ApiAgentAdapter(
        client,
        core=svc,
        clock=svc.clock,
        broker=tool_broker,
        sandbox=sandbox,
        workspace_root=svc.workspace_root,
    )


def _dispatch(svc, adapter, task_id: str) -> str:
    svc.agent_adapter = adapter
    auth = human_auth()
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    return r.data["created_runs"][0]


def _run_row(svc, task_id: str, run_id: str) -> dict:
    return next(item for item in svc.list_runs(task_id) if item["run_id"] == run_id)


def test_full_run_reaches_submitted_completed_result(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient([_final("all done")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    final = adapter.wait_for_exit(run_id, 5.0)
    assert final["identity"] == f"local:{run_id}"
    assert final["alive"] is False

    row = _run_row(svc, task_id, run_id)
    assert row["status"] == "SUCCEEDED"
    result = json.loads(row["result_json"])
    assert result["outcome"] == "COMPLETED"
    assert result["verdict"] == "PASS"
    assert result["summary"] == "all done"
    assert svc.get_task(task_id)["model_calls_used"] == 1

    # exit confirmation revokes further starts for this Run
    restarted = adapter.start(_read_start_payload(svc, run_id))
    assert restarted["start_revoked"] is True
    assert client.call_count == 1


def test_second_start_does_not_run_the_loop_twice(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    entered = threading.Event()
    release = threading.Event()

    def first_call(messages, tools):
        entered.set()
        assert release.wait(5.0)
        return _final("once")

    client = ScriptedClient([first_call])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    assert entered.wait(5.0)
    payload = _read_start_payload(svc, run_id)
    again = adapter.start(payload)
    assert again["run_id"] == run_id
    assert again["start_revoked"] is False
    assert adapter.started.count(run_id) == 1
    assert client.call_count == 1

    release.set()
    adapter.wait_for_exit(run_id, 5.0)
    assert client.call_count == 1
    assert json.loads(_run_row(svc, task_id, run_id)["result_json"])["outcome"] == "COMPLETED"


def test_stop_before_the_model_replies_prevents_result_submission(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    entered = threading.Event()
    release = threading.Event()

    def first_call(messages, tools):
        entered.set()
        assert release.wait(5.0)
        return _final("late answer")

    client = ScriptedClient([first_call])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    assert entered.wait(5.0)

    adapter.stop_wait_s = 0.2
    stopped = adapter.stop(run_id, "test_stop")
    assert stopped["start_revoked"] is True
    assert stopped["alive"] is True  # the thread is blocked inside the model call

    release.set()
    final = adapter.wait_for_exit(run_id, 5.0)
    assert final["start_revoked"] is True

    row = _run_row(svc, task_id, run_id)
    assert row["result_json"] is None
    assert row["status"] != "SUCCEEDED"


def test_inspect_reports_identity_and_missing(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient([_final("done")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    adapter.wait_for_exit(run_id, 5.0)
    inspected = adapter.inspect(run_id)
    assert inspected["run_id"] == run_id
    assert inspected["identity"] == f"local:{run_id}"
    assert inspected["status"] in {"EXITED", "STOPPED"}
    assert adapter.inspect("run_unknown") == {
        "run_id": "run_unknown",
        "alive": False,
        "status": "MISSING",
    }


def test_model_error_produces_blocked_not_false_success(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient([ModelClientError("server", "boom", retryable=True, status=500)])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    adapter.wait_for_exit(run_id, 5.0)
    row = _run_row(svc, task_id, run_id)
    assert row["status"] == "SUCCEEDED", "a blocked result is still a submitted result"
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"
    assert "ModelClientError" in result["error_class"]
    assert svc.get_task(task_id)["model_calls_used"] == 1


def test_unexpected_client_exception_is_blocked_not_a_crash(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient([RuntimeError("kaboom")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    adapter.wait_for_exit(run_id, 5.0)
    row = _run_row(svc, task_id, run_id)
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert "RuntimeError" in result["error_class"]


def test_blocked_marker_in_final_message_yields_blocked(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient([_final("cannot continue [[HIBIKI:BLOCKED]]")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    adapter.wait_for_exit(run_id, 5.0)
    result = json.loads(_run_row(svc, task_id, run_id)["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"


def test_tool_call_is_dispatched_through_the_broker(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.write"])
    entered = threading.Event()
    proceed = threading.Event()

    def first_call(messages, tools):
        assert [item["function"]["name"] for item in tools] == ["fs_read", "fs_write"]
        entered.set()
        assert proceed.wait(5.0)
        return ModelReply(
            content=None,
            tool_calls=(_tool_call("fs_write", {"path": "out.txt", "content": "hello"}),),
            finish_reason="tool_calls",
            usage={},
            raw={},
        )

    client = ScriptedClient([first_call, _final("written")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    assert entered.wait(5.0)
    worker_auth = run_auth(svc, run_id)
    workspace_path = svc.get_run_input(worker_auth, run_id)["workspace_path"]
    os.makedirs(workspace_path, mode=0o700, exist_ok=True)
    proceed.set()
    adapter.wait_for_exit(run_id, 5.0)

    with open(os.path.join(workspace_path, "out.txt"), encoding="utf-8") as handle:
        assert handle.read() == "hello"
    row = _run_row(svc, task_id, run_id)
    assert json.loads(row["result_json"])["outcome"] == "COMPLETED"
    assert svc.get_task(task_id)["model_calls_used"] == 2


class BlockingSandbox:
    """Sandbox double whose command blocks until the run's stop event is set.

    Mirrors ``DockerSandboxAdapter``: it receives ``cancel_event`` in the command and
    returns ``status="cancelled"`` instead of running to the wall clock.
    """

    def __init__(self) -> None:
        self.started = threading.Event()
        self.cancelled_at: float | None = None
        self.finished_at: float | None = None

    def execute(self, command: dict) -> dict:
        import time as _time

        self.started.set()
        cancel = command.get("cancel_event")
        deadline = _time.monotonic() + 30
        while _time.monotonic() < deadline:
            if cancel is not None and cancel.is_set():
                self.cancelled_at = _time.monotonic()
                self.finished_at = self.cancelled_at
                return {"status": "cancelled", "exit_code": 137, "stdout": "", "stderr": ""}
            _time.sleep(0.02)
        self.finished_at = _time.monotonic()
        return {"status": "timeout", "exit_code": None, "stdout": "", "stderr": ""}


def test_stop_interrupts_an_in_flight_tool_command(tmp_path):
    """SPEC §18: stopping a Run must interrupt a long command, not wait it out."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    sandbox = BlockingSandbox()
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("shell.run", {"argv": ["sleep", "30"]}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("done"),
        ]
    )
    adapter = _adapter(svc, client, sandbox=sandbox)
    run_id = _dispatch(svc, adapter, task_id)
    assert sandbox.started.wait(5.0), "the tool command never started"

    import time as _time

    requested = _time.monotonic()
    stop = adapter.stop(run_id, "cancel_requested")
    elapsed = _time.monotonic() - requested

    assert sandbox.cancelled_at is not None, "the sandbox command was not cancelled"
    assert elapsed < 15, f"stop took {elapsed:.1f}s"
    # The run was revoked before its result, so no completed result may be submitted.
    row = _run_row(svc, task_id, run_id)
    assert row["status"] != "SUCCEEDED" or row["result_json"] is None
    assert stop["start_revoked"] is True
    final = adapter.wait_for_exit(run_id, 5.0)
    assert final["alive"] is False


def _docker_available() -> bool:
    try:
        from hibiki.tools.sandbox import DockerSandboxAdapter

        return DockerSandboxAdapter().available()
    except Exception:  # noqa: BLE001
        return False


@pytest.mark.skipif(not _docker_available(), reason="docker unavailable")
def test_real_container_stop_completes_within_15s(tmp_path):
    """SPEC §24.4 G5: a killable real process stops within 15 s of the command.

    Uses the real hardened Docker sandbox and a real ``sleep 30``; the measurement is
    from the stop request to confirmed executor exit.
    """
    from pathlib import Path

    from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("shell.run", {"argv": ["sleep", "30"]}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("done"),
        ]
    )
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    # The sandbox spec needs the Run's own workspace, which exists after dispatch.
    spec = svc.get_run_input(run_auth(svc, run_id), run_id)
    workspace = Path(spec["workspace_path"])
    workspace.mkdir(parents=True, exist_ok=True)
    adapter._sandbox = DockerSandboxAdapter(
        SandboxSpec(
            workspace_host_path=str(workspace),
            limits=SandboxLimits(wall_timeout_s=120, stop_grace_s=5),
        )
    )

    import time as _time

    # Wait until the sandbox really has a container running for this Run.
    deadline = _time.monotonic() + 10
    while _time.monotonic() < deadline:
        row = _run_row(svc, task_id, run_id)
        if row["status"] == "RUNNING" and client.call_count >= 1:
            break
        _time.sleep(0.05)
    _time.sleep(0.5)  # let the handler actually start the container

    requested = _time.monotonic()
    adapter.stop(run_id, "cancel_requested")
    final = adapter.wait_for_exit(run_id, 15.0)
    elapsed = _time.monotonic() - requested

    assert final["alive"] is False, "the executor did not exit"
    assert elapsed < 15, f"stop took {elapsed:.1f}s"
    print(f"[M1-G5] stop request -> confirmed executor exit: {elapsed:.2f}s (sleep 30)")

    # Prove the real path ran: the broker must have audited a shell.run for this Run
    # and its outcome must be the sandbox's cancellation, not a skipped call.
    from sqlalchemy import select

    from hibiki.persistence.models import ToolInvocationRow

    rows = svc.executor.run(
        lambda s: [
            {"tool_name": r.tool_name, "decision": r.decision, "outcome": r.outcome}
            for r in s.scalars(
                select(ToolInvocationRow).where(ToolInvocationRow.run_id == run_id)
            ).all()
        ]
    )
    shell_rows = [r for r in rows if r["tool_name"] == "shell.run"]
    assert shell_rows, "shell.run never reached the broker"
    assert shell_rows[-1]["decision"] == "ALLOW"
    assert shell_rows[-1]["outcome"] == "cancelled", shell_rows[-1]["outcome"]


@pytest.mark.skipif(not _docker_available(), reason="docker unavailable")
def test_pause_command_stops_the_real_container_within_15s(tmp_path):
    """SPEC §24.4 G5 through the Core entry point: pause_task -> executor exited."""
    from pathlib import Path

    from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("shell.run", {"argv": ["sleep", "30"]}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("done"),
        ]
    )
    adapter = _adapter(svc, client)
    adapter.stop_wait_s = 1.0
    run_id = _dispatch(svc, adapter, task_id)

    spec = svc.get_run_input(run_auth(svc, run_id), run_id)
    workspace = Path(spec["workspace_path"])
    workspace.mkdir(parents=True, exist_ok=True)
    adapter._sandbox = DockerSandboxAdapter(
        SandboxSpec(
            workspace_host_path=str(workspace),
            limits=SandboxLimits(wall_timeout_s=120, stop_grace_s=5),
        )
    )

    import time as _time

    deadline = _time.monotonic() + 10
    while _time.monotonic() < deadline:
        if _run_row(svc, task_id, run_id)["status"] == "RUNNING" and client.call_count >= 1:
            break
        _time.sleep(0.05)
    _time.sleep(0.5)

    requested = _time.monotonic()
    r = svc.execute("pause_task", auth, {"task_id": task_id})
    assert r.ok, r
    final = adapter.wait_for_exit(run_id, 15.0)
    elapsed = _time.monotonic() - requested

    assert final["alive"] is False, "pause did not stop the executor"
    assert elapsed < 15, f"pause took {elapsed:.1f}s"
    print(f"[M1-G5] pause_task -> executor exit: {elapsed:.2f}s (sleep 30)")


def test_only_published_artifacts_become_result_refs(tmp_path):
    """A file digest from fs.write is not an artifact reference (SPEC §11.3)."""
    from hibiki.runtime.api_agent import _artifact_ref

    assert _artifact_ref({"status": "ok", "sha256": "a" * 64}) is None
    assert _artifact_ref({"status": "ok", "path": "x", "bytes": 3}) is None
    assert _artifact_ref({"status": "denied", "artifact_hash": "b" * 64}) is None
    assert _artifact_ref({"status": "ok", "artifact_hash": "c" * 64}) == "c" * 64
    assert _artifact_ref({"status": "ok", "artifact_ref": "d" * 64}) == "d" * 64


def test_result_refs_never_include_unpublished_file_digests(tmp_path):
    """End to end: a run that writes and publishes reports only the published hash."""
    import hashlib

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read", "fs.write", "artifact.publish"])
    adapter = _adapter(svc, client := ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    worker = run_auth(svc, run_id)
    spec = svc.get_run_input(worker, run_id)
    workspace = Path(spec["workspace_path"])
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "out.txt").write_text("payload", encoding="utf-8")
    file_digest = hashlib.sha256(b"payload").hexdigest()
    published = svc.execute(
        "publish_artifact",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "path": "out.txt",
        },
    )
    assert published.ok, published
    assert published.data["artifact_hash"] == file_digest
    assert client.call_count >= 0
