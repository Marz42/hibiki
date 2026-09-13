"""Integration tests for the real API worker adapter against the real Core.

The model client is duck-typed and local: no network, no Docker.  The point is the
adapter contract — idempotent start, honest stop, identity, result submission and
failure-as-BLOCKED — driven through ``ApplicationService`` exactly as dispatch does.
"""

from __future__ import annotations

import json
import os
import threading

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
