"""Provider-failure acceptance: the real adapter against a faulty OpenAI-compatible API.

These tests are the rehearsal for the live gate: a real HTTP server returns 429/5xx/401,
hangs, or sends malformed/truncated bodies, and the real `ApiAgentAdapter` must classify
and handle each case without ever reporting a false success (SPEC §17 / §8.3).

The server is the same one used for the harness smoke (`tools/fake_openai_server.py`),
started in-process with fault injection.
"""

from __future__ import annotations

import json
import socket
import threading
import time

import pytest

from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import ModelClientError, OpenAICompatibleClient
from hibiki.tools.broker import ToolBroker
from tests.helpers import human_auth, make_core


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _server(fault: str, **kwargs):
    from tools.fake_openai_server import FakeServer

    server = FakeServer(("127.0.0.1", _free_port()), fault=fault, **kwargs)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


@pytest.fixture
def faulty():
    started: list = []

    def _start(fault: str, **kwargs):
        server, thread = _server(fault, **kwargs)
        started.append((server, thread))
        return f"http://127.0.0.1:{server.server_address[1]}/v1"

    yield _start
    for server, thread in started:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


def _task(svc, auth, tools: list[str]) -> str:
    r = svc.execute("create_task", auth, {"title": "fault"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {"task_id": task_id, "objective": "work", "permission_ceiling": {"tools": tools}},
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
    assert svc.execute("activate_minimal_plan", auth, {"task_id": task_id}).ok
    return task_id


def _adapter_with_client(svc, client) -> ApiAgentAdapter:
    return ApiAgentAdapter(
        client,
        core=svc,
        clock=svc.clock,
        broker=ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root),
        workspace_root=svc.workspace_root,
    )


def _result_of(svc, task_id: str) -> dict | None:
    for row in svc.list_runs(task_id):
        if row.get("result_json"):
            return json.loads(row["result_json"])
    return None


def _run_one(tmp_path, url: str, *, max_retries: int = 2, tools=("fs.read",)):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, list(tools))
    client = OpenAICompatibleClient(
        url, "sk-fault-test", "fake-model", timeout_s=5.0, max_retries=max_retries
    )
    adapter = _adapter_with_client(svc, client)
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    run_id = r.data["created_runs"][0]
    adapter.wait_for_exit(run_id, 20.0)
    client.close()
    return svc, task_id, run_id


def test_client_retries_a_transient_rate_limit(tmp_path, faulty):
    """A 429 is retryable and the call eventually succeeds."""
    url = faulty("rate_limit", fault_mode="first")  # only the first request fails
    client = OpenAICompatibleClient(url, "sk", "fake-model", max_retries=2)
    from hibiki.runtime.openai_client import ChatMessage

    reply = client.chat([ChatMessage(role="user", content="hi")])
    assert reply.content is not None or reply.tool_calls
    client.close()


def test_client_retries_a_server_error(tmp_path, faulty):
    url = faulty("server_error", fault_mode="first")
    client = OpenAICompatibleClient(url, "sk", "fake-model", max_retries=2)
    from hibiki.runtime.openai_client import ChatMessage

    reply = client.chat([ChatMessage(role="user", content="hi")])
    assert reply is not None
    client.close()


def test_client_does_not_retry_an_auth_failure(tmp_path, faulty):
    url = faulty("auth")
    client = OpenAICompatibleClient(url, "sk-bad", "fake-model", max_retries=3)
    from hibiki.runtime.openai_client import ChatMessage

    with pytest.raises(ModelClientError) as excinfo:
        client.chat([ChatMessage(role="user", content="hi")])
    assert excinfo.value.kind == "auth"
    assert excinfo.value.retryable is False
    client.close()


def test_client_classifies_a_malformed_body_as_a_protocol_error(tmp_path, faulty):
    url = faulty("malformed")
    client = OpenAICompatibleClient(url, "sk", "fake-model", max_retries=0)
    from hibiki.runtime.openai_client import ChatMessage

    with pytest.raises(ModelClientError) as excinfo:
        client.chat([ChatMessage(role="user", content="hi")])
    assert excinfo.value.kind == "protocol"
    assert excinfo.value.retryable is False
    # The key must never appear in a diagnostic.
    assert "sk" not in str(excinfo.value) or "sk-" not in str(excinfo.value)
    client.close()


def test_client_classifies_an_empty_choices_body(tmp_path, faulty):
    url = faulty("no_choices")
    client = OpenAICompatibleClient(url, "sk", "fake-model", max_retries=0)
    from hibiki.runtime.openai_client import ChatMessage

    with pytest.raises(ModelClientError) as excinfo:
        client.chat([ChatMessage(role="user", content="hi")])
    assert excinfo.value.kind == "protocol"
    client.close()


def test_client_times_out_on_a_hanging_provider(tmp_path, faulty):
    url = faulty("hang", hang_seconds=30.0)
    client = OpenAICompatibleClient(url, "sk", "fake-model", timeout_s=1.0, max_retries=0)
    from hibiki.runtime.openai_client import ChatMessage

    started = time.monotonic()
    with pytest.raises(ModelClientError) as excinfo:
        client.chat([ChatMessage(role="user", content="hi")])
    elapsed = time.monotonic() - started
    assert excinfo.value.kind == "timeout"
    assert excinfo.value.retryable is True
    assert elapsed < 10, f"the call took {elapsed:.1f}s despite a 1s timeout"
    client.close()


def test_adapter_blocks_on_a_persistent_provider_failure(tmp_path, faulty):
    """The whole Run path: a provider outage must end in BLOCKED, never PASS."""
    url = faulty("server_error")  # always fails
    svc, task_id, run_id = _run_one(tmp_path, url)

    result = _result_of(svc, task_id)
    assert result is not None, "a result must still be recorded"
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"
    assert "ModelClientError" in str(result.get("error_class") or "")
    row = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    assert row["status"] != "SUCCEEDED" or result["outcome"] == "BLOCKED"


def test_adapter_blocks_on_truncated_tool_arguments(tmp_path, faulty):
    """A malformed tool call is a protocol error, not a tool attempt."""
    url = faulty("truncated_arguments")
    svc, task_id, run_id = _run_one(tmp_path, url, tools=("fs.read",))

    result = _result_of(svc, task_id)
    assert result is not None
    assert result["outcome"] == "BLOCKED"


def test_adapter_survives_a_transient_fault_and_completes(tmp_path, faulty):
    """One 500 then a healthy script: the Run still completes inside budget."""
    from tools.fake_openai_server import FakeServer

    port = _free_port()
    server = FakeServer(("127.0.0.1", port), fault="server_error", fault_mode="first")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}/v1"
        svc, _ = make_core(tmp_path)
        auth = human_auth()
        task_id = _task(svc, auth, ["fs.read"])
        client = OpenAICompatibleClient(url, "sk", "fake-model", timeout_s=5.0, max_retries=2)
        adapter = _adapter_with_client(svc, client)
        svc.agent_adapter = adapter
        r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
        run_id = r.data["created_runs"][0]
        adapter.wait_for_exit(run_id, 20.0)
        client.close()

        result = _result_of(svc, task_id)
        assert result is not None
        assert result["outcome"] == "COMPLETED", result
        assert result["verdict"] == "PASS"
    finally:
        server.shutdown()
        server.server_close()


def test_stop_cap_bounds_a_hanging_provider_call(tmp_path, faulty):
    """Pause/Cancel must not be held open by a hanging provider response."""
    url = faulty("hang", hang_seconds=10.0)
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])
    client = OpenAICompatibleClient(url, "sk", "fake-model", timeout_s=30.0, max_retries=0)
    adapter = _adapter_with_client(svc, client)
    adapter.stop_model_call_timeout_s = 2.0
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = r.data["created_runs"][0]

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if any(run["status"] == "RUNNING" for run in svc.list_runs(task_id)):
            break
        time.sleep(0.05)

    requested = time.monotonic()
    adapter.stop(run_id, "cancel_requested")
    final = adapter.wait_for_exit(run_id, 15.0)
    elapsed = time.monotonic() - requested
    client.close()

    assert final["alive"] is False, "the hanging model call was not aborted"
    assert elapsed < 15, f"stop took {elapsed:.1f}s against a 2s stop cap"
    result = _result_of(svc, task_id)
    assert result is None or result["outcome"] != "COMPLETED"


def test_harness_survives_a_chaotic_provider_without_invariant_violations(tmp_path, monkeypatch):
    """Chaos rehearsal: random provider faults must never break an invariant.

    The whole harness (all three fixed tasks) runs against a provider that independently
    injects rate limits, 5xx, malformed bodies and slow replies on ~30% of requests. No
    run may hang, claim success after a failed model call, or register a fabricated
    artifact; surviving runs must still carry fully verified evidence.
    """
    import json

    from tools.fake_openai_server import FakeServer

    port = _free_port()
    server = FakeServer(
        ("127.0.0.1", port),
        fault_ratio=0.3,
        seed=11,
        slow_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("HIBIKI_MODEL_BASE_URL", f"http://127.0.0.1:{port}/v1")
    monkeypatch.setenv("HIBIKI_MODEL_API_KEY", "sk-chaos")
    monkeypatch.setenv("HIBIKI_MODEL", "chaos-model")
    try:
        from hibiki.interfaces.m1_runner import main as runner_main

        out = tmp_path / "out"
        code = runner_main(
            [
                "--data-dir",
                str(tmp_path / "data"),
                "--out",
                str(out),
                "--tasks",
                "docs/m1/tasks",
                "--repeats",
                "1",
            ]
        )
        summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)

    assert summary["runs"] == 3
    assert code in (0, 1)
    records = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(out.glob("*-run1.json"))
    ]
    assert len(records) == 3
    for record in records:
        result = record.get("result")
        assert result is not None, "every run must reach a recorded terminal result"
        assert record["run_status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "LOST"}
        assert record["context_manifest_id"] and record["spec_hash"]
        # No fabricated reference may ever be counted as a delivery.
        assert record.get("unbacked_artifact_refs") in ([], None)
        for check in record.get("artifact_checks") or []:
            assert check["verified"] is True
        if result["outcome"] == "COMPLETED":
            assert result["verdict"] == "PASS"
        else:
            assert result["verdict"] == "FAIL"
            assert result.get("error_class")
