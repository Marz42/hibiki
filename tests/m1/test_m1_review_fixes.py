"""Regression tests for the M1 adversarial review findings.

Each test names the finding it locks down. The rule is that a stopped Run must be
powerless, a revoked or foreign credential must be refused, and no failure may be
reported as success.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from sqlalchemy import select

from hibiki.persistence.models import WorkUnitExecutionRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import ModelReply
from hibiki.tools.broker import ToolBroker, ToolRequest
from tests.helpers import human_auth, make_core, run_auth, user_agent_auth
from tests.runtime.test_api_agent import ScriptedClient, _final, _tool_call


def _task(svc, auth, tools: list[str], *, limits: dict | None = None) -> str:
    r = svc.execute("create_task", auth, {"title": "review"})
    task_id = r.data["task_id"]
    payload = {"task_id": task_id, "objective": "work", "permission_ceiling": {"tools": tools}}
    if limits:
        payload["resource_limits"] = limits
    r = svc.execute("submit_contract", auth, payload)
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


def _adapter(svc, client, **kwargs) -> ApiAgentAdapter:
    return ApiAgentAdapter(
        client,
        core=svc,
        clock=svc.clock,
        broker=ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root),
        workspace_root=svc.workspace_root,
        **kwargs,
    )


def _dispatch(svc, adapter, task_id: str) -> str:
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", human_auth(), {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    return r.data["created_runs"][0]


def _workspace(svc, run_id: str) -> Path:
    spec = svc.get_run_input(run_auth(svc, run_id), run_id)
    return Path(spec["workspace_path"])


def _seed(workspace: Path, files: dict[str, str]) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    workspace.chmod(0o755)
    for name, content in files.items():
        (workspace / name).write_text(content, encoding="utf-8")


def _work_unit_id(svc, task_id: str) -> str:
    return svc.executor.run(
        lambda s: s.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first().work_unit_id
    )


def _run_status(svc, task_id: str, run_id: str) -> str:
    return next(r["status"] for r in svc.list_runs(task_id) if r["run_id"] == run_id)


# ---------------------------------------------------------------- P1: stop revokes


def test_p1_stop_terminates_the_run_and_its_credential_loses_tool_access(tmp_path):
    """Stopping a live Run must revoke it: no more tools, no more publication."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "fs.write", "artifact.publish"])

    class Hanging:
        def __init__(self) -> None:
            self.entered = threading.Event()
            self.release = threading.Event()

        def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
            self.entered.set()
            if self.release.wait(timeout_s or 30):
                return _final("late completion")
            raise TimeoutError("model call timed out")

        def close(self) -> None:
            return None

    client = Hanging()
    adapter = _adapter(svc, client)
    adapter.stop_model_call_timeout_s = 0.5
    run_id = _dispatch(svc, adapter, task_id)
    assert client.entered.wait(5.0), "the worker never started"
    assert _run_status(svc, task_id, run_id) == "RUNNING"
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"x.txt": "x"})
    worker = run_auth(svc, run_id)
    broker = ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root)

    stop = svc.execute("confirm_run_exit", auth, {"run_id": run_id})
    assert stop.ok, stop
    # The stop request makes the worker's model call abort at the stop cap.
    adapter.wait_for_exit(run_id, 15.0)
    svc.drain_outbox()
    assert _run_status(svc, task_id, run_id) == "CANCELLED"

    after = broker.authorize(
        worker,
        ToolRequest(
            run_id=run_id,
            task_id=task_id,
            work_unit_id=None,
            tool_name="fs.write",
            parameters={"path": "after.txt", "content": "after"},
            grant_epoch=int(worker.bound_grant_epoch or 0),
            fencing_epoch=int(worker.bound_fencing_epoch or 0),
            sequence_no=1,
        ),
    )
    assert after.allowed is False
    assert not (workspace / "after.txt").exists()
    publish = svc.execute(
        "publish_artifact",
        worker,
        {"run_id": run_id, "fencing_epoch": worker.bound_fencing_epoch, "path": "x.txt"},
    )
    assert not publish.ok


def test_p1_result_after_stop_is_history_and_never_completes_the_work_unit(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])

    class Hanging:
        def __init__(self) -> None:
            self.entered = threading.Event()
            self.release = threading.Event()

        def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
            self.entered.set()
            self.release.wait(20)
            raise RuntimeError("executor vanished")

        def close(self) -> None:
            return None

    client = Hanging()
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    assert client.entered.wait(5.0)
    svc.execute("confirm_run_exit", auth, {"run_id": run_id})
    adapter.wait_for_exit(run_id, 15.0)
    svc.drain_outbox()
    assert _run_status(svc, task_id, run_id) == "CANCELLED"

    # The (now stopped) runtime reports a result: history only, nothing advances.
    worker = run_auth(svc, run_id)
    late = svc.execute(
        "submit_result",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "result": {"outcome": "COMPLETED", "verdict": "PASS", "artifact_refs": ["late"]},
        },
    )
    assert late.ok and late.data.get("accepted_as_history") is True
    assert _run_status(svc, task_id, run_id) == "CANCELLED"

    def _wu(session):
        return session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first().status

    assert svc.executor.run(_wu) != "DONE", "a late result must not complete the Work Unit"
    events = [e["event_type"] for e in svc.list_events(task_id)]
    assert "run.late_result_after_stop" in events or "run.late_result" in events


def test_p1_a_credential_bound_to_another_run_cannot_publish_or_append(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"a.txt": "A"})
    worker = run_auth(svc, run_id)

    # A forged credential that claims a different Run / actor must be refused.
    from dataclasses import replace

    forged = replace(worker, bound_run_id="run_somewhere_else")
    publish = svc.execute(
        "publish_artifact",
        forged,
        {"run_id": run_id, "fencing_epoch": worker.bound_fencing_epoch, "path": "a.txt"},
    )
    assert not publish.ok
    append = svc.execute(
        "context_append",
        forged,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "reason": "user_addition",
            "authorized_ref": "a.txt",
        },
    )
    assert not append.ok

    forged_actor = replace(worker, actor_id="someone_else")
    assert not svc.execute(
        "publish_artifact",
        forged_actor,
        {"run_id": run_id, "fencing_epoch": worker.bound_fencing_epoch, "path": "a.txt"},
    ).ok


# ------------------------------------------- P2: usage accounting is bound and sane


def test_p2_model_usage_requires_a_run_bound_internal_credential(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"], limits={"max_model_calls": 5})
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    worker = run_auth(svc, run_id)

    # A User Agent cannot spend the budget at all.
    forged = svc.execute(
        "record_model_usage",
        user_agent_auth(),
        {"task_id": task_id, "run_id": run_id, "calls": 1},
    )
    assert not forged.ok

    # Negative usage cannot be used to reset the budget.
    negative = svc.execute(
        "record_model_usage",
        worker,
        {"task_id": task_id, "run_id": run_id, "calls": -500},
    )
    assert not negative.ok
    assert svc.get_task(task_id)["model_calls_used"] >= 0

    # A credential bound to another Run cannot spend this Task's budget.
    other_run = "run_not_real"
    foreign = svc.execute(
        "record_model_usage",
        worker,
        {"task_id": task_id, "run_id": other_run, "calls": 1},
    )
    assert not foreign.ok


# --------------------------------------------- P2: wall clock and empty completion


def test_p2_empty_model_completion_is_blocked_not_passed(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])
    adapter = _adapter(svc, ScriptedClient([_final("")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    row = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    assert row["result_json"], "a result must still be recorded"
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"
    assert "empty" in str(result.get("error_class") or "").lower()


def test_p2_run_wall_timeout_bounds_the_worker_loop(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"], limits={"wall_timeout_seconds": 1})

    class SlowClient:
        def __init__(self) -> None:
            self.calls = 0

        def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
            self.calls += 1
            time.sleep(1.2)
            return ModelReply(
                content=None,
                tool_calls=(_tool_call("fs.list", {"path": "."}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            )

        def close(self) -> None:
            return None

    client = SlowClient()
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    final = adapter.wait_for_exit(run_id, 15.0)
    assert final["alive"] is False
    row = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    assert row["result_json"]
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert client.calls <= 3, "the wall clock must stop the loop, not the turn budget"


# ------------------------------------------------- P2: lost-run policy is live


def test_p2_lost_run_with_a_possible_effect_blocks_instead_of_retrying(tmp_path):
    """A LOST Run that already published must not be silently retried."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "artifact.publish"])

    class Hanging:
        def __init__(self) -> None:
            self.entered = threading.Event()
            self.release = threading.Event()

        def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
            self.entered.set()
            # Honour the per-call cap the adapter passes, like the real client.
            if self.release.wait(timeout_s or 30):
                return _final("late completion")
            raise TimeoutError("model call timed out")

        def close(self) -> None:
            return None

    client = Hanging()
    adapter = _adapter(svc, client)
    # Keep the abort fast so the simulated crash is quick.
    adapter.stop_model_call_timeout_s = 0.5
    run_id = _dispatch(svc, adapter, task_id)
    assert client.entered.wait(5.0), "the worker never started"
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"out.txt": "content"})
    worker = run_auth(svc, run_id)
    published = svc.execute(
        "publish_artifact",
        worker,
        {"run_id": run_id, "fencing_epoch": worker.bound_fencing_epoch, "path": "out.txt"},
    )
    assert published.ok, published
    assert _run_status(svc, task_id, run_id) == "RUNNING"

    # Simulate a hard crash: the executor's thread dies while the database is restored
    # to the state a killed process leaves behind (Run RUNNING, Work Unit RUNNING).
    adapter.stop(run_id, "simulated_crash")
    adapter.wait_for_exit(run_id, 10.0)
    assert adapter.inspect(run_id)["alive"] is False

    def _crash_window(session):
        from hibiki.persistence.models import AgentRunRow

        run = session.get(AgentRunRow, run_id)
        run.status = "RUNNING"
        run.finished_at = None
        run.result_json = None
        run.terminal_reason = None
        wu = session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first()
        wu.status = "RUNNING"
        wu.active_run_id = run_id
        wu.blocked_reason = None

    svc.executor.run(_crash_window)
    svc.reconcile()

    def _wu(session):
        wu = session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first()
        return {"status": wu.status, "reason": wu.blocked_reason}

    state = svc.executor.run(_wu)
    assert state["status"] == "BLOCKED", state
    assert state["reason"] == "lost_run_effect_unknown", state
    # A Task that may have produced an effect waits for a human and dispatches nothing.
    assert svc.get_task(task_id)["state"] == "WAITING_HUMAN"
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    # Either the state gate refuses the dispatch outright or it dispatches nothing.
    assert (not r.ok and r.error_code == "dispatch_blocked_state") or (
        r.ok and r.data["created_runs"] == []
    ), r
    assert len(svc.list_runs(task_id)) == 1
    client.release.set()
    adapter.wait_for_exit(run_id, 10.0)


# ------------------------------------------------------------- P2: context sizing


def test_p2_dependency_result_bytes_count_towards_the_context_budget(tmp_path):
    from hibiki.application.service import _mandatory_context_bytes

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "fs.write"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"big.txt": "x" * 4096})
    worker = run_auth(svc, run_id)
    published = svc.execute(
        "publish_artifact",
        worker,
        {"run_id": run_id, "fencing_epoch": worker.bound_fencing_epoch, "path": "big.txt"},
    )
    assert published.ok, published

    refs = [
        {
            "kind": "dependency_result",
            "work_unit_id": "wu_upstream",
            "ref": "result-1",
            "hash": published.data["artifact_hash"],
            "required": True,
        }
    ]

    def _measure(session):
        return _mandatory_context_bytes(session, [], refs, svc.workspace_root, None, task_id)

    assert svc.executor.run(_measure) >= 4096


# ------------------------------------------- P1: fabricated references are not deliveries


def _publish_client() -> ScriptedClient:
    """A model script that publishes out.txt and then finishes."""
    publish = ModelReply(
        content=None,
        tool_calls=(_tool_call("artifact.publish", {"path": "out.txt"}),),
        finish_reason="tool_calls",
        usage={},
        raw={},
    )
    return ScriptedClient([publish, _final("published out.txt")])


def test_p1_published_artifact_is_a_verified_delivery(tmp_path):
    """A published artifact becomes the Work Unit's verified delivery."""
    import hashlib

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "artifact.publish"])
    workspace = Path(svc.workspace_root) / f"ws_{_work_unit_id(svc, task_id)}"
    _seed(workspace, {"out.txt": "real"})
    digest = hashlib.sha256(b"real").hexdigest()

    adapter = _adapter(svc, _publish_client())
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    def _wu(session):
        wu = session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first()
        return {"hash": wu.verified_artifact_hash, "verdict": wu.selected_verdict}

    state = svc.executor.run(_wu)
    assert state["hash"] == digest, state
    assert state["verdict"] == "PASS"
    assert svc.verify_artifact_content(task_id, digest)["verified"] is True


def test_p1_fabricated_artifact_reference_is_never_a_verified_delivery(tmp_path):
    """A hash the Core has no bytes for must not become the Work Unit's delivery.

    The worker publishes a real artifact, then a late submission claims a fabricated
    hash as its deliverable. The real digest stays the verified delivery; the claim is
    registered for audit but verifies as unbacked.
    """
    import hashlib

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "artifact.publish"])
    workspace = Path(svc.workspace_root) / f"ws_{_work_unit_id(svc, task_id)}"
    _seed(workspace, {"out.txt": "real"})
    real = hashlib.sha256(b"real").hexdigest()

    adapter = _adapter(svc, _publish_client())
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    worker = run_auth(svc, run_id)
    fabricated = "a" * 64

    r = svc.execute(
        "submit_result",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": [fabricated],
                "acceptance_evidence": [
                    {"criterion_id": "c1", "artifact_hash": fabricated, "verdict": "PASS"}
                ],
            },
        },
    )
    assert r.ok or r.error_code in {"run_terminal", "invalid_transition"}, r

    def _wu(session):
        return session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first().verified_artifact_hash

    # Whatever the submission did, the verified delivery is the published content and
    # never the fabricated hash.
    assert svc.executor.run(_wu) == real
    # The fabricated hash is not a registered delivery at all, so nothing can verify it.
    try:
        check = svc.verify_artifact_content(task_id, fabricated)
    except Exception:  # noqa: BLE001 - unregistered is the expected outcome
        check = {"verified": False}
    assert check["verified"] is False
