"""Regression tests for the M1 P1/P2 findings (artifact isolation, stop confirm,
acceptance evidence, materialized context, ContextAppend consistency, tool deadlines).
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path

import pytest

from hibiki.persistence.models import AgentRunRow, WorkUnitExecutionRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.artifacts import LocalArtifactStore, artifact_digest
from hibiki.runtime.openai_client import ModelReply
from hibiki.tools.broker import ToolBroker
from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec
from sqlalchemy import select
from tests.helpers import human_auth, make_core, run_auth
from tests.runtime.test_api_agent import ScriptedClient, _final, _tool_call


def _task(svc, auth, tools: list[str], *, limits: dict | None = None, **contract_extra) -> str:
    r = svc.execute("create_task", auth, {"title": "p1-fix"})
    task_id = r.data["task_id"]
    payload = {
        "task_id": task_id,
        "objective": "work",
        "permission_ceiling": {"tools": tools},
        **contract_extra,
    }
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
    return Path(svc.get_run_input(run_auth(svc, run_id), run_id)["workspace_path"])


# ---------------------------------------------------------------- P1: artifact URI


def test_artifact_store_refuses_path_traversal(tmp_path):
    store = LocalArtifactStore(tmp_path / "artifacts")
    marker = tmp_path / "review-host-marker.txt"
    marker.write_text("SECRET", encoding="utf-8")
    with pytest.raises(ValueError):
        artifact_digest("artifact://../review-host-marker.txt")
    with pytest.raises(ValueError):
        store.get("artifact://../review-host-marker.txt")
    with pytest.raises(ValueError):
        store.get("artifact:///etc/passwd")
    assert store.exists("artifact://../review-host-marker.txt") is False


def test_context_append_refuses_foreign_and_traversal_artifact_uris(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_a = _task(svc, auth, ["fs.read", "fs.write", "artifact.publish"])
    task_b = _task(svc, auth, ["fs.read", "fs.write", "artifact.publish"])

    adapter = _adapter(svc, ScriptedClient([_final("hold")]))
    run_a = _dispatch(svc, adapter, task_a)
    adapter.wait_for_exit(run_a, timeout=5)

    # Seed a real artifact on task B.
    adapter_b = _adapter(
        svc,
        ScriptedClient(
            [
                ModelReply(
                    content=None,
                    tool_calls=(
                        _tool_call("fs_write", {"path": "out.txt", "content": "owned-by-b"}),
                    ),
                    finish_reason="tool_calls",
                    usage={},
                    raw={},
                ),
                ModelReply(
                    content=None,
                    tool_calls=(_tool_call("artifact_publish", {"path": "out.txt"}),),
                    finish_reason="tool_calls",
                    usage={},
                    raw={},
                ),
                _final("published"),
            ]
        ),
    )
    # Re-dispatch isn't available after DONE; publish via a fresh task path instead.
    ws = _workspace(svc, run_a)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "local.txt").write_text("local", encoding="utf-8")

    worker = run_auth(svc, run_a)
    # Absolute / traversal URI
    bad = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_a,
            "fencing_epoch": worker.bound_fencing_epoch,
            "reason": "probe",
            "authorized_ref": "artifact://../review-host-marker.txt",
        },
    )
    assert not bad.ok
    assert bad.error_code in {"context_ref_refused", "artifact_uri_invalid"}

    # Cross-task: put bytes in the store under a digest and register only on task_b.
    digest = hashlib.sha256(b"foreign").hexdigest()
    store = svc._artifact_store()
    uri = store.put(b"foreign", content_hash=digest)
    from hibiki.persistence.models import ArtifactRow

    def _register(session):
        session.add(
            ArtifactRow(
                task_id=task_b,
                artifact_hash=digest,
                artifact_uri=uri,
                size_bytes=7,
                created_at=svc.clock.now(),
            )
        )

    svc.executor.run(_register)
    cross = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_a,
            "fencing_epoch": worker.bound_fencing_epoch,
            "reason": "probe",
            "authorized_ref": uri,
        },
    )
    assert not cross.ok
    assert cross.error_code in {"context_ref_refused", "artifact_not_owned"}


# ---------------------------------------------------------------- P1: empty work is not PASS


def test_expected_outputs_without_artifacts_are_blocked(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "need-artifact"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "write summary.txt",
            "permission_ceiling": {"tools": ["fs.read", "fs.write", "artifact.publish"]},
            "acceptance_criteria": [
                {
                    "criterion_id": "c1",
                    "statement": "summary exists",
                    "evidence_kind": "artifact",
                    "required": True,
                }
            ],
            "deliverables": [
                {"deliverable_id": "d1", "description": "summary", "expected_kind": "text"}
            ],
        },
    )
    assert r.ok, r
    assert svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    ).ok
    assert svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {
                    "work_unit_id": f"{task_id}_wu",
                    "spec_version": 1,
                    "work_type": "EXECUTE",
                    "expected_outputs": ["summary.txt"],
                    "acceptance_criteria": [
                        {
                            "criterion_id": "c1",
                            "statement": "summary exists",
                            "evidence_kind": "artifact",
                            "required": True,
                        }
                    ],
                }
            ],
            "edges": [],
        },
    ).ok

    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, timeout=5)
    row = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"
    assert result["acceptance_evidence"] == []
    assert "missing_expected" in (result.get("error_class") or "")


# ---------------------------------------------------------------- P1: materialized context + dependency direction


def test_acceptance_criteria_and_contract_enter_worker_prompt(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(
        svc,
        auth,
        ["fs.read"],
        acceptance_criteria=[
            {
                "criterion_id": "c_k1",
                "statement": "summary.txt covers all three input sentences",
                "evidence_kind": "artifact",
                "required": True,
            }
        ],
    )
    client = ScriptedClient([_final("ok")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, timeout=5)
    assert client.calls, "model was never called"
    prompt = "\n".join(
        str(getattr(m, "content", m) or "") for m in client.calls[0]
    )
    assert "c_k1" in prompt
    assert "summary.txt covers all three input sentences" in prompt
    assert "Authorized context:" in prompt
    assert '"objective"' in prompt or "objective" in prompt.lower()


def test_dependency_result_refs_use_incoming_edges(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "producer-consumer"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "chain",
            "permission_ceiling": {"tools": ["fs.read", "fs.write", "artifact.publish"]},
        },
    )
    assert r.ok, r
    assert svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    ).ok
    producer = f"{task_id}_prod"
    consumer = f"{task_id}_cons"
    assert svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {"work_unit_id": producer, "spec_version": 1, "work_type": "EXECUTE"},
                {"work_unit_id": consumer, "spec_version": 1, "work_type": "EXECUTE"},
            ],
            "edges": [
                {
                    "from_work_unit_id": producer,
                    "to_work_unit_id": consumer,
                    "predicate": "DONE",
                }
            ],
        },
    ).ok

    # Mark producer DONE with a verified artifact so the consumer manifest can see it.
    digest = hashlib.sha256(b"upstream").hexdigest()
    store = svc._artifact_store()
    uri = store.put(b"upstream", content_hash=digest)

    def _seed(session):
        from hibiki.domain.enums import WorkUnitStatus
        from hibiki.persistence.models import ArtifactRow

        wu = session.get(WorkUnitExecutionRow, producer)
        wu.status = WorkUnitStatus.DONE
        wu.selected_result_ref = "res_up"
        wu.verified_artifact_hash = digest
        session.add(
            ArtifactRow(
                task_id=task_id,
                artifact_hash=digest,
                artifact_uri=uri,
                size_bytes=8,
                work_unit_id=producer,
                created_at=svc.clock.now(),
            )
        )

    svc.executor.run(_seed)

    adapter = _adapter(svc, ScriptedClient([_final("hold")]))
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    # Only the producer was DONE; consumer should now be dispatchable.
    # If producer was already DONE before dispatch, consumer is the new run.
    run_id = r.data["created_runs"][-1]
    adapter.wait_for_exit(run_id, timeout=5)
    ctx = svc.get_run_context(run_auth(svc, run_id), run_id)
    deps = ctx["manifest"]["dependency_result_refs"]
    assert any(d.get("work_unit_id") == producer for d in deps), deps


# ---------------------------------------------------------------- P1: ContextAppend consistency


def test_context_append_hash_mismatch_is_refused_for_artifacts(tmp_path):
    """Pinned ContextAppend hashes must match the bytes Core re-reads."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])
    adapter = _adapter(svc, ScriptedClient([_final("hold")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, timeout=5)

    content = b"immutable-context-bytes"
    digest = hashlib.sha256(content).hexdigest()
    uri = svc._artifact_store().put(content, content_hash=digest)

    def _register(session):
        from hibiki.persistence.models import ArtifactRow

        session.add(
            ArtifactRow(
                task_id=task_id,
                artifact_hash=digest,
                artifact_uri=uri,
                size_bytes=len(content),
                created_at=svc.clock.now(),
            )
        )

    svc.executor.run(_register)
    worker = run_auth(svc, run_id)
    ok = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "reason": "fixture",
            "authorized_ref": uri,
        },
    )
    assert ok.ok, ok
    # Forge a stale hash on the recorded append and refuse re-materialization.
    forged = {
        "authorized_ref": uri,
        "materialized_hash": "0" * 64,
        "content_hash": "0" * 64,
    }
    loaded = svc.materialize_context_append(worker, run_id, forged)
    assert loaded["ok"] is False
    assert loaded["error"] == "hash_mismatch"


@pytest.mark.skipif(
    os.name == "nt",
    reason="WorkspacePaths dir_fd walk requires POSIX open flags",
)
def test_fs_read_registers_context_append(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read", "fs.write"])
    wu_id = svc.executor.run(
        lambda s: s.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        )
        .first()
        .work_unit_id
    )
    ws = Path(svc.workspace_root) / f"ws_{wu_id}"
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "note.txt").write_text("original", encoding="utf-8")

    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("fs_read", {"path": "note.txt"}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("read"),
        ]
    )
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, timeout=5)

    appends = svc.get_run_context(run_auth(svc, run_id), run_id)["appends"]
    assert any(a.get("reason") == "tool_fs_read" for a in appends), appends


# ---------------------------------------------------------------- P1: stop confirmation


def test_failed_docker_kill_leaves_exit_unconfirmed(tmp_path):
    """A simulated docker CLI that fails ``kill`` must not report a clean cancel."""
    import sys

    marker = tmp_path / "kill-requested"
    script = tmp_path / "fake_docker.py"
    script.write_text(
        f"""
import sys, time, pathlib
marker = pathlib.Path(r"{marker}")
args = sys.argv[1:]
if args and args[0] == "kill":
    marker.write_text("1", encoding="utf-8")
    sys.exit(1)
if args and args[0] == "inspect":
    print("false" if marker.exists() else "true")
    sys.exit(0)
if args and args[0] == "run":
    if "--cidfile" in args:
        cidfile = args[args.index("--cidfile") + 1]
        pathlib.Path(cidfile).write_text("deadbeefcafebabe", encoding="utf-8")
    for _ in range(300):
        if marker.exists():
            # Still "running" from inspect's perspective until we exit; sleep a bit
            # so the first inspect after kill still sees us as alive if needed.
            time.sleep(0.05)
            sys.exit(0)
        time.sleep(0.05)
    sys.exit(0)
sys.exit(0)
""",
        encoding="utf-8",
    )
    docker_bin = tmp_path / ("fake-docker.cmd" if sys.platform == "win32" else "fake-docker")
    if sys.platform == "win32":
        docker_bin.write_text(
            f'@echo off\r\n"{sys.executable}" "{script}" %*\r\n',
            encoding="utf-8",
        )
    else:
        docker_bin.write_text(
            f"#!/bin/sh\nexec '{sys.executable}' '{script}' \"$@\"\n",
            encoding="utf-8",
        )
        docker_bin.chmod(0o755)

    workspace = tmp_path / "ws"
    workspace.mkdir()
    sandbox = DockerSandboxAdapter(
        SandboxSpec(
            image="unused",
            workspace_host_path=str(workspace),
            limits=SandboxLimits(wall_timeout_s=60, stop_grace_s=1),
        ),
        docker_bin=str(docker_bin),
    )
    cancel = threading.Event()

    def _cancel_soon():
        time.sleep(0.2)
        cancel.set()

    threading.Thread(target=_cancel_soon, daemon=True).start()
    result = sandbox.execute({"argv": ["true"], "cancel_event": cancel})
    assert result["status"] == "stop_unconfirmed"
    assert result["exit_confirmed"] is False
    assert result["container_id"] == "deadbeefcafebabe"


def test_reconcile_quarantines_unconfirmed_sandbox_exit(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(svc, auth, ["fs.read"])

    hang = threading.Event()

    class Hanging:
        def chat(self, messages, **kwargs):
            hang.wait(timeout=10)
            return _final("late")

        def close(self) -> None:
            return None

    adapter = _adapter(svc, Hanging())
    run_id = _dispatch(svc, adapter, task_id)
    time.sleep(0.1)

    def _mark(session):
        run = session.get(AgentRunRow, run_id)
        run.sandbox_container_id = "lingering"
        run.sandbox_exit_unconfirmed = True

    svc.executor.run(_mark)
    notes = svc.reconcile()["notes"]
    hang.set()
    assert any("quarantine_unconfirmed" in n for n in notes), notes
    workspace_id = svc.executor.run(
        lambda s: s.get(AgentRunRow, run_id).workspace_id
    )
    ws = svc.get_workspace(workspace_id)
    assert ws["state"] == "QUARANTINED"
    assert ws["writer_alive"] is True


# ---------------------------------------------------------------- P2: tool deadline


def test_run_wall_budget_is_checked_before_each_tool(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task(
        svc,
        auth,
        ["fs.read", "fs.write", "shell.run"],
        limits={"wall_timeout_seconds": 1},
    )

    class SlowThenWrite:
        def __init__(self) -> None:
            self.calls = 0
            self.timeouts: list[float | None] = []

        def execute(self, command):
            self.calls += 1
            self.timeouts.append(command.get("timeout_s"))
            time.sleep(1.4)
            return {
                "status": "ok",
                "exit_code": 0,
                "stdout": "",
                "stderr": "",
                "container_id": None,
                "exit_confirmed": True,
                "oom_killed": False,
                "duration_ms": 1400,
            }

    slow = SlowThenWrite()
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(
                    _tool_call("shell_run", {"command": "sleep 2"}, "c1"),
                    _tool_call("fs_write", {"path": "after.txt", "content": "nope"}, "c2"),
                ),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("should-not-matter"),
        ]
    )
    adapter = _adapter(svc, client, sandbox=slow)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, timeout=10)
    # Second tool in the same turn must not succeed after the wall is spent.
    ws = _workspace(svc, run_id)
    assert not (ws / "after.txt").exists()
    row = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    result = json.loads(row["result_json"] or "{}")
    assert result.get("outcome") == "BLOCKED"
    assert "run_wall_timeout" in (result.get("error_class") or "")
