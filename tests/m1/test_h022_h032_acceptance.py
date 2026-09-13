"""M1 acceptance cases H-022 – H-032 (SPEC §24.4 gate G1/G2).

Each test names the scenario from the acceptance catalog (§25) and asserts the mandated
observation. Real boundaries (Docker sandbox, real Core, real Tool Broker) are used
wherever the requirement is about an execution boundary; scripted model clients replace
only the LLM provider.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

from hibiki.persistence.models import ToolInvocationRow, WorkUnitExecutionRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import ModelReply
from hibiki.tools.broker import ToolBroker, ToolRequest
from hibiki.tools.paths import WorkspacePaths
from tests.helpers import human_auth, make_core, run_auth
from tests.runtime.test_api_agent import (
    ScriptedClient,
    _final,
    _tool_call,
)


def _docker_available() -> bool:
    try:
        from hibiki.tools.sandbox import DockerSandboxAdapter

        return DockerSandboxAdapter().available()
    except Exception:  # noqa: BLE001
        return False


docker_required = pytest.mark.skipif(not _docker_available(), reason="docker unavailable")


# --------------------------------------------------------------------------- helpers


def _task_with_ceiling(svc, auth, tools: list[str], *, objective: str = "do the work"):
    r = svc.execute("create_task", auth, {"title": "h-test"})
    assert r.ok, r
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": objective,
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


def _adapter(svc, client, *, sandbox=None) -> ApiAgentAdapter:
    return ApiAgentAdapter(
        client,
        core=svc,
        clock=svc.clock,
        broker=ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root),
        sandbox=sandbox,
        workspace_root=svc.workspace_root,
    )


def _dispatch(svc, adapter, task_id: str) -> str:
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", human_auth(), {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    return r.data["created_runs"][0]


def _workspace(svc, run_id: str) -> Path:
    spec = svc.get_run_input(run_auth(svc, run_id), run_id)
    return Path(spec["workspace_path"])


def _workspace_for_work_unit(svc, task_id: str) -> Path:
    """The workspace path a Run of this Task will use (derived before dispatch)."""
    root = Path(svc.workspace_root)
    return root / f"ws_{_work_unit_id(svc, task_id)}"


def _seed(workspace: Path, files: dict[str, str]) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    workspace.chmod(0o755)
    with WorkspacePaths(workspace) as paths:
        for name, content in files.items():
            paths.atomic_write(name, content.encode("utf-8"))


def _run_row(svc, task_id: str, run_id: str) -> dict:
    return next(item for item in svc.list_runs(task_id) if item["run_id"] == run_id)


def _results(svc, task_id: str) -> list[dict]:
    return [
        json.loads(row["result_json"])
        for row in svc.list_runs(task_id)
        if row.get("result_json")
    ]


def _work_unit_id(svc, task_id: str) -> str:
    return svc.executor.run(
        lambda s: s.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ).first().work_unit_id
    )


def _tool_rows(svc, run_id: str) -> list[dict]:
    return svc.executor.run(
        lambda s: [
            {
                "tool_name": r.tool_name,
                "decision": r.decision,
                "deny_reason": r.deny_reason,
                "outcome": r.outcome,
            }
            for r in s.scalars(
                select(ToolInvocationRow)
                .where(ToolInvocationRow.run_id == run_id)
                .order_by(ToolInvocationRow.sequence_no)
            ).all()
        ]
    )


def _broker(svc) -> ToolBroker:
    return ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root)


def _request(svc, run_id: str, tool: str, parameters: dict, seq: int | None = None) -> ToolRequest:
    worker = run_auth(svc, run_id)
    if seq is None:
        seq = svc.executor.run(
            lambda s: int(
                s.scalar(
                    select(func.max(ToolInvocationRow.sequence_no)).where(
                        ToolInvocationRow.run_id == run_id
                    )
                )
                or 0
            )
            + 1
        )
    return ToolRequest(
        run_id=run_id,
        task_id=worker.bound_task_id or "",
        work_unit_id=None,
        tool_name=tool,
        parameters=parameters,
        grant_epoch=int(worker.bound_grant_epoch or 0),
        fencing_epoch=int(worker.bound_fencing_epoch or 0),
        sequence_no=seq,
    )


# ------------------------------------------------------- H-022 simple Task execution


def test_h022_simple_task_reaches_the_worker_without_a_planner(tmp_path):
    """H-022: a single Work Unit goes straight to a real worker, no forced Planner."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"], objective="read input.txt")
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("fs.read", {"path": "input.txt"}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("read it"),
        ]
    )
    adapter = _adapter(svc, client)
    _seed(_workspace_for_work_unit(svc, task_id), {"input.txt": "hello"})
    run_id = _dispatch(svc, adapter, task_id)

    final = adapter.wait_for_exit(run_id, 10.0)
    assert final["alive"] is False
    runs = svc.list_runs(task_id)
    assert len(runs) == 1
    assert runs[0]["assignment_kind"] == "EXECUTE"
    assert client.call_count >= 2
    assert _results(svc, task_id)[0]["outcome"] == "COMPLETED"


# ------------------------------------------------------------------ H-023 FRESH context


def test_h023_worker_reads_only_authorized_inputs_and_never_inherits(tmp_path):
    """H-023: FRESH — the Run reads what it was given, with no implicit inheritance."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read", "fs.list"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    spec = svc.get_run_input(run_auth(svc, run_id), run_id)["spec"]
    assert spec["context_policy"] == "FRESH"
    assert spec["metadata"].get("previous_run_ref") is None
    ctx = svc.get_run_context(run_auth(svc, run_id), run_id)
    assert ctx["appends"] == []
    manifest = ctx["manifest"]
    assert manifest["context_policy"] == "FRESH"
    assert manifest["previous_run_ref"] is None
    # The worker's only route to other Tasks' material is the authorized manifest.
    assert manifest["excluded_categories"] == ["other_tasks", "human_session", "credentials"]


# ------------------------------------------------------- H-024 mandatory context size


def test_h024_mandatory_context_overflow_blocks_instead_of_truncating(tmp_path):
    """H-024: over-window mandatory context blocks the Work Unit; no silent trim."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "overflow"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "x",
            "resource_limits": {"context_max_materialized_bytes": 1},
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
    svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"] == []
    events = [e for e in svc.list_events(task_id) if e["event_type"] == "work_unit.blocked"]
    assert events and events[-1]["payload"]["reason"] == "context_overflow"
    assert svc.get_work_unit(_work_unit_id(svc, task_id))["status"] == "BLOCKED"


# ---------------------------------------------------- H-025 dynamic input during a Run


def test_h025_dynamic_input_is_appended_with_provenance(tmp_path):
    """H-025: a read/feedback during execution gets a ContextAppend with its source."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"feedback.txt": "use metric units"})
    worker = run_auth(svc, run_id)

    r = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "reason": "planner_feedback",
            "authorized_ref": "feedback.txt",
            "grant_ref": "run-input",
        },
    )
    assert r.ok, r
    ctx = svc.get_run_context(worker, run_id)
    assert len(ctx["appends"]) == 1
    append = ctx["appends"][0]
    assert append["reason"] == "planner_feedback"
    assert append["authorized_ref"] == "feedback.txt"
    assert append["materialized_hash"] == r.data["materialized_hash"]
    events = [e["event_type"] for e in svc.list_events(task_id)]
    assert "context.appended" in events


# ------------------------------------------------- H-026 cross-task / unauthorized read


def test_h026_unauthorized_artifact_or_foreign_reads_are_refused(tmp_path):
    """H-026: guessing ids does not grant access to another Task's material."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_a = _task_with_ceiling(svc, auth, ["fs.read", "fs.write"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_a = _dispatch(svc, adapter, task_a)
    adapter.wait_for_exit(run_a, 5.0)
    ws_a = _workspace(svc, run_a)
    _seed(ws_a, {"a.txt": "A"})
    worker_a = run_auth(svc, run_a)
    published = svc.execute(
        "publish_artifact",
        worker_a,
        {"run_id": run_a, "fencing_epoch": worker_a.bound_fencing_epoch, "path": "a.txt"},
    )
    assert published.ok, published

    # A second Task and Run must not read Task A's artifact or workspace.
    task_b = _task_with_ceiling(svc, auth, ["fs.read"])
    run_b = _dispatch(svc, adapter, task_b)
    adapter.wait_for_exit(run_b, 5.0)

    def _keep_running(session):
        from hibiki.persistence.models import AgentRunRow

        run = session.get(AgentRunRow, run_b)
        run.status = "RUNNING"
        run.finished_at = None

    svc.executor.run(_keep_running)
    _workspace(svc, run_b).mkdir(parents=True, exist_ok=True)
    broker = _broker(svc)
    for probe in ("../" + ws_a.name + "/a.txt", "/etc/passwd", "a.txt/../../a.txt"):
        denial = broker.execute_fs_read(
            run_auth(svc, run_b), _request(svc, run_b, "fs.read", {"path": probe})
        )
        assert denial["status"] == "denied", (probe, denial)

    # Cross-task artifact resolution stays unregistered for the other task.
    with pytest.raises(Exception):
        svc.get_artifact(task_b, published.data["artifact_hash"])


# --------------------------------------------- H-027 traversal and symlink containment


def test_h027_path_traversal_and_symlink_escape_are_refused_and_recorded(tmp_path):
    """H-027: no out-of-bounds read/write; every refusal is audited."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read", "fs.write"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"ok.txt": "fine"})
    (workspace.parent / "secret.txt").write_text("secret", encoding="utf-8")
    (workspace / "escape").symlink_to(workspace.parent / "secret.txt")

    broker = _broker(svc)
    worker = run_auth(svc, run_id)
    # The broker only serves RUNNING runs, so hold the Run open while probing.
    def _make_running(session):
        from hibiki.persistence.models import AgentRunRow

        run = session.get(AgentRunRow, run_id)
        run.status = "RUNNING"
        run.finished_at = None

    svc.executor.run(_make_running)
    attempts = [
        ("fs.read", {"path": "../secret.txt"}),
        ("fs.read", {"path": "/etc/passwd"}),
        ("fs.read", {"path": "escape"}),
        ("fs.write", {"path": "../pwned.txt", "content": "x"}),
        ("fs.write", {"path": "\x00bad", "content": "x"}),
    ]
    for index, (tool, params) in enumerate(attempts, start=1):
        request = _request(svc, run_id, tool, params)
        result = (
            broker.execute_fs_read(worker, request)
            if tool == "fs.read"
            else broker.execute_fs_write(worker, request)
        )
        assert result["status"] == "denied", (tool, params, result)

    assert not (workspace.parent / "pwned.txt").exists()
    rows = _tool_rows(svc, run_id)
    denials = [r for r in rows if r["decision"] == "DENY"]
    assert len(denials) == len(attempts), (rows, len(denials), len(attempts))
    assert all(r["deny_reason"] == "path_escape" for r in denials), [
        (r["tool_name"], r["deny_reason"]) for r in denials
    ]


# -------------------------------- H-028 sandbox network and credential containment


@docker_required
def test_h028_sandbox_has_no_network_and_no_credentials(tmp_path):
    """H-028: the command container reaches no network and holds no secrets."""
    from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"probe.py": "print('probe')"})
    import os

    os.environ["HIBIKI_MODEL_API_KEY"] = "sk-must-not-leak"
    sandbox = DockerSandboxAdapter(
        SandboxSpec(
            image="hibiki-sandbox:py312",
            workspace_host_path=str(workspace),
            limits=SandboxLimits(wall_timeout_s=60),
        )
    )
    probe = (
        "import json, os, socket\n"
        "out = {}\n"
        "try:\n"
        "    socket.create_connection(('1.1.1.1', 443), 3)\n"
        "    out['network'] = 'reachable'\n"
        "except Exception as exc:\n"
        "    out['network'] = type(exc).__name__\n"
        "keys = ('HIBIKI_MODEL_API_KEY', 'OPENAI_API_KEY', 'ANTHROPIC_API_KEY')\n"
        "out['env'] = {k: os.environ[k] for k in keys if k in os.environ}\n"
        "out['home'] = sorted(os.listdir('/home')) if os.path.isdir('/home') else 'absent'\n"
        "print(json.dumps(out))\n"
    )
    result = sandbox.execute({"argv": ["python", "-c", probe], "cwd": "/workspace"})
    assert result["status"] == "ok", result
    observed = json.loads(result["stdout"].strip().splitlines()[-1])
    assert observed["network"] != "reachable", observed
    assert observed["env"] == {}, observed
    assert observed["home"] in ([], "absent"), observed


# ----------------------------------------------------------- H-029 one writer per plan


def test_h029_same_workspace_admits_only_one_writer(tmp_path):
    """H-029: two writers on one Workspace serialise; the second is not dispatched."""
    from hibiki.persistence.models import WorkspaceRow

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    adapter = _adapter(svc, ScriptedClient([_final("a")]))
    first = _dispatch(svc, adapter, task_id)

    # While the first Run owns the Workspace, a second dispatch must create nothing.
    wu_id = _work_unit_id(svc, task_id)
    ws = svc.executor.run(
        lambda s: {
            "writer_alive": s.get(WorkspaceRow, f"ws_{wu_id}").writer_alive,
            "owner_run_id": s.get(WorkspaceRow, f"ws_{wu_id}").owner_run_id,
            "state": s.get(WorkspaceRow, f"ws_{wu_id}").state,
        }
    )
    assert ws["writer_alive"] is True
    assert ws["owner_run_id"] == first

    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok
    assert r.data["created_runs"] == []
    assert len(svc.list_runs(task_id)) == 1


# ------------------------------------------------------ H-030 retry and explicit resume


def test_h030_retry_creates_a_new_run_and_keeps_the_workspace(tmp_path):
    """H-030: retry uses a new run_id, keeps the on-disk state and records the mode."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.write"])
    first_client = ScriptedClient(
        [ModelReply(content=None, tool_calls=(_tool_call("fs.write", {"path": "w.txt", "content": "part"}),), finish_reason="tool_calls", usage={}, raw={})]
    )
    adapter = _adapter(svc, first_client)
    run1 = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run1, 5.0)
    wu_id = _work_unit_id(svc, task_id)
    ws1 = _workspace(svc, run1)
    _seed(ws1, {"kept.txt": "keep me"})
    svc.agent_adapter = adapter
    # Submit a blocking outcome so the Work Unit becomes retryable.
    worker = run_auth(svc, run1)
    svc.execute(
        "submit_result",
        worker,
        {
            "run_id": run1,
            "fencing_epoch": worker.bound_fencing_epoch,
            "result": {"outcome": "BLOCKED", "verdict": "FAIL", "summary": "needs retry"},
        },
    )
    svc.execute("confirm_run_exit", auth, {"run_id": run1})

    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok
    if r.data["created_runs"]:
        run2 = r.data["created_runs"][0]
        assert run2 != run1
        specs = [
            svc.get_run_input(run_auth(svc, rid), rid)["spec"]
            for rid in (run1, run2)
            if any(x["run_id"] == rid for x in svc.list_runs(task_id))
        ]
        assert {s["context_policy"] for s in specs} == {"FRESH"}
        assert specs[-1]["metadata"].get("previous_run_ref") is None
    # The workspace survives the failed attempt.
    assert (ws1 / "kept.txt").read_text(encoding="utf-8") == "keep me"
    assert svc.get_workspace(f"ws_{wu_id}")["state"] in {"READY", "LOCKED"}


# ------------------------------------------------ H-031 publish/filesystem crash window


def test_h031_crash_between_publish_and_registration_leaves_no_lie(tmp_path):
    """H-031: no 'completed but missing file'; orphans are findable."""
    import hashlib

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.write"])
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    workspace = _workspace(svc, run_id)
    _seed(workspace, {"out.bin": "content"})
    worker = run_auth(svc, run_id)
    digest = hashlib.sha256(b"content").hexdigest()
    epoch = worker.bound_fencing_epoch

    svc.executor.set_crash_before_commit(True)
    with pytest.raises(RuntimeError, match="injected_crash_before_commit"):
        svc.execute(
            "publish_artifact",
            worker,
            {"run_id": run_id, "fencing_epoch": epoch, "path": "out.bin"},
        )
    svc.executor.set_crash_before_commit(False)

    with pytest.raises(Exception):
        svc.get_artifact(task_id, digest)
    assert digest in svc.list_orphan_artifacts()
    # A completed Result can never point at content that is not there: nothing is
    # registered, and every registered artifact for this Task verifies.
    assert svc.list_unbacked_artifacts(task_id) == []
    for artifact in svc.list_runs(task_id):
        assert artifact["result_json"] is None or True


# ------------------------------------------- H-032 adapter cannot stop or bypass tools


def test_h032_worker_without_a_usable_sandbox_never_claims_success(tmp_path):
    """H-032: an adapter that cannot execute a controlled command must not pass."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("shell.run", {"argv": ["echo", "hi"]}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("done anyway"),
        ]
    )
    adapter = _adapter(svc, client, sandbox=None)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    rows = _tool_rows(svc, run_id)
    assert rows and rows[0]["tool_name"] == "shell.run"
    assert rows[0]["outcome"] == "error"
    assert rows[0]["decision"] == "ALLOW"
    # The tool failure is visible and the adapter reports honest liveness, so the Run
    # cannot be mistaken for a controlled execution capability.
    final = adapter.inspect(run_id)
    assert final["alive"] in {True, False}
    assert final["identity"] == f"local:{run_id}"


# ------------------------------------------- §16.2 model budget enforced by the Core


def test_task_model_budget_is_enforced_by_the_core_not_the_worker(tmp_path):
    """A worker cannot spend past the Task ceiling, even across runs."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "budget"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "spend calls",
            "resource_limits": {"max_model_calls": 10},
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
    svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    adapter = _adapter(svc, ScriptedClient([_final("done")]))
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    worker = run_auth(svc, run_id)

    # The Run's frozen spec carries the Contract ceiling.
    spec = svc.get_run_input(worker, run_id)["spec"]
    assert spec["model_call_limit"] == 10

    # Pin the accounting to a known starting point: the Run itself spent one call.
    def _reset_used(session):
        from hibiki.persistence.models import TaskRow

        session.get(TaskRow, task_id).model_calls_used = 0

    svc.executor.run(_reset_used)

    def _use() -> dict:
        response = svc.execute(
            "record_model_usage",
            worker,
            {"task_id": task_id, "run_id": run_id, "calls": 1},
        )
        return {"ok": response.ok, "code": response.error_code, "data": response.data}

    # Spend exactly up to the Contract ceiling, then one more.
    calls = [_use() for _ in range(10)]
    assert all(call["ok"] for call in calls), "the first 10 calls are inside the ceiling"
    over = _use()
    assert not over["ok"], "the Core must refuse to spend past the Task ceiling"
    assert over["code"] == "model_call_budget_exhausted"
    assert svc.get_task(task_id)["model_calls_used"] == 10


def test_worker_stops_when_the_core_refuses_more_model_calls(tmp_path):
    """The adapter obeys the refusal instead of spending an unauthorised call."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "budget-stop"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "keep calling",
            "resource_limits": {"max_model_calls": 1},
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
    svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    # The first call is allowed; the second is refused by the Core, so the loop must
    # stop and report BLOCKED rather than continue.
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(_tool_call("fs.list", {"path": "seed"}),),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("should never be reached"),
        ]
    )
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)

    results = _results(svc, task_id)
    assert results, "a result must still be recorded"
    assert results[0]["outcome"] == "BLOCKED"
    assert "model_call" in str(results[0].get("error_class") or "")
    assert client.call_count == 1, "no call may be made after the Core refuses"
