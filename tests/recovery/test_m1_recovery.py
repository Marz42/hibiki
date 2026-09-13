"""Task I — the three repeatable M1 recovery cases (SPEC §17 / §19.1).

Required by §24.4: a model request interrupted, a worker crash, and a crash during
artifact publication. Recovery may leave the Run LOST/FAILED and the Work Unit
retryable or BLOCKED, but it must never claim success and must never silently replay.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
from sqlalchemy import select

from hibiki.application.bootstrap import bootstrap_core
from hibiki.application.service import lost_run_recovery_policy
from hibiki.domain.transitions import is_terminal_run
from hibiki.persistence.models import AgentRunRow, WorkUnitExecutionRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import ModelClientError
from hibiki.tools.broker import ToolBroker
from tests.helpers import human_auth, make_core, run_auth
from tests.runtime.test_api_agent import ScriptedClient, _final


def _task_with_ceiling(svc, auth, tools: list[str]) -> str:
    r = svc.execute("create_task", auth, {"title": "recovery"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {"task_id": task_id, "objective": "do the work", "permission_ceiling": {"tools": tools}},
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


def _adapter(svc, client) -> ApiAgentAdapter:
    return ApiAgentAdapter(
        client,
        core=svc,
        clock=svc.clock,
        broker=ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root),
        workspace_root=svc.workspace_root,
    )


def _dispatch(svc, adapter, task_id: str) -> str:
    svc.agent_adapter = adapter
    r = svc.execute("dispatch_ready_runs", human_auth(), {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    return r.data["created_runs"][0]


def _run_row(svc, task_id: str, run_id: str) -> dict:
    return next(item for item in svc.list_runs(task_id) if item["run_id"] == run_id)


def _work_unit_ids(svc, task_id: str) -> list[str]:
    return svc.executor.run(
        lambda s: [
            wu.work_unit_id
            for wu in s.scalars(
                select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
            ).all()
        ]
    )


def test_recovery_model_request_interrupted_never_claims_success(tmp_path):
    """Case 1: the model request is interrupted, so no result may claim success."""
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])
    client = ScriptedClient(
        [ModelClientError("timeout", "model request interrupted", retryable=True)]
    )
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)

    final = adapter.wait_for_exit(run_id, 5.0)
    assert final["alive"] is False
    row = _run_row(svc, task_id, run_id)
    assert row["status"] != "RUNNING"
    # §8.3: the Run may be SUCCEEDED (it submitted a result) while the *result* is
    # BLOCKED. The Work Unit is what must not be marked done.
    assert row["result_json"], "an interrupted model call must still record a result"
    result = json.loads(row["result_json"])
    assert result["outcome"] == "BLOCKED"
    assert result["verdict"] == "FAIL"
    assert "ModelClientError" in str(result.get("error_class"))

    # The audit trail shows the failure, and the Work Unit is not marked done.
    notes = svc.reconcile()
    assert isinstance(notes["notes"], list)
    wu_id = _work_unit_ids(svc, task_id)[0]
    assert svc.get_work_unit(wu_id)["status"] != "DONE"

    # A restart keeps the same truth and adds no invented progress.
    ctx["lock"].release()
    svc2, ctx2 = bootstrap_core(tmp_path / "data", run_migrate=True)
    try:
        svc2.reconcile()
        assert svc2.get_work_unit(wu_id)["status"] != "DONE"
    finally:
        ctx2["lock"].release()


def test_recovery_worker_crash_marks_run_lost_and_recovers_the_work_unit(tmp_path):
    """Case 2: the executor disappears; reconcile marks LOST, never SUCCEEDED."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read"])

    class HangingClient:
        """Blocks in the model call until released, so the Run stays RUNNING."""

        def __init__(self) -> None:
            self.entered = threading.Event()
            self.release = threading.Event()

        def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
            self.entered.set()
            self.release.wait(30)
            raise RuntimeError("executor vanished")

        def close(self) -> None:
            return None

    client = HangingClient()
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    assert client.entered.wait(5.0), "the worker never reached the model call"
    assert _run_row(svc, task_id, run_id)["status"] == "RUNNING"

    # The executor "crashes": its thread dies while the database still believes the Run
    # is RUNNING — exactly what a killed process leaves behind.
    client.release.set()
    adapter.wait_for_exit(run_id, 5.0)
    assert adapter.inspect(run_id)["alive"] is False

    # Erase any result the dying worker managed to submit: a hard kill submits nothing.
    def _hard_kill(session):
        run = session.get(AgentRunRow, run_id)
        run.status = "RUNNING"
        run.finished_at = None
        run.result_json = None
        run.terminal_reason = None

    svc.executor.run(_hard_kill)

    notes = svc.reconcile()
    row = _run_row(svc, task_id, run_id)
    assert row["status"] == "LOST", row["status"]
    assert f"lost:{run_id}" in notes["notes"]
    events = [e["event_type"] for e in svc.list_events(task_id)]
    assert "run.lost" in events
    # The Work Unit is recoverable, not stuck: reconcile frees it for a new attempt.
    wu_id = _work_unit_ids(svc, task_id)[0]
    wu = svc.get_work_unit(wu_id)
    assert wu["status"] in {"PENDING", "BLOCKED"}, wu
    assert wu["status"] != "DONE"
    assert wu["active_run_id"] is None
    assert svc.get_workspace(f"ws_{wu_id}")["writer_alive"] is False
    assert svc.get_workspace(f"ws_{wu_id}")["state"] == "READY"


def test_recovery_artifact_publish_crash_then_republication(tmp_path):
    """Case 3: crash while publishing; nothing is registered, republishing succeeds."""
    import hashlib

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["fs.read", "fs.write"])
    client = ScriptedClient([_final("noop")])
    adapter = _adapter(svc, client)
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 5.0)
    worker = run_auth(svc, run_id)
    spec = svc.get_run_input(worker, run_id)
    workspace = Path(spec["workspace_path"])
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "out.txt").write_text("recovered", encoding="utf-8")
    digest = hashlib.sha256(b"recovered").hexdigest()
    epoch = worker.bound_fencing_epoch

    svc.executor.set_crash_before_commit(True)
    with pytest.raises(RuntimeError, match="injected_crash_before_commit"):
        svc.execute(
            "publish_artifact",
            worker,
            {"run_id": run_id, "fencing_epoch": epoch, "path": "out.txt"},
        )
    svc.executor.set_crash_before_commit(False)

    assert digest in svc.list_orphan_artifacts()
    with pytest.raises(Exception):
        svc.get_artifact(task_id, digest)

    r = svc.execute(
        "publish_artifact",
        worker,
        {"run_id": run_id, "fencing_epoch": epoch, "path": "out.txt"},
    )
    assert r.ok, r
    assert r.data["artifact_hash"] == digest
    assert svc.verify_artifact_content(task_id, digest)["verified"] is True
    assert digest not in svc.list_orphan_artifacts()


def test_lost_run_recovery_policy_is_conservative():
    # No effect, attempts left -> a fresh attempt is allowed.
    assert lost_run_recovery_policy(
        has_effect=False, attempts=1, max_attempts=3
    ) == "RETRY"
    # No effect, attempts exhausted -> blocked, never a blind loop.
    assert lost_run_recovery_policy(
        has_effect=False, attempts=3, max_attempts=3
    ) == "BLOCKED"
    # An effect may have reached the outside world -> human review, never a retry.
    assert lost_run_recovery_policy(
        has_effect=True, attempts=1, max_attempts=3
    ) == "BLOCKED"


def test_terminal_run_status_helper_matches_core():
    assert is_terminal_run("LOST")
    assert not is_terminal_run("RUNNING")
