"""Task A — the frozen execution contract handed to a real worker (SPEC §9.2 / §13 / §15).

A dispatched Run must carry a persisted ``RunInputRow`` that states exactly what the worker
may see and do, and that record must be readable only through the run's own credential.
"""

from __future__ import annotations

import json

import pytest

from hibiki.application.service import ApplicationService
from hibiki.domain.errors import AuthorizationError, NotFoundError
from hibiki.persistence.models import AgentRunRow, OutboxRow, RunInputRow
from tests.helpers import approve_flow, human_auth, make_core, run_auth, submit_result_and_exit


def _dispatch_one(svc, auth, ceiling=None):
    """Approve a contract with an explicit tool ceiling, dispatch, return (task_id, run_id)."""
    r = svc.execute("create_task", auth, {"title": "task-a"})
    task_id = r.data["task_id"]
    payload = {"task_id": task_id, "objective": "produce a file"}
    if ceiling is not None:
        payload["permission_ceiling"] = ceiling
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
    r = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert r.ok, r
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    return task_id, r.data["created_runs"][0]


def _read_run_input(svc, run_id: str) -> dict:
    """Snapshot the persisted run input (the session closes after the read)."""

    def _read(session):
        row = session.get(RunInputRow, run_id)
        assert row is not None
        return {
            "run_id": row.run_id,
            "task_id": row.task_id,
            "workspace_id": row.workspace_id,
            "workspace_path": row.workspace_path,
            "profile_id": row.profile_id,
            "profile_version": row.profile_version,
            "context_manifest_id": row.context_manifest_id,
            "granted_tools": json.loads(row.granted_tools_json),
            "permission_ceiling": json.loads(row.permission_ceiling_json),
            "spec_hash": row.spec_hash,
            "spec": json.loads(row.spec_json),
        }

    return svc.executor.run(_read)


def _read_outbox_payload(svc, run_id: str) -> dict:
    from sqlalchemy import select

    def _read(session):
        rows = session.scalars(
            select(OutboxRow).where(OutboxRow.command_type == "agent.start")
        ).all()
        for row in rows:
            payload = json.loads(row.payload_json)
            if payload.get("run_id") == run_id:
                return payload
        raise AssertionError(f"no agent.start outbox row for {run_id}")

    return svc.executor.run(_read)


def test_dispatch_freezes_execution_contract(tmp_path):
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(
        svc, auth, ceiling={"tools": ["fs.read", "fs.write", "artifact.publish"]}
    )

    row = _read_run_input(svc, run_id)
    assert row["task_id"] == task_id
    assert row["profile_id"] == "local"
    assert row["spec_hash"]
    assert row["granted_tools"] == ["fs.read", "fs.write", "artifact.publish"]
    assert row["permission_ceiling"]["tools"] == [
        "fs.read",
        "fs.write",
        "artifact.publish",
    ]

    spec = row["spec"]
    assert spec["run_id"] == run_id
    assert spec["workspace_id"], "a real Run must be bound to a workspace"
    assert row["workspace_path"] and row["workspace_path"].endswith(spec["workspace_id"])
    assert str(ctx["data_dir"]) in row["workspace_path"]
    assert spec["context_manifest_id"] == row["context_manifest_id"]
    assert spec["granted_permissions"] == {
        "network": False,
        "host_paths": False,
        "credentials": False,
    }
    assert spec["fencing_epoch"] == 1
    assert spec["grant_epoch"] == 0
    assert spec["model_call_limit"] > 0
    assert spec["max_turns"] > 0
    assert spec["wall_timeout_seconds"] > 0

    def _manifest_hash(session):
        from hibiki.persistence.models import ContextManifestRow

        return session.get(ContextManifestRow, row["context_manifest_id"]).manifest_hash

    mh = svc.executor.run(_manifest_hash)
    assert spec["context_manifest_hash"] == mh


def test_start_payload_carries_binding_and_spec_reference(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth, ceiling={"tools": ["fs.read", "fs.write"]})

    payload = _read_outbox_payload(svc, run_id)
    assert payload["run_id"] == run_id
    assert payload["context_manifest_id"]
    assert payload["context_manifest_hash"]
    assert payload["workspace_id"]
    assert payload["workspace_path"]
    assert payload["granted_tools"] == ["fs.read", "fs.write"]
    assert payload["spec_ref"] == f"run_inputs:{run_id}"
    row = _read_run_input(svc, run_id)
    assert payload["spec_hash"] == row["spec_hash"]
    assert payload["agent_instance_id"] == f"local:{run_id}"


def test_unknown_ceiling_tools_are_not_granted_but_reported(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(
        svc, auth, ceiling={"tools": ["fs.read", "fs.write", "publish_everything"]}
    )

    spec = _read_run_input(svc, run_id)["spec"]
    assert "publish_everything" not in spec["granted_tools"]
    assert spec["metadata"]["unknown_ceiling_tools"] == ["publish_everything"]


def test_fs_read_is_always_granted(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth, ceiling={"tools": []})

    spec = _read_run_input(svc, run_id)["spec"]
    assert spec["granted_tools"] == ["fs.read"]


def test_run_input_is_readable_only_through_the_run_credential(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth, ceiling={"tools": ["fs.read"]})

    worker = run_auth(svc, run_id)
    data = svc.get_run_input(worker, run_id)
    assert data["run_id"] == run_id
    assert data["spec"]["run_id"] == run_id
    assert data["granted_tools"] == ["fs.read"]

    with pytest.raises(AuthorizationError):
        svc.get_run_input(auth, run_id)

    with pytest.raises(NotFoundError):
        svc.get_run_input(worker, "run_missing")


def test_flipping_fencing_epoch_invalidates_the_credential(tmp_path):
    from hibiki.domain.types import AuthContext

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth, ceiling={"tools": ["fs.read"]})
    worker = run_auth(svc, run_id)

    forged = AuthContext(
        principal_id=worker.principal_id,
        actor_id=worker.actor_id,
        actor_type=worker.actor_type,
        auth_context_id=worker.auth_context_id,
        bound_task_id=worker.bound_task_id,
        bound_run_id=worker.bound_run_id,
        bound_fencing_epoch=worker.bound_fencing_epoch + 1,
        bound_grant_epoch=worker.bound_grant_epoch,
    )
    with pytest.raises(AuthorizationError):
        svc.get_run_input(forged, run_id)


def test_result_submission_from_the_bound_worker_still_works(tmp_path):
    """Task A must not break the M0 worker write path."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, wu_id = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = r.data["created_runs"][0]

    result = submit_result_and_exit(svc, auth, run_id)
    assert result.ok
    assert svc.get_work_unit(wu_id)["status"] in {"DONE", "RUNNING", "PENDING"}


def test_run_input_survives_restart_and_stays_bound(tmp_path):
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth, ceiling={"tools": ["fs.read", "fs.write"]})
    before = _read_run_input(svc, run_id)["spec_hash"]
    ctx["lock"].release()

    from hibiki.application.bootstrap import bootstrap_core

    svc2, ctx2 = bootstrap_core(ctx["data_dir"], run_migrate=True)
    try:
        row = _read_run_input(svc2, run_id)
        assert row["spec_hash"] == before
        assert row["workspace_path"] == _read_run_input(svc, run_id)["workspace_path"]
        instance_id = svc2.executor.run(
            lambda s: s.get(AgentRunRow, run_id).agent_instance_id
        )
        assert instance_id == f"local:{run_id}"
    finally:
        ctx2["lock"].release()


def test_application_service_defaults_to_no_workspace_root(tmp_path):
    """The workspace path is optional so unit tests can build a service directly."""
    svc, _ = make_core(tmp_path)
    assert isinstance(svc, ApplicationService)
    assert svc.workspace_root and svc.workspace_root.endswith("workspaces")
