"""Task G — ContextManifest materialization and ContextAppend (SPEC §10.2 / §10.3).

The initial manifest is immutable and names fixed versions/hashes; dynamic inputs are
append-only records with a reason, an authorization reference and a materialized hash.
Reading either requires the Run's own credential.
"""

from __future__ import annotations

import pytest

from hibiki.domain.errors import AuthorizationError
from tests.helpers import human_auth, make_core, run_auth


def _dispatch_one(svc, auth, *, ceiling=None, resource_limits=None):
    r = svc.execute("create_task", auth, {"title": "task-g"})
    task_id = r.data["task_id"]
    payload = {"task_id": task_id, "objective": "inspect the inputs"}
    if ceiling is not None:
        payload["permission_ceiling"] = ceiling
    if resource_limits is not None:
        payload["resource_limits"] = resource_limits
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


def _context(svc, run_id):
    return svc.get_run_context(run_auth(svc, run_id), run_id)


def _workspace(svc, run_id):
    from pathlib import Path

    return Path(svc.get_run_input(run_auth(svc, run_id), run_id)["workspace_path"])


def test_initial_manifest_is_materialized_with_fixed_refs(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(svc, auth)

    ctx = _context(svc, run_id)
    manifest = ctx["manifest"]
    assert manifest is not None
    assert manifest["task_id"] == task_id
    assert manifest["run_id"] == run_id
    assert manifest["context_policy"] == "FRESH"
    assert manifest["profile_ref"].startswith("local@v")

    kinds = {ref["kind"] for ref in manifest["mandatory_refs"]}
    assert {"contract", "deliverable", "plan"} <= kinds
    contract_ref = next(r for r in manifest["mandatory_refs"] if r["kind"] == "contract")
    assert contract_ref["hash"] and contract_ref["version"] == 1

    assert manifest["excluded_categories"] == [
        "other_tasks",
        "human_session",
        "credentials",
    ]
    assert manifest["context_budget"]["max_materialized_bytes"] > 0
    assert ctx["appends"] == []
    assert ctx["manifest_hash"]


def test_manifest_survives_and_appends_are_ordered_and_provenanced(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    worker = run_auth(svc, run_id)
    ws = _workspace(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "notes.txt").write_text("extra context", encoding="utf-8")

    epoch = worker.bound_fencing_epoch
    r = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": epoch,
            "reason": "user_addition",
            "authorized_ref": "notes.txt",
            "grant_ref": "run-input",
        },
    )
    assert r.ok, r
    import hashlib

    assert r.data["materialized_hash"] == hashlib.sha256(b"extra context").hexdigest()

    r2 = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": epoch,
            "reason": "tool_read",
            "authorized_ref": "notes.txt",
        },
    )
    assert r2.ok, r2

    ctx = _context(svc, run_id)
    assert [a["sequence_no"] for a in ctx["appends"]] == [1, 2]
    assert ctx["appends"][0]["reason"] == "user_addition"
    assert ctx["appends"][0]["authorized_ref"] == "notes.txt"
    assert ctx["appends"][0]["materialized_hash"]


def test_context_append_refuses_a_lying_hash_and_escaping_refs(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    worker = run_auth(svc, run_id)
    ws = _workspace(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "notes.txt").write_text("real", encoding="utf-8")
    (ws.parent / "outside.txt").write_text("secret", encoding="utf-8")
    epoch = worker.bound_fencing_epoch

    bad = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": epoch,
            "reason": "user_addition",
            "authorized_ref": "notes.txt",
            "content_hash": "0" * 64,
        },
    )
    assert not bad.ok
    assert bad.error_code == "context_hash_mismatch"

    escape = svc.execute(
        "context_append",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": epoch,
            "reason": "user_addition",
            "authorized_ref": "../outside.txt",
        },
    )
    assert not escape.ok

    missing_reason = svc.execute(
        "context_append",
        worker,
        {"run_id": run_id, "fencing_epoch": epoch, "authorized_ref": "notes.txt"},
    )
    assert not missing_reason.ok
    assert missing_reason.error_code == "context_reason_required"


def test_context_append_requires_the_run_credential(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    ws = _workspace(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "notes.txt").write_text("x", encoding="utf-8")

    with pytest.raises(AuthorizationError):
        svc.get_run_context(auth, run_id)

    r = svc.execute(
        "context_append",
        auth,
        {"run_id": run_id, "reason": "user_addition", "authorized_ref": "notes.txt"},
    )
    assert not r.ok
    assert r.error_code in {"authorization_denied", "precondition_failed"}


def test_contract_resource_limits_tighten_the_frozen_spec(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(
        svc,
        auth,
        resource_limits={"wall_timeout_seconds": 7, "max_turns": 3, "max_model_calls": 5},
    )
    spec = svc.get_run_input(run_auth(svc, run_id), run_id)["spec"]
    assert spec["wall_timeout_seconds"] == 7
    assert spec["max_turns"] == 3
    assert spec["model_call_limit"] == 5
    assert spec["metadata"]["resource_limits"]["wall_timeout_seconds"] == 7


def test_resource_limits_cannot_raise_system_defaults(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(
        svc,
        auth,
        resource_limits={"wall_timeout_seconds": 10**9, "max_turns": 10**6},
    )
    from hibiki.domain.defaults import DEFAULTS

    spec = svc.get_run_input(run_auth(svc, run_id), run_id)["spec"]
    assert spec["wall_timeout_seconds"] == DEFAULTS.run_wall_timeout_seconds
    assert spec["max_turns"] == DEFAULTS.max_run_model_turns


def test_mandatory_context_overflow_blocks_instead_of_truncating(tmp_path):
    """SPEC §10.2: over-budget mandatory context blocks the Work Unit."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "overflow"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": "big mandatory context",
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
    r = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert r.ok, r

    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok, r
    assert r.data["created_runs"] == []

    events = [e for e in svc.list_events(task_id) if e["event_type"] == "work_unit.blocked"]
    assert events, "the refusal must be auditable"
    assert events[-1]["payload"]["reason"] == "context_overflow"
    assert events[-1]["payload"]["mandatory_bytes"] > 1
    assert svc.list_runs(task_id) == []
