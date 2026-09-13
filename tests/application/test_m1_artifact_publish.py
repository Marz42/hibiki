"""Task F — artifact publication and the file/DB consistency window (SPEC §11.2 / §11.3).

Publishing must be Core-side and run-bound: the worker names a file inside its own
workspace, the Core hashes the bytes, stores them immutably and only then registers the
Artifact. A crash in between may leave an orphaned content file but must never leave a
registered Artifact whose content is missing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hibiki.domain.errors import PreconditionError
from tests.helpers import human_auth, make_core, run_auth


def _dispatch_one(svc, auth, ceiling=None):
    r = svc.execute("create_task", auth, {"title": "task-f"})
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


def _workspace_of(svc, run_id: str) -> Path:
    spec = svc.get_run_input(run_auth(svc, run_id), run_id)
    return Path(spec["workspace_path"])


def _publish(svc, auth, run_id, *, fencing_epoch=None, **payload):
    """Publish through the Core. The fencing epoch is read outside the transaction so a
    crash-injection test does not trip on the helper's own read."""
    if fencing_epoch is None:
        fencing_epoch = run_auth(svc, run_id).bound_fencing_epoch
    body = {"run_id": run_id, "fencing_epoch": fencing_epoch}
    body.update(payload)
    return svc.execute("publish_artifact", auth, body)


def test_publish_registers_content_and_hash(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("hello hibiki", encoding="utf-8")

    worker = run_auth(svc, run_id)
    r = _publish(svc, worker, run_id, path="out.txt")
    assert r.ok, r
    digest = r.data["artifact_hash"]
    assert r.data["size"] == len("hello hibiki")
    assert r.data["uri"].startswith("artifact://")

    meta = svc.get_artifact(task_id, digest)
    assert meta["uri"] == r.data["uri"]
    assert meta["size"] == len("hello hibiki")
    assert meta["run_id"] == run_id
    assert meta["source_path"] == "out.txt"

    check = svc.verify_artifact_content(task_id, digest)
    assert check["verified"] is True
    assert check["actual_hash"] == digest


def test_publish_accepts_a_matching_claimed_hash_and_rejects_a_wrong_one(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("data", encoding="utf-8")
    worker = run_auth(svc, run_id)

    import hashlib

    good = hashlib.sha256(b"data").hexdigest()
    assert _publish(svc, worker, run_id, path="out.txt", expected_hash=good).ok
    bad = _publish(svc, worker, run_id, path="out.txt", expected_hash="0" * 64)
    assert not bad.ok
    assert bad.error_code == "artifact_hash_mismatch"


def test_publish_refuses_paths_outside_the_workspace(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    worker = run_auth(svc, run_id)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    outside = ws.parent / "secret.txt"
    outside.write_text("nope", encoding="utf-8")
    (ws / "link.txt").symlink_to(outside)

    for bad in ("../secret.txt", str(outside), "link.txt", "", "a/../../x"):
        r = _publish(svc, worker, run_id, path=bad)
        assert not r.ok, f"path {bad!r} must be refused"


def test_publish_requires_the_run_bound_credential(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("x", encoding="utf-8")

    r = _publish(svc, auth, run_id, path="out.txt")
    assert not r.ok
    assert r.error_code in {"authorization_denied", "precondition_failed"}


def test_crash_between_content_write_and_registration_leaves_an_orphan_not_a_lie(tmp_path):
    """The §11.3 window: bytes promoted, database transaction crashes."""
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("crash window", encoding="utf-8")
    worker = run_auth(svc, run_id)

    import hashlib

    digest = hashlib.sha256(b"crash window").hexdigest()
    epoch = worker.bound_fencing_epoch
    svc.executor.set_crash_before_commit(True)
    with pytest.raises(RuntimeError, match="injected_crash_before_commit"):
        _publish(svc, worker, run_id, fencing_epoch=epoch, path="out.txt")
    svc.executor.set_crash_before_commit(False)

    # The Artifact was not registered, so no delivery can claim it…
    with pytest.raises(Exception):
        svc.get_artifact(task_id, digest)
    # …but the content file is present and scannable as an orphan for later cleanup.
    assert digest in svc.list_orphan_artifacts()

    # Publishing again after the crash registers exactly the same content hash.
    r = _publish(svc, worker, run_id, fencing_epoch=epoch, path="out.txt")
    assert r.ok and r.data["artifact_hash"] == digest
    assert digest not in svc.list_orphan_artifacts()
    assert svc.verify_artifact_content(task_id, digest)["verified"] is True


def test_frozen_task_cannot_publish(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("x", encoding="utf-8")
    worker = run_auth(svc, run_id)

    svc.execute("pause_task", auth, {"task_id": task_id})
    r = _publish(svc, worker, run_id, path="out.txt")
    assert not r.ok


def test_publish_missing_file_is_not_a_precondition_error(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    _, run_id = _dispatch_one(svc, auth)
    ws = _workspace_of(svc, run_id)
    ws.mkdir(parents=True, exist_ok=True)
    worker = run_auth(svc, run_id)

    r = _publish(svc, worker, run_id, path="missing.txt")
    assert not r.ok
    assert r.error_code == "artifact_source_missing"


def test_unconfigured_artifact_store_refuses_publication(tmp_path):
    from hibiki.application.bootstrap import bootstrap_core

    svc, ctx = bootstrap_core(tmp_path / "data")
    try:
        svc._artifacts = None
        with pytest.raises(PreconditionError):
            svc._artifact_store()
    finally:
        ctx["lock"].release()
