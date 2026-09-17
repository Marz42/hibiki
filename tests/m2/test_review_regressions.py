"""Regression tests for the post-1eb2477 M2 review findings."""

from __future__ import annotations

from pathlib import Path

import pytest

from hibiki.domain.enums import ActorType
from hibiki.domain.types import AuthContext
from hibiki.runtime.api_agent import ApiAgentAdapter, _parse_explicit_result
from hibiki.runtime.fake_planner import FakePlannerAdapter
from tests.helpers import approve_flow, human_auth, make_core, run_auth, submit_result_and_exit


def test_worker_cannot_dispatch_foreign_task(tmp_path: Path) -> None:
    svc, _ = make_core(tmp_path / "a")
    auth_a = human_auth("alice")
    task_a, _ = approve_flow(svc, auth_a, title="alice-task")
    d = svc.execute("dispatch_ready_runs", auth_a, {"task_id": task_a})
    assert d.ok, d
    run_a = d.data["created_runs"][0]
    svc.drain_outbox()
    worker_a = run_auth(svc, run_a)

    svc_b, _ = make_core(tmp_path / "b")
    # Same DB process isolation: use a second principal on the same core.
    auth_b = human_auth("bob")
    task_b, _ = approve_flow(svc, auth_b, title="bob-task")

    # Worker of A must not dispatch B's task (Human-only + principal).
    denied = svc.execute("dispatch_ready_runs", worker_a, {"task_id": task_b})
    assert not denied.ok
    assert denied.error_code == "authorization_denied"

    # Even Human Alice cannot orchestrate Bob's task.
    denied_h = svc.execute("dispatch_ready_runs", auth_a, {"task_id": task_b})
    assert not denied_h.ok
    assert denied_h.error_code == "authorization_denied"


def test_post_message_requires_run_binding(tmp_path: Path) -> None:
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    svc.drain_outbox()
    worker = run_auth(svc, run_id)

    # Omit run_id → refuse.
    bare = svc.execute(
        "post_task_message",
        worker,
        {"task_id": task_id, "kind": "note", "body": {"x": 1}},
    )
    assert not bare.ok
    assert bare.error_code == "authorization_denied"

    # Foreign principal's Internal credential cannot post.
    other = AuthContext(
        principal_id="other",
        actor_id=worker.actor_id,
        actor_type=ActorType.INTERNAL,
        auth_context_id=worker.auth_context_id,
        bound_task_id=worker.bound_task_id,
        bound_run_id=worker.bound_run_id,
        bound_fencing_epoch=worker.bound_fencing_epoch,
        bound_grant_epoch=worker.bound_grant_epoch,
    )
    foreign = svc.execute(
        "post_task_message",
        other,
        {
            "task_id": task_id,
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "kind": "note",
            "body": {"x": 1},
        },
    )
    assert not foreign.ok
    assert foreign.error_code == "authorization_denied"


def test_generation_bump_revokes_old_plan_run(tmp_path: Path) -> None:
    agent = FakePlannerAdapter()
    svc, _ = make_core(tmp_path, agent=agent)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute("replace_planner_generation", auth, {"task_id": task_id})
    gen1 = r.data["generation"]
    r = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    run_id = r.data["run_id"]
    svc.drain_outbox()
    planner = run_auth(svc, run_id)

    # PLAN RunInput + ContextManifest must exist for recovery.
    run_input = svc.get_run_input(planner, run_id)
    assert run_input.get("spec")
    assert run_input["spec"].get("generation") == gen1

    bump = svc.execute("replace_planner_generation", auth, {"task_id": task_id})
    gen2 = bump.data["generation"]
    assert gen2 == gen1 + 1

    # Old credential + new generation must still be refused (run revoked / not active).
    hijack = svc.execute(
        "submit_plan_proposal",
        planner,
        {
            "task_id": task_id,
            "generation": gen2,
            "nodes": [{"work_unit_id": "wu_hijack", "spec_version": 1}],
            "edges": [],
        },
    )
    assert not hijack.ok
    assert hijack.error_code == "authorization_denied"

    runs = {row["run_id"]: row for row in svc.list_runs(task_id)}
    assert runs[run_id]["status"] == "CANCELLED"


def test_worker_result_posts_task_message(tmp_path: Path) -> None:
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    svc.drain_outbox()
    submit_result_and_exit(
        svc,
        auth,
        run_id,
        result={"outcome": "COMPLETED", "verdict": "PASS", "artifact_refs": ["a1"]},
    )
    messages = svc.list_task_messages(task_id)
    assert any(m["kind"] == "worker.result" for m in messages)


def test_work_unit_objective_and_input_refs_preserved(tmp_path: Path) -> None:
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    nodes = [
        {
            "work_unit_id": "wu_alpha",
            "spec_version": 1,
            "work_type": "EXECUTE",
            "objective": "only do alpha",
            "input_refs": ["attachments/a.txt"],
        },
        {
            "work_unit_id": "wu_beta",
            "spec_version": 1,
            "work_type": "EXECUTE",
            "objective": "only do beta",
            "input_refs": ["attachments/b.txt"],
        },
    ]
    r = svc.execute(
        "activate_plan",
        auth,
        {"task_id": task_id, "nodes": nodes, "edges": []},
    )
    assert r.ok, r
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert len(d.data["created_runs"]) == 2
    svc.drain_outbox()
    specs = []
    for rid in d.data["created_runs"]:
        worker = run_auth(svc, rid)
        specs.append(svc.get_run_input(worker, rid)["spec"])
    objectives = {s["objective"] for s in specs}
    assert objectives == {"only do alpha", "only do beta"}
    refs = {tuple(s.get("input_refs") or []) for s in specs}
    assert refs == {("attachments/a.txt",), ("attachments/b.txt",)}


def test_parse_explicit_verify_fail() -> None:
    parsed = _parse_explicit_result(
        'report done\n{"outcome":"COMPLETED","verdict":"FAIL","acceptance_evidence":[]}'
    )
    assert parsed["outcome"] == "COMPLETED"
    assert parsed["verdict"] == "FAIL"


def test_api_agent_preserves_completed_fail(tmp_path: Path) -> None:
    """VERIFY FAIL must not be rewritten to PASS with auto evidence."""
    from hibiki.runtime.api_agent import _RunRecord
    from hibiki.runtime.clock import SystemClock

    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    wu = "wu_verify_only"
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {
                    "work_unit_id": wu,
                    "spec_version": 1,
                    "work_type": "VERIFY",
                    "acceptance_criteria": [
                        {
                            "criterion_id": "c1",
                            "statement": "ok",
                            "evidence_kind": "artifact",
                            "required": True,
                        }
                    ],
                }
            ],
            "edges": [],
        },
    )
    assert r.ok, r
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    svc.drain_outbox()
    worker = run_auth(svc, run_id)
    spec = svc.get_run_input(worker, run_id)["spec"]

    adapter = ApiAgentAdapter(None, core=svc, clock=SystemClock(), broker=None)
    record = _RunRecord(run_id=run_id, spec=dict(spec), task_id=task_id)
    record.artifact_refs = ["art-fail"]
    record.published_paths = ["report.md"]
    adapter._submit_result(
        record,
        worker,
        spec,
        '[[HIBIKI:FAIL]]\n{"outcome":"COMPLETED","verdict":"FAIL"}',
        None,
    )
    runs = {row["run_id"]: row for row in svc.list_runs(task_id)}
    import json

    result = json.loads(runs[run_id]["result_json"])
    assert result["outcome"] == "COMPLETED"
    assert result["verdict"] == "FAIL"
    assert not any(
        ev.get("verdict") == "PASS" for ev in (result.get("acceptance_evidence") or [])
    )


def test_docker_inspect_query_failure_keeps_isolation() -> None:
    from hibiki.tools.sandbox import DockerSandboxAdapter

    class Boom(DockerSandboxAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.docker_bin = "docker-not-installed-xyz"

    probe = Boom().inspect_container("deadbeef")
    assert probe["known"] is False
    assert probe["query_failed"] is True
    assert probe["running"] is True


def test_empty_and_dot_paths_rejected(tmp_path: Path) -> None:
    from hibiki.tools.paths import PathSafetyError, WorkspacePaths

    root = tmp_path / "ws"
    root.mkdir()
    with WorkspacePaths(root) as paths:
        for bad in ("", ".", "./"):
            with pytest.raises(PathSafetyError):
                paths.list_dir(bad)
            with pytest.raises(PathSafetyError):
                paths.exists(bad)
