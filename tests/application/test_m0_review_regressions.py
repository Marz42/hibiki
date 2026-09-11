"""Regression tests for M0 review counterexamples (P0/P1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from hibiki.application.bootstrap import bootstrap_core
from hibiki.domain.enums import (
    AgentRunStatus,
    OutboxStatus,
    SideEffectState,
    TaskState,
    WorkUnitStatus,
    WorkspaceState,
)
from hibiki.persistence.session import InstanceLock
from hibiki.runtime.fake_external import FakeExternalAdapter
from tests.helpers import (
    approve_flow,
    human_auth,
    internal_auth,
    make_core,
    run_auth,
    run_fencing_epoch,
    submit_result_and_exit,
    user_agent_auth,
)


class ThrowingOnceExternal(FakeExternalAdapter):
    def __init__(self) -> None:
        super().__init__(supports_query=False)
        self.calls = 0

    def dispatch(self, effect: dict) -> dict:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError("injected_timeout")
        return super().dispatch(effect)


def test_p0_user_agent_cannot_approve_side_effect(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    ua = user_agent_auth()
    task_id, _ = approve_flow(svc, human)
    r = svc.execute(
        "propose_side_effect",
        human,
        {
            "task_id": task_id,
            "logical_action_key": "pub",
            "target_ref": "t",
            "parameters": {"a": 1},
        },
    )
    assert r.ok
    r2 = svc.execute(
        "approve_side_effect",
        ua,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["action_digest"],
        },
    )
    assert not r2.ok
    assert r2.error_code == "authorization_denied"
    assert svc.get_side_effect(r.data["effect_id"])["state"] == SideEffectState.WAITING_APPROVAL
    assert ctx["external"].effect_counts.get(f"ext:{task_id}:pub", 0) == 0


def test_p0_unknown_exception_does_not_blind_replay(tmp_path):
    external = ThrowingOnceExternal()
    svc, _ = make_core(tmp_path, external=external)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    r = svc.execute(
        "propose_side_effect",
        human,
        {
            "task_id": task_id,
            "logical_action_key": "once",
            "target_ref": "t",
            "parameters": {},
            "supports_idempotent_query": False,
        },
    )
    effect_id = r.data["effect_id"]
    svc.execute(
        "approve_side_effect",
        human,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    svc.execute("dispatch_side_effect", human, {"effect_id": effect_id})
    assert external.calls == 1
    assert svc.get_side_effect(effect_id)["state"] == SideEffectState.UNKNOWN
    # drain again must not call adapter
    svc.drain_outbox()
    assert external.calls == 1
    r2 = svc.execute("dispatch_side_effect", human, {"effect_id": effect_id})
    assert not r2.ok
    assert external.calls == 1


def test_p0_cancel_blocks_unsent_side_effect(tmp_path):
    svc, ctx = make_core(tmp_path, dispatch_enabled=False)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    r = svc.execute(
        "propose_side_effect",
        human,
        {
            "task_id": task_id,
            "logical_action_key": "del",
            "target_ref": "t",
            "parameters": {},
        },
    )
    effect_id = r.data["effect_id"]
    svc.execute(
        "approve_side_effect",
        human,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    svc.execute("dispatch_side_effect", human, {"effect_id": effect_id})
    assert svc.count_outbox(status=OutboxStatus.PENDING, task_id=task_id) >= 1
    svc.execute("cancel_task", human, {"task_id": task_id})
    svc.dispatch_enabled = True
    svc.drain_outbox()
    key = f"ext:{task_id}:del"
    assert ctx["external"].effect_counts.get(key, 0) == 0
    assert svc.get_side_effect(effect_id)["state"] in {
        SideEffectState.CANCELLED,
        SideEffectState.UNKNOWN,
    }


def test_p1_acceptance_rejects_empty_and_stale_snapshot(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    # empty acceptance before work done
    r = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not r.ok
    assert r.error_code == "incomplete_work"

    # complete work, prepare, then bump plan, then old decision must fail
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run_id,
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["h1"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "h1", "verdict": "PASS"}
            ],
        },
    )
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert prep.ok
    old_dec = prep.data["decision_id"]
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "nodes": [
                {"work_unit_id": wu, "spec_version": 1},
                {"work_unit_id": "wu_new", "spec_version": 1},
            ],
            "edges": [],
        },
    )
    # pending acceptance superseded
    assert svc.get_decision(old_dec)["status"] == "SUPERSEDED"
    r = svc.execute("accept_result", human, {"decision_id": old_dec})
    assert not r.ok
    assert svc.get_task(task_id)["state"] != TaskState.COMPLETED
    assert svc.get_work_unit("wu_new")["status"] == WorkUnitStatus.PENDING


def test_p1_completed_cannot_pause(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    submit_result_and_exit(
        svc,
        human,
        d.data["created_runs"][0],
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["h"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "h", "verdict": "PASS"}
            ],
        },
    )
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    svc.execute("accept_result", human, {"decision_id": prep.data["decision_id"]})
    assert svc.get_task(task_id)["state"] == TaskState.COMPLETED
    r = svc.execute("pause_task", human, {"task_id": task_id})
    assert not r.ok
    assert svc.get_task(task_id)["state"] == TaskState.COMPLETED


def test_p1_quiescent_refuses_live_writer(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    ctx["agent"].keep_alive_after_lease.add(run_id)
    svc.execute("pause_task", human, {"task_id": task_id})
    # force stop path to leave writer alive
    ctx["agent"].stop(run_id, "pause")
    r = svc.execute("runtime_quiescent", human, {"task_id": task_id})
    assert not r.ok
    assert r.error_code == "writer_alive"
    assert svc.get_task(task_id)["state"] == TaskState.PAUSING


def test_p1_pause_resume_clears_markers(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    # ensure stop kills writer
    svc.execute("pause_task", human, {"task_id": task_id})
    ctx["agent"].stop(run_id, "pause")
    ctx["agent"].mark_dead(run_id)
    auth = run_auth(svc, run_id)
    svc.execute(
        "set_writer_alive",
        auth,
        {
            "workspace_id": f"ws_{wu}",
            "run_id": run_id,
            "fencing_epoch": auth.bound_fencing_epoch,
            "alive": False,
        },
    )
    r = svc.execute("runtime_quiescent", human, {"task_id": task_id})
    assert r.ok
    assert svc.get_task(task_id)["state"] == TaskState.PAUSED
    svc.execute("resume_task", human, {"task_id": task_id})
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert len(r.data["created_runs"]) == 1


def test_p1_gate_resolve_restores_dispatch(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    g = svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "need_info"})
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    assert svc.get_task(task_id)["state"] == TaskState.EXECUTING
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert r.data["created_runs"]


def test_p1_plan_rejects_cross_task_work_unit(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_a, wu_a = approve_flow(svc, human, title="A")
    task_b, _ = approve_flow(svc, human, title="B")
    r = svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_b,
            "expected_plan_version": 1,
            "nodes": [{"work_unit_id": wu_a, "spec_version": 1}],
            "edges": [],
        },
    )
    assert not r.ok
    assert r.error_code == "cross_task_work_unit"
    assert svc.get_work_unit(wu_a)["status"] != WorkUnitStatus.RUNNING


def test_p1_plan_binds_spec_version(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "x"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", human, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "nodes": [{"work_unit_id": "wu_s", "spec_version": 1}],
            "edges": [],
        },
    )
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "nodes": [{"work_unit_id": "wu_s", "spec_version": 2}],
            "edges": [],
        },
    )
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run = svc.list_runs(task_id)[0]
    assert run["work_unit_id"] == "wu_s"
    # profile_version field exists; check work_unit_spec via DB helper
    assert d.data["created_runs"]
    wu = svc.get_work_unit("wu_s")
    assert wu["status"] == WorkUnitStatus.RUNNING

    def _spec(session):
        from hibiki.persistence.models import AgentRunRow

        row = session.get(AgentRunRow, d.data["created_runs"][0])
        return row.work_unit_spec_version

    assert svc.executor.run(_spec) == 2


def test_p1_instance_lock_mutex(tmp_path):
    data = tmp_path / "d"
    data.mkdir()
    lock1 = InstanceLock(data)
    lock1.acquire()
    lock2 = InstanceLock(data)
    with pytest.raises(RuntimeError, match="another Core"):
        lock2.acquire()
    lock1.release()
    lock2.acquire()
    lock2.release()


def test_p1_outbox_inflight_recovery(tmp_path):
    from hibiki.runtime.clock import FakeClock

    clock = FakeClock()
    svc, _ = make_core(tmp_path, clock=clock, dispatch_enabled=False)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    svc.execute("dispatch_ready_runs", human, {"task_id": task_id})

    def _claim(session):
        return svc._claim_one_outbox(session)

    item = svc.executor.run(_claim)
    assert item is not None
    assert svc.count_outbox(status=OutboxStatus.IN_FLIGHT) == 1
    clock.advance(seconds=60)
    notes = svc.reconcile()
    assert any("outbox_fence_start" in n for n in notes["notes"])
    svc.dispatch_enabled = True
    svc.drain_outbox()
    runs = svc.list_runs(task_id)
    assert runs
    assert runs[0]["status"] in {
        AgentRunStatus.RUNNING,
        AgentRunStatus.CREATED,
        AgentRunStatus.CANCELLED,
    }


def test_p1_inbox_dedup_same_message_id(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    r1 = svc.execute(
        "record_model_usage",
        human,
        {"task_id": task_id, "calls": 1},
        message_id="msg-same",
        idempotency_key="k1",
    )
    assert r1.ok
    r2 = svc.execute(
        "record_model_usage",
        human,
        {"task_id": task_id, "calls": 1},
        message_id="msg-same",
        idempotency_key="k2",
    )
    assert r2.ok and r2.replayed
    assert svc.get_task(task_id)["model_calls_used"] == 1
    assert svc.count_inbox() >= 1
    r3 = svc.execute(
        "record_model_usage",
        human,
        {"task_id": task_id, "calls": 5},
        message_id="msg-same",
        idempotency_key="k3",
    )
    assert not r3.ok
    assert r3.error_code == "inbox_conflict"


def test_p1_concurrency_limits(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "many"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", human, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    nodes = [{"work_unit_id": f"wu_{i}", "spec_version": 1} for i in range(6)]
    svc.execute(
        "activate_plan",
        human,
        {"task_id": task_id, "nodes": nodes, "edges": []},
    )
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    # per-task concurrency default 2
    assert len(d.data["created_runs"]) == 2


def test_p1_contract_resource_limit_synced(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "budget"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        human,
        {
            "task_id": task_id,
            "objective": "budget",
            "resource_limits": {"model_call_limit": 1},
        },
    )
    svc.execute(
        "approve_contract",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    assert svc.get_task(task_id)["model_call_limit"] == 1
    svc.execute("activate_minimal_plan", human, {"task_id": task_id})
    svc.execute("record_model_usage", human, {"task_id": task_id, "calls": 1})
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert not d.ok
    assert d.error_code == "resource_limit"


def _approve_side_effect(svc, human, task_id, key="pub"):
    r = svc.execute(
        "propose_side_effect",
        human,
        {
            "task_id": task_id,
            "logical_action_key": key,
            "target_ref": "t",
            "parameters": {"a": 1},
        },
    )
    assert r.ok
    effect_id = r.data["effect_id"]
    svc.execute(
        "approve_side_effect",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["action_digest"],
        },
    )
    return effect_id


def test_p0_paused_blocks_side_effect_dispatch(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    effect_id = _approve_side_effect(svc, human, task_id, "paused")
    svc.execute("pause_task", human, {"task_id": task_id})
    svc.execute("runtime_quiescent", human, {"task_id": task_id})
    assert svc.get_task(task_id)["state"] == TaskState.PAUSED
    r = svc.execute("dispatch_side_effect", human, {"effect_id": effect_id})
    assert not r.ok
    assert r.error_code == "dispatch_frozen_pause"
    svc.drain_outbox()
    assert ctx["external"].effect_counts.get(f"ext:{task_id}:paused", 0) == 0


def test_p0_blocking_gate_blocks_side_effect_dispatch(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    effect_id = _approve_side_effect(svc, human, task_id, "gated")
    svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "other"})
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    r = svc.execute("dispatch_side_effect", human, {"effect_id": effect_id})
    assert not r.ok
    assert r.error_code == "dispatch_frozen_gate"
    svc.drain_outbox()
    assert ctx["external"].effect_counts.get(f"ext:{task_id}:gated", 0) == 0


def test_p1_acceptance_rejects_fail_without_criterion_evidence(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run_id,
        {"outcome": "COMPLETED", "verdict": "FAIL"},
    )
    g = svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "need_info"})
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not prep.ok
    assert prep.error_code in {"missing_evidence", "blocking_gate_open", "evidence_failed"}
    # Even without the gate, FAIL without artifact cannot satisfy required criteria
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    prep2 = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not prep2.ok
    assert prep2.error_code in {"missing_evidence", "evidence_failed"}
    assert svc.get_task(task_id)["state"] != TaskState.COMPLETED


def test_p1_result_does_not_imply_executor_exit(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    svc.execute(
        "submit_result",
        run_auth(svc, run_id),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["h"],
            },
        },
    )
    assert ctx["agent"].is_alive(run_id)
    ws = svc.get_workspace(f"ws_{wu}")
    assert ws["owner_run_id"] == run_id
    assert ws["writer_alive"] is True
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not prep.ok
    assert prep.error_code == "writer_alive"
    r = svc.execute("confirm_run_exit", human, {"run_id": run_id})
    assert r.ok
    assert not ctx["agent"].is_alive(run_id)
    ws2 = svc.get_workspace(f"ws_{wu}")
    assert ws2["owner_run_id"] is None
    assert ws2["writer_alive"] is False


def test_p1_lost_run_recovers_work_unit_for_retry(tmp_path):
    from hibiki.runtime.clock import FakeClock

    clock = FakeClock()
    svc, ctx = make_core(tmp_path, clock=clock)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    ctx["agent"].mark_dead(run_id)
    notes = svc.reconcile()
    assert any(f"lost:{run_id}" in n for n in notes["notes"])
    run = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    assert run["status"] == AgentRunStatus.LOST
    wu_row = svc.get_work_unit(wu)
    assert wu_row["status"] == WorkUnitStatus.PENDING
    assert wu_row["active_run_id"] is None
    # advance past backoff
    clock.advance(seconds=5)
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert len(r.data["created_runs"]) == 1
    assert r.data["created_runs"][0] != run_id


def test_p1_inbox_replay_preserves_failure(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "inbox-fail"})
    task_id = r.data["task_id"]
    r1 = svc.execute(
        "dispatch_ready_runs",
        human,
        {"task_id": task_id},
        message_id="same-dispatch-msg",
    )
    assert not r1.ok
    assert r1.error_code == "dispatch_blocked_state"
    r2 = svc.execute(
        "dispatch_ready_runs",
        human,
        {"task_id": task_id},
        message_id="same-dispatch-msg",
    )
    assert not r2.ok
    assert r2.replayed
    assert r2.error_code == "dispatch_blocked_state"
    assert r2.error_message == r1.error_message


def test_p1_client_extra_evidence_cannot_bypass_fail(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    ua = user_agent_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run_id,
        {
            "outcome": "COMPLETED",
            "verdict": "FAIL",
            "artifact_refs": ["failed-hash"],
        },
    )
    prep = svc.execute(
        "prepare_acceptance",
        ua,
        {
            "task_id": task_id,
            "evidence": [
                {
                    "criterion_id": "c1",
                    "artifact_hash": "unregistered-hash",
                    "verdict": "PASS",
                }
            ],
        },
    )
    assert not prep.ok
    assert prep.error_code == "unregistered_artifact"
    # Registered FAIL hash also cannot be declared PASS by the client
    prep2 = svc.execute(
        "prepare_acceptance",
        human,
        {
            "task_id": task_id,
            "evidence": [
                {
                    "criterion_id": "c1",
                    "artifact_hash": "failed-hash",
                    "verdict": "PASS",
                }
            ],
        },
    )
    assert not prep2.ok
    assert prep2.error_code in {
        "evidence_failed",
        "missing_evidence",
        "evidence_not_current",
    }
    assert svc.get_task(task_id)["state"] != TaskState.COMPLETED


def test_p1_spec_change_rejects_old_run_completing_new_definition(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "spec"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", human, {"task_id": task_id, "objective": "v1"})
    svc.execute(
        "approve_contract",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "objective": "v1-obj",
            "nodes": [{"work_unit_id": "wu_spec", "spec_version": 1}],
            "edges": [],
        },
    )
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    # In-place definition change while Run is active must be rejected
    bump = svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "objective": "v2-obj-changed",
            "nodes": [{"work_unit_id": "wu_spec", "spec_version": 2}],
            "edges": [],
        },
    )
    assert not bump.ok
    assert bump.error_code == "work_unit_spec_immutable"
    # New Work Unit for the new definition; old Run result is history only
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "objective": "v2-obj-changed",
            "nodes": [{"work_unit_id": "wu_spec_v2", "spec_version": 1}],
            "edges": [],
        },
    )
    late = svc.execute(
        "submit_result",
        run_auth(svc, run_id),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["old-v1"],
            },
        },
    )
    assert late.ok
    assert late.data.get("late_arrival") or late.data.get("accepted_as_history")
    assert svc.get_work_unit("wu_spec")["status"] == WorkUnitStatus.CANCELLED
    assert svc.get_work_unit("wu_spec_v2")["status"] == WorkUnitStatus.PENDING
    # DONE terminal must not revive on later plan mention
    d2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert d2.ok
    run2 = d2.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run2,
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["new-v2"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "new-v2", "verdict": "PASS"}
            ],
        },
    )
    assert svc.get_work_unit("wu_spec_v2")["status"] == WorkUnitStatus.DONE
    revive = svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "expected_plan_version": 2,
            "nodes": [
                {"work_unit_id": "wu_spec_v2", "spec_version": 2},
            ],
            "edges": [],
        },
    )
    assert not revive.ok
    assert revive.error_code == "work_unit_spec_immutable"
    assert svc.get_work_unit("wu_spec_v2")["status"] == WorkUnitStatus.DONE
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert prep.ok, prep
    acc = svc.execute("accept_result", human, {"decision_id": prep.data["decision_id"]})
    assert acc.ok, acc
    assert svc.get_task(task_id)["state"] == TaskState.COMPLETED


def test_p1_gate_revoke_start_allows_redispatch(tmp_path):
    svc, ctx = make_core(tmp_path, dispatch_enabled=False)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    assert svc.count_outbox(status=OutboxStatus.PENDING, task_id=task_id) >= 1
    g = svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "hold"})
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    run = next(r for r in svc.list_runs(task_id) if r["run_id"] == run_id)
    assert run["status"] == AgentRunStatus.CANCELLED
    assert svc.get_work_unit(wu)["status"] == WorkUnitStatus.PENDING
    assert svc.get_work_unit(wu)["active_run_id"] is None
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    assert svc.get_task(task_id)["state"] == TaskState.EXECUTING
    svc.dispatch_enabled = True
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert len(r.data["created_runs"]) == 1
    assert r.data["created_runs"][0] != run_id
    assert len(ctx["agent"].started) == 1


def test_p1_pause_stops_succeeded_but_alive_executor(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    svc.execute(
        "submit_result",
        run_auth(svc, run_id),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["alive-hash"],
            },
        },
    )
    assert ctx["agent"].is_alive(run_id)
    before_stops = len(ctx["agent"].stopped)
    # Only Pause — do not mark_dead / confirm_run_exit first
    r = svc.execute("pause_task", human, {"task_id": task_id})
    assert r.ok
    assert len(ctx["agent"].stopped) > before_stops
    assert any(rid == run_id for rid, _ in ctx["agent"].stopped)
    assert not ctx["agent"].is_alive(run_id)
    q = svc.execute("runtime_quiescent", human, {"task_id": task_id})
    assert q.ok
    assert svc.get_task(task_id)["state"] == TaskState.PAUSED
    _ = wu


def test_p0_inflight_start_gate_no_double_writer(tmp_path):
    """IN_FLIGHT agent.start + Gate must not free Workspace for a second writer."""
    svc, ctx = make_core(tmp_path, dispatch_enabled=False)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]

    item = svc.executor.run(svc._claim_one_outbox)
    assert item is not None
    assert item["command_type"] == "agent.start"
    assert svc.count_outbox(status=OutboxStatus.IN_FLIGHT) == 1
    # Adapter already started; ACK not yet committed
    ctx["agent"].start(item["payload"])
    assert ctx["agent"].is_alive(run_id)

    g = svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "mid_start"})
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    ws = svc.get_workspace(f"ws_{wu}")
    assert ws["owner_run_id"] == run_id
    # First process still the owner — no second dispatch yet
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    svc.dispatch_enabled = True
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    # Cannot create second writer while first still owns workspace
    assert r.ok
    assert r.data["created_runs"] == []
    # Drain stop for fenced start, settle, then redisatch once
    svc.drain_outbox()
    assert not ctx["agent"].is_alive(run_id)
    r2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r2.ok
    assert len(r2.data["created_runs"]) == 1
    assert r2.data["created_runs"][0] != run_id
    alive = [rid for rid in ctx["agent"].started if ctx["agent"].is_alive(rid)]
    assert len(alive) <= 1


def test_p1_structured_result_same_hash_dual_refs(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    r = svc.execute(
        "submit_result",
        run_auth(svc, run_id),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["h"],
                "acceptance_evidence": [
                    {"criterion_id": "c1", "artifact_hash": "h", "verdict": "PASS"},
                ],
            },
        },
    )
    assert r.ok, r
    svc.execute("confirm_run_exit", human, {"run_id": run_id})
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert prep.ok, prep


def test_p1_verify_fail_not_masked_by_prior_pass_artifact(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "verify"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", human, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        human,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    svc.execute(
        "activate_plan",
        human,
        {
            "task_id": task_id,
            "nodes": [
                {"work_unit_id": "wu_exec", "spec_version": 1, "work_type": "EXECUTE"},
                {"work_unit_id": "wu_verify", "spec_version": 1, "work_type": "VERIFY"},
            ],
            "edges": [
                {
                    "from_work_unit_id": "wu_exec",
                    "to_work_unit_id": "wu_verify",
                    "predicate": "DONE",
                }
            ],
        },
    )
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    exec_run = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        exec_run,
        {"outcome": "COMPLETED", "verdict": "PASS", "artifact_refs": ["hashX"]},
    )
    d2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    verify_run = d2.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        verify_run,
        {
            "outcome": "COMPLETED",
            "verdict": "FAIL",
            "artifact_refs": ["hashX"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "hashX", "verdict": "FAIL"}
            ],
        },
    )
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not prep.ok
    assert prep.error_code in {"evidence_failed", "missing_evidence"}


def test_p1_user_agent_cannot_submit_result(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    ua = user_agent_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    r = svc.execute(
        "submit_result",
        ua,
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["forged"],
            },
        },
    )
    assert not r.ok
    assert r.error_code == "authorization_denied"
    assert svc.list_runs(task_id)[0]["status"] in {
        AgentRunStatus.CREATED,
        AgentRunStatus.RUNNING,
    }


def test_p1_worker_bound_gate_then_redispatch(tmp_path):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    g = svc.execute(
        "open_blocking_gate",
        human,
        {"task_id": task_id, "reason": "need_info", "run_id": run_id},
    )
    assert svc.get_work_unit(wu)["status"] == WorkUnitStatus.BLOCKED
    assert svc.get_work_unit(wu)["active_run_id"] is None
    svc.drain_outbox()
    svc.execute("confirm_run_exit", human, {"run_id": run_id})
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    assert svc.get_task(task_id)["state"] == TaskState.EXECUTING
    assert svc.get_work_unit(wu)["status"] == WorkUnitStatus.PENDING
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert len(r.data["created_runs"]) == 1
    assert r.data["created_runs"][0] != run_id


def test_p1_contract_delta_stays_pending_apply(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    assert svc.get_task(task_id)["model_call_limit"] == 200
    gen = svc.execute("replace_planner_generation", human, {"task_id": task_id}).data[
        "generation"
    ]
    r = svc.execute(
        "submit_plan_proposal",
        human,
        {
            "task_id": task_id,
            "generation": gen,
            "changes_authorization": True,
            "delta": {"resource_limits": {"model_call_limit": 500}},
            "nodes": [{"work_unit_id": "wu_x", "spec_version": 1}],
            "edges": [],
        },
    )
    assert r.ok
    decision_id = r.data["decision_id"]
    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": decision_id, "choice": "APPROVE"},
    )
    # Approved but not applied
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    assert svc.get_task(task_id)["contract_version"] == 1
    assert svc.get_task(task_id)["model_call_limit"] == 200
    dec = svc.get_decision(decision_id)
    assert dec["status"] == "APPROVED"
    # Explicit apply after writers drained
    applied = svc.execute(
        "apply_contract_delta", human, {"decision_id": decision_id}
    )
    assert applied.ok, applied
    assert svc.get_task(task_id)["contract_version"] == 2
    assert svc.get_task(task_id)["model_call_limit"] == 500


def test_p0_reconcile_inflight_start_then_gate_no_double_writer(tmp_path):
    """Claimed start + lease expiry + Gate must not free Workspace for a second writer."""
    from hibiki.runtime.clock import FakeClock

    clock = FakeClock()
    svc, ctx = make_core(tmp_path, clock=clock, dispatch_enabled=False)
    human = human_auth()
    task_id, wu = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]

    item = svc.executor.run(svc._claim_one_outbox)
    assert item is not None
    assert item["command_type"] == "agent.start"
    ctx["agent"].start(item["payload"])
    assert ctx["agent"].is_alive(run_id)
    # ACK never persisted — lease expires, reconcile must not erase claim
    clock.advance(seconds=60)
    notes = svc.reconcile()
    assert any("outbox_fence_start" in n for n in notes["notes"])
    assert svc.count_outbox(status=OutboxStatus.IN_FLIGHT, task_id=task_id) == 0
    from hibiki.persistence.models import OutboxRow
    from sqlalchemy import select

    def _pending_starts(session):
        return [
            r.outbox_id
            for r in session.scalars(
                select(OutboxRow).where(
                    OutboxRow.task_id == task_id,
                    OutboxRow.status == OutboxStatus.PENDING,
                    OutboxRow.command_type == "agent.start",
                )
            )
        ]

    assert svc.executor.run(_pending_starts) == []

    g = svc.execute("open_blocking_gate", human, {"task_id": task_id, "reason": "hold"})
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    ws = svc.get_workspace(f"ws_{wu}")
    assert ws["owner_run_id"] == run_id
    assert ws["writer_alive"] is True

    svc.execute(
        "resolve_decision",
        human,
        {"decision_id": g.data["decision_id"], "choice": "APPROVE"},
    )
    assert ctx["agent"].is_alive(run_id)
    svc.dispatch_enabled = True
    r = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r.ok
    assert r.data["created_runs"] == []

    svc.drain_outbox()
    assert not ctx["agent"].is_alive(run_id)
    r2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert r2.ok
    assert len(r2.data["created_runs"]) == 1
    assert r2.data["created_runs"][0] != run_id
    alive = [rid for rid in ctx["agent"].started if ctx["agent"].is_alive(rid)]
    assert len(alive) <= 1


def test_p1_evidence_sequence_beats_same_created_at_pass(tmp_path):
    """PASS then FAIL at identical FakeClock time — Core sequence selects FAIL."""
    from hibiki.runtime.clock import FakeClock

    from sqlalchemy import select

    from hibiki.persistence.models import WorkUnitExecutionRow

    clock = FakeClock()
    svc, _ = make_core(tmp_path, clock=clock)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run1 = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run1,
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["hashX"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "hashX", "verdict": "PASS"}
            ],
        },
    )

    def _reopen(session):
        for wu in session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ):
            wu.status = WorkUnitStatus.PENDING
            wu.selected_verdict = None
            wu.verified_artifact_hash = None
            wu.active_run_id = None

    svc.executor.run(_reopen)
    d2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run2 = d2.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run2,
        {
            "outcome": "COMPLETED",
            "verdict": "FAIL",
            "artifact_refs": ["hashX"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "hashX", "verdict": "FAIL"}
            ],
        },
    )
    prep = svc.execute("prepare_acceptance", human, {"task_id": task_id})
    assert not prep.ok
    assert prep.error_code in {"evidence_failed", "missing_evidence"}


def test_p1_extra_evidence_cannot_select_stale_pass_hash(tmp_path):
    """Client citing old PASS hash while current delivery is FAIL must be rejected."""
    from sqlalchemy import select

    from hibiki.persistence.models import WorkUnitExecutionRow

    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run1 = d.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run1,
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["old-hash"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "old-hash", "verdict": "PASS"}
            ],
        },
    )

    def _reopen(session):
        for wu in session.scalars(
            select(WorkUnitExecutionRow).where(WorkUnitExecutionRow.task_id == task_id)
        ):
            wu.status = WorkUnitStatus.PENDING
            wu.selected_verdict = None
            wu.verified_artifact_hash = None
            wu.active_run_id = None

    svc.executor.run(_reopen)
    d2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run2 = d2.data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        run2,
        {
            "outcome": "COMPLETED",
            "verdict": "FAIL",
            "artifact_refs": ["new-hash"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "new-hash", "verdict": "FAIL"}
            ],
        },
    )
    prep = svc.execute(
        "prepare_acceptance",
        human,
        {
            "task_id": task_id,
            "evidence": [
                {"criterion_id": "c1", "artifact_hash": "old-hash", "verdict": "PASS"}
            ],
        },
    )
    assert not prep.ok
    assert prep.error_code in {
        "evidence_not_current",
        "evidence_failed",
        "missing_evidence",
    }
    assert svc.get_task(task_id)["state"] != TaskState.COMPLETED


def test_p1_internal_must_bind_principal_and_fencing(tmp_path):
    from dataclasses import replace

    svc, _ = make_core(tmp_path)
    human = human_auth("human_1")
    task_id, _ = approve_flow(svc, human)
    task_b, _ = approve_flow(svc, human, title="other-task")
    d = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    run_id = d.data["created_runs"][0]
    auth = run_auth(svc, run_id)
    fencing = auth.bound_fencing_epoch

    foreign = svc.execute(
        "submit_result",
        internal_auth("human_2"),
        {
            "run_id": run_id,
            "fencing_epoch": fencing,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["x"],
            },
        },
    )
    assert not foreign.ok
    assert foreign.error_code == "authorization_denied"

    wrong_epoch = svc.execute(
        "submit_result",
        replace(auth, bound_fencing_epoch=fencing + 99),
        {
            "run_id": run_id,
            "fencing_epoch": fencing + 99,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["x"],
            },
        },
    )
    assert not wrong_epoch.ok
    assert wrong_epoch.error_code in {"fencing_conflict", "authorization_denied"}

    # Cross-task: bound run A with task_id of task B in body
    cross_task = svc.execute(
        "submit_result",
        auth,
        {
            "run_id": run_id,
            "task_id": task_b,
            "fencing_epoch": fencing,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["x"],
            },
        },
    )
    assert not cross_task.ok
    assert cross_task.error_code == "authorization_denied"

    d2 = svc.execute("dispatch_ready_runs", human, {"task_id": task_b})
    run_b = d2.data["created_runs"][0]
    cross_run = svc.execute(
        "heartbeat",
        auth,
        {
            "run_id": run_b,
            "task_id": task_id,
            "fencing_epoch": run_fencing_epoch(svc, run_b),
        },
    )
    assert not cross_run.ok
    assert cross_run.error_code == "authorization_denied"

    ok = svc.execute(
        "submit_result",
        auth,
        {
            "run_id": run_id,
            "fencing_epoch": fencing,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["ok"],
            },
        },
    )
    assert ok.ok, ok


def test_p1_dual_approved_delta_second_apply_conflicts(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    task_id, _ = approve_flow(svc, human)
    gen = svc.execute("replace_planner_generation", human, {"task_id": task_id}).data[
        "generation"
    ]
    r1 = svc.execute(
        "submit_plan_proposal",
        human,
        {
            "task_id": task_id,
            "generation": gen,
            "changes_authorization": True,
            "delta": {"resource_limits": {"model_call_limit": 300}},
            "nodes": [{"work_unit_id": "wu_x", "spec_version": 1}],
            "edges": [],
        },
    )
    assert r1.ok, r1
    # Second proposal against same baseline v1
    r2 = svc.execute(
        "submit_plan_proposal",
        human,
        {
            "task_id": task_id,
            "generation": gen,
            "changes_authorization": True,
            "delta": {"resource_limits": {"model_call_limit": 400}},
            "nodes": [{"work_unit_id": "wu_x", "spec_version": 1}],
            "edges": [],
        },
    )
    assert r2.ok, r2
    d1, d2 = r1.data["decision_id"], r2.data["decision_id"]
    assert svc.execute(
        "resolve_decision", human, {"decision_id": d1, "choice": "APPROVE"}
    ).ok
    assert svc.execute(
        "resolve_decision", human, {"decision_id": d2, "choice": "APPROVE"}
    ).ok
    first = svc.execute("apply_contract_delta", human, {"decision_id": d1})
    assert first.ok, first
    assert svc.get_task(task_id)["contract_version"] == 2
    assert svc.get_task(task_id)["model_call_limit"] == 300
    second = svc.execute("apply_contract_delta", human, {"decision_id": d2})
    assert not second.ok
    assert second.error_code in {"baseline_stale", "target_version_taken"}
    # Approval history preserved; apply gate closed without renumber
    assert svc.get_decision(d2)["status"] == "APPROVED"
    assert svc.get_task(task_id)["contract_version"] == 2
    assert svc.get_task(task_id)["model_call_limit"] == 300
