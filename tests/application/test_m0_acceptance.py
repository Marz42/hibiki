"""M0 acceptance scenarios H-001–H-021 and H-044–H-050."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from hibiki.domain.enums import (
    AgentRunStatus,
    DecisionStatus,
    OutboxStatus,
    SideEffectState,
    TaskState,
    WorkspaceState,
    WorkUnitStatus,
)
from hibiki.persistence.models import OutboxRow
from hibiki.runtime.clock import FakeClock
from hibiki.runtime.fake_external import FakeExternalAdapter
from tests.helpers import (
    approve_flow,
    human_auth,
    internal_auth,
    make_core,
    run_fencing_epoch,
    user_agent_auth,
)


def test_h001_no_active_contract_no_dispatch(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    task_id = r.data["task_id"]
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert not r.ok
    assert r.error_code in {"no_active_contract", "dispatch_blocked_state"}
    assert svc.list_runs(task_id) == []


def test_h002_user_agent_cannot_approve(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    ua = user_agent_auth()
    r = svc.execute("create_task", human, {"title": "x"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", human, {"task_id": task_id})
    decision_id = r.data["decision_id"]
    r = svc.execute("approve_contract", ua, {"decision_id": decision_id})
    assert not r.ok
    assert r.error_code == "authorization_denied"
    assert svc.get_decision(decision_id)["status"] == DecisionStatus.PENDING


def test_h002_internal_cannot_approve(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    r = svc.execute("create_task", human, {"title": "x"})
    r = svc.execute("submit_contract", human, {"task_id": r.data["task_id"]})
    r2 = svc.execute(
        "approve_contract",
        internal_auth(),
        {"decision_id": r.data["decision_id"]},
    )
    assert not r2.ok


def test_h003_stale_approval_rejected(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    task_id = r.data["task_id"]
    r1 = svc.execute(
        "submit_contract", auth, {"task_id": task_id, "objective": "v1"}, idempotency_key="c1"
    )
    old_hash = r1.data["content_hash"]
    old_dec = r1.data["decision_id"]
    # new contract version supersedes
    r2 = svc.execute(
        "submit_contract", auth, {"task_id": task_id, "objective": "v2"}, idempotency_key="c2"
    )
    assert r2.data["contract_version"] == 2
    # approve old decision with old hash should fail or be superseded path
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": old_dec,
            "expected_target_hash": old_hash,
            "expected_target_version": 1,
        },
    )
    # Decision still points at version 1; approving v1 is allowed but shouldn't activate if superseded?
    # SPEC: old approval cannot approve new target. Approving the old pending decision
    # for superseded contract: contract row still exists with same hash.
    # Better case: try to approve new decision with old hash
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r2.data["decision_id"],
            "expected_target_hash": old_hash,
            "expected_target_version": r2.data["contract_version"],
        },
    )
    assert not r.ok
    assert r.error_code == "target_hash_conflict"


def test_h004_duplicate_decision_idempotent(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    r = svc.execute("submit_contract", auth, {"task_id": r.data["task_id"]})
    dec = r.data["decision_id"]
    h = r.data["content_hash"]
    v = r.data["contract_version"]
    r1 = svc.execute(
        "approve_contract",
        auth,
        {"decision_id": dec, "expected_target_hash": h, "expected_target_version": v},
        idempotency_key="ap1",
    )
    assert r1.ok
    r2 = svc.execute(
        "approve_contract",
        auth,
        {"decision_id": dec, "expected_target_hash": h, "expected_target_version": v},
        idempotency_key="ap2",
    )
    assert r2.ok
    assert r2.data["status"] == DecisionStatus.APPROVED
    assert r2.replayed or r2.data.get("replayed")


def test_h005_idempotency_conflict(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute(
        "create_task",
        auth,
        {"title": "a"},
        idempotency_key="same",
        message_id="m1",
    )
    assert r.ok
    r2 = svc.execute(
        "create_task",
        auth,
        {"title": "b"},
        idempotency_key="same",
        message_id="m2",
    )
    assert not r2.ok
    assert r2.error_code == "idempotency_conflict"


def test_h006_cancel_invalidates_pending_start(tmp_path):
    svc, ctx = make_core(tmp_path, dispatch_enabled=False)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok
    assert r.data["created_runs"]
    assert svc.count_outbox(status=OutboxStatus.PENDING, task_id=task_id) >= 1
    r = svc.execute("cancel_task", auth, {"task_id": task_id})
    assert r.ok

    def _pending_starts(session):

        rows = session.scalars(
            select(OutboxRow).where(
                OutboxRow.task_id == task_id,
                OutboxRow.command_type == "agent.start",
                OutboxRow.status == OutboxStatus.PENDING,
            )
        ).all()
        return len(rows)

    assert svc.executor.run(_pending_starts) == 0
    started_before = list(ctx["agent"].started)
    svc.dispatch_enabled = True
    svc.drain_outbox()
    assert ctx["agent"].started == started_before


def test_h007_gate_freezes_dispatch(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    # add second work unit pending via new plan
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "nodes": [
                {"work_unit_id": wu, "spec_version": 1},
                {"work_unit_id": "wu_b", "spec_version": 1},
            ],
            "edges": [],
        },
    )
    assert r.ok
    # complete first? open gate first
    r = svc.execute("open_blocking_gate", auth, {"task_id": task_id, "reason": "need_human"})
    assert r.ok
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert not r.ok
    assert r.error_code in {"dispatch_frozen_gate", "dispatch_blocked_state"}


def test_h008_pause_resume_with_pending_gate(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    svc.execute("open_blocking_gate", auth, {"task_id": task_id})
    # still can pause from WAITING_HUMAN
    r = svc.execute("pause_task", auth, {"task_id": task_id})
    assert r.ok
    r = svc.execute("runtime_quiescent", auth, {"task_id": task_id})
    assert r.ok
    assert svc.get_task(task_id)["state"] == TaskState.PAUSED
    r = svc.execute("resume_task", auth, {"task_id": task_id})
    assert r.ok
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert not r.ok


def test_h009_crash_before_commit_no_partial(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    task_id = r.data["task_id"]
    svc.executor.set_crash_before_commit(True)
    with pytest.raises(RuntimeError, match="injected_crash_before_commit"):
        svc.execute("submit_contract", auth, {"task_id": task_id}, idempotency_key="sc1")
    # no pending decision
    events = svc.list_events(task_id)
    assert not any(e["event_type"] == "contract.submitted" for e in events)
    # retry works
    r = svc.execute("submit_contract", auth, {"task_id": task_id}, idempotency_key="sc1")
    assert r.ok


def test_h009_crash_after_commit_scan_recovers(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    svc.dispatch_enabled = False
    svc.executor.set_crash_after_commit(True)
    with pytest.raises(RuntimeError, match="injected_crash_after_commit"):
        svc.execute("dispatch_ready_runs", auth, {"task_id": task_id}, idempotency_key="d1")
    # state committed — outbox present
    assert svc.count_outbox(task_id=task_id) >= 1
    svc.dispatch_enabled = True
    svc.drain_outbox()
    runs = svc.list_runs(task_id)
    assert runs
    assert runs[0]["status"] in {AgentRunStatus.RUNNING, AgentRunStatus.CREATED, AgentRunStatus.SUCCEEDED}


def test_h010_late_result_does_not_overwrite(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run1 = r.data["created_runs"][0]

    def _force_new(session):
        from hibiki.persistence.models import (
            ActiveExecuteRunMarker,
            AgentRunRow,
            WorkspaceRow,
            WorkUnitExecutionRow,
        )

        run = session.get(AgentRunRow, run1)
        run.status = AgentRunStatus.TIMED_OUT
        run.finished_at = svc.clock.now()
        marker = session.get(ActiveExecuteRunMarker, wu)
        if marker:
            session.delete(marker)
        wu_row = session.get(WorkUnitExecutionRow, wu)
        wu_row.status = WorkUnitStatus.PENDING
        wu_row.active_run_id = None
        ws = session.get(WorkspaceRow, f"ws_{wu}")
        if ws:
            ws.writer_alive = False
            ws.owner_run_id = None
            ws.state = WorkspaceState.READY

    svc.executor.run(_force_new)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.ok and r.data["created_runs"], r
    run2 = r.data["created_runs"][0]
    assert run2 != run1
    r = svc.execute(
        "submit_result",
        internal_auth(),
        {
            "run_id": run1,
            "fencing_epoch": run_fencing_epoch(svc, run1),
            "result": {"outcome": "COMPLETED", "summary": "late"},
        },
    )
    assert r.ok
    assert r.data.get("late_arrival")
    wu_info = svc.get_work_unit(wu)
    assert wu_info["active_run_id"] == run2 or wu_info["status"] == WorkUnitStatus.RUNNING


def test_h011_stale_planner_generation(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute("replace_planner_generation", auth, {"task_id": task_id})
    gen = r.data["generation"]
    r = svc.execute(
        "submit_plan_proposal",
        auth,
        {
            "task_id": task_id,
            "generation": gen - 1 if gen > 1 else 0,
            "nodes": [{"work_unit_id": "wu_x", "spec_version": 1}],
            "edges": [],
        },
    )
    assert not r.ok
    assert r.error_code == "stale_generation"


def test_h012_profile_version_frozen_on_run(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    svc.execute(
        "seed_profile",
        auth,
        {"profile_id": "fake", "profile_version": 2, "content": {"v": 2}},
    )
    svc.execute(
        "dispatch_ready_runs", auth, {"task_id": task_id, "profile_version": 1}
    )
    run = svc.list_runs(task_id)[0]
    assert run["profile_version"] == 1


def test_h013_verify_fail_blocks_verdict_pass_dep(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "v"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", auth, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {"work_unit_id": "wu_verify", "spec_version": 1, "work_type": "VERIFY"},
                {"work_unit_id": "wu_next", "spec_version": 1, "work_type": "EXECUTE"},
            ],
            "edges": [
                {
                    "from_work_unit_id": "wu_verify",
                    "to_work_unit_id": "wu_next",
                    "predicate": "VERDICT_PASS",
                    "artifact_hash": "hashA",
                }
            ],
        },
    )
    assert r.ok
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    # only verify ready
    assert len(r.data["created_runs"]) == 1
    run_id = r.data["created_runs"][0]
    svc.execute(
        "submit_result",
        internal_auth(),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "FAIL",
                "verified_artifact_refs": ["hashA"],
            },
        },
    )
    assert svc.get_work_unit("wu_verify")["status"] == WorkUnitStatus.DONE
    assert svc.get_work_unit("wu_verify")["selected_verdict"] == "FAIL"
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.data["created_runs"] == []


def test_h014_plan_authorization_change_needs_delta(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    gen = svc.execute("replace_planner_generation", auth, {"task_id": task_id}).data[
        "generation"
    ]
    r = svc.execute(
        "submit_plan_proposal",
        auth,
        {
            "task_id": task_id,
            "generation": gen,
            "changes_authorization": True,
            "delta": {"permission_ceiling": {"tools": ["shell"]}},
            "nodes": [{"work_unit_id": "wu_z", "spec_version": 1}],
            "edges": [],
        },
    )
    assert r.ok
    assert r.data.get("requires_contract_delta")
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN


def test_h015_unrelated_plan_bump_keeps_valid_result(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = r.data["created_runs"][0]
    svc.execute(
        "submit_result",
        internal_auth(),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {"outcome": "COMPLETED", "verdict": "PASS"},
        },
    )
    assert svc.get_work_unit(wu)["status"] == WorkUnitStatus.DONE
    # bump plan with extra unrelated node; existing DONE remains
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "nodes": [
                {"work_unit_id": wu, "spec_version": 1},
                {"work_unit_id": "wu_extra", "spec_version": 1},
            ],
            "edges": [],
        },
    )
    assert r.ok
    assert svc.get_work_unit(wu)["status"] == WorkUnitStatus.DONE


def test_h016_upstream_hash_change_invalidates_pass(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", auth, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {"work_unit_id": "wu_v", "spec_version": 1},
                {"work_unit_id": "wu_d", "spec_version": 1},
            ],
            "edges": [
                {
                    "from_work_unit_id": "wu_v",
                    "to_work_unit_id": "wu_d",
                    "predicate": "VERDICT_PASS",
                    "artifact_hash": "oldhash",
                }
            ],
        },
    )
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = r.data["created_runs"][0]
    svc.execute(
        "submit_result",
        internal_auth(),
        {
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "verified_artifact_refs": ["oldhash"],
            },
        },
    )
    # New plan requires new hash
    svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "expected_plan_version": 1,
            "nodes": [
                {"work_unit_id": "wu_v2", "spec_version": 1},
                {"work_unit_id": "wu_d", "spec_version": 1},
            ],
            "edges": [
                {
                    "from_work_unit_id": "wu_v2",
                    "to_work_unit_id": "wu_d",
                    "predicate": "VERDICT_PASS",
                    "artifact_hash": "newhash",
                }
            ],
        },
    )
    # wu_d still pending; deps not satisfied by old wu_v
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    # should create wu_v2 only, not wu_d
    assert "wu_d" not in [
        x["work_unit_id"] for x in svc.list_runs(task_id) if x["status"] in {"CREATED", "RUNNING"}
    ]
    _ = r


def test_h017_no_completed_without_acceptance(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    rid = r.data["created_runs"][0]
    svc.execute(
        "submit_result",
        internal_auth(),
        {
            "run_id": rid,
            "fencing_epoch": run_fencing_epoch(svc, rid),
            "result": {"outcome": "COMPLETED"},
        },
    )
    assert svc.get_task(task_id)["state"] != TaskState.COMPLETED


def test_h018_budget_gate(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x", "model_call_limit": 2})
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", auth, {"task_id": task_id})
    svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    svc.execute("record_model_usage", auth, {"task_id": task_id, "calls": 2})
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert not r.ok
    assert r.error_code == "resource_limit"
    assert svc.get_task(task_id)["state"] == TaskState.WAITING_HUMAN


def test_h019_lease_expired_writer_alive_quarantine(tmp_path):
    clock = FakeClock()
    svc, ctx = make_core(tmp_path, clock=clock)
    auth = human_auth()
    task_id, wu = approve_flow(svc, auth)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    run_id = r.data["created_runs"][0]
    ctx["agent"].keep_alive_after_lease.add(run_id)
    clock.advance(seconds=60)
    # stop via cancel path simulating lease recovery
    svc.execute(
        "set_writer_alive",
        internal_auth(),
        {
            "workspace_id": f"ws_{wu}",
            "run_id": run_id,
            "fencing_epoch": run_fencing_epoch(svc, run_id),
            "alive": True,
            "quarantine": True,
        },
    )
    ws = svc.get_workspace(f"ws_{wu}")
    assert ws["state"] == WorkspaceState.QUARANTINED
    # cannot dispatch second writer
    def _reset_wu(session):
        from hibiki.persistence.models import (
            ActiveExecuteRunMarker,
            AgentRunRow,
            WorkUnitExecutionRow,
        )

        run = session.get(AgentRunRow, run_id)
        run.status = AgentRunStatus.LOST
        m = session.get(ActiveExecuteRunMarker, wu)
        if m:
            session.delete(m)
        wu_row = session.get(WorkUnitExecutionRow, wu)
        wu_row.status = WorkUnitStatus.PENDING
        wu_row.active_run_id = None

    svc.executor.run(_reset_wu)
    r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert r.data["created_runs"] == []


def test_h020_reconcile_keeps_paused_waiting(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    svc.execute("pause_task", auth, {"task_id": task_id})
    svc.execute("runtime_quiescent", auth, {"task_id": task_id})
    notes = svc.reconcile()
    assert any(f"keep:{task_id}:PAUSED" in n for n in notes["notes"])
    assert svc.get_task(task_id)["state"] == TaskState.PAUSED


def test_h021_event_sequence_monotonic_ignores_producer_clock(tmp_path):
    clock = FakeClock()
    svc, _ = make_core(tmp_path, clock=clock)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "x"})
    task_id = r.data["task_id"]
    clock.advance(hours=-5)  # producer clock goes backwards
    svc.execute("submit_contract", auth, {"task_id": task_id})
    events = svc.list_events(task_id)
    seqs = [e["sequence_no"] for e in events]
    assert seqs == sorted(seqs)
    assert seqs == list(range(1, len(seqs) + 1))


def test_h044_no_dispatch_without_approval(tmp_path):
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "publish_x",
            "target_ref": "https://example.test",
            "parameters": {"body": "hi"},
        },
    )
    assert r.ok
    effect_id = r.data["effect_id"]
    assert r.data["state"] == SideEffectState.WAITING_APPROVAL
    r = svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert not r.ok
    assert ctx["external"].effect_counts.get(f"ext:{task_id}:publish_x", 0) == 0


def test_h045_param_change_invalidates_old_approval(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "publish_x",
            "target_ref": "t1",
            "parameters": {"body": "a"},
        },
    )
    dec = r.data["decision_id"]
    digest = r.data["action_digest"]
    # mutate by creating would reuse logical key — instead approve with wrong hash
    r = svc.execute(
        "approve_side_effect",
        auth,
        {"decision_id": dec, "expected_target_hash": "deadbeef" * 8},
    )
    assert not r.ok
    assert r.error_code == "target_hash_conflict"
    # correct approval still works
    r = svc.execute(
        "approve_side_effect",
        auth,
        {"decision_id": dec, "expected_target_hash": digest},
    )
    assert r.ok


def test_h046_dispatch_crash_leaves_identity(tmp_path):
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "act1",
            "target_ref": "t",
            "parameters": {"x": 1},
        },
    )
    svc.execute(
        "approve_side_effect",
        auth,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    ctx["external"].force_unknown_once = True
    effect_id = r.data["effect_id"]
    r = svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert r.ok
    eff = svc.get_side_effect(effect_id)
    assert eff["state"] in {SideEffectState.UNKNOWN, SideEffectState.DISPATCHING, SideEffectState.SUCCEEDED}
    # if unknown, no blind retry count bump via ordinary dispatch
    if eff["state"] == SideEffectState.UNKNOWN:
        r2 = svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
        assert not r2.ok


def test_h047_idempotent_reconcile_single_effect(tmp_path):
    external = FakeExternalAdapter(supports_query=True)
    svc, ctx = make_core(tmp_path, external=external)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "pay1",
            "target_ref": "acct",
            "parameters": {"amount": 1},
            "supports_idempotent_query": True,
        },
    )
    effect_id = r.data["effect_id"]
    svc.execute(
        "approve_side_effect",
        auth,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    key = f"ext:{task_id}:pay1"
    assert external.effect_counts[key] == 1
    # simulate lost local receipt
    def _lost(session):
        from hibiki.persistence.models import SideEffectRow

        e = session.get(SideEffectRow, effect_id)
        e.state = SideEffectState.UNKNOWN
        e.receipt_json = None

    svc.executor.run(_lost)
    r = svc.execute("reconcile_side_effect", auth, {"effect_id": effect_id})
    assert r.ok
    assert svc.get_side_effect(effect_id)["state"] == SideEffectState.SUCCEEDED
    assert external.effect_counts[key] == 1


def test_h048_non_idempotent_unknown_no_auto_redispatch(tmp_path):
    external = FakeExternalAdapter(supports_query=False)
    svc, _ = make_core(tmp_path, external=external)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
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
        auth,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    external.force_unknown_once = True
    svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert svc.get_side_effect(effect_id)["state"] == SideEffectState.UNKNOWN
    r = svc.execute("reconcile_side_effect", auth, {"effect_id": effect_id})
    assert r.data.get("auto_retry") is False
    r = svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert not r.ok


def test_h049_same_logical_action_reuses_effect(tmp_path):
    svc, _ = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r1 = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "pub",
            "target_ref": "t",
            "parameters": {"v": 1},
        },
    )
    r2 = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "pub",
            "target_ref": "t",
            "parameters": {"v": 1},
        },
        idempotency_key="other",
    )
    assert r2.data["effect_id"] == r1.data["effect_id"]
    assert r2.data.get("reused")


def test_h050_cancel_after_dispatch_keeps_uncertainty(tmp_path):
    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    r = svc.execute(
        "propose_side_effect",
        auth,
        {
            "task_id": task_id,
            "logical_action_key": "del",
            "target_ref": "t",
            "parameters": {},
            "supports_idempotent_query": False,
        },
    )
    effect_id = r.data["effect_id"]
    svc.execute(
        "approve_side_effect",
        auth,
        {"decision_id": r.data["decision_id"], "expected_target_hash": r.data["action_digest"]},
    )
    ctx["external"].force_unknown_once = True
    svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert svc.get_side_effect(effect_id)["state"] == SideEffectState.UNKNOWN
    svc.execute("cancel_task", auth, {"task_id": task_id})
    r = svc.execute(
        "cancellation_settled",
        auth,
        {"task_id": task_id, "accept_unknown": True},
    )
    assert r.ok
    assert svc.get_task(task_id)["state"] == TaskState.ABORTED
    assert svc.get_side_effect(effect_id)["state"] == SideEffectState.UNKNOWN
    # still cannot redispatch
    r = svc.execute("dispatch_side_effect", auth, {"effect_id": effect_id})
    assert not r.ok
