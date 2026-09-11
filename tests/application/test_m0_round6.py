"""Counterexamples from the ceea43b review, exercised through real commands."""

from dataclasses import replace
from threading import Event, Thread

import pytest

from hibiki.domain.enums import TaskState, WorkspaceState
from hibiki.runtime.clock import FakeClock
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


def test_delayed_start_after_precheck_cannot_outlive_stop_barrier(tmp_path):
    clock = FakeClock()
    svc, ctx = make_core(tmp_path, clock=clock, dispatch_enabled=False)
    human = human_auth()
    tid, wu = approve_flow(svc, human)
    old = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    item = svc.executor.run(svc._claim_one_outbox)
    entered, release = Event(), Event()
    original_start = ctx["agent"].start

    def delayed_start(spec):
        if spec["run_id"] == old:
            entered.set()
            assert release.wait(10), "test dispatcher was not released"
        return original_start(spec)

    ctx["agent"].start = delayed_start
    worker = Thread(target=svc._dispatch_outbox_item, args=(item,))
    worker.start()
    try:
        assert entered.wait(5)  # Already past the Core's final pre-send check.
        clock.advance(seconds=60)
        svc.reconcile()
        gate = svc.execute("open_blocking_gate", human, {"task_id": tid, "reason": "hold"})
        assert svc.get_workspace(f"ws_{wu}")["owner_run_id"] == old
        assert svc.execute(
            "resolve_decision",
            human,
            {
                "decision_id": gate.data["decision_id"],
                "choice": "APPROVE",
            },
        ).ok
        assert (
            svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"] == []
        )
        svc.drain_outbox()  # Stop must revoke even though start is still delayed.
        svc.dispatch_enabled = True
        new = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
        assert new != old
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not ctx["agent"].is_alive(old)
    assert ctx["agent"].is_alive(new)
    assert svc.get_workspace(f"ws_{wu}")["owner_run_id"] == new
    # Direct replays at the Adapter boundary are revoked too.
    assert original_start(item["payload"])["start_revoked"] is True
    assert not ctx["agent"].is_alive(old)


def test_missing_process_without_revocation_ack_keeps_workspace(tmp_path):
    svc, ctx = make_core(tmp_path, dispatch_enabled=False)
    human = human_auth()
    tid, wu = approve_flow(svc, human)
    svc.execute("dispatch_ready_runs", human, {"task_id": tid})
    svc.executor.run(svc._claim_one_outbox)
    ctx["agent"].stop = lambda rid, reason: {"alive": False, "writer_alive": False}
    svc.execute("open_blocking_gate", human, {"task_id": tid})
    svc.drain_outbox()
    ws = svc.get_workspace(f"ws_{wu}")
    assert ws["owner_run_id"] is not None
    assert ws["writer_alive"] is True
    assert ws["state"] == WorkspaceState.QUARANTINED


def test_secondary_deliverable_fail_blocks_acceptance(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid, _ = approve_flow(svc, human)
    assert svc.execute(
        "activate_plan",
        human,
        {
            "task_id": tid,
            "nodes": [{"work_unit_id": "prod"}, {"work_unit_id": "verify", "work_type": "VERIFY"}],
            "edges": [
                {"from_work_unit_id": "prod", "to_work_unit_id": "verify", "predicate": "DONE"}
            ],
        },
    ).ok
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        rid,
        {
            "outcome": "COMPLETED",
            "verdict": "PASS",
            "artifact_refs": ["a", "b"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "b", "verdict": "PASS"}
            ],
        },
    )
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    submit_result_and_exit(
        svc,
        human,
        rid,
        {
            "outcome": "COMPLETED",
            "verdict": "FAIL",
            "artifact_refs": ["report"],
            "acceptance_evidence": [
                {"criterion_id": "c1", "artifact_hash": "b", "verdict": "FAIL"}
            ],
        },
    )
    result = svc.execute("prepare_acceptance", human, {"task_id": tid})
    assert not result.ok and result.error_code == "evidence_failed"
    assert svc.get_task(tid)["state"] != TaskState.COMPLETED


@pytest.mark.parametrize("evidence_hash, expected", [(None, "missing_evidence"), ("b", None)])
def test_acceptance_requires_explicit_evidence_and_supports_secondary_hash(
    tmp_path, evidence_hash, expected
):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid, _ = approve_flow(svc, human)
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    result = {"outcome": "COMPLETED", "verdict": "PASS", "artifact_refs": ["a", "b"]}
    if evidence_hash:
        result["acceptance_evidence"] = [
            {"criterion_id": "c1", "artifact_hash": evidence_hash, "verdict": "PASS"}
        ]
    submit_result_and_exit(svc, human, rid, result)
    prep = svc.execute("prepare_acceptance", human, {"task_id": tid})
    assert prep.error_code == expected
    if expected is None:
        assert svc.execute("accept_result", human, {"decision_id": prep.data["decision_id"]}).ok


@pytest.mark.parametrize(
    "operation", ["submit_result", "heartbeat", "set_writer_alive", "confirm_run_exit"]
)
def test_runtime_credentials_cannot_write_another_run_or_replay_its_command(tmp_path, operation):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    ta, _ = approve_flow(svc, human)
    tb, wb = approve_flow(svc, human, title="other")
    ra = svc.execute("dispatch_ready_runs", human, {"task_id": ta}).data["created_runs"][0]
    rb = svc.execute("dispatch_ready_runs", human, {"task_id": tb}).data["created_runs"][0]
    auth_a, auth_b = run_auth(svc, ra), run_auth(svc, rb)
    assert auth_a.bound_fencing_epoch == auth_b.bound_fencing_epoch
    payload = {"run_id": rb, "fencing_epoch": auth_b.bound_fencing_epoch}
    if operation == "submit_result":
        payload["result"] = {"outcome": "COMPLETED", "verdict": "PASS", "artifact_refs": ["b"]}
    if operation == "set_writer_alive":
        payload.update(workspace_id=f"ws_{wb}", alive=True)
    rejected = svc.execute(operation, auth_a, payload)
    assert rejected.error_code == "authorization_denied"
    assert svc.execute(operation, auth_b, payload, message_id="legit", idempotency_key="once").ok
    replay = svc.execute(operation, auth_a, payload, message_id="new", idempotency_key="once")
    assert not replay.ok and replay.error_code == "authorization_denied"


@pytest.mark.parametrize(
    "caller", [internal_auth(), user_agent_auth(), user_agent_auth("other"), human_auth("other")]
)
def test_exit_confirmation_rejects_unbound_or_foreign_identity_before_stop(tmp_path, caller):
    svc, ctx = make_core(tmp_path)
    human = human_auth()
    tid, wu = approve_flow(svc, human)
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    r = svc.execute(
        "confirm_run_exit", caller, {"run_id": rid, "fencing_epoch": run_fencing_epoch(svc, rid)}
    )
    assert not r.ok and r.error_code == "authorization_denied"
    assert ctx["agent"].stopped == []
    assert ctx["agent"].is_alive(rid)
    assert svc.get_workspace(f"ws_{wu}")["owner_run_id"] == rid


def test_identity_and_epoch_binding_cannot_be_supplied_in_body(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid, _ = approve_flow(svc, human)
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    good = run_auth(svc, rid)
    payload = {
        "run_id": rid,
        "fencing_epoch": good.bound_fencing_epoch,
        "bound_run_id": rid,
        "bound_task_id": tid,
        "actor_id": good.actor_id,
    }
    for bad in [
        internal_auth(),
        replace(good, actor_id="other"),
        replace(good, bound_grant_epoch=99),
    ]:
        assert svc.execute("heartbeat", bad, payload).error_code == "authorization_denied"


def test_decision_idempotency_replays_across_message_ids(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid = svc.execute("create_task", human, {"title": "retry"}).data["task_id"]
    contract = svc.execute("submit_contract", human, {"task_id": tid})
    payload = {
        "decision_id": contract.data["decision_id"],
        "expected_target_hash": contract.data["content_hash"],
        "expected_target_version": contract.data["contract_version"],
    }
    first = svc.execute(
        "approve_contract", human, payload, message_id="approve1", idempotency_key="approve"
    )
    replay = svc.execute(
        "approve_contract", human, payload, message_id="approve2", idempotency_key="approve"
    )
    assert first.ok and replay.ok and replay.replayed and replay.data == first.data
    assert svc.execute("activate_minimal_plan", human, {"task_id": tid}).ok
    gen = svc.execute("replace_planner_generation", human, {"task_id": tid}).data["generation"]
    delta = svc.execute(
        "submit_plan_proposal",
        human,
        {
            "task_id": tid,
            "generation": gen,
            "changes_authorization": True,
            "delta": {"resource_limits": {"model_call_limit": 300}},
        },
    )
    decision = {"decision_id": delta.data["decision_id"], "choice": "APPROVE"}
    first = svc.execute(
        "resolve_decision", human, decision, message_id="resolve1", idempotency_key="resolve"
    )
    replay = svc.execute(
        "resolve_decision", human, decision, message_id="resolve2", idempotency_key="resolve"
    )
    assert first.ok and replay.ok and replay.replayed and replay.data == first.data
    payload = {"decision_id": delta.data["decision_id"]}
    first = svc.execute(
        "apply_contract_delta", human, payload, message_id="apply1", idempotency_key="apply"
    )
    replay = svc.execute(
        "apply_contract_delta", human, payload, message_id="apply2", idempotency_key="apply"
    )
    assert first.ok and replay.ok and replay.replayed and replay.data == first.data
    assert svc.get_task(tid)["contract_version"] == 2
    conflict = svc.execute(
        "apply_contract_delta",
        human,
        {**payload, "extra": True},
        message_id="apply3",
        idempotency_key="apply",
    )
    assert conflict.error_code == "idempotency_conflict"


def test_legacy_empty_namespace_result_can_be_replayed(tmp_path):
    from sqlalchemy import select

    from hibiki.persistence.models import IdempotencyRow

    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid, _ = approve_flow(svc, human)
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    auth = run_auth(svc, rid)
    payload = {
        "run_id": rid,
        "fencing_epoch": auth.bound_fencing_epoch,
        "result": {"outcome": "COMPLETED", "verdict": "PASS"},
    }
    first = svc.execute("submit_result", auth, payload, idempotency_key="legacy")
    assert first.ok

    def old_namespace(session):
        row = session.scalars(
            select(IdempotencyRow).where(
                IdempotencyRow.operation_type == "submit_result",
                IdempotencyRow.idempotency_key == "legacy",
            )
        ).one()
        row.task_id = ""

    svc.executor.run(old_namespace)
    replay = svc.execute("submit_result", auth, payload, idempotency_key="legacy")
    assert replay.ok and replay.replayed and replay.data == first.data


def test_duplicate_criterion_evidence_is_rejected_atomically(tmp_path):
    svc, _ = make_core(tmp_path)
    human = human_auth()
    tid, wu = approve_flow(svc, human)
    rid = svc.execute("dispatch_ready_runs", human, {"task_id": tid}).data["created_runs"][0]
    auth = run_auth(svc, rid)
    result = svc.execute(
        "submit_result",
        auth,
        {
            "run_id": rid,
            "fencing_epoch": auth.bound_fencing_epoch,
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": ["a"],
                "acceptance_evidence": [
                    {"criterion_id": "c1", "artifact_hash": "a", "verdict": verdict}
                    for verdict in ["PASS", "FAIL"]
                ],
            },
        },
    )
    assert result.error_code == "duplicate_evidence"
    assert svc.get_work_unit(wu)["status"] == "RUNNING"
