"""Property / random-schedule invariant tests with fixed seeds.

Each trajectory must submit a measurable number of inputs (legal, illegal,
duplicate, and late/terminal), and must actually reach the guarded paths
(dispatch, agent start, worker result, terminal state). Terminal task states do
not stop the loop — post-terminal illegal and replay inputs continue until the
step budget is met.
"""

from __future__ import annotations

import random
from collections import Counter

from sqlalchemy import select

from hibiki.domain.enums import DecisionStatus, TaskState
from hibiki.persistence.models import DecisionRow
from tests.helpers import human_auth, make_core, run_auth, run_fencing_epoch, user_agent_auth

OPS = (
    "submit_contract",
    "approve",
    "activate",
    "dispatch",
    "submit_result",
    "confirm_exit",
    "pause",
    "quiesce",
    "resume",
    "cancel",
    "ua_approve",  # illegal
    "duplicate_approve",
    "duplicate_dispatch",
    "late_result",
    "terminal_illegal",
)

#: Operations that are always safe to submit (may still be refused on
#: preconditions); used when no state-specific legal operation is available.
GENERIC_OPS = (
    "dispatch",
    "submit_result",
    "confirm_exit",
    "late_result",
    "duplicate_dispatch",
    "pause",
    "quiesce",
    "resume",
    "cancel",
    "duplicate_approve",
)

#: Illegal / unsupported inputs, injected on a small slice of every trajectory.
INVALID_OPS = ("ua_approve", "terminal_illegal")


def _legal_ops_for(
    state: TaskState,
    decision_id: str | None,
    approved_once: bool,
    known_run_ids: list[str],
) -> tuple[str, ...]:
    """Operations that advance the legal M0 path from ``state``."""
    ops: list[str] = []
    if decision_id is None and state in {TaskState.NEW, TaskState.WAITING_HUMAN}:
        ops.append("submit_contract")
    if decision_id is not None and not approved_once:
        ops.append("approve")
    if approved_once and state in {TaskState.PLANNING, TaskState.EXECUTING}:
        ops.append("activate")
    if known_run_ids and state in {
        TaskState.EXECUTING,
        TaskState.PLANNING,
        TaskState.VERIFYING,
    }:
        ops.append("dispatch")
    if known_run_ids and state == TaskState.EXECUTING:
        ops.extend(("submit_result", "confirm_exit"))
    if state == TaskState.PAUSED:
        ops.append("resume")
    return tuple(ops)


def _pick_op(
    rng: random.Random,
    state: TaskState,
    decision_id: str | None,
    approved_once: bool,
    known_run_ids: list[str],
) -> str:
    """Choose the next input: mostly legal-path progress, sometimes an illegal one."""
    if rng.random() < 0.12:
        return rng.choice(INVALID_OPS)
    legal = _legal_ops_for(state, decision_id, approved_once, known_run_ids)
    if legal and rng.random() < 0.85:
        return rng.choice(legal)
    return rng.choice(GENERIC_OPS)


def _approved_decisions_by_target(svc) -> Counter:
    """Authoritative count of APPROVED decision rows per target hash."""

    def _read(session):
        rows = session.scalars(
            select(DecisionRow).where(DecisionRow.status == "APPROVED")
        ).all()
        return Counter(row.target_hash for row in rows)

    return svc.executor.run(_read)


def _run_trajectory(tmp_path, seed: int, steps: int = 200) -> dict:
    rng = random.Random(seed)
    svc, _ = make_core(tmp_path / f"seed_{seed}")
    human = human_auth()
    ua = user_agent_auth()
    execute_count = 0
    op_counts: Counter[str] = Counter()

    def _exec(op: str, auth, payload, **kwargs):
        nonlocal execute_count
        execute_count += 1
        op_counts[op] += 1
        return svc.execute(op, auth, payload, **kwargs)

    r = _exec("create_task", human, {"title": f"seed-{seed}"})
    assert r.ok
    task_id = r.data["task_id"]
    decision_id = None
    content_hash = None
    version = None
    approved_once = False
    formal_approvals = 0
    valid_dispatches = 0
    worker_submissions = 0
    known_run_ids: list[str] = []

    # ------------------------------------------------------------- checkpoint
    # Every seed must drive the full legal M0 path at least once: approval ->
    # activation -> dispatch -> agent start -> worker result -> exit confirm.
    # A uniform random walk over OPS never satisfies the guards, so a green run
    # without this checkpoint proves nothing about the protected paths.
    r = _exec(
        "submit_contract",
        human,
        {"task_id": task_id, "objective": "checkpoint"},
        idempotency_key="cp-sc",
        message_id=f"cp-sc-{seed}",
    )
    assert r.ok, r
    decision_id = r.data["decision_id"]
    content_hash = r.data["content_hash"]
    version = r.data["contract_version"]

    r = _exec(
        "approve_contract",
        human,
        {
            "decision_id": decision_id,
            "expected_target_hash": content_hash,
            "expected_target_version": version,
        },
        idempotency_key="cp-approve",
        message_id=f"cp-ap-{seed}",
    )
    assert r.ok, r
    approved_once = True
    formal_approvals += 1

    # The same approval again must replay, never create a second formal decision.
    r = _exec(
        "approve_contract",
        human,
        {
            "decision_id": decision_id,
            "expected_target_hash": content_hash,
            "expected_target_version": version,
        },
        message_id=f"cp-dup-ap-{seed}",
    )
    assert r.ok and (r.replayed or r.data.get("replayed")), r

    # A User Agent self-reporting as approver must be refused.
    r = _exec(
        "approve_contract",
        ua,
        {"decision_id": decision_id},
        message_id=f"cp-ua-ap-{seed}",
    )
    assert not r.ok

    r = _exec(
        "activate_minimal_plan",
        human,
        {"task_id": task_id},
        idempotency_key="cp-plan",
        message_id=f"cp-plan-{seed}",
    )
    assert r.ok, r

    r = _exec(
        "dispatch_ready_runs",
        human,
        {"task_id": task_id},
        message_id=f"cp-di-{seed}",
    )
    assert r.ok and r.data["created_runs"], r
    checkpoint_run_id = r.data["created_runs"][0]
    known_run_ids.append(checkpoint_run_id)
    valid_dispatches += 1

    checkpoint_run = next(
        x for x in svc.list_runs(task_id) if x["run_id"] == checkpoint_run_id
    )
    r = _exec(
        "submit_result",
        run_auth(svc, checkpoint_run_id),
        {
            "run_id": checkpoint_run_id,
            "fencing_epoch": checkpoint_run["fencing_epoch"],
            "result": {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": [f"art-{checkpoint_run_id}"],
            },
        },
        message_id=f"cp-sr-{seed}",
    )
    assert r.ok, r
    worker_submissions += 1

    # ----------------------------------------------------------- random walk
    remaining = max(steps - execute_count, 1)
    for step in range(remaining):
        state = svc.get_task(task_id)["state"]
        op = _pick_op(rng, state, decision_id, approved_once, known_run_ids)
        terminal = state in {TaskState.ABORTED, TaskState.COMPLETED, TaskState.FAILED}

        if terminal or op == "terminal_illegal":
            # Keep submitting illegal / duplicate / late inputs after terminal
            if rng.random() < 0.5 and decision_id:
                r = _exec(
                    "approve_contract",
                    ua,
                    {"decision_id": decision_id},
                    message_id=f"term-ua-{seed}-{step}",
                )
                assert not r.ok
            elif known_run_ids:
                rid = known_run_ids[0]
                try:
                    fencing = run_fencing_epoch(svc, rid)
                except AssertionError:
                    fencing = 0
                try:
                    worker = run_auth(svc, rid)
                    fencing = worker.bound_fencing_epoch
                except AssertionError:
                    worker = None
                if worker is not None:
                    _exec(
                        "submit_result",
                        worker,
                        {
                            "run_id": rid,
                            "fencing_epoch": fencing,
                            "result": {"outcome": "COMPLETED", "artifact_refs": ["late"]},
                        },
                        message_id=f"term-late-{seed}-{step}",
                    )
                else:
                    _exec(
                        "submit_result",
                        human,
                        {
                            "run_id": rid,
                            "fencing_epoch": fencing,
                            "result": {"outcome": "COMPLETED", "artifact_refs": ["late"]},
                        },
                        message_id=f"term-late-{seed}-{step}",
                    )
                # may fail (terminal run) or be late history — must not revive task
            else:
                _exec(
                    "dispatch_ready_runs",
                    human,
                    {"task_id": task_id},
                    message_id=f"term-di-{seed}-{step}",
                )
            assert svc.get_task(task_id)["state"] in {
                TaskState.ABORTED,
                TaskState.COMPLETED,
                TaskState.FAILED,
                TaskState.PAUSED,
                TaskState.WAITING_HUMAN,
                TaskState.EXECUTING,
                TaskState.PLANNING,
                TaskState.VERIFYING,
                TaskState.PAUSING,
                TaskState.CANCELLING,
                TaskState.NEW,
            }
            if terminal:
                assert svc.get_task(task_id)["state"] == state
            continue

        if op == "submit_contract" and state in {TaskState.NEW, TaskState.WAITING_HUMAN}:
            r = _exec(
                "submit_contract",
                human,
                {"task_id": task_id, "objective": f"o-{rng.randrange(3)}"},
                idempotency_key=f"sc-{rng.randrange(50)}",
                message_id=f"m-{seed}-{step}",
            )
            if r.ok and not r.replayed:
                decision_id = r.data["decision_id"]
                content_hash = r.data["content_hash"]
                version = r.data["contract_version"]

        elif op == "approve" and decision_id and not approved_once:
            r = _exec(
                "approve_contract",
                human,
                {
                    "decision_id": decision_id,
                    "expected_target_hash": content_hash,
                    "expected_target_version": version,
                },
                idempotency_key="approve-once",
                message_id=f"ap-{seed}-{step}",
            )
            if r.ok and r.data.get("status") == DecisionStatus.APPROVED:
                if not r.replayed and not r.data.get("replayed"):
                    formal_approvals += 1
                    approved_once = True

        elif op == "duplicate_approve" and decision_id and approved_once:
            r = _exec(
                "approve_contract",
                human,
                {
                    "decision_id": decision_id,
                    "expected_target_hash": content_hash,
                    "expected_target_version": version,
                },
                message_id=f"dap-{seed}-{step}",
            )
            if r.ok:
                assert (
                    r.replayed
                    or r.data.get("replayed")
                    or r.data["status"] == DecisionStatus.APPROVED
                )

        elif op == "ua_approve" and decision_id:
            r = _exec(
                "approve_contract",
                ua,
                {"decision_id": decision_id},
                message_id=f"ua-{seed}-{step}",
            )
            assert not r.ok

        elif op == "activate" and approved_once and state in {
            TaskState.PLANNING,
            TaskState.EXECUTING,
        }:
            _exec(
                "activate_minimal_plan",
                human,
                {"task_id": task_id},
                idempotency_key="plan1",
                message_id=f"pl-{seed}-{step}",
            )

        elif op == "dispatch" and state in {
            TaskState.EXECUTING,
            TaskState.PLANNING,
            TaskState.VERIFYING,
        }:
            before = len(svc.list_runs(task_id))
            r = _exec(
                "dispatch_ready_runs",
                human,
                {"task_id": task_id},
                message_id=f"di-{seed}-{step}",
            )
            after = len(svc.list_runs(task_id))
            if r.ok and after > before:
                valid_dispatches += 1
                for run in svc.list_runs(task_id):
                    if run["run_id"] not in known_run_ids:
                        known_run_ids.append(run["run_id"])

        elif op == "duplicate_dispatch" and state == TaskState.EXECUTING:
            _exec(
                "dispatch_ready_runs",
                human,
                {"task_id": task_id},
                message_id=f"dd-{seed}-{step}",
            )
            runs_after = [
                x for x in svc.list_runs(task_id) if x["status"] in {"CREATED", "RUNNING"}
            ]
            assert len(runs_after) <= 2

        elif op == "submit_result":
            for run in svc.list_runs(task_id):
                if run["status"] == "RUNNING":
                    r = _exec(
                        "submit_result",
                        run_auth(svc, run["run_id"]),
                        {
                            "run_id": run["run_id"],
                            "fencing_epoch": run["fencing_epoch"],
                            "result": {
                                "outcome": "COMPLETED",
                                "verdict": "PASS",
                                "artifact_refs": [f"art-{run['run_id']}"],
                            },
                        },
                        message_id=f"sr-{seed}-{step}",
                    )
                    if r.ok:
                        worker_submissions += 1
                    if run["run_id"] not in known_run_ids:
                        known_run_ids.append(run["run_id"])
                    break
            else:
                # No running run — still count an illegal/no-op input
                if known_run_ids:
                    bad = run_auth(svc, known_run_ids[0])
                    _exec(
                        "submit_result",
                        bad,
                        {
                            "run_id": "run_missing",
                            "fencing_epoch": 0,
                            "result": {"outcome": "COMPLETED"},
                        },
                        message_id=f"sr-miss-{seed}-{step}",
                    )
                else:
                    _exec(
                        "submit_result",
                        human,
                        {
                            "run_id": "run_missing",
                            "fencing_epoch": 0,
                            "result": {"outcome": "COMPLETED"},
                        },
                        message_id=f"sr-miss-{seed}-{step}",
                    )

        elif op == "confirm_exit":
            for run in svc.list_runs(task_id):
                if run["status"] == "SUCCEEDED":
                    _exec(
                        "confirm_run_exit",
                        human,
                        {"run_id": run["run_id"]},
                        message_id=f"ex-{seed}-{step}",
                    )
                    break
            else:
                _exec(
                    "confirm_run_exit",
                    human,
                    {"run_id": "run_missing"},
                    message_id=f"ex-miss-{seed}-{step}",
                )

        elif op == "late_result" and known_run_ids:
            rid = known_run_ids[0]
            try:
                fencing = run_fencing_epoch(svc, rid)
            except AssertionError:
                fencing = 0
            try:
                worker = run_auth(svc, rid)
                fencing = worker.bound_fencing_epoch
            except AssertionError:
                worker = None
            if worker is not None:
                _exec(
                    "submit_result",
                    worker,
                    {
                        "run_id": rid,
                        "fencing_epoch": fencing,
                        "result": {"outcome": "COMPLETED", "artifact_refs": ["late"]},
                    },
                    message_id=f"late-{seed}-{step}",
                )
            else:
                _exec(
                    "submit_result",
                    human,
                    {
                        "run_id": rid,
                        "fencing_epoch": fencing,
                        "result": {"outcome": "COMPLETED", "artifact_refs": ["late"]},
                    },
                    message_id=f"late-{seed}-{step}",
                )

        elif op == "pause" and state not in {
            TaskState.PAUSING,
            TaskState.PAUSED,
            TaskState.CANCELLING,
        }:
            _exec("pause_task", human, {"task_id": task_id}, message_id=f"pa-{seed}-{step}")

        elif op == "quiesce" and state == TaskState.PAUSING:
            # Ensure writers are dead so quiesce can succeed when possible
            for run in svc.list_runs(task_id):
                if run["status"] in {"CREATED", "RUNNING", "SUCCEEDED"}:
                    svc.agent_adapter.mark_dead(run["run_id"])
                    _exec(
                        "confirm_run_exit",
                        human,
                        {"run_id": run["run_id"]},
                        message_id=f"qex-{seed}-{step}-{run['run_id']}",
                    )
            _exec(
                "runtime_quiescent",
                human,
                {"task_id": task_id},
                message_id=f"q-{seed}-{step}",
            )

        elif op == "resume" and state == TaskState.PAUSED:
            _exec("resume_task", human, {"task_id": task_id}, message_id=f"r-{seed}-{step}")

        elif op == "cancel" and state != TaskState.CANCELLING:
            for run in svc.list_runs(task_id):
                if run["status"] in {"CREATED", "RUNNING", "SUCCEEDED"}:
                    svc.agent_adapter.mark_dead(run["run_id"])
                    _exec(
                        "confirm_run_exit",
                        human,
                        {"run_id": run["run_id"]},
                        message_id=f"cex-{seed}-{step}-{run['run_id']}",
                    )
            _exec("cancel_task", human, {"task_id": task_id}, message_id=f"c-{seed}-{step}")
            _exec(
                "cancellation_settled",
                human,
                {"task_id": task_id, "accept_unknown": True},
                message_id=f"cs-{seed}-{step}",
            )

        else:
            # Chosen op not applicable — still submit a counted no-op/illegal input
            _exec(
                "dispatch_ready_runs",
                human,
                {"task_id": task_id},
                message_id=f"noop-{seed}-{step}",
            )

        # Invariant: never COMPLETED without acceptance event
        st = svc.get_task(task_id)["state"]
        if st == TaskState.COMPLETED:
            events = [e["event_type"] for e in svc.list_events(task_id)]
            assert "decision.resolved" in events

    assert formal_approvals <= 1
    approved_by_target = _approved_decisions_by_target(svc)
    assert approved_by_target[content_hash] == 1, (
        f"seed {seed}: checkpoint target hash approved "
        f"{approved_by_target[content_hash]} times"
    )
    assert max(approved_by_target.values()) == 1, (
        f"seed {seed}: a target hash was formally approved more than once: "
        f"{dict(approved_by_target)}"
    )
    assert execute_count >= steps, (
        f"seed {seed}: expected >= {steps} execute() calls, got {execute_count}"
    )
    return {
        "seed": seed,
        "execute_count": execute_count,
        "op_counts": dict(op_counts),
        "formal_approvals": formal_approvals,
        "valid_dispatches": valid_dispatches,
        "worker_submissions": worker_submissions,
        "final_state": svc.get_task(task_id)["state"],
    }


def test_100_fixed_seeds(tmp_path):
    summaries = []
    for seed in range(100):
        summaries.append(_run_trajectory(tmp_path, seed, steps=200))
    assert all(s["execute_count"] >= 200 for s in summaries)
    # At least create_task + trajectory steps
    assert min(s["execute_count"] for s in summaries) >= 200
    # The measurement must exercise the paths it exists to protect: every seed
    # reaches a live dispatch and a worker result through the checkpoint, and
    # almost every seed ends in a terminal state after the random walk.
    assert all(s["valid_dispatches"] >= 1 for s in summaries)
    assert all(s["worker_submissions"] >= 1 for s in summaries)
    terminal_states = {TaskState.COMPLETED, TaskState.ABORTED, TaskState.FAILED}
    assert sum(1 for s in summaries if s["final_state"] in terminal_states) >= 95


def test_random_schedule_reports_real_input_counts(tmp_path):
    """Sanity: first three seeds each perform measurable execute() volume."""
    for seed in range(3):
        summary = _run_trajectory(tmp_path, seed, steps=200)
        assert summary["execute_count"] >= 200
        assert sum(summary["op_counts"].values()) == summary["execute_count"]
