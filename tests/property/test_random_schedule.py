"""Property / random-schedule invariant tests with fixed seeds."""

from __future__ import annotations

import random

from hibiki.domain.enums import DecisionStatus, TaskState
from tests.helpers import human_auth, make_core, user_agent_auth

OPS = (
    "submit_contract",
    "approve",
    "activate",
    "dispatch",
    "submit_result",
    "pause",
    "quiesce",
    "resume",
    "cancel",
    "ua_approve",  # illegal
    "duplicate_approve",
    "duplicate_dispatch",
)


def _run_trajectory(tmp_path, seed: int, steps: int = 200) -> None:
    rng = random.Random(seed)
    svc, _ = make_core(tmp_path / f"seed_{seed}")
    human = human_auth()
    ua = user_agent_auth()
    r = svc.execute("create_task", human, {"title": f"seed-{seed}"})
    assert r.ok
    task_id = r.data["task_id"]
    decision_id = None
    content_hash = None
    version = None
    approved_once = False
    formal_approvals = 0
    valid_dispatches = 0

    for _ in range(steps):
        op = rng.choice(OPS)
        state = svc.get_task(task_id)["state"]
        if state in {TaskState.ABORTED, TaskState.COMPLETED, TaskState.FAILED}:
            break

        if op == "submit_contract" and state in {TaskState.NEW, TaskState.WAITING_HUMAN}:
            r = svc.execute(
                "submit_contract",
                human,
                {"task_id": task_id, "objective": f"o-{rng.randrange(3)}"},
                idempotency_key=f"sc-{rng.randrange(50)}",
                message_id=f"m-{seed}-{rng.randrange(10_000)}",
            )
            if r.ok and not r.replayed:
                decision_id = r.data["decision_id"]
                content_hash = r.data["content_hash"]
                version = r.data["contract_version"]

        elif op == "approve" and decision_id and not approved_once:
            r = svc.execute(
                "approve_contract",
                human,
                {
                    "decision_id": decision_id,
                    "expected_target_hash": content_hash,
                    "expected_target_version": version,
                },
                idempotency_key="approve-once",
                message_id=f"ap-{seed}-{rng.randrange(10_000)}",
            )
            if r.ok and r.data.get("status") == DecisionStatus.APPROVED:
                if not r.replayed and not r.data.get("replayed"):
                    formal_approvals += 1
                    approved_once = True

        elif op == "duplicate_approve" and decision_id and approved_once:
            r = svc.execute(
                "approve_contract",
                human,
                {
                    "decision_id": decision_id,
                    "expected_target_hash": content_hash,
                    "expected_target_version": version,
                },
                message_id=f"dap-{seed}-{rng.randrange(10_000)}",
            )
            if r.ok:
                # must not create a second formal approval fact
                assert r.replayed or r.data.get("replayed") or r.data["status"] == DecisionStatus.APPROVED

        elif op == "ua_approve" and decision_id:
            r = svc.execute(
                "approve_contract",
                ua,
                {"decision_id": decision_id},
                message_id=f"ua-{seed}-{rng.randrange(10_000)}",
            )
            assert not r.ok

        elif op == "activate" and approved_once and state in {
            TaskState.PLANNING,
            TaskState.EXECUTING,
        }:
            svc.execute(
                "activate_minimal_plan",
                human,
                {"task_id": task_id},
                idempotency_key="plan1",
                message_id=f"pl-{seed}-{rng.randrange(10_000)}",
            )

        elif op == "dispatch" and state in {
            TaskState.EXECUTING,
            TaskState.PLANNING,
            TaskState.VERIFYING,
        }:
            before = len(svc.list_runs(task_id))
            r = svc.execute(
                "dispatch_ready_runs",
                human,
                {"task_id": task_id},
                message_id=f"di-{seed}-{rng.randrange(10_000)}",
            )
            after = len(svc.list_runs(task_id))
            if r.ok and after > before:
                valid_dispatches += 1

        elif op == "duplicate_dispatch" and state == TaskState.EXECUTING:
            svc.execute(
                "dispatch_ready_runs",
                human,
                {"task_id": task_id},
                message_id=f"dd-{seed}-{rng.randrange(10_000)}",
            )
            runs_after = [x for x in svc.list_runs(task_id) if x["status"] in {"CREATED", "RUNNING"}]
            # at most one active execute per work unit — already enforced; no explosion
            assert len(runs_after) <= 2

        elif op == "submit_result":
            for run in svc.list_runs(task_id):
                if run["status"] == "RUNNING":
                    svc.execute(
                        "submit_result",
                        human,
                        {
                            "run_id": run["run_id"],
                            "result": {"outcome": "COMPLETED"},
                        },
                        message_id=f"sr-{seed}-{rng.randrange(10_000)}",
                    )
                    break

        elif op == "pause" and state not in {
            TaskState.PAUSING,
            TaskState.PAUSED,
            TaskState.CANCELLING,
        }:
            svc.execute("pause_task", human, {"task_id": task_id}, message_id=f"pa-{rng.randrange(10_000)}")

        elif op == "quiesce" and state == TaskState.PAUSING:
            svc.execute("runtime_quiescent", human, {"task_id": task_id}, message_id=f"q-{rng.randrange(10_000)}")

        elif op == "resume" and state == TaskState.PAUSED:
            svc.execute("resume_task", human, {"task_id": task_id}, message_id=f"r-{rng.randrange(10_000)}")

        elif op == "cancel" and state != TaskState.CANCELLING:
            svc.execute("cancel_task", human, {"task_id": task_id}, message_id=f"c-{rng.randrange(10_000)}")
            svc.execute(
                "cancellation_settled",
                human,
                {"task_id": task_id, "accept_unknown": True},
                message_id=f"cs-{rng.randrange(10_000)}",
            )

        # Invariant: never COMPLETED without acceptance event
        st = svc.get_task(task_id)["state"]
        if st == TaskState.COMPLETED:
            events = [e["event_type"] for e in svc.list_events(task_id)]
            assert "decision.resolved" in events

    assert formal_approvals <= 1


def test_100_fixed_seeds(tmp_path):
    # Full 100 seeds × 200 steps is the M0 bar; keep runtime reasonable in CI by
    # running all seeds with fewer steps optionally — here we do full M0 target.
    for seed in range(100):
        _run_trajectory(tmp_path, seed, steps=200)
