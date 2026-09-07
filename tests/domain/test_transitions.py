from __future__ import annotations

import pytest

from hibiki.domain.enums import AgentRunStatus, TaskState
from hibiki.domain.errors import InvalidTransitionError, PreconditionError
from hibiki.domain.guards import guard_dispatch
from hibiki.domain.plan import PlanEdge, PlanNode, validate_dag
from hibiki.domain.transitions import transition_run, transition_task


def test_task_transition_happy_path():
    assert transition_task(TaskState.NEW, "contract.submitted") == TaskState.WAITING_HUMAN


def test_illegal_task_transition():
    with pytest.raises(InvalidTransitionError):
        transition_task(TaskState.COMPLETED, "pause.requested")


def test_run_terminal_irreversible():
    with pytest.raises(InvalidTransitionError):
        transition_run(AgentRunStatus.SUCCEEDED, "start")


def test_guard_dispatch_requires_contract():
    with pytest.raises(PreconditionError) as ei:
        guard_dispatch(
            task_state=TaskState.EXECUTING,
            has_active_contract=False,
            has_blocking_gate=False,
            cancel_intent=False,
            pause_intent=False,
        )
    assert ei.value.code == "no_active_contract"


def test_dag_cycle_rejected():
    nodes = [
        PlanNode("a", 1),
        PlanNode("b", 1),
    ]
    edges = [
        PlanEdge("a", "b"),
        PlanEdge("b", "a"),
    ]
    with pytest.raises(PreconditionError) as ei:
        validate_dag(task_id="t", nodes=nodes, edges=edges)
    assert ei.value.code == "plan_cycle"
