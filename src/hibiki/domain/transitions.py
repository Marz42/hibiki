from __future__ import annotations

from hibiki.domain.enums import AgentRunStatus, SideEffectState, TaskState, WorkUnitStatus
from hibiki.domain.errors import InvalidTransitionError

# Task transitions: (from_state, event) -> to_state
TASK_TRANSITIONS: dict[tuple[TaskState, str], TaskState] = {
    (TaskState.NEW, "clarification"): TaskState.WAITING_HUMAN,
    (TaskState.NEW, "contract.submitted"): TaskState.WAITING_HUMAN,
    (TaskState.WAITING_HUMAN, "contract.approved"): TaskState.PLANNING,  # or EXECUTING via apply
    (TaskState.WAITING_HUMAN, "clarification.answered"): TaskState.NEW,  # intake continues
    (TaskState.PLANNING, "plan.activated"): TaskState.EXECUTING,
    (TaskState.PLANNING, "blocking_gate.opened"): TaskState.WAITING_HUMAN,
    (TaskState.EXECUTING, "blocking_gate.opened"): TaskState.WAITING_HUMAN,
    (TaskState.VERIFYING, "blocking_gate.opened"): TaskState.WAITING_HUMAN,
    (TaskState.WAITING_HUMAN, "decision.resolved"): TaskState.EXECUTING,  # recomputed later
    (TaskState.EXECUTING, "required_outputs.ready"): TaskState.VERIFYING,
    (TaskState.VERIFYING, "verification.failed"): TaskState.EXECUTING,
    (TaskState.VERIFYING, "acceptance.ready"): TaskState.WAITING_HUMAN,
    (TaskState.WAITING_HUMAN, "final.accepted"): TaskState.COMPLETED,
    (TaskState.NEW, "pause.requested"): TaskState.PAUSING,
    (TaskState.WAITING_HUMAN, "pause.requested"): TaskState.PAUSING,
    (TaskState.PLANNING, "pause.requested"): TaskState.PAUSING,
    (TaskState.EXECUTING, "pause.requested"): TaskState.PAUSING,
    (TaskState.VERIFYING, "pause.requested"): TaskState.PAUSING,
    (TaskState.PAUSING, "runtime.quiescent"): TaskState.PAUSED,
    (TaskState.PAUSED, "resume.requested"): TaskState.WAITING_HUMAN,  # or runnable; recomputed
    (TaskState.NEW, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.WAITING_HUMAN, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.PLANNING, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.EXECUTING, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.VERIFYING, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.PAUSING, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.PAUSED, "cancel.requested"): TaskState.CANCELLING,
    (TaskState.CANCELLING, "cancellation.settled"): TaskState.ABORTED,
    (TaskState.NEW, "task.failed"): TaskState.FAILED,
    (TaskState.WAITING_HUMAN, "task.failed"): TaskState.FAILED,
    (TaskState.PLANNING, "task.failed"): TaskState.FAILED,
    (TaskState.EXECUTING, "task.failed"): TaskState.FAILED,
    (TaskState.VERIFYING, "task.failed"): TaskState.FAILED,
    (TaskState.PAUSING, "task.failed"): TaskState.FAILED,
    (TaskState.PAUSED, "task.failed"): TaskState.FAILED,
}

TERMINAL_TASK_STATES = frozenset(
    {TaskState.COMPLETED, TaskState.FAILED, TaskState.ABORTED}
)

DISPATCHABLE_TASK_STATES = frozenset(
    {TaskState.PLANNING, TaskState.EXECUTING, TaskState.VERIFYING}
)

WORK_UNIT_TRANSITIONS: dict[tuple[WorkUnitStatus, str], WorkUnitStatus] = {
    (WorkUnitStatus.PENDING, "run.started"): WorkUnitStatus.RUNNING,
    (WorkUnitStatus.RUNNING, "run.retryable_failed"): WorkUnitStatus.PENDING,
    (WorkUnitStatus.RUNNING, "run.blocked"): WorkUnitStatus.BLOCKED,
    (WorkUnitStatus.RUNNING, "run.completed"): WorkUnitStatus.DONE,
    (WorkUnitStatus.BLOCKED, "block.cleared"): WorkUnitStatus.PENDING,
    (WorkUnitStatus.PENDING, "unit.failed"): WorkUnitStatus.FAILED,
    (WorkUnitStatus.RUNNING, "unit.failed"): WorkUnitStatus.FAILED,
    (WorkUnitStatus.BLOCKED, "unit.failed"): WorkUnitStatus.FAILED,
    (WorkUnitStatus.PENDING, "unit.cancelled"): WorkUnitStatus.CANCELLED,
    (WorkUnitStatus.RUNNING, "unit.cancelled"): WorkUnitStatus.CANCELLED,
    (WorkUnitStatus.BLOCKED, "unit.cancelled"): WorkUnitStatus.CANCELLED,
}

TERMINAL_WORK_UNIT = frozenset(
    {WorkUnitStatus.DONE, WorkUnitStatus.FAILED, WorkUnitStatus.CANCELLED}
)

RUN_TRANSITIONS: dict[tuple[AgentRunStatus, str], AgentRunStatus] = {
    (AgentRunStatus.CREATED, "start"): AgentRunStatus.RUNNING,
    (AgentRunStatus.CREATED, "fail"): AgentRunStatus.FAILED,
    (AgentRunStatus.CREATED, "cancel"): AgentRunStatus.CANCELLED,
    (AgentRunStatus.RUNNING, "succeed"): AgentRunStatus.SUCCEEDED,
    (AgentRunStatus.RUNNING, "fail"): AgentRunStatus.FAILED,
    (AgentRunStatus.RUNNING, "timeout"): AgentRunStatus.TIMED_OUT,
    (AgentRunStatus.RUNNING, "cancel"): AgentRunStatus.CANCELLED,
    (AgentRunStatus.RUNNING, "lost"): AgentRunStatus.LOST,
}

TERMINAL_RUN = frozenset(
    {
        AgentRunStatus.SUCCEEDED,
        AgentRunStatus.FAILED,
        AgentRunStatus.TIMED_OUT,
        AgentRunStatus.CANCELLED,
        AgentRunStatus.LOST,
    }
)

SIDE_EFFECT_TRANSITIONS: dict[tuple[SideEffectState, str], SideEffectState] = {
    (SideEffectState.PROPOSED, "request_approval"): SideEffectState.WAITING_APPROVAL,
    (SideEffectState.PROPOSED, "preauthorize"): SideEffectState.AUTHORIZED,
    (SideEffectState.WAITING_APPROVAL, "approve"): SideEffectState.AUTHORIZED,
    (SideEffectState.WAITING_APPROVAL, "reject"): SideEffectState.CANCELLED,
    (SideEffectState.AUTHORIZED, "claim_dispatch"): SideEffectState.DISPATCHING,
    (SideEffectState.AUTHORIZED, "cancel"): SideEffectState.CANCELLED,
    (SideEffectState.DISPATCHING, "succeed"): SideEffectState.SUCCEEDED,
    (SideEffectState.DISPATCHING, "fail_confirmed"): SideEffectState.FAILED_CONFIRMED,
    (SideEffectState.DISPATCHING, "uncertain"): SideEffectState.UNKNOWN,
    (SideEffectState.PROPOSED, "cancel"): SideEffectState.CANCELLED,
    (SideEffectState.WAITING_APPROVAL, "cancel"): SideEffectState.CANCELLED,
}


def transition(
    table: dict[tuple[object, str], object],
    current: object,
    event: str,
    *,
    entity: str,
) -> object:
    key = (current, event)
    if key not in table:
        raise InvalidTransitionError(
            f"{entity} cannot apply {event!r} from {current}",
            code="invalid_transition",
        )
    return table[key]


def transition_task(current: TaskState, event: str) -> TaskState:
    return transition(TASK_TRANSITIONS, current, event, entity="Task")  # type: ignore[return-value]


def transition_work_unit(current: WorkUnitStatus, event: str) -> WorkUnitStatus:
    return transition(  # type: ignore[return-value]
        WORK_UNIT_TRANSITIONS, current, event, entity="WorkUnit"
    )


def transition_run(current: AgentRunStatus, event: str) -> AgentRunStatus:
    return transition(RUN_TRANSITIONS, current, event, entity="AgentRun")  # type: ignore[return-value]


def transition_side_effect(current: SideEffectState, event: str) -> SideEffectState:
    return transition(  # type: ignore[return-value]
        SIDE_EFFECT_TRANSITIONS, current, event, entity="SideEffect"
    )


def can_dispatch_business_run(state: TaskState) -> bool:
    return state in DISPATCHABLE_TASK_STATES


def is_terminal_task(state: TaskState) -> bool:
    return state in TERMINAL_TASK_STATES


def is_terminal_run(status: AgentRunStatus) -> bool:
    return status in TERMINAL_RUN


def is_terminal_work_unit(status: WorkUnitStatus) -> bool:
    return status in TERMINAL_WORK_UNIT
