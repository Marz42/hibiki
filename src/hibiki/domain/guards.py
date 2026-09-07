from __future__ import annotations

from hibiki.domain.enums import ActorType, DecisionKind, TaskState
from hibiki.domain.errors import AuthorizationError, PreconditionError
from hibiki.domain.transitions import can_dispatch_business_run, is_terminal_task
from hibiki.domain.types import AuthContext

HUMAN_ONLY_OPS = frozenset(
    {
        "approve_contract",
        "resolve_decision",
        "accept_result",
        "resume_task",
    }
)

INTERRUPT_OPS = frozenset({"pause_task", "cancel_task"})


def require_human(auth: AuthContext, operation: str) -> None:
    if auth.actor_type != ActorType.HUMAN:
        raise AuthorizationError(
            f"operation {operation!r} requires Human actor; got {auth.actor_type}",
            code="authorization_denied",
        )


def require_interrupt_scope(auth: AuthContext, operation: str) -> None:
    if auth.is_human():
        return
    if not auth.has_scope("task:interrupt"):
        raise AuthorizationError(
            f"operation {operation!r} requires task:interrupt scope",
            code="authorization_denied",
        )


def guard_operation(auth: AuthContext, operation: str) -> None:
    if operation in HUMAN_ONLY_OPS:
        require_human(auth, operation)
    elif operation in INTERRUPT_OPS:
        require_interrupt_scope(auth, operation)


def guard_dispatch(
    *,
    task_state: TaskState,
    has_active_contract: bool,
    has_blocking_gate: bool,
    cancel_intent: bool,
    pause_intent: bool,
) -> None:
    if not can_dispatch_business_run(task_state):
        raise PreconditionError(
            f"task state {task_state} does not allow business run dispatch",
            code="dispatch_blocked_state",
        )
    if not has_active_contract:
        raise PreconditionError(
            "ACTIVE Contract required before PLAN/EXECUTE dispatch",
            code="no_active_contract",
        )
    if has_blocking_gate:
        raise PreconditionError(
            "blocking gate open; dispatch frozen",
            code="dispatch_frozen_gate",
        )
    if cancel_intent:
        raise PreconditionError("cancel intent active", code="dispatch_frozen_cancel")
    if pause_intent:
        raise PreconditionError("pause intent active", code="dispatch_frozen_pause")


def guard_final_complete(*, task_state: TaskState, has_acceptance: bool) -> None:
    if is_terminal_task(task_state) and task_state != TaskState.COMPLETED:
        raise PreconditionError("task already terminal", code="already_terminal")
    if not has_acceptance:
        raise PreconditionError(
            "Human Final Acceptance required before COMPLETED",
            code="acceptance_required",
        )


def guard_decision_kind_human(kind: DecisionKind) -> bool:
    return kind in {
        DecisionKind.CONTRACT_APPROVAL,
        DecisionKind.CONTRACT_DELTA,
        DecisionKind.SIDE_EFFECT_APPROVAL,
        DecisionKind.FINAL_ACCEPTANCE,
    }
