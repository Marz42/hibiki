from __future__ import annotations

from hibiki.domain.enums import ActorType, DecisionKind, SideEffectState, TaskState
from hibiki.domain.errors import AuthorizationError, PreconditionError
from hibiki.domain.transitions import can_dispatch_business_run, is_terminal_task
from hibiki.domain.types import AuthContext

HUMAN_ONLY_OPS = frozenset(
    {
        "approve_contract",
        "approve_side_effect",
        "resolve_decision",
        "accept_result",
        "resume_task",
        "apply_contract_delta",
    }
)

INTERRUPT_OPS = frozenset({"pause_task", "cancel_task"})

# Executor / run-bound writes — not callable by User Agent or unaudited Human UI
INTERNAL_ONLY_OPS = frozenset(
    {
        "submit_result",
        "set_writer_alive",
        "heartbeat",
    }
)

HUMAN_DECISION_KINDS = frozenset(
    {
        DecisionKind.CONTRACT_APPROVAL,
        DecisionKind.CONTRACT_DELTA,
        DecisionKind.SIDE_EFFECT_APPROVAL,
        DecisionKind.FINAL_ACCEPTANCE,
    }
)


def require_human(auth: AuthContext, operation: str) -> None:
    if auth.actor_type != ActorType.HUMAN:
        raise AuthorizationError(
            f"operation {operation!r} requires Human actor; got {auth.actor_type}",
            code="authorization_denied",
        )


def require_internal(auth: AuthContext, operation: str) -> None:
    if auth.actor_type != ActorType.INTERNAL:
        raise AuthorizationError(
            f"operation {operation!r} requires Internal actor; got {auth.actor_type}",
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
    elif operation in INTERNAL_ONLY_OPS:
        require_internal(auth, operation)


def guard_human_decision(auth: AuthContext, kind: DecisionKind | str) -> None:
    """Formal Decision kinds always require Human, regardless of command alias."""
    k = DecisionKind(kind) if not isinstance(kind, DecisionKind) else kind
    if k in HUMAN_DECISION_KINDS:
        require_human(auth, f"decision:{k}")


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


def guard_side_effect_send(
    *,
    task_state: TaskState,
    cancel_intent: bool,
    pause_intent: bool,
    has_blocking_gate: bool,
    has_active_contract: bool,
    effect_contract_version: int | None,
    active_contract_version: int | None,
    approval_expired: bool,
    effect_state: SideEffectState,
    allowed_states: frozenset[SideEffectState],
) -> None:
    """Shared eligibility for registering and transmitting external side effects (§7.3, §14.2)."""
    if cancel_intent or task_state == TaskState.CANCELLING:
        raise PreconditionError("cancel intent active", code="dispatch_frozen_cancel")
    if pause_intent or task_state in {TaskState.PAUSING, TaskState.PAUSED}:
        raise PreconditionError(
            "pause active; external dispatch frozen",
            code="dispatch_frozen_pause",
        )
    if is_terminal_task(task_state):
        raise PreconditionError(
            f"task state {task_state} forbids external dispatch",
            code="dispatch_blocked_state",
        )
    if has_blocking_gate:
        raise PreconditionError(
            "blocking gate open; external dispatch frozen",
            code="dispatch_frozen_gate",
        )
    if not has_active_contract or active_contract_version is None:
        raise PreconditionError(
            "ACTIVE Contract required before external dispatch",
            code="no_active_contract",
        )
    if (
        effect_contract_version is not None
        and effect_contract_version != active_contract_version
    ):
        raise PreconditionError(
            "side effect authorized under superseded contract",
            code="contract_replaced",
        )
    if approval_expired:
        raise PreconditionError("approval expired", code="approval_expired")
    if effect_state not in allowed_states:
        raise PreconditionError(
            f"effect not eligible for send: {effect_state}",
            code="not_authorized",
        )


def guard_final_complete(*, task_state: TaskState, has_acceptance: bool) -> None:
    if is_terminal_task(task_state) and task_state != TaskState.COMPLETED:
        raise PreconditionError("task already terminal", code="already_terminal")
    if not has_acceptance:
        raise PreconditionError(
            "Human Final Acceptance required before COMPLETED",
            code="acceptance_required",
        )


def guard_decision_kind_human(kind: DecisionKind) -> bool:
    return kind in HUMAN_DECISION_KINDS
