from __future__ import annotations

from enum import StrEnum


class ActorType(StrEnum):
    HUMAN = "HUMAN"
    USER_AGENT = "USER_AGENT"
    INTERNAL = "INTERNAL"


class TaskState(StrEnum):
    NEW = "NEW"
    WAITING_HUMAN = "WAITING_HUMAN"
    PLANNING = "PLANNING"
    EXECUTING = "EXECUTING"
    VERIFYING = "VERIFYING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class ContractStatus(StrEnum):
    DRAFT = "DRAFT"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    REJECTED = "REJECTED"


class DecisionKind(StrEnum):
    CONTRACT_APPROVAL = "CONTRACT_APPROVAL"
    CONTRACT_DELTA = "CONTRACT_DELTA"
    SIDE_EFFECT_APPROVAL = "SIDE_EFFECT_APPROVAL"
    FINAL_ACCEPTANCE = "FINAL_ACCEPTANCE"
    CLARIFICATION = "CLARIFICATION"
    RESOURCE_LIMIT = "RESOURCE_LIMIT"


class DecisionStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"


class GateLifecycle(StrEnum):
    OPEN = "OPEN"
    APPROVED_PENDING_APPLY = "APPROVED_PENDING_APPLY"
    RESOLVED = "RESOLVED"


class PlanStatus(StrEnum):
    PROPOSED = "PROPOSED"
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    REJECTED = "REJECTED"


class WorkUnitStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    BLOCKED = "BLOCKED"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AgentRunStatus(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    LOST = "LOST"


class AssignmentKind(StrEnum):
    PLAN = "PLAN"
    EXECUTE = "EXECUTE"


class SideEffectState(StrEnum):
    PROPOSED = "PROPOSED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    AUTHORIZED = "AUTHORIZED"
    DISPATCHING = "DISPATCHING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_CONFIRMED = "FAILED_CONFIRMED"
    UNKNOWN = "UNKNOWN"
    CANCELLED = "CANCELLED"


class WorkspaceState(StrEnum):
    READY = "READY"
    LOCKED = "LOCKED"
    QUARANTINED = "QUARANTINED"
    ARCHIVED = "ARCHIVED"


class Verdict(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"


class DependencyPredicate(StrEnum):
    DONE = "DONE"
    VERDICT_PASS = "VERDICT_PASS"


class OutboxStatus(StrEnum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    ACKED = "ACKED"
    DEAD = "DEAD"


class WaitingReason(StrEnum):
    CLARIFICATION = "clarification"
    CONTRACT_APPROVAL = "contract_approval"
    CONTRACT_DELTA = "contract_delta"
    SIDE_EFFECT_APPROVAL = "side_effect_approval"
    FINAL_ACCEPTANCE = "final_acceptance"
    RESOURCE_LIMIT = "resource_limit"
    EXECUTION_BLOCKED = "execution_blocked"
    EXECUTION_UNCERTAIN = "execution_uncertain"
