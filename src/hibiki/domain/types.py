from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hibiki.domain.enums import ActorType


@dataclass(frozen=True, slots=True)
class AuthContext:
    """Server-filled identity. Client-supplied identity fields are ignored."""

    principal_id: str
    actor_id: str
    actor_type: ActorType
    auth_context_id: str
    scopes: frozenset[str] = frozenset()
    delegation_id: str | None = None
    # Runtime binding for Internal run writes (server-issued credentials).
    bound_task_id: str | None = None
    bound_run_id: str | None = None
    bound_fencing_epoch: int | None = None
    bound_grant_epoch: int | None = None

    def is_human(self) -> bool:
        return self.actor_type == ActorType.HUMAN

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes or self.is_human()


@dataclass(frozen=True, slots=True)
class CommandResult:
    ok: bool
    data: dict[str, Any]
    error_code: str | None = None
    error_message: str | None = None
    replayed: bool = False

    @classmethod
    def success(cls, data: dict[str, Any] | None = None, *, replayed: bool = False) -> CommandResult:
        return cls(ok=True, data=data or {}, replayed=replayed)

    @classmethod
    def failure(cls, code: str, message: str, data: dict[str, Any] | None = None) -> CommandResult:
        return cls(ok=False, data=data or {}, error_code=code, error_message=message)
