from __future__ import annotations


class DomainError(Exception):
    """Base domain error."""

    code: str = "domain_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.message = message


class InvalidTransitionError(DomainError):
    code = "invalid_transition"


class AuthorizationError(DomainError):
    code = "authorization_denied"


class PreconditionError(DomainError):
    code = "precondition_failed"


class ConflictError(DomainError):
    code = "conflict"


class IdempotencyConflictError(DomainError):
    code = "idempotency_conflict"


class NotFoundError(DomainError):
    code = "not_found"
