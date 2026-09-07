from __future__ import annotations

from typing import Any

from hibiki.domain.ports import ExternalAdapter


class FakeExternalAdapter(ExternalAdapter):
    """Controllable external side-effect adapter for M0."""

    def __init__(self, *, supports_query: bool = True) -> None:
        self.supports_query = supports_query
        self.dispatched: dict[str, dict[str, Any]] = {}
        self.effect_counts: dict[str, int] = {}
        self.next_outcome: str = "succeeded"  # succeeded|failed|unknown
        self.force_unknown_once: bool = False

    def dispatch(self, effect: dict[str, Any]) -> dict[str, Any]:
        effect_id = effect["effect_id"]
        key = effect["external_idempotency_key"]
        outcome = self.next_outcome
        if self.force_unknown_once:
            outcome = "unknown"
            self.force_unknown_once = False

        if outcome == "succeeded":
            # Count unique business effects by idempotency key
            if key not in self.effect_counts:
                self.effect_counts[key] = 0
            # Only increment on first successful apply for this key when query-capable
            if key not in self.dispatched or self.dispatched[key].get("status") != "succeeded":
                self.effect_counts[key] += 1
            receipt = {
                "status": "succeeded",
                "effect_id": effect_id,
                "external_idempotency_key": key,
                "provider_operation_id": f"op_{key}",
            }
            self.dispatched[key] = receipt
            return receipt
        if outcome == "failed":
            receipt = {
                "status": "failed",
                "effect_id": effect_id,
                "external_idempotency_key": key,
                "error": "fake_failed",
            }
            return receipt
        return {
            "status": "unknown",
            "effect_id": effect_id,
            "external_idempotency_key": key,
        }

    def query(self, effect_id: str, external_idempotency_key: str) -> dict[str, Any] | None:
        if not self.supports_query:
            return None
        return self.dispatched.get(external_idempotency_key)
