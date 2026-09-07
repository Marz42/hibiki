"""Tool broker boundary (M0: interfaces + fake external only)."""

from __future__ import annotations

from typing import Any

from hibiki.domain.errors import AuthorizationError


class ToolBroker:
    def __init__(self, *, allowed_tools: set[str] | None = None) -> None:
        self.allowed_tools = allowed_tools or set()

    def authorize(self, run_id: str, tool_name: str, params: dict[str, Any]) -> None:
        if tool_name not in self.allowed_tools:
            raise AuthorizationError(
                f"tool {tool_name!r} not granted to run {run_id}",
                code="tool_denied",
            )
