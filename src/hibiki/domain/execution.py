"""Authoritative execution contract handed to a real worker (M1, SPEC §9.2 / §13 / §15).

The Core builds one ``RunExecutionSpec`` per Run at dispatch time, persists it as a
``RunInputRow`` and references its hash from the Outbox ``agent.start`` payload. A worker
must read the spec through its run-bound credential and may never widen it: the granted
tools, permission ceiling and workspace path are exactly what the Core admitted.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from hibiki.domain.hashing import canonical_json, content_hash

#: Tools a real M1 worker may be granted. The list is closed: anything else is denied
#: by the Tool Broker even if the Contract ceiling asks for it.
M1_TOOL_CATALOG: tuple[str, ...] = (
    "fs.read",
    "fs.write",
    "fs.list",
    "shell.run",
    "artifact.publish",
)

#: Tools that always require an explicit Contract ceiling entry.
M1_MUTATING_TOOLS: frozenset[str] = frozenset({"fs.write", "shell.run", "artifact.publish"})


@dataclass(frozen=True)
class RunExecutionSpec:
    """What a worker is allowed to do, frozen at dispatch time."""

    run_id: str
    task_id: str
    work_unit_id: str | None
    assignment_kind: str
    profile_id: str
    profile_version: int
    contract_version: int
    plan_version: int | None
    context_manifest_id: str | None
    context_manifest_hash: str | None
    workspace_id: str | None
    workspace_path: str | None
    workspace_isolation: str
    granted_tools: tuple[str, ...]
    permission_ceiling: dict[str, Any]
    granted_permissions: dict[str, Any]
    principal_id: str
    agent_instance_id: str
    grant_epoch: int
    fencing_epoch: int
    revoke_epoch: int
    model_call_limit: int
    max_turns: int
    wall_timeout_seconds: int
    objective: str = ""
    work_type: str = ""
    goal_label: str = ""
    context_policy: str = "FRESH"
    input_refs: tuple[str, ...] = ()
    expected_outputs: tuple[str, ...] = ()
    acceptance_criteria: tuple[dict[str, Any], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return canonical_json(asdict(self))

    @property
    def spec_hash(self) -> str:
        return content_hash(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_ceiling_tools(permission_ceiling: Any) -> tuple[tuple[str, ...], list[str]]:
    """Return (granted tools, unknown tools) from a Contract permission ceiling.

    Accepts ``{"tools": [...]}`` or a bare list. Only names in ``M1_TOOL_CATALOG`` are
    granted; unknown names are reported so the caller can audit the refusal instead of
    silently widening or silently dropping.
    """
    if permission_ceiling is None:
        requested: list[Any] = []
    elif isinstance(permission_ceiling, dict):
        requested = list(permission_ceiling.get("tools") or [])
    elif isinstance(permission_ceiling, (list, tuple, set, frozenset)):
        requested = list(permission_ceiling)
    elif isinstance(permission_ceiling, str):
        requested = [permission_ceiling]
    else:
        requested = []

    granted: list[str] = []
    unknown: list[str] = []
    for name in requested:
        tool = str(name)
        if tool in M1_TOOL_CATALOG:
            if tool not in granted:
                granted.append(tool)
        else:
            unknown.append(tool)
    # Reads are always available inside the workspace; they never need a ceiling entry.
    if "fs.read" not in granted:
        granted.insert(0, "fs.read")
    return tuple(granted), unknown
