from __future__ import annotations

from dataclasses import dataclass

from hibiki.domain.enums import DependencyPredicate
from hibiki.domain.errors import PreconditionError


@dataclass(frozen=True, slots=True)
class PlanNode:
    work_unit_id: str
    spec_version: int
    work_type: str = "EXECUTE"


@dataclass(frozen=True, slots=True)
class PlanEdge:
    from_work_unit_id: str
    to_work_unit_id: str
    predicate: DependencyPredicate = DependencyPredicate.DONE
    artifact_hash: str | None = None


def validate_dag(
    *,
    task_id: str,
    nodes: list[PlanNode],
    edges: list[PlanEdge],
    permission_ok: bool = True,
) -> None:
    if not nodes:
        raise PreconditionError("plan must contain at least one node", code="plan_empty")
    ids = [n.work_unit_id for n in nodes]
    if len(ids) != len(set(ids)):
        raise PreconditionError("duplicate plan node", code="plan_duplicate_node")
    id_set = set(ids)
    for e in edges:
        if e.from_work_unit_id not in id_set or e.to_work_unit_id not in id_set:
            raise PreconditionError("dangling plan edge", code="plan_dangling_edge")
        if e.predicate == DependencyPredicate.VERDICT_PASS and not e.artifact_hash:
            raise PreconditionError(
                "VERDICT_PASS requires artifact_hash",
                code="plan_missing_artifact_hash",
            )
    # cycle detection
    adj: dict[str, list[str]] = {i: [] for i in ids}
    for e in edges:
        adj[e.from_work_unit_id].append(e.to_work_unit_id)
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(u: str) -> None:
        if u in visiting:
            raise PreconditionError("plan contains cycle", code="plan_cycle")
        if u in visited:
            return
        visiting.add(u)
        for v in adj[u]:
            dfs(v)
        visiting.remove(u)
        visited.add(u)

    for n in ids:
        dfs(n)
    if not permission_ok:
        raise PreconditionError("plan exceeds permission ceiling", code="plan_permission")
    _ = task_id  # same-task enforced by caller binding
