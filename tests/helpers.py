from __future__ import annotations

from pathlib import Path

from hibiki.application.bootstrap import bootstrap_core
from hibiki.domain.enums import ActorType
from hibiki.domain.types import AuthContext


def human_auth(principal: str = "human_1") -> AuthContext:
    return AuthContext(
        principal_id=principal,
        actor_id=principal,
        actor_type=ActorType.HUMAN,
        auth_context_id="test_human",
    )


def user_agent_auth(principal: str = "human_1") -> AuthContext:
    return AuthContext(
        principal_id=principal,
        actor_id="ua_1",
        actor_type=ActorType.USER_AGENT,
        auth_context_id="test_ua",
        scopes=frozenset({"task:read", "task:create"}),
    )


def internal_auth(principal: str = "human_1") -> AuthContext:
    return AuthContext(
        principal_id=principal,
        actor_id="agent_internal",
        actor_type=ActorType.INTERNAL,
        auth_context_id="test_internal",
    )


def make_core(tmp_path: Path, **kwargs):
    return bootstrap_core(tmp_path / "data", acquire_lock=True, **kwargs)


def approve_flow(svc, auth, *, title: str = "t1"):
    r = svc.execute("create_task", auth, {"title": title})
    assert r.ok, r
    task_id = r.data["task_id"]
    r = svc.execute("submit_contract", auth, {"task_id": task_id, "objective": title})
    assert r.ok, r
    decision_id = r.data["decision_id"]
    content_hash = r.data["content_hash"]
    version = r.data["contract_version"]
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": decision_id,
            "expected_target_hash": content_hash,
            "expected_target_version": version,
        },
    )
    assert r.ok, r
    r = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert r.ok, r
    return task_id, r.data.get("nodes", [None])[0]
