"""Tests for the Tool Broker (M1 Task C).

A real Core is bootstrapped with ``tests.helpers`` (``make_core`` / ``approve_flow``),
a Run is dispatched and its run-bound Internal credential is used for every request.
``run_inputs.workspace_path`` is derived by the Core as
``<workspace_root>/<workspace_id>`` and the broker does not create it, so each
harness creates that directory explicitly.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from hibiki.domain.errors import AuthorizationError, ConflictError
from hibiki.persistence.models import AgentRunRow, ToolInvocationRow
from hibiki.tools.broker import ToolBroker, ToolRequest
from tests.helpers import approve_flow, human_auth, internal_auth, make_core, run_auth


def _approve_with_tools(svc: Any, auth: Any, tools: list[str], *, title: str = "t1") -> tuple[str, str]:
    """``approve_flow`` with an explicit Contract permission ceiling."""
    created = svc.execute("create_task", auth, {"title": title})
    assert created.ok, created
    task_id = created.data["task_id"]
    submitted = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "objective": title,
            "permission_ceiling": {"tools": list(tools)},
        },
    )
    assert submitted.ok, submitted
    approved = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": submitted.data["decision_id"],
            "expected_target_hash": submitted.data["content_hash"],
            "expected_target_version": submitted.data["contract_version"],
        },
    )
    assert approved.ok, approved
    activated = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert activated.ok, activated
    return task_id, activated.data["nodes"][0]


def _epochs(svc: Any, run_id: str) -> tuple[int, int]:
    def _read(session: Any) -> tuple[int, int]:
        run = session.get(AgentRunRow, run_id)
        assert run is not None
        return int(run.grant_epoch), int(run.fencing_epoch)

    return svc.executor.run(_read)


def _start(base: Path, *, tools: list[str] | None) -> dict[str, Any]:
    """Boot a core, approve one task and dispatch a live RUNNING run."""
    svc, ctx = make_core(base)
    human = human_auth()
    if tools is None:
        task_id, work_unit_id = approve_flow(svc, human)
    else:
        task_id, work_unit_id = _approve_with_tools(svc, human, tools)
    dispatched = svc.execute("dispatch_ready_runs", human, {"task_id": task_id})
    assert dispatched.ok and dispatched.data["created_runs"], dispatched
    run_id = dispatched.data["created_runs"][0]
    # The spec derives the workspace as <workspace_root>/<workspace_id>; creating it
    # is the caller's job (the broker never invents a workspace).
    workspace = Path(ctx["data_dir"]) / "workspaces" / f"ws_{work_unit_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    broker = ToolBroker(
        svc.executor,
        ctx["clock"],
        workspace_root=str(Path(ctx["data_dir"]) / "workspaces"),
    )
    auth = run_auth(svc, run_id)
    grant, fencing = _epochs(svc, run_id)
    return {
        "svc": svc,
        "ctx": ctx,
        "human": human,
        "task_id": task_id,
        "work_unit_id": work_unit_id,
        "run_id": run_id,
        "workspace": workspace,
        "broker": broker,
        "auth": auth,
        "grant": grant,
        "fencing": fencing,
    }


def _request(
    harness: dict[str, Any],
    *,
    tool: str,
    params: dict[str, Any],
    seq: int,
    grant: int | None = None,
    fencing: int | None = None,
) -> ToolRequest:
    return ToolRequest(
        run_id=harness["run_id"],
        task_id=harness["task_id"],
        work_unit_id=harness["work_unit_id"],
        tool_name=tool,
        parameters=params,
        grant_epoch=harness["grant"] if grant is None else grant,
        fencing_epoch=harness["fencing"] if fencing is None else fencing,
        sequence_no=seq,
    )


def _rows(svc: Any, run_id: str) -> list[dict[str, Any]]:
    def _read(session: Any) -> list[dict[str, Any]]:
        rows = session.scalars(
            select(ToolInvocationRow).where(ToolInvocationRow.run_id == run_id)
        ).all()
        return [
            {
                "sequence_no": row.sequence_no,
                "tool_name": row.tool_name,
                "decision": row.decision,
                "deny_reason": row.deny_reason,
                "outcome": row.outcome,
                "result_json": row.result_json,
                "grant_epoch": row.grant_epoch,
                "fencing_epoch": row.fencing_epoch,
                "work_unit_id": row.work_unit_id,
                "finished_at": row.finished_at,
            }
            for row in rows
        ]

    return sorted(svc.executor.run(_read), key=lambda row: row["sequence_no"])


def _row(svc: Any, run_id: str, seq: int) -> dict[str, Any]:
    matches = [row for row in _rows(svc, run_id) if row["sequence_no"] == seq]
    assert len(matches) == 1, f"expected one invocation for sequence {seq}: {matches}"
    return matches[0]


def test_allowed_write_and_read_round_trip(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write", "fs.list"])
    broker, auth = harness["broker"], harness["auth"]

    written = broker.execute_fs_write(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "out.txt", "content": "hello"},
            seq=1,
        ),
    )
    assert written == {
        "status": "ok",
        "path": "out.txt",
        "sha256": hashlib.sha256(b"hello").hexdigest(),
        "bytes": 5,
    }
    assert (harness["workspace"] / "out.txt").read_bytes() == b"hello"

    read = broker.execute_fs_read(
        auth, _request(harness, tool="fs.read", params={"path": "out.txt"}, seq=2)
    )
    assert read["status"] == "ok"
    assert read["content"] == "hello"
    assert read["sha256"] == written["sha256"]
    assert read["bytes"] == 5
    assert read["truncated"] is False

    (harness["workspace"] / "sub").mkdir()
    (harness["workspace"] / "sub" / "nested.txt").write_text("nested")
    listed = broker.execute_fs_list(
        auth, _request(harness, tool="fs.list", params={"path": "sub"}, seq=3)
    )
    assert listed == {"status": "ok", "path": "sub", "entries": ["nested.txt"]}

    rows = _rows(harness["svc"], harness["run_id"])
    assert [row["decision"] for row in rows] == ["ALLOW", "ALLOW", "ALLOW"]
    assert [row["outcome"] for row in rows] == ["ok", "ok", "ok"]
    assert all(row["finished_at"] is not None for row in rows)


def test_fs_read_honours_the_size_cap(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    broker, auth = harness["broker"], harness["auth"]
    broker.execute_fs_write(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "big.txt", "content": "x" * 100},
            seq=1,
        ),
    )

    read = broker.execute_fs_read(
        auth,
        _request(harness, tool="fs.read", params={"path": "big.txt", "max_bytes": 10}, seq=2),
    )

    assert read["status"] == "ok"
    assert read["bytes"] == 10
    assert read["content"] == "x" * 10
    assert read["truncated"] is True


def test_tool_outside_the_ceiling_is_denied_and_recorded(tmp_path: Path) -> None:
    # approve_flow's default ceiling grants only fs.read.
    harness = _start(tmp_path, tools=None)
    broker, auth = harness["broker"], harness["auth"]

    denied = broker.execute_fs_write(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "out.txt", "content": "nope"},
            seq=1,
        ),
    )
    assert denied == {"status": "denied", "reason": "tool_not_granted"}
    assert not (harness["workspace"] / "out.txt").exists()

    unknown = broker.authorize(
        auth,
        _request(harness, tool="fs.exec", params={"path": "out.txt"}, seq=2),
    )
    assert unknown.allowed is False and unknown.reason == "tool_not_granted"

    rows = _rows(harness["svc"], harness["run_id"])
    assert [row["decision"] for row in rows] == ["DENY", "DENY"]
    assert {row["deny_reason"] for row in rows} == {"tool_not_granted"}
    assert all(row["outcome"] == "denied" for row in rows)


def test_foreign_and_unbound_credentials_are_denied_and_recorded(tmp_path: Path) -> None:
    harness = _start(tmp_path / "a", tools=["fs.write"])
    other = _start(tmp_path / "b", tools=["fs.write"])
    broker = harness["broker"]

    foreign = broker.authorize(
        other["auth"],
        _request(
            harness, tool="fs.write", params={"path": "a.txt", "content": "x"}, seq=1
        ),
    )
    assert foreign.allowed is False and foreign.reason == "authorization_denied"

    unbound = broker.authorize(
        internal_auth(),
        _request(
            harness, tool="fs.write", params={"path": "b.txt", "content": "x"}, seq=2
        ),
    )
    assert unbound.allowed is False and unbound.reason == "authorization_denied"

    rows = _rows(harness["svc"], harness["run_id"])
    assert [row["decision"] for row in rows] == ["DENY", "DENY"]
    assert {row["deny_reason"] for row in rows} == {"authorization_denied"}
    assert not (harness["workspace"] / "a.txt").exists()


def test_run_not_running_is_denied_and_recorded(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    human = harness["human"]
    assert harness["svc"].execute("pause_task", human, {"task_id": harness["task_id"]}).ok
    quiescent = harness["svc"].execute(
        "runtime_quiescent", human, {"task_id": harness["task_id"]}
    )
    assert quiescent.ok, quiescent

    decision = harness["broker"].authorize(
        harness["auth"],
        _request(
            harness, tool="fs.write", params={"path": "x.txt", "content": "x"}, seq=1
        ),
    )
    assert decision.allowed is False and decision.reason == "run_not_running"

    row = _row(harness["svc"], harness["run_id"], 1)
    assert row["decision"] == "DENY"
    assert row["deny_reason"] == "run_not_running"


def test_frozen_task_is_denied_and_recorded(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    human = harness["human"]
    cancelled = harness["svc"].execute(
        "cancel_task", human, {"task_id": harness["task_id"]}
    )
    assert cancelled.ok, cancelled

    decision = harness["broker"].authorize(
        harness["auth"],
        _request(
            harness, tool="fs.write", params={"path": "x.txt", "content": "x"}, seq=1
        ),
    )
    assert decision.allowed is False and decision.reason == "task_frozen"

    row = _row(harness["svc"], harness["run_id"], 1)
    assert row["decision"] == "DENY"
    assert row["deny_reason"] == "task_frozen"


def test_blocking_gate_freezes_tools(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    gate = harness["svc"].execute(
        "open_blocking_gate", harness["human"], {"task_id": harness["task_id"], "reason": "hold"}
    )
    assert gate.ok, gate

    decision = harness["broker"].authorize(
        harness["auth"],
        _request(
            harness, tool="fs.write", params={"path": "x.txt", "content": "x"}, seq=1
        ),
    )
    assert decision.allowed is False and decision.reason == "task_frozen"
    assert _row(harness["svc"], harness["run_id"], 1)["decision"] == "DENY"


def test_epoch_mismatch_is_denied_and_recorded(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    broker, auth = harness["broker"], harness["auth"]

    fencing = broker.authorize(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "x.txt", "content": "x"},
            seq=1,
            fencing=harness["fencing"] + 1,
        ),
    )
    assert fencing.allowed is False and fencing.reason == "fencing_conflict"

    grant = broker.authorize(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "x.txt", "content": "x"},
            seq=2,
            grant=harness["grant"] + 1,
        ),
    )
    assert grant.allowed is False and grant.reason == "grant_conflict"

    rows = _rows(harness["svc"], harness["run_id"])
    assert [(row["decision"], row["deny_reason"]) for row in rows] == [
        ("DENY", "fencing_conflict"),
        ("DENY", "grant_conflict"),
    ]
    # The offending epochs are preserved for audit.
    assert rows[0]["fencing_epoch"] == harness["fencing"] + 1
    assert rows[1]["grant_epoch"] == harness["grant"] + 1


def test_path_escape_is_refused_and_recorded_as_denied_outcome(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    broker, auth = harness["broker"], harness["auth"]
    outside = harness["workspace"].parent

    traversal = broker.execute_fs_write(
        auth,
        _request(
            harness,
            tool="fs.write",
            params={"path": "../escaped.txt", "content": "nope"},
            seq=1,
        ),
    )
    assert traversal == {"status": "denied", "reason": "path_escape"}
    assert not (outside / "escaped.txt").exists()

    absolute = broker.execute_fs_read(
        auth, _request(harness, tool="fs.read", params={"path": "/etc/passwd"}, seq=2)
    )
    assert absolute == {"status": "denied", "reason": "path_escape"}

    secret = tmp_path / "outside"
    secret.mkdir()
    (secret / "secret.txt").write_text("secret")
    (harness["workspace"] / "link.txt").symlink_to(secret / "secret.txt")
    (harness["workspace"] / "linkdir").symlink_to(secret, target_is_directory=True)

    final_symlink = broker.execute_fs_read(
        auth, _request(harness, tool="fs.read", params={"path": "link.txt"}, seq=3)
    )
    assert final_symlink == {"status": "denied", "reason": "path_escape"}

    dir_symlink = broker.execute_fs_read(
        auth,
        _request(harness, tool="fs.read", params={"path": "linkdir/secret.txt"}, seq=4),
    )
    assert dir_symlink == {"status": "denied", "reason": "path_escape"}

    rows = _rows(harness["svc"], harness["run_id"])
    assert [row["decision"] for row in rows] == ["ALLOW"] * 4
    assert [row["outcome"] for row in rows] == ["denied"] * 4
    assert all(json.loads(row["result_json"])["reason"] == "path_escape" for row in rows)


def test_every_denial_is_still_recorded(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=None)  # ceiling grants fs.read only
    broker, auth = harness["broker"], harness["auth"]
    requests = [
        _request(harness, tool="fs.write", params={"path": "a", "content": "x"}, seq=1),
        _request(harness, tool="shell.run", params={"argv": ["true"]}, seq=2),
        _request(harness, tool="artifact.publish", params={}, seq=3),
    ]
    decisions = [broker.authorize(auth, request) for request in requests]
    assert all(decision.allowed is False for decision in decisions)

    rows = _rows(harness["svc"], harness["run_id"])
    assert len(rows) == len(requests)
    assert [row["decision"] for row in rows] == ["DENY", "DENY", "DENY"]
    assert all(row["deny_reason"] == "tool_not_granted" for row in rows)
    assert [row["sequence_no"] for row in rows] == [1, 2, 3]


def test_record_outcome_never_overwrites_with_a_different_outcome(tmp_path: Path) -> None:
    harness = _start(tmp_path, tools=["fs.write"])
    broker, auth = harness["broker"], harness["auth"]
    decision = broker.authorize(
        auth,
        _request(
            harness, tool="fs.write", params={"path": "x.txt", "content": "x"}, seq=1
        ),
    )
    assert decision.allowed is True and decision.invocation_id

    broker.record_outcome(auth, decision.invocation_id, outcome="ok", result={"a": 1})
    with pytest.raises(ConflictError):
        broker.record_outcome(auth, decision.invocation_id, outcome="error")

    # A foreign credential cannot finish another run's invocation.
    with pytest.raises(AuthorizationError):
        broker.record_outcome(
            internal_auth(), decision.invocation_id, outcome="ok", result={"a": 1}
        )

    row = _row(harness["svc"], harness["run_id"], 1)
    assert row["outcome"] == "ok"
    assert json.loads(row["result_json"]) == {"a": 1}
