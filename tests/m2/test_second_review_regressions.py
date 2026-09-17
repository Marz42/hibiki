"""Regression tests for the second M2 adversarial review (post-8932533).

Each test pins one of the blocking P1s that the earlier suite missed because its
scaffolding bypassed the real code path:

* ``_parse_explicit_result`` must keep nested ``acceptance_evidence`` and must never
  mint a verdict out of a truncated JSON fragment;
* the PLAN manifest must use the key names the reader materializes, and the Fake
  Planner must actually consume the Task message log;
* ``advance_planner_checkpoint`` must enforce principal + active PLAN Run binding,
  not just the planner generation;
* a normal, confirmed sandbox exit must hand the Workspace back.
"""

from __future__ import annotations

from pathlib import Path

from hibiki.domain.enums import ActorType, WorkspaceState
from hibiki.domain.types import AuthContext
from hibiki.persistence.models import PlannerSessionRow, WorkspaceRow
from hibiki.runtime.api_agent import _parse_explicit_result
from hibiki.runtime.fake_planner import FakePlannerAdapter
from tests.helpers import human_auth, make_core, run_auth, submit_result_and_exit

# --------------------------------------------------------------------------- #
# P1 #3 — structured result parsing
# --------------------------------------------------------------------------- #


def test_parse_keeps_nested_acceptance_evidence() -> None:
    """A full object with a nested evidence array must survive intact."""
    reply = (
        "All checks done.\n"
        "```json\n"
        "{\n"
        '  "outcome": "COMPLETED",\n'
        '  "verdict": "PASS",\n'
        '  "acceptance_evidence": [\n'
        '    {"criterion_id": "c1", "artifact_hash": "abc", "verdict": "PASS"}\n'
        "  ]\n"
        "}\n"
        "```"
    )
    parsed = _parse_explicit_result(reply)
    assert parsed["outcome"] == "COMPLETED"
    assert parsed["verdict"] == "PASS"
    assert parsed["acceptance_evidence"] == [
        {"criterion_id": "c1", "artifact_hash": "abc", "verdict": "PASS"}
    ]


def test_parse_keeps_evidence_when_verdict_follows_evidence() -> None:
    reply = (
        '{"acceptance_evidence":[{"criterion_id":"c1"}],'
        '"outcome":"COMPLETED","verdict":"PASS"}'
    )
    parsed = _parse_explicit_result(reply)
    assert parsed["verdict"] == "PASS"
    assert parsed["acceptance_evidence"] == [{"criterion_id": "c1"}]


def test_parse_rejects_truncated_json_instead_of_guessing_pass() -> None:
    """A truncated object must not yield a PASS scraped from its own fragments."""
    assert _parse_explicit_result('{"verdict":"PASS","acceptance_evidence":[{') == {}


def test_parse_ignores_json_shaped_prose() -> None:
    assert _parse_explicit_result('I set "verdict": "pass" for this.') == {}


def test_parse_still_honors_explicit_markers() -> None:
    assert _parse_explicit_result("done [[HIBIKI:PASS]]")["verdict"] == "PASS"
    assert _parse_explicit_result("done [[HIBIKI:FAIL]]")["verdict"] == "FAIL"


def test_parse_handles_brace_inside_string() -> None:
    parsed = _parse_explicit_result(
        '{"outcome":"COMPLETED","verdict":"PASS","note":"closing } brace"}'
    )
    assert parsed["verdict"] == "PASS"


def test_negated_blocked_marker_is_not_a_block() -> None:
    """Found live: a model that delivered everything ended with a negated marker.

    ``deepseek-flash`` wrote "[[HIBIKI:BLOCKED]] is not applicable — the objective was
    fully satisfied" after publishing its artifact. Substring matching read that as a
    real block and reported a complete delivery as BLOCKED/FAIL.
    """
    from hibiki.runtime.api_agent import _asserted_blocked, _marker_verdict

    reply = (
        "COMPLETED - worker B scope only.\n"
        "- Wrote `edits/b.md` (65 bytes).\n"
        "- Published the artifact `edits/b.md`.\n\n"
        "[[HIBIKI:BLOCKED]] is not applicable - the objective was fully satisfied."
    )
    assert _asserted_blocked(reply) is False
    assert _marker_verdict(reply) is None


def test_asserted_blocked_marker_still_blocks() -> None:
    from hibiki.runtime.api_agent import _asserted_blocked

    assert _asserted_blocked("Could not reach the path.\n\n[[HIBIKI:BLOCKED]]") is True
    assert (
        _asserted_blocked(
            "Gave up.\n[[HIBIKI:BLOCKED]] blocking reason: no writable path."
        )
        is True
    )
    assert (
        _asserted_blocked("Status: [[HIBIKI:BLOCKED]] because no path exists.") is True
    )


def test_negated_markers_do_not_mint_a_verdict() -> None:
    assert _parse_explicit_result("[[HIBIKI:PASS]] is not applicable here.") == {}


# --------------------------------------------------------------------------- #
# Shared fixture: a complex (non-simple) Task with a real Contract
# --------------------------------------------------------------------------- #


def _complex_task(planner: FakePlannerAdapter, tmp_path: Path):
    svc, ctx = make_core(tmp_path, agent=planner)
    auth = human_auth()
    r = svc.execute("create_task", auth, {"title": "plan-recovery"})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "contract": {
                "objective": "materialize me",
                "simple": False,
                "deliverables": [
                    {"deliverable_id": "d1", "description": "x", "expected_kind": "text"}
                ],
                "acceptance_criteria": [
                    {
                        "criterion_id": "c1",
                        "statement": "evidence required",
                        "evidence_kind": "artifact",
                        "required": True,
                    }
                ],
                "permission_ceiling": {"tools": ["fs.read", "fs.write", "artifact.publish"]},
                "resource_limits": {
                    "wall_timeout_seconds": 60,
                    "max_model_calls": 10,
                    "max_turns": 5,
                },
            },
        },
    )
    assert r.ok, r
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": r.data["decision_id"],
            "expected_target_hash": r.data["content_hash"],
            "expected_target_version": r.data["contract_version"],
        },
    )
    assert r.ok, r
    return svc, ctx, auth, task_id


# --------------------------------------------------------------------------- #
# P1 #4 — PLAN manifest materialization and Planner message consumption
# --------------------------------------------------------------------------- #


def test_plan_manifest_materializes_contract(tmp_path: Path) -> None:
    """The Contract must reach the Planner as real text, not just a DB row."""
    planner = FakePlannerAdapter()
    svc, ctx, auth, task_id = _complex_task(planner, tmp_path)
    planner.bind_core(svc)
    r = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    assert r.ok, r
    plan_run = r.data["run_id"]
    svc.drain_outbox()

    ctx_data = svc.get_run_context(run_auth(svc, plan_run), plan_run)
    manifest = ctx_data["manifest"]
    # The reader materializes ``mandatory_refs``; the writer used to emit ``mandatory``.
    assert "mandatory_refs" in manifest
    assert "mandatory" not in manifest
    materialized = ctx_data["materialized"]
    assert materialized, "Contract was not materialized into the PLAN Run input"
    contract = next(item for item in materialized if item["kind"] == "contract")
    assert contract["text"], "Materialized Contract had no text"
    assert "materialize me" in contract["text"]
    assert manifest["mandatory_bytes"] > 0
    ctx["lock"].release()


def test_fake_planner_consumes_task_messages_on_recovery(tmp_path: Path) -> None:
    """A resumed PLAN Run must drain the message log into its checkpoint cursor."""
    planner = FakePlannerAdapter()
    svc, ctx, auth, task_id = _complex_task(planner, tmp_path)
    planner.bind_core(svc)

    r = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    assert r.ok, r
    svc.drain_outbox()
    first_run = r.data["run_id"]
    # Finish the first PLAN Run so the session can be resumed by a second one.
    submit_result_and_exit(
        svc, auth, first_run, result={"outcome": "COMPLETED", "verdict": "PASS"}
    )

    # A worker run posts collaboration traffic; the resumed Planner must see it.
    r = svc.execute("activate_minimal_plan", auth, {"task_id": task_id})
    assert r.ok, r
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert d.ok and d.data["created_runs"], d
    worker_run = d.data["created_runs"][0]
    svc.drain_outbox()
    submit_result_and_exit(svc, auth, worker_run)

    messages = svc.list_task_messages(task_id)
    assert messages, "expected worker.result messages in the Task log"
    max_seq = max(int(m["sequence_no"]) for m in messages)

    r2 = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    assert r2.ok, r2
    resumed_run = r2.data["run_id"]
    svc.drain_outbox()

    assert resumed_run in planner.plan_started
    assert planner.consumed_sequences.get(resumed_run) == max_seq, (
        "Fake Planner did not advance its cursor over the Task message log"
    )
    session_id = r2.data["planner_session_id"]

    def _cursor(session):
        return int(session.get(PlannerSessionRow, session_id).last_consumed_message_seq or 0)

    assert svc.executor.run(_cursor) == max_seq
    ctx["lock"].release()


# --------------------------------------------------------------------------- #
# P1 #1 — checkpoint authorization
# --------------------------------------------------------------------------- #


def test_checkpoint_requires_active_plan_run_and_principal(tmp_path: Path) -> None:
    planner = FakePlannerAdapter()
    svc, ctx, auth, task_id = _complex_task(planner, tmp_path)
    planner.bind_core(svc)

    r1 = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    assert r1.ok, r1
    run1, gen1 = r1.data["run_id"], r1.data["generation"]
    svc.drain_outbox()

    # The legitimately bound Run may advance.
    ok = svc.execute(
        "advance_planner_checkpoint",
        run_auth(svc, run1),
        {
            "task_id": task_id,
            "run_id": run1,
            "generation": gen1,
            "fencing_epoch": 1,
            "last_consumed_message_seq": 0,
            "checkpoint_ref": "ck-valid",
        },
    )
    assert ok.ok, ok

    # Bumping the generation revokes run1 and clears the session's active Run.
    r2 = svc.execute("replace_planner_generation", auth, {"task_id": task_id})
    assert r2.ok, r2
    gen2 = r2.data["generation"]
    svc.drain_outbox()

    # The revoked Run must not write with the new generation.
    stale = svc.execute(
        "advance_planner_checkpoint",
        run_auth(svc, run1),
        {
            "task_id": task_id,
            "run_id": run1,
            "generation": gen2,
            "fencing_epoch": 1,
            "last_consumed_message_seq": 0,
            "checkpoint_ref": "ck-stale",
        },
    )
    assert not stale.ok, "revoked PLAN Run advanced the checkpoint"
    assert stale.error_code in {"stale_generation", "authorization_denied"}

    # A different principal must not write this Task's checkpoint.
    intruder = AuthContext(
        principal_id="human_evil",
        actor_id="human_evil",
        actor_type=ActorType.HUMAN,
        auth_context_id="evil",
    )
    denied = svc.execute(
        "advance_planner_checkpoint",
        intruder,
        {
            "task_id": task_id,
            "generation": gen2,
            "last_consumed_message_seq": 0,
            "checkpoint_ref": "ck-evil",
        },
    )
    assert not denied.ok, "cross-principal checkpoint write was accepted"
    assert denied.error_code == "authorization_denied"
    ctx["lock"].release()


# --------------------------------------------------------------------------- #
# P1 #2 — Workspace quarantine lifecycle
# --------------------------------------------------------------------------- #


class _OkSandbox:
    """Sandbox double reporting a definitive, successful exit."""

    def execute(self, command: dict) -> dict:  # noqa: ARG002
        return {
            "status": "ok",
            "exit_code": 0,
            "stdout": "ok",
            "stderr": "",
            "container_id": "container-ok-1",
        }


def _workspace(svc, workspace_id: str) -> dict:
    def _read(session):
        ws = session.get(WorkspaceRow, workspace_id)
        return {
            "state": ws.state,
            "owner_run_id": ws.owner_run_id,
            "writer_alive": ws.writer_alive,
        }

    return svc.executor.run(_read)


def test_confirmed_shell_exit_releases_workspace(tmp_path: Path) -> None:
    """A successful shell Run must leave its Workspace schedulable again."""
    from hibiki.runtime.openai_client import ModelReply
    from tests.runtime.test_api_agent import (
        ScriptedClient,
        _adapter,
        _dispatch,
        _final,
        _task_with_ceiling,
    )

    svc, ctx = make_core(tmp_path)
    auth = human_auth()
    task_id = _task_with_ceiling(svc, auth, ["shell.run"])
    shell_call = {
        "id": "call_1",
        "type": "function",
        "function": {"name": "shell_run", "arguments": '{"argv": ["true"]}'},
    }
    client = ScriptedClient(
        [
            ModelReply(
                content=None,
                tool_calls=(shell_call,),
                finish_reason="tool_calls",
                usage={},
                raw={},
            ),
            _final("done"),
        ]
    )
    adapter = _adapter(svc, client, sandbox=_OkSandbox())
    run_id = _dispatch(svc, adapter, task_id)
    adapter.wait_for_exit(run_id, 10.0)

    workspace_id = svc.get_run_input(run_auth(svc, run_id), run_id)["workspace_id"]
    ws = _workspace(svc, workspace_id)
    assert ws["state"] == WorkspaceState.READY, (
        "a normal shell execution left the Workspace unschedulable"
    )
    assert ws["owner_run_id"] is None
    assert ws["writer_alive"] is False
    ctx["lock"].release()


def test_pending_claim_does_not_quarantine_but_unconfirmed_exit_does(
    tmp_path: Path,
) -> None:
    """Negative control: only a genuinely unconfirmed exit may quarantine."""
    planner = FakePlannerAdapter()
    svc, ctx, auth, task_id = _complex_task(planner, tmp_path)
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": task_id,
            "nodes": [
                {
                    "work_unit_id": "wu_a",
                    "spec_version": 1,
                    "workspace_id": "ws_shared",
                },
                {
                    "work_unit_id": "wu_b",
                    "spec_version": 1,
                    "workspace_id": "ws_shared",
                },
            ],
            "edges": [],
        },
    )
    assert r.ok, r
    d = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert d.ok and d.data["created_runs"], d
    run_id = d.data["created_runs"][0]
    svc.drain_outbox()
    worker = run_auth(svc, run_id)

    # A pre-execution (pending) claim is normal operation, not uncertainty.
    r = svc.execute(
        "register_sandbox_identity",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "container_id": f"pending:{run_id}:1",
            "exit_confirmed": False,
        },
    )
    assert r.ok, r
    ws = _workspace(svc, "ws_shared")
    assert ws["state"] != WorkspaceState.QUARANTINED
    assert ws["writer_alive"] is True
    # The writer claim alone must still serialize a second writer on that Workspace.
    d2 = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
    assert d2.ok, d2
    assert run_id not in d2.data["created_runs"]

    # A real container that never confirmed its exit must still quarantine.
    r = svc.execute(
        "register_sandbox_identity",
        worker,
        {
            "run_id": run_id,
            "fencing_epoch": worker.bound_fencing_epoch,
            "container_id": "container-stuck-1",
            "exit_confirmed": False,
        },
    )
    assert r.ok, r
    notes = svc.reconcile()["notes"]
    assert any("quarantine_unconfirmed" in note for note in notes), notes
    ws = _workspace(svc, "ws_shared")
    assert ws["state"] == WorkspaceState.QUARANTINED
    assert ws["writer_alive"] is True
    ctx["lock"].release()
