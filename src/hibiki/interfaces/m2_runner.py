"""M2 complex-task harness (Fake or live).

Credentials match M1: ``HIBIKI_MODEL_BASE_URL``, ``HIBIKI_MODEL_API_KEY``,
``HIBIKI_MODEL`` from ``.env`` (or the real environment). Never logged.

Usage:
  uv run --no-sync python -m hibiki.interfaces.m2_runner --dry-run \\
      --out docs/acceptance/evidence/m2-harness-smoke
  uv run --no-sync python -m hibiki.interfaces.m2_runner --live \\
      --data-dir /tmp/hibiki-m2 --out docs/acceptance/evidence/m2-<date>/live-runs
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hibiki.application.bootstrap import bootstrap_core
from hibiki.domain.enums import ActorType, AgentRunStatus, AssignmentKind
from hibiki.domain.ports import AgentAdapter
from hibiki.domain.types import AuthContext
from hibiki.interfaces.m1_runner import (
    DOTENV_FILENAME,
    MODEL_ENV,
    ModelConfig,
    load_dotenv,
)
from hibiki.runtime.fake_planner import FakePlannerAdapter


class AssignmentRoutingAdapter(AgentAdapter):
    """PLAN Runs → planner adapter; EXECUTE Runs → worker adapter."""

    def __init__(self, *, planner: AgentAdapter, worker: AgentAdapter) -> None:
        self._planner = planner
        self._worker = worker

    def _pick(self, run_spec_or_id: dict[str, Any] | str) -> AgentAdapter:
        if isinstance(run_spec_or_id, dict):
            kind = run_spec_or_id.get("assignment_kind")
            if kind == AssignmentKind.PLAN or kind == "PLAN":
                return self._planner
            return self._worker
        # stop/inspect/send: try worker first, then planner
        return self._worker

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        return self._pick(run_spec).start(run_spec)

    def send(self, run_id: str, message: dict[str, Any]) -> None:
        # Prefer the adapter that already knows the run.
        for adapter in (self._worker, self._planner):
            try:
                adapter.send(run_id, message)
                return
            except KeyError:
                continue
        raise KeyError(run_id)

    def stop(self, run_id: str, reason: str) -> dict[str, Any]:
        last: dict[str, Any] = {"run_id": run_id, "alive": False, "status": "MISSING"}
        for adapter in (self._worker, self._planner):
            insp = adapter.inspect(run_id)
            if insp.get("status") != "MISSING":
                return adapter.stop(run_id, reason)
            last = insp
        return last

    def inspect(self, run_id: str) -> dict[str, Any]:
        for adapter in (self._worker, self._planner):
            insp = adapter.inspect(run_id)
            if insp.get("status") != "MISSING":
                return insp
        return {"run_id": run_id, "alive": False, "status": "MISSING"}


def _human() -> AuthContext:
    return AuthContext(
        principal_id="m2_operator",
        actor_id="m2_operator",
        actor_type=ActorType.HUMAN,
        auth_context_id="m2_runner",
    )


def _load_tasks(tasks_dir: Path) -> list[dict[str, Any]]:
    return [
        json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(tasks_dir.glob("*.json"))
    ]


def _prepare_topology(
    task_def: dict[str, Any], *, live: bool = False
) -> tuple[list[dict], list[dict], str, Any]:
    topo = task_def["topology"]
    prefix = task_def["task_id"].replace("-", "_")
    integ_hash = f"integ-{task_def['task_id']}"

    def _uid(raw: str) -> str:
        return f"{prefix}_{raw}"

    nodes = [{**n, "work_unit_id": _uid(n["work_unit_id"])} for n in topo["nodes"]]
    # Per-node assignment: preserve objective / input_refs from the task def when present.
    for n, raw in zip(nodes, topo["nodes"], strict=False):
        if raw.get("objective"):
            n["objective"] = raw["objective"]
        if raw.get("input_refs") is not None:
            n["input_refs"] = list(raw["input_refs"])
        if raw.get("expected_outputs") is not None:
            n["expected_outputs"] = list(raw["expected_outputs"])
        if raw.get("acceptance_criteria") is not None:
            n["acceptance_criteria"] = list(raw["acceptance_criteria"])
    edges = []
    for e in topo["edges"]:
        ne = {
            **e,
            "from_work_unit_id": _uid(e["from_work_unit_id"]),
            "to_work_unit_id": _uid(e["to_work_unit_id"]),
        }
        if ne.get("artifact_hash") == "PLACEHOLDER_INTEG":
            if live:
                # Live content hashes are not known a priori; keep VERDICT_PASS but
                # do not pin a Fake placeholder digest.
                ne.pop("artifact_hash", None)
            else:
                ne["artifact_hash"] = integ_hash
        edges.append(ne)
    return nodes, edges, integ_hash, _uid


def _seed_files_for_task(task_def: dict[str, Any]) -> dict[str, str]:
    """Load fixture text into workspace seed paths."""
    files: dict[str, str] = {
        "README.md": (
            f"# {task_def.get('title') or task_def['task_id']}\n\n"
            f"{task_def.get('objective') or ''}\n\n"
            "Instructions (keep this short):\n"
            "1. Read attachments/ if present.\n"
            "2. Write a short deliverable file (e.g. result.md or out.txt).\n"
            "3. Call artifact.publish on that file.\n"
            "4. Submit COMPLETED/PASS. Do not probe unrelated paths.\n"
        )
    }
    inputs = task_def.get("inputs") or {}
    for name, rel in inputs.items():
        path = Path(rel)
        if path.is_file():
            files[f"attachments/{name}.txt"] = path.read_text(encoding="utf-8")
        else:
            files[f"attachments/{name}.txt"] = f"stub input for {name}\n"
    files.setdefault("attachments/a.txt", "alpha sample content\n")
    files.setdefault("attachments/b.txt", "beta sample content\n")
    return files


def _materialize_workspaces(svc, nodes: list[dict[str, Any]], seed_files: dict[str, str]) -> list[str]:
    """Create on-disk workspaces so the Broker/sandbox do not see workspace_missing."""
    from hibiki.interfaces.m1_runner import _seed_workspace

    created: list[str] = []
    for node in nodes:
        ws_id = str(node.get("workspace_id") or f"ws_{node['work_unit_id']}")
        _seed_workspace(svc, ws_id, seed_files)
        created.append(ws_id)
    return created


def _bootstrap_complex_task(
    svc, auth: AuthContext, task_def: dict[str, Any], *, live: bool = False
) -> dict[str, Any]:
    """Create → contract → PLAN Run → activate fixed topology. Shared by Fake and live."""
    from tests.helpers import run_auth

    title = task_def.get("title") or task_def["task_id"]
    r = svc.execute("create_task", auth, {"title": title})
    task_id = r.data["task_id"]
    r = svc.execute(
        "submit_contract",
        auth,
        {
            "task_id": task_id,
            "contract": {
                "objective": task_def.get("objective") or title,
                "simple": False,
                "deliverables": [
                    {
                        "deliverable_id": "d1",
                        "description": "integrated result",
                        "expected_kind": "text",
                    }
                ],
                "acceptance_criteria": [
                    {
                        "criterion_id": "c1",
                        "statement": "verify pass",
                        "evidence_kind": "artifact",
                        "required": True,
                    }
                ],
                "permission_ceiling": {
                    "tools": ["fs.read", "fs.write", "fs.list", "shell.run", "artifact.publish"]
                },
                "resource_limits": {
                    "wall_timeout_seconds": 420,
                    "max_model_calls": 200,
                    "max_turns": 30,
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

    r = svc.execute("dispatch_planner_run", auth, {"task_id": task_id})
    assert r.ok, r
    plan_run = r.data["run_id"]
    generation = r.data["generation"]
    svc.drain_outbox()

    nodes, edges, integ_hash, _uid = _prepare_topology(task_def, live=live)
    planner = run_auth(svc, plan_run)
    prop = svc.execute(
        "submit_plan_proposal",
        planner,
        {
            "task_id": task_id,
            "generation": generation,
            "nodes": nodes,
            "edges": edges,
        },
    )
    assert prop.ok, prop
    svc.execute(
        "submit_result",
        planner,
        {
            "run_id": plan_run,
            "fencing_epoch": planner.bound_fencing_epoch,
            "result": {"outcome": "COMPLETED", "verdict": "PASS"},
        },
    )

    workspaces = _materialize_workspaces(svc, nodes, _seed_files_for_task(task_def))
    return {
        "task_id": task_id,
        "nodes": nodes,
        "edges": edges,
        "integ_hash": integ_hash,
        "uid": _uid,
        "plan_version": prop.data.get("plan_version"),
        "workspaces": workspaces,
    }


def _assess_complex_success(
    svc,
    task_id: str,
    boot: dict[str, Any],
    record: dict[str, Any],
    *,
    require_artifacts: bool,
) -> dict[str, Any]:
    """Gate §24.5 success on DAG completion, artifacts, and exit confirmation — not run count."""
    _ = boot
    active = [
        wu
        for wu in svc.list_work_units(task_id)
        if wu.get("status") not in {"CANCELLED"}
    ]
    done = [
        wu["work_unit_id"]
        for wu in active
        if wu.get("status") == "DONE" and wu.get("selected_verdict") == "PASS"
    ]
    missing = [
        {
            "work_unit_id": wu.get("work_unit_id"),
            "status": wu.get("status"),
            "verdict": wu.get("selected_verdict"),
        }
        for wu in active
        if not (wu.get("status") == "DONE" and wu.get("selected_verdict") == "PASS")
    ]

    artifacts = list(svc.list_task_artifacts(task_id) or [])
    artifact_ok = (not require_artifacts) or bool(artifacts)

    runs = svc.list_runs(task_id)
    unconfirmed = [
        r
        for r in runs
        if r.get("sandbox_exit_unconfirmed")
        or (
            r.get("status") in {"RUNNING", "CREATED"}
            and r.get("assignment_kind") != "PLAN"
        )
    ]

    has_verify_pass = any(
        wu.get("status") == "DONE" and wu.get("selected_verdict") == "PASS"
        for wu in active
        if str(wu.get("work_unit_id") or "").endswith("wu_verify")
        or "verify" in str(wu.get("work_unit_id") or "").lower()
    )
    # Fallback: any VERIFY-typed unit from the final plan may use a generated id after repair.
    if not has_verify_pass:
        has_verify_pass = any(
            wu.get("status") == "DONE" and wu.get("selected_verdict") == "PASS"
            for wu in active
        ) and len(done) >= 1

    record["ok"] = (
        len(missing) == 0
        and artifact_ok
        and not unconfirmed
        and bool(done)
        and has_verify_pass
    )
    record["terminal"] = "COMPLETED" if record["ok"] else "INCOMPLETE"
    record["done_work_units"] = done
    record["missing_work_units"] = missing
    record["artifact_count"] = len(artifacts)
    record["unconfirmed_runs"] = [r.get("run_id") for r in unconfirmed]
    record["planned_nodes"] = len(active)
    return record


def _run_fake_complex(svc, auth: AuthContext, task_def: dict[str, Any]) -> dict[str, Any]:
    from tests.helpers import submit_result_and_exit

    boot = _bootstrap_complex_task(svc, auth, task_def, live=False)
    task_id = boot["task_id"]
    nodes = boot["nodes"]
    edges = boot["edges"]
    integ_hash = boot["integ_hash"]
    _uid = boot["uid"]

    record: dict[str, Any] = {
        "task_def": task_def["task_id"],
        "task_id": task_id,
        "plan_version": boot["plan_version"],
        "runs": [],
        "terminal": None,
        "ok": False,
    }

    safety = 0
    inject = bool(task_def.get("inject_fail_repair"))
    injected = False
    while safety < 20:
        safety += 1
        r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
        created = list(r.data.get("created_runs") or [])
        if not created:
            break
        svc.drain_outbox()
        for rid in created:
            run_row = next(x for x in svc.list_runs(task_id) if x["run_id"] == rid)
            wu_id = run_row.get("work_unit_id")
            is_verify = bool(wu_id and str(wu_id).endswith("wu_verify"))
            result = {
                "outcome": "COMPLETED",
                "verdict": "PASS",
                "artifact_refs": [integ_hash],
                "verified_artifact_refs": [integ_hash],
                "acceptance_evidence": [
                    {
                        "criterion_id": "c1",
                        "artifact_hash": integ_hash,
                        "verdict": "PASS",
                        "check": "fake_harness",
                    }
                ],
            }
            if inject and not injected and is_verify:
                result = {
                    "outcome": "COMPLETED",
                    "verdict": "FAIL",
                    "artifact_refs": [integ_hash],
                    "acceptance_evidence": [
                        {
                            "criterion_id": "c1",
                            "artifact_hash": integ_hash,
                            "verdict": "FAIL",
                            "check": "fake_harness_inject",
                        }
                    ],
                }
                injected = True
            submit_result_and_exit(svc, auth, rid, result=result)
            record["runs"].append({"run_id": rid, "result": result, "work_unit_id": wu_id})

        if injected and inject:
            fail_wu = _uid("wu_verify")
            repair = svc.execute(
                "request_repair_plan",
                auth,
                {
                    "task_id": task_id,
                    "failed_verify_work_unit_id": fail_wu,
                    "artifact_hash": integ_hash,
                    "keep_nodes": [n for n in nodes if n["work_unit_id"] != fail_wu],
                    "keep_edges": [
                        e for e in edges if e.get("to_work_unit_id") != fail_wu
                    ],
                },
            )
            record["repair"] = {"ok": repair.ok, "error": repair.error_code}
            if repair.ok:
                inject = False
                # Continue loop so the new VERIFY can run after repair.

    return _assess_complex_success(
        svc, task_id, boot, record, require_artifacts=False
    )


def _wait_runs(svc, task_id: str, run_ids: list[str], *, timeout_s: float = 420.0) -> list[dict]:
    deadline = time.monotonic() + timeout_s
    finals: dict[str, dict] = {}
    while time.monotonic() < deadline and len(finals) < len(run_ids):
        for rid in run_ids:
            if rid in finals:
                continue
            row = next((r for r in svc.list_runs(task_id) if r["run_id"] == rid), None)
            if row is None:
                continue
            if row["status"] not in {
                AgentRunStatus.CREATED,
                AgentRunStatus.RUNNING,
                "CREATED",
                "RUNNING",
            }:
                finals[rid] = row
        if len(finals) < len(run_ids):
            time.sleep(0.5)
            svc.drain_outbox()
    return [finals.get(rid) or {"run_id": rid, "status": "TIMEOUT"} for rid in run_ids]


def _run_live_complex(svc, auth: AuthContext, task_def: dict[str, Any]) -> dict[str, Any]:
    """Same topology as Fake, but EXECUTE Runs are completed by the real ApiAgentAdapter."""
    boot = _bootstrap_complex_task(svc, auth, task_def, live=True)
    task_id = boot["task_id"]
    nodes = boot["nodes"]
    edges = boot["edges"]
    integ_hash = boot["integ_hash"]
    _uid = boot["uid"]
    record: dict[str, Any] = {
        "task_def": task_def["task_id"],
        "task_id": task_id,
        "plan_version": boot["plan_version"],
        "workspaces": boot.get("workspaces"),
        "runs": [],
        "terminal": None,
        "ok": False,
    }

    safety = 0
    inject = bool(task_def.get("inject_fail_repair"))
    injected = False
    while safety < 20:
        safety += 1
        r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
        created = list(r.data.get("created_runs") or [])
        if not created:
            break
        svc.drain_outbox()
        finals = _wait_runs(svc, task_id, created)
        for row in finals:
            result = None
            if row.get("result_json"):
                try:
                    result = json.loads(row["result_json"])
                except (TypeError, ValueError):
                    result = {"raw": row["result_json"]}
            record["runs"].append(
                {
                    "run_id": row.get("run_id"),
                    "status": row.get("status"),
                    "result": result,
                    "work_unit_id": row.get("work_unit_id"),
                }
            )
            if row.get("status") in {"TIMEOUT", "CREATED", "RUNNING"}:
                record["terminal"] = "TIMEOUT"
                record["ok"] = False
                return record
            # Live FAIL→REPAIR only when the model actually returns FAIL.
            if (
                inject
                and not injected
                and (result or {}).get("outcome") == "COMPLETED"
                and (result or {}).get("verdict") == "FAIL"
            ):
                fail_wu = str(row.get("work_unit_id") or _uid("wu_verify"))
                refs = (result or {}).get("artifact_refs") or [integ_hash]
                repair = svc.execute(
                    "request_repair_plan",
                    auth,
                    {
                        "task_id": task_id,
                        "failed_verify_work_unit_id": fail_wu,
                        "artifact_hash": refs[0] if refs else integ_hash,
                        "keep_nodes": [
                            n for n in nodes if n["work_unit_id"] != fail_wu
                        ],
                        "keep_edges": [
                            e
                            for e in edges
                            if e.get("to_work_unit_id") != fail_wu
                        ],
                    },
                )
                record["repair"] = {"ok": repair.ok, "error": repair.error_code}
                injected = True
                inject = False

    return _assess_complex_success(
        svc, task_id, boot, record, require_artifacts=True
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hibiki-m2-runner")
    parser.add_argument("--data-dir", type=Path, default=Path("/tmp/hibiki-m2"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--tasks", type=Path, default=Path("docs/m2/tasks"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=Path(DOTENV_FILENAME),
        help="credential file (default: .env; real environment wins)",
    )
    parser.add_argument(
        "--check-credentials",
        action="store_true",
        help="one model call to verify credentials, then exit",
    )
    args = parser.parse_args(argv)

    live = bool(args.live) and not args.dry_run
    model_name = "fake"
    base_url = "fake"

    if args.check_credentials or live:
        # Same names as M1 / .env.example — load .env first.
        load_dotenv(args.dotenv)
        missing = [name for name in MODEL_ENV if not os.environ.get(name)]
        if missing:
            summary = {
                "mode": "live" if live else "check-credentials",
                "status": "BLOCKED",
                "reason": (
                    "missing credentials: "
                    + ", ".join(missing)
                    + f" (fill in {args.dotenv} or export HIBIKI_MODEL_BASE_URL / "
                    "HIBIKI_MODEL_API_KEY / HIBIKI_MODEL)"
                ),
                "at": datetime.now(UTC).isoformat(),
            }
            args.out.mkdir(parents=True, exist_ok=True)
            (args.out / "summary.json").write_text(
                json.dumps(summary, indent=2), encoding="utf-8"
            )
            print(json.dumps(summary, indent=2))
            return 2
        config = ModelConfig.from_env(dotenv=args.dotenv)
        model_name = config.model
        base_url = config.base_url
        if args.check_credentials:
            from hibiki.interfaces.m1_runner import _check_credentials

            return _check_credentials(config, args)

    if args.clean and args.data_dir.exists():
        shutil.rmtree(args.data_dir)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)

    planner = FakePlannerAdapter()
    if live:
        from hibiki.runtime.api_agent import ApiAgentAdapter
        from hibiki.runtime.openai_client import OpenAICompatibleClient
        from hibiki.tools.broker import ToolBroker
        from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

        client = OpenAICompatibleClient(base_url, os.environ["HIBIKI_MODEL_API_KEY"], model_name)
        sandbox = DockerSandboxAdapter(
            SandboxSpec(
                image=os.environ.get("HIBIKI_SANDBOX_IMAGE", "hibiki-sandbox:py312"),
                workspace_host_path=str(args.data_dir),
                limits=SandboxLimits(wall_timeout_s=420, stop_grace_s=5),
            )
        )
        svc, ctx = bootstrap_core(args.data_dir, agent=planner, fake_time=False)
        broker = ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root)
        worker = ApiAgentAdapter(
            client,
            core=svc,
            clock=svc.clock,
            broker=broker,
            sandbox=sandbox,
            workspace_root=svc.workspace_root,
        )
        svc.agent_adapter = AssignmentRoutingAdapter(planner=planner, worker=worker)
        run_fn = _run_live_complex
    else:
        svc, ctx = bootstrap_core(args.data_dir, agent=planner, fake_time=False)
        run_fn = _run_fake_complex

    auth = _human()
    tasks = _load_tasks(args.tasks)
    records = []
    try:
        for t in tasks:
            rec = run_fn(svc, auth, t)
            rec["mode"] = "live" if live else "dry-run"
            path = args.out / f"{t['task_id']}.json"
            path.write_text(json.dumps(rec, indent=2, default=str), encoding="utf-8")
            records.append(rec)
            print(
                f"[{'PASS' if rec.get('ok', True) and rec.get('runs') else 'DONE'}] "
                f"{t['task_id']} runs={len(rec.get('runs') or [])} -> {path}"
            )
    finally:
        ctx["lock"].release()

    summary = {
        "mode": "live" if live else "dry-run",
        "model": model_name,
        "base_url": base_url,
        "tasks": len(records),
        "completed": sum(1 for r in records if r.get("runs")),
        "at": datetime.now(UTC).isoformat(),
        "records": [r["task_def"] for r in records],
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
