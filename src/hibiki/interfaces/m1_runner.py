"""Run the three fixed M1 acceptance tasks against a real model (SPEC §24.4 G3).

Usage (credentials come from the environment only and are never persisted):

    HIBIKI_MODEL_BASE_URL=https://api.deepseek.com \
    HIBIKI_MODEL_API_KEY=... \
    HIBIKI_MODEL=deepseek-chat \
    uv run --no-sync python -m hibiki.interfaces.m1_runner \
        --data-dir /tmp/hibiki-m1 --out docs/acceptance/evidence/m1-<date>

Each task is run twice (six runs). For every run the harness records the contract,
plan, workspace seed files, the run input spec hash, tool invocations, the submitted
result, published artifacts and hashes, timing and model usage, and writes one JSON
file per run plus a summary. A run counts as passing only when a COMPLETED result with
a PASS verdict was submitted inside its budget and every published artifact verifies.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select

from hibiki.application.bootstrap import bootstrap_core
from hibiki.application.service import ApplicationService
from hibiki.domain.types import AuthContext
from hibiki.persistence.models import AgentRunRow, ArtifactRow, TaskRow, ToolInvocationRow
from hibiki.runtime.api_agent import ApiAgentAdapter
from hibiki.runtime.openai_client import OpenAICompatibleClient
from hibiki.tools.broker import ToolBroker
from hibiki.tools.paths import WorkspacePaths
from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

MODEL_ENV = ("HIBIKI_MODEL_BASE_URL", "HIBIKI_MODEL_API_KEY", "HIBIKI_MODEL")

#: File the operator can fill in so no credential has to be typed on the command line.
#: Real environment variables always win over the file, and the file is gitignored.
DOTENV_FILENAME = ".env"


def load_dotenv(path: Path | None = None) -> dict[str, str]:
    """Load ``KEY=value`` pairs from ``.env`` into ``os.environ`` (no overrides).

    Minimal on purpose: no dependency, no interpolation, no export of secrets anywhere
    else. Lines may be blank, ``#`` comments, or ``KEY=value`` with optional surrounding
    single/double quotes around the value. Returns only the keys it set.
    """
    env_path = path or Path(DOTENV_FILENAME)
    if not env_path.is_file():
        return {}
    applied: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export ") :].strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value
        applied[key] = value
    return applied


class DryRunClient:
    """A client that answers without a provider, for validating the harness yourself.

    It never claims success: every completion is empty, which the adapter must turn into
    a BLOCKED result. Running with ``--dry-run`` therefore proves the task → contract →
    plan → workspace → dispatch → worker → result path end to end (and that no failure is
    reported as a pass) without spending a single provider call.
    """

    def __init__(self) -> None:
        self.calls = 0

    def chat(self, messages, *, tools=None, temperature=0.0, timeout_s=None):
        from hibiki.runtime.openai_client import ModelReply

        self.calls += 1
        return ModelReply(content="", tool_calls=(), finish_reason="stop", usage={}, raw={})

    def close(self) -> None:
        return None


@dataclass
class ModelConfig:
    base_url: str
    api_key: str
    model: str

    @classmethod
    def from_env(cls, *, dotenv: Path | None = None) -> ModelConfig:
        load_dotenv(dotenv)
        missing = [name for name in MODEL_ENV if not os.environ.get(name)]
        if missing:
            raise SystemExit(
                "missing credentials: "
                + ", ".join(missing)
                + f" (fill in {DOTENV_FILENAME} or export them in the environment)"
            )
        return cls(
            base_url=os.environ["HIBIKI_MODEL_BASE_URL"],
            api_key=os.environ["HIBIKI_MODEL_API_KEY"],
            model=os.environ["HIBIKI_MODEL"],
        )


def _human_auth(principal: str = "human_1") -> AuthContext:
    from hibiki.domain.enums import ActorType

    return AuthContext(
        principal_id=principal,
        actor_id=principal,
        actor_type=ActorType.HUMAN,
        auth_context_id="m1_runner",
    )


def _load_task(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    for key in ("task_id", "title", "objective", "permission_ceiling", "seed_files"):
        if key not in data:
            raise SystemExit(f"{path}: missing required key {key!r}")
    return data


def _workspace_root(svc: ApplicationService) -> Path:
    assert svc.workspace_root is not None
    return Path(svc.workspace_root)


SANDBOX_IMAGE = os.environ.get("HIBIKI_SANDBOX_IMAGE", "hibiki-sandbox:py312")


def _chmod_for_sandbox(path: Path) -> None:
    """The sandbox runs as uid 65534, so the mounted workspace must be world-readable."""
    for target in [path, *path.rglob("*")]:
        try:
            mode = 0o755 if target.is_dir() else 0o644
            target.chmod(mode)
        except OSError:
            pass


def _seed_workspace(svc: ApplicationService, workspace_id: str, files: dict[str, str]) -> Path:
    root = _workspace_root(svc) / workspace_id
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o755)
    with WorkspacePaths(root) as paths:
        for name, content in files.items():
            paths.atomic_write(name, content.encode("utf-8"))
    _chmod_for_sandbox(root)
    return root


def _run_auth(
    principal_id: str,
    task_id: str,
    run_id: str,
    agent_instance_id: str,
    fencing_epoch: int,
    grant_epoch: int,
) -> AuthContext:
    """Build the run-bound Internal credential the Core requires for worker writes."""
    from hibiki.domain.enums import ActorType

    return AuthContext(
        principal_id=principal_id,
        actor_id=agent_instance_id,
        actor_type=ActorType.INTERNAL,
        auth_context_id=f"run:{run_id}",
        bound_task_id=task_id,
        bound_run_id=run_id,
        bound_fencing_epoch=fencing_epoch,
        bound_grant_epoch=grant_epoch,
    )


def _run_row(svc: ApplicationService, task_id: str, run_id: str) -> dict[str, Any]:
    return next(item for item in svc.list_runs(task_id) if item["run_id"] == run_id)


def _tool_rows(svc: ApplicationService, run_id: str) -> list[dict[str, Any]]:
    return svc.executor.run(
        lambda s: [
            {
                "sequence_no": r.sequence_no,
                "tool_name": r.tool_name,
                "decision": r.decision,
                "deny_reason": r.deny_reason,
                "outcome": r.outcome,
                "parameters_hash": r.parameters_hash,
            }
            for r in s.scalars(
                select(ToolInvocationRow)
                .where(ToolInvocationRow.run_id == run_id)
                .order_by(ToolInvocationRow.sequence_no)
            ).all()
        ]
    )


def _artifact_rows(svc: ApplicationService, task_id: str) -> list[dict[str, Any]]:
    return svc.executor.run(
        lambda s: [
            {
                "artifact_hash": a.artifact_hash,
                "uri": a.artifact_uri,
                "size": a.size_bytes,
                "source_path": a.source_path,
                "run_id": a.run_id,
            }
            for a in s.scalars(select(ArtifactRow).where(ArtifactRow.task_id == task_id)).all()
        ]
    )


def _run_state(svc: ApplicationService, task_id: str, run_id: str) -> dict[str, Any]:
    return svc.executor.run(
        lambda s: {
            "status": s.get(AgentRunRow, run_id).status,
            "result_json": s.get(AgentRunRow, run_id).result_json,
            "terminal_reason": s.get(AgentRunRow, run_id).terminal_reason,
        }
    )


def _approve(svc: ApplicationService, auth: AuthContext, task_id: str, contract: dict) -> None:
    r = svc.execute(
        "approve_contract",
        auth,
        {
            "decision_id": contract["decision_id"],
            "expected_target_hash": contract["content_hash"],
            "expected_target_version": contract["contract_version"],
        },
    )
    if not r.ok:
        raise SystemExit(f"approve failed: {r.error_code} {r.error_message}")


def run_once(
    svc: ApplicationService,
    adapter: ApiAgentAdapter,
    auth: AuthContext,
    spec: dict[str, Any],
    attempt: int,
) -> dict[str, Any]:
    task_id = spec["task_id"]
    started = time.monotonic()
    record: dict[str, Any] = {"task_id": task_id, "attempt": attempt, "ok": False}

    r = svc.execute("create_task", auth, {"title": spec["title"], "intent": spec.get("intent", "")})
    if not r.ok:
        record["error"] = f"create_task: {r.error_code}"
        return record
    internal_task_id = r.data["task_id"]

    payload = {
        "task_id": internal_task_id,
        "objective": spec["objective"],
        "permission_ceiling": spec["permission_ceiling"],
        "resource_limits": spec.get("resource_limits") or {},
        "deliverables": spec.get("deliverables"),
        "acceptance_criteria": spec.get("acceptance_criteria"),
    }
    r = svc.execute("submit_contract", auth, payload)
    if not r.ok:
        record["error"] = f"submit_contract: {r.error_code}"
        return record
    _approve(svc, auth, internal_task_id, r.data)

    wu_id = f"{task_id}_wu1"
    r = svc.execute(
        "activate_plan",
        auth,
        {
            "task_id": internal_task_id,
            "objective": spec["objective"],
            "nodes": [
                {"work_unit_id": wu_id, "spec_version": 1, "work_type": "EXECUTE"},
            ],
            "edges": [],
        },
    )
    if not r.ok:
        record["error"] = f"activate_plan: {r.error_code} {r.error_message}"
        return record

    workspace_id = f"ws_{wu_id}"
    workspace = _seed_workspace(svc, workspace_id, spec["seed_files"])
    record["workspace"] = str(workspace)
    record["seed_files"] = sorted(spec["seed_files"])

    r = svc.execute("dispatch_ready_runs", auth, {"task_id": internal_task_id})
    if not r.ok or not r.data.get("created_runs"):
        record["error"] = f"dispatch: {r.error_code or 'no run'}"
        return record
    run_id = r.data["created_runs"][0]
    record["run_id"] = run_id

    deadline = time.monotonic() + float(
        (spec.get("resource_limits") or {}).get("wall_timeout_seconds", 300)
    )
    final: dict[str, Any] = {}
    while time.monotonic() < deadline:
        state = _run_state(svc, internal_task_id, run_id)
        final = state
        if state["status"] not in {"CREATED", "RUNNING"}:
            break
        time.sleep(0.5)
    record["elapsed_s"] = round(time.monotonic() - started, 2)
    record["run_status"] = final.get("status")
    record["terminal_reason"] = final.get("terminal_reason")

    bound = svc.executor.run(
        lambda s: {
            "task_id": s.get(AgentRunRow, run_id).task_id,
            "principal_id": s.get(TaskRow, s.get(AgentRunRow, run_id).task_id).principal_id,
            "agent_instance_id": s.get(AgentRunRow, run_id).agent_instance_id,
            "fencing_epoch": s.get(AgentRunRow, run_id).fencing_epoch,
            "grant_epoch": s.get(AgentRunRow, run_id).grant_epoch,
        }
    )
    worker_auth = _run_auth(
        bound["principal_id"],
        bound["task_id"],
        run_id,
        bound["agent_instance_id"],
        int(bound["fencing_epoch"]),
        int(bound["grant_epoch"]),
    )
    spec_snapshot = svc.get_run_input(worker_auth, run_id)
    record["spec_hash"] = spec_snapshot["spec_hash"]
    record["context_manifest_id"] = spec_snapshot["context_manifest_id"]
    record["granted_tools"] = spec_snapshot["granted_tools"]

    if final.get("result_json"):
        result = json.loads(final["result_json"])
        record["result"] = result
        record["ok"] = result.get("outcome") == "COMPLETED" and result.get("verdict") == "PASS"
    else:
        record["result"] = None

    record["tool_invocations"] = _tool_rows(svc, run_id)
    artifacts = _artifact_rows(svc, internal_task_id)
    record["artifacts"] = artifacts
    record["artifact_checks"] = [
        svc.verify_artifact_content(internal_task_id, a["artifact_hash"]) for a in artifacts
    ]
    if not all(check.get("verified") for check in record["artifact_checks"]):
        record["ok"] = False
    unbacked = svc.list_unbacked_artifacts(internal_task_id)
    record["unbacked_artifact_refs"] = unbacked
    if unbacked:
        # A result that names hashes the Core never received bytes for is not a
        # verifiable delivery (§11.3 / §20.1).
        record["ok"] = False
    record["task_state"] = svc.get_task(internal_task_id)["state"]
    record["model_calls_used"] = svc.get_task(internal_task_id)["model_calls_used"]
    record["internal_task_id"] = internal_task_id
    return record


def _check_credentials(config: ModelConfig, args: argparse.Namespace) -> int:
    """One real call, no task state: is the endpoint reachable and the key accepted?"""
    from hibiki.runtime.openai_client import ChatMessage, ModelClientError, OpenAICompatibleClient

    client = OpenAICompatibleClient(config.base_url, config.api_key, config.model, timeout_s=30.0)
    try:
        reply = client.chat(
            [
                ChatMessage(role="system", content="Reply with the single word: ready"),
                ChatMessage(role="user", content="ping"),
            ],
            timeout_s=30.0,
        )
    except ModelClientError as exc:
        print(f"credential check FAILED: {exc.kind}: {exc}", file=sys.stderr)
        return 2
    finally:
        client.close()
    text = (reply.content or "").strip()
    print(f"credential check OK: model={config.model} base_url={config.base_url}")
    print(f"model replied: {text[:200]!r}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hibiki-m1-runner")
    parser.add_argument("--data-dir", type=Path, default=Path("/tmp/hibiki-m1"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--tasks", type=Path, default=Path("docs/m1/tasks"))
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--clean", action="store_true", help="wipe the data dir first")
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=Path(DOTENV_FILENAME),
        help="credential file to load (default: .env; real environment wins)",
    )
    parser.add_argument(
        "--check-credentials",
        action="store_true",
        help="make one model call to verify the credentials, then exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate the whole harness path without a provider (no credentials needed)",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        config = ModelConfig(base_url="dry-run", api_key="", model="dry-run")
    else:
        config = ModelConfig.from_env(dotenv=args.dotenv)
    if args.check_credentials:
        return _check_credentials(config, args)
    if args.clean and args.data_dir.exists():
        shutil.rmtree(args.data_dir)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)

    client = (
        DryRunClient()
        if args.dry_run
        else OpenAICompatibleClient(config.base_url, config.api_key, config.model)
    )
    # The adapter re-mounts this spec against each Run's own workspace, so the
    # placeholder path here is never used to run a command.
    sandbox = DockerSandboxAdapter(
        SandboxSpec(
            image=SANDBOX_IMAGE,
            workspace_host_path=str(args.data_dir),
            limits=SandboxLimits(wall_timeout_s=420, stop_grace_s=5),
        )
    )
    for task_file in sorted(args.tasks.glob("*.json")):
        spec = _load_task(task_file)
        for attempt in range(1, args.repeats + 1):
            run_data_dir = args.data_dir / spec["task_id"] / f"run{attempt}"
            if run_data_dir.exists():
                shutil.rmtree(run_data_dir)
            svc, ctx = bootstrap_core(run_data_dir)
            try:
                broker = ToolBroker(svc.executor, svc.clock, workspace_root=svc.workspace_root)
                adapter = ApiAgentAdapter(
                    client,
                    core=svc,
                    clock=svc.clock,
                    broker=broker,
                    sandbox=sandbox,
                    workspace_root=svc.workspace_root,
                )
                svc.agent_adapter = adapter
                record = run_once(svc, adapter, _human_auth(), spec, attempt)
            finally:
                ctx["lock"].release()
            out_file = args.out / f"{spec['task_id']}-run{attempt}.json"
            out_file.write_text(json.dumps(record, indent=2, default=str), encoding="utf-8")
            status = "PASS" if record.get("ok") else "FAIL"
            print(
                f"[{status}] {spec['task_id']} run{attempt} "
                f"status={record.get('run_status')} elapsed={record.get('elapsed_s')}s "
                f"-> {out_file}"
            )

    records = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(args.out.glob("*-run*.json"))
    ]
    passed = sum(1 for rec in records if rec.get("ok"))
    summary = {
        "dry_run": bool(args.dry_run),
        "model": config.model,
        "base_url": config.base_url,
        "runs": len(records),
        "passed": passed,
        "gate_met": passed >= 5,
        "records": [
            {
                "task_id": rec.get("task_id"),
                "attempt": rec.get("attempt"),
                "ok": rec.get("ok"),
                "run_status": rec.get("run_status"),
                "elapsed_s": rec.get("elapsed_s"),
                "model_calls_used": rec.get("model_calls_used"),
                "artifact_hashes": [a.get("artifact_hash") for a in rec.get("artifacts") or []],
                "error": rec.get("error"),
            }
            for rec in records
        ],
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(
        f"gate: {passed}/{len(records)} runs passed "
        f"({'MET' if summary['gate_met'] else 'NOT MET'}, need >=5/6)"
    )
    return 0 if summary["gate_met"] else 1


if __name__ == "__main__":
    sys.exit(main())
