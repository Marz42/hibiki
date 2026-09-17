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
import hashlib
import json
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hibiki.application.bootstrap import bootstrap_core
from hibiki.domain.defaults import DEFAULTS
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
    task_def: dict[str, Any],
    *,
    pin_integrate_digest: bool = True,
) -> tuple[list[dict], list[dict], Any]:
    """Translate the task definition into plan nodes/edges.

    A ``PLACEHOLDER_INTEG`` edge hash is a task-definition marker meaning "bind the
    INTEGRATE deliverable". It is resolved differently per mode, because the two modes
    genuinely differ:

    * **Fake** — the harness knows the exact bytes a Work Unit will publish, so the pin
      is the real content digest (a genuine content address). Replacing it with a
      synthetic label is what previously made the edge unsatisfiable.
    * **live** — the real model decides the bytes, so no digest can exist before the Run
      does. Pinning the Fake digest would make ``VERDICT_PASS`` permanently unsatisfiable
      (observed: ``artifact_binding_mismatch`` / VERIFY never dispatched). The edge is
      emitted without a pin and binds to whatever digest INTEGRATE actually published;
      acceptance still proves the delivery is content-backed via
      ``unbacked_artifacts`` / ``verify_artifact_content``.
    """
    topo = task_def["topology"]
    prefix = task_def["task_id"].replace("-", "_")

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
        edges.append(ne)

    # A node's output content depends on which upstream deliverables it merges, so the
    # merge list must be derived here — computing the digest with one merge list and
    # publishing with another is exactly how the edge pin went stale before.
    node_by_id = {n["work_unit_id"]: n for n in nodes}
    for e in edges:
        src = node_by_id.get(e["from_work_unit_id"])
        dst = node_by_id.get(e["to_work_unit_id"])
        if src is None or dst is None:
            continue
        outputs = list(src.get("expected_outputs") or [])
        if outputs:
            dst.setdefault("_upstream_paths", []).append(str(outputs[0]))

    for e in edges:
        if e.get("artifact_hash") == "PLACEHOLDER_INTEG":
            upstream = node_by_id.get(e["from_work_unit_id"])
            if upstream is None:
                raise ValueError(
                    f"edge from {e['from_work_unit_id']} has no node to derive a digest from"
                )
            if pin_integrate_digest:
                e["artifact_hash"] = _outputs_digest(_fake_worker_outputs(upstream))
            else:
                e.pop("artifact_hash", None)
    return nodes, edges, _uid


def _seed_files_for_task(task_def: dict[str, Any]) -> dict[str, str]:
    """Load fixture text into workspace seed paths."""
    files: dict[str, str] = {
        "README.md": (
            f"# {task_def.get('title') or task_def['task_id']}\n\n"
            f"{task_def.get('objective') or ''}\n\n"
            "Instructions (keep this short):\n"
            "1. Read attachments/ if present.\n"
            "2. Write your deliverable with the `fs.write` tool at the exact path the\n"
            "   objective names, then call `artifact.publish` on that path.\n"
            "3. Submit COMPLETED/PASS. Do not probe unrelated paths.\n"
            "4. Only `fs.write` can create files: `shell.run` executes unprivileged and\n"
            "   cannot write to the workspace, and parent directories are pre-created.\n"
            "   Do not test alternative write strategies.\n\n"
            "Files delivered by prerequisite work units appear at the top level as\n"
            "`<work_unit_id>.<ext>` (for a VERIFY unit, under `delivered/`); files this\n"
            "unit produces must use the exact path named in the objective.\n"
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


def _materialize_workspaces(
    svc,
    nodes: list[dict[str, Any]],
    seed_files: dict[str, str],
    *,
    seeded: set[str] | None = None,
) -> list[str]:
    """Create on-disk workspaces so the Broker/sandbox do not see workspace_missing.

    Output directories are pre-created from each node's ``expected_outputs``: the
    granted writer cannot create a parent directory (``fs.write`` opens a file only
    under an existing parent) and ``shell.run`` executes as an unprivileged user with
    no write access to the Workspace, so a nested deliverable such as ``edits/a.md``
    would otherwise be permanently unreachable (observed live as
    ``missing_expected_artifacts``).

    ``seeded`` carries the ids already provisioned, so a Repair revision can be
    materialized later without re-seeding a Workspace that already holds real results.
    """
    from hibiki.interfaces.m1_runner import _seed_workspace
    from hibiki.tools.paths import WorkspacePaths

    created: list[str] = []
    for node in nodes:
        ws_id = str(node.get("workspace_id") or f"ws_{node['work_unit_id']}")
        if seeded is not None and ws_id in seeded:
            created.append(ws_id)
            continue
        # The sandbox mounts the workspace as uid 65534, so a directory left owned by
        # root (e.g. one Docker created first) is unusable: fs.write runs as the owner
        # in-process, but the model's shell sees a read-only mount.
        root = Path(svc.workspace_root) / ws_id if svc.workspace_root else None
        if root is not None and root.exists() and root.stat().st_uid == 0:
            try:
                shutil.rmtree(root)
            except OSError:
                pass
        _seed_workspace(svc, ws_id, seed_files)
        parents = {
            str(Path(str(name)).parent).replace("\\", "/")
            for name in (node.get("expected_outputs") or [])
        }
        parents.discard(".")
        parents.discard("")
        if parents:
            root = Path(svc.workspace_root) / ws_id
            with WorkspacePaths(root) as paths:
                for parent in sorted(parents):
                    paths.mkdir(parent)
        created.append(ws_id)
    return created


def _deliver_dependency_artifacts(
    svc,
    task_id: str,
    nodes: list[dict[str, Any]],
    *,
    delivered: set[tuple[str, str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Copy each finished Work Unit's published deliverable into its dependents.

    A downstream Work Unit runs in its **own** Workspace (SPEC §11.1): the Core exposes
    upstream results as context materialization, not as files. A real worker can only
    re-read a prerequisite by its content address, and an agent asked to "merge
    left_summary.md and right_summary.md" otherwise finds neither file on disk — which
    is exactly how the live runs produced a report from nothing and then failed verify.

    Files are staged under the standard name ``<work_unit_id>.<ext>`` so a consumer can
    never confuse its own output with a dependency's. Returns one record per delivery.
    """
    from hibiki.tools.paths import WorkspacePaths

    units = {wu["work_unit_id"]: wu for wu in svc.list_work_units(task_id)}
    node_by_id = {str(n["work_unit_id"]): n for n in nodes}
    plan = svc.get_active_plan(task_id) or {}
    dependents: dict[str, set[str]] = {}
    for edge in plan.get("edges") or []:
        src = str(edge.get("from_work_unit_id") or "")
        dst = str(edge.get("to_work_unit_id") or "")
        if src and dst:
            dependents.setdefault(src, set()).add(dst)

    deliveries: list[dict[str, Any]] = []
    for upstream_id, downstream_ids in dependents.items():
        unit = units.get(upstream_id)
        digest = str((unit or {}).get("verified_artifact_hash") or "")
        if not digest or not svc.workspace_root:
            continue
        meta = svc.get_artifact(task_id, digest)
        uri = meta.get("uri")
        if not uri:
            continue
        try:
            data = svc._artifact_store().get(uri)
        except Exception as exc:  # noqa: BLE001 — recorded, never silent
            deliveries.append(
                {"from": upstream_id, "error": f"{type(exc).__name__}: {exc}"}
            )
            continue
        suffix = Path(str(meta.get("source_path") or "deliverable.md")).suffix or ".md"
        filename = f"{upstream_id}{suffix}"
        for downstream_id in sorted(downstream_ids):
            node = node_by_id.get(downstream_id, {})
            is_verify = str(node.get("work_type") or "").upper() == "VERIFY"
            ws_id = str(node.get("workspace_id") or f"ws_{downstream_id}")
            root = Path(svc.workspace_root) / ws_id
            if not root.is_dir():
                continue
            # A VERIFY unit must inspect what it judges, so it receives the artifact
            # too — but under a read-only ``delivered/`` prefix, so it cannot mistake a
            # dependency's file for one of its own outputs (SPEC §19.1).
            target = f"delivered/{filename}" if is_verify else filename
            key = (upstream_id, downstream_id, target)
            if delivered is not None:
                if key in delivered:
                    # Already staged in an earlier round; re-copying would inflate the
                    # record without changing what the consumer sees.
                    continue
                delivered.add(key)
            with WorkspacePaths(root) as paths:
                if is_verify:
                    paths.mkdir("delivered")
                paths.atomic_write(target, data)
            deliveries.append(
                {
                    "from": upstream_id,
                    "to": downstream_id,
                    "file": target,
                    "artifact_hash": digest,
                    "bytes": len(data),
                }
            )
            node.setdefault("_dependency_files", []).append(target)
    return deliveries


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
                    # Ask for the full SPEC §27 ceiling rather than a lower value: the
                    # Core clamps to DEFAULTS.max_run_model_turns, so an over-ask is
                    # simply capped while a low ask would throttle the real worker.
                    "max_model_calls": DEFAULTS.max_task_model_calls,
                    "max_turns": DEFAULTS.max_run_model_turns,
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

    nodes, edges, _uid = _prepare_topology(
        task_def, pin_integrate_digest=not live
    )
    planner = run_auth(svc, plan_run)
    proposal: dict[str, Any] = {
        "task_id": task_id,
        "generation": generation,
        "nodes": nodes,
        "edges": edges,
    }
    if live:
        # Live content digests do not exist yet, so a pinned VERDICT_PASS edge is not
        # an option; the acceptance gate proves the real binding instead.
        proposal["require_verdict_artifact_hash"] = False
    prop = svc.execute("submit_plan_proposal", planner, proposal)
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

    seed_files = _seed_files_for_task(task_def)
    workspaces = _materialize_workspaces(svc, nodes, seed_files)
    return {
        "task_id": task_id,
        "nodes": nodes,
        "edges": edges,
        "uid": _uid,
        "plan_version": prop.data.get("plan_version"),
        "workspaces": workspaces,
        "seeded": set(workspaces),
        "seed_files": seed_files,
    }


def _fake_worker_outputs(node: dict[str, Any]) -> dict[str, str]:
    """Deterministic file bodies for one Work Unit's ``expected_outputs``.

    INTEGRATE additionally folds in every deliverable its prerequisites already wrote
    to the Workspace, so the artifact it publishes is a real merge of upstream work
    rather than a fixed placeholder.
    """
    uid = str(node.get("_content_node_id") or node.get("work_unit_id") or "wu")
    work_type = str(
        node.get("_content_work_type") or node.get("work_type") or "EXECUTE"
    ).upper()
    lines = [
        f"# {uid}",
        f"work_type: {work_type}",
        f"objective: {node.get('objective') or ''}",
    ]
    if work_type == "INTEGRATE":
        lines.append("")
        lines.append("## integrated inputs")
        for path in sorted(node.get("_upstream_paths") or []):
            lines.append(f"- merged {path}")
    body = "\n".join(lines) + "\n"
    outputs = list(node.get("expected_outputs") or [])
    if not outputs:
        outputs = ["verify.md" if work_type == "VERIFY" else "result.md"]
    return {str(name): body for name in outputs}


def _outputs_digest(outputs: dict[str, str]) -> str:
    """sha256 of the first output's exact bytes, as the artifact store computes it.

    Lets the topology bind a ``VERDICT_PASS`` edge to the *real* digest before the
    Work Unit runs: the value is derived from the same bytes the Fake worker will
    publish, so it is a genuine content address rather than a synthetic label.
    """
    first_path = next(iter(outputs))
    return hashlib.sha256(outputs[first_path].encode("utf-8")).hexdigest()


def _publish_fake_outputs(
    svc, run_id: str, node: dict[str, Any], workspace_id: str
) -> dict[str, Any]:
    """Write this Work Unit's deliverables and publish the first as real bytes.

    Publishing is what makes the digest content-backed: a bare hash in
    ``artifact_refs`` registers an Artifact row with a null ``artifact_uri``, which
    cannot bind ``verified_artifact_hash`` and so can never satisfy a
    ``VERDICT_PASS`` edge.
    """
    from hibiki.interfaces.m1_runner import _seed_workspace
    from tests.helpers import run_auth

    outputs = _fake_worker_outputs(node)
    _seed_workspace(svc, workspace_id, outputs)
    first_path = next(iter(outputs))
    worker = run_auth(svc, run_id)
    try:
        published = svc.execute(
            "publish_artifact",
            worker,
            {
                "run_id": run_id,
                "fencing_epoch": worker.bound_fencing_epoch,
                "path": first_path,
            },
        )
    except Exception as exc:  # noqa: BLE001 — surfaced in the record, never silent
        return {"path": first_path, "published": False, "error": f"{type(exc).__name__}: {exc}"}
    if not published.ok:
        return {
            "path": first_path,
            "published": False,
            "error": published.error_code,
            "message": published.error_message,
        }
    return {
        "path": first_path,
        "published": True,
        "artifact_hash": published.data["artifact_hash"],
        "uri": published.data.get("uri"),
    }


def _assess_complex_success(
    svc,
    task_id: str,
    boot: dict[str, Any],
    record: dict[str, Any],
    *,
    require_artifacts: bool,
) -> dict[str, Any]:
    """Gate §24.5 success on the ACTIVE plan, real artifacts, and exit confirmation.

    Judgement is restricted to the Work Units of the *final* ACTIVE Plan (a revision
    may have superseded earlier nodes), requires an actual ``work_type=VERIFY`` unit
    to have passed, and refuses artifacts the Core never received bytes for — the
    M1 gate already did this, and dropping it let a synthetic digest pass as delivery.
    """
    _ = boot
    active_plan = svc.get_active_plan(task_id) or {}
    plan_nodes = [str(n.get("work_unit_id")) for n in active_plan.get("nodes") or []]
    all_units = {wu["work_unit_id"]: wu for wu in svc.list_work_units(task_id)}
    # A Repair revision keeps the original FAILed VERIFY in the plan as history (its
    # Execution row is terminal and must not revive), so it is excluded from the
    # completeness requirement — the *new* VERIFY is what has to pass.
    superseded = {str(record.get("injected_fail_work_unit") or "")} - {""}
    if plan_nodes:
        scoped = [
            all_units[wid] for wid in plan_nodes if wid in all_units and wid not in superseded
        ]
    else:
        # No ACTIVE plan (activation refused earlier): fall back to every unit so the
        # record still explains why the Task is incomplete.
        scoped = [
            wu
            for wu in all_units.values()
            if wu.get("status") != "CANCELLED" and wu["work_unit_id"] not in superseded
        ]

    def _passed(wu: dict[str, Any]) -> bool:
        return wu.get("status") == "DONE" and wu.get("selected_verdict") == "PASS"

    done = [wu["work_unit_id"] for wu in scoped if _passed(wu)]
    missing = [
        {
            "work_unit_id": wu.get("work_unit_id"),
            "work_type": wu.get("work_type"),
            "status": wu.get("status"),
            "verdict": wu.get("selected_verdict"),
        }
        for wu in scoped
        if not _passed(wu)
    ]

    # The complex sample must include a genuine VERIFY that reached PASS. An earlier
    # version accepted any DONE/PASS unit here, which let the edge be bypassed.
    verify_pass = [
        wu["work_unit_id"]
        for wu in scoped
        if str(wu.get("work_type") or "").upper() == "VERIFY" and _passed(wu)
    ]
    requires_verify = any(
        str(wu.get("work_type") or "").upper() == "VERIFY" for wu in scoped
    )

    artifacts = list(svc.list_task_artifacts(task_id) or [])
    unbacked = list(svc.list_unbacked_artifacts(task_id) or [])
    content_checks = {
        a["artifact_hash"]: svc.verify_artifact_content(task_id, a["artifact_hash"])
        for a in artifacts
        if a.get("artifact_uri")
    }
    unverified_content = [
        digest for digest, check in content_checks.items() if not check.get("verified")
    ]
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

    # G2's FAIL→REPAIR→new-VERIFY path is only meaningful when the injected FAIL
    # actually occurred. Two legitimate shapes exist:
    #   * injection fired  → the revision ran and the NEW verify must PASS;
    #   * the model verified PASS first time → no repair was needed, and the original
    #     verify is a valid PASS. (The path itself is proven by the Fake gate and by
    #     live runs that did trigger it; both are recorded in the evidence.)
    injection_fired = bool(record.get("inject_triggered"))
    requires_repair = bool(record.get("inject_required")) and injection_fired
    repair_ok = bool((record.get("repair") or {}).get("ok")) if requires_repair else True
    # A second VERIFY must exist after repair: reusing the FAILed unit's own result as
    # the new evidence is exactly the bypass H-038 forbids.
    if requires_repair and repair_ok:
        passed_verifies = [
            wid
            for wid, wu in all_units.items()
            if str(wu.get("work_type") or "").upper() == "VERIFY" and _passed(wu)
        ]
        repair_ok = bool(passed_verifies)
    # A task whose injected FAIL was deliberately superseded must still show a passing
    # VERIFY somewhere in the final plan.
    requires_verify = requires_verify and not requires_repair

    # Binding: the VERIFY PASS must rest on the INTEGRATE deliverable.
    # - A pinned plan (Fake) must name a digest that was actually delivered.
    # - An unpinned plan (live: no digest exists before the Run does) is proven by the
    #   Core's own binding: the INTEGRATE unit is DONE/PASS with a content-backed
    #   ``verified_artifact_hash``, and a VERIFY unit that depends on it is DONE/PASS.
    #   The VERIFY Run's own report digest is a different artifact and is not the pin.
    pinned = {
        str(e.get("artifact_hash"))
        for e in (active_plan.get("edges") or [])
        if str(e.get("predicate") or "").upper() == "VERDICT_PASS"
    } - {"None", ""}
    delivered = {str(h) for h in record.get("publish_hashes") or []}
    binding_ok = True
    binding_evidence: dict[str, Any] = {"pinned": sorted(pinned)}
    if pinned:
        binding_ok = bool(pinned & delivered)
        binding_evidence["delivered"] = sorted(delivered)
    else:
        integrate_units = [
            wu
            for wu in scoped
            if str(wu.get("work_type") or "").upper() == "INTEGRATE"
        ]
        integrate = integrate_units[0] if integrate_units else None
        integrate_hash = str((integrate or {}).get("verified_artifact_hash") or "")
        verify_units = [
            wu
            for wu in scoped
            if str(wu.get("work_type") or "").upper() == "VERIFY"
        ]
        content_backed = bool(integrate_hash) and (
            not integrate_hash or not artifacts or any(
                a["artifact_hash"] == integrate_hash and a.get("artifact_uri")
                for a in artifacts
            )
        )
        binding_ok = bool(
            integrate is not None
            and _passed(integrate)
            and content_backed
            and any(_passed(wu) for wu in verify_units)
        )
        binding_evidence.update(
            {
                "integrate_work_unit": (integrate or {}).get("work_unit_id"),
                "integrate_verified_artifact_hash": integrate_hash or None,
                "verify_work_units": [wu.get("work_unit_id") for wu in verify_units],
                "verify_pass_work_units": verify_pass,
                "integrate_content_backed": content_backed,
            }
        )
    failures: list[str] = []
    if missing:
        failures.append("incomplete_work_units")
    if not artifact_ok:
        failures.append("no_artifacts")
    if unbacked:
        failures.append("unbacked_artifacts")
    if unverified_content:
        failures.append("unverified_artifact_content")
    if unconfirmed:
        failures.append("unconfirmed_runs")
    if not done:
        failures.append("no_completed_work")
    if requires_verify and not verify_pass:
        failures.append("no_verify_pass")
    if requires_repair and record.get("inject_triggered") and not repair_ok:
        failures.append("repair_not_executed")
    if not binding_ok:
        failures.append("artifact_binding_mismatch")

    record["ok"] = not failures
    record["failures"] = failures
    record["terminal"] = "COMPLETED" if record["ok"] else "INCOMPLETE"
    record["plan_version"] = active_plan.get("plan_version") or record.get("plan_version")
    record["done_work_units"] = done
    record["missing_work_units"] = missing
    record["verify_pass_work_units"] = verify_pass
    record["artifact_count"] = len(artifacts)
    record["unbacked_artifacts"] = unbacked
    record["unverified_artifact_content"] = unverified_content
    record["artifact_binding_ok"] = binding_ok
    record["artifact_binding"] = binding_evidence
    record["unconfirmed_runs"] = [r.get("run_id") for r in unconfirmed]
    record["planned_nodes"] = len(scoped)
    return record


def _run_fake_complex(svc, auth: AuthContext, task_def: dict[str, Any]) -> dict[str, Any]:
    """Drive the fixed topology with Fake workers that publish real artifacts.

    Every Work Unit writes its ``expected_outputs`` into its Workspace and publishes
    the first one, so each digest is content-backed and ``verified_artifact_hash``
    binds. That is what lets the INTEGRATE→VERIFY ``VERDICT_PASS`` edge become ready,
    and what makes the injected VERIFY FAIL able to reach the REPAIR path.
    """
    from tests.helpers import submit_result_and_exit

    boot = _bootstrap_complex_task(svc, auth, task_def)
    task_id = boot["task_id"]
    nodes = boot["nodes"]
    edges = boot["edges"]
    _uid = boot["uid"]
    node_by_id = {n["work_unit_id"]: n for n in nodes}
    workspace_by_node = {
        n["work_unit_id"]: str(n.get("workspace_id") or f"ws_{n['work_unit_id']}")
        for n in nodes
    }

    record: dict[str, Any] = {
        "task_def": task_def["task_id"],
        "task_id": task_id,
        "plan_version": boot["plan_version"],
        "runs": [],
        "publish_hashes": [],
        "terminal": None,
        "ok": False,
        # G2 only counts when the FAIL→REPAIR→new-VERIFY path really ran.
        "inject_required": bool(task_def.get("inject_fail_repair")),
    }

    def _latest_delivery() -> str | None:
        """Digest of the most recent INTEGRATE deliverable, else any delivery."""
        integ = record.get("integrate_artifact_hash")
        if integ:
            return str(integ)
        hashes = [r["artifact_hash"] for r in record["runs"] if r.get("artifact_hash")]
        return hashes[-1] if hashes else None

    safety = 0
    inject = bool(task_def.get("inject_fail_repair"))
    injected = False
    repair_requested = False
    while safety < 30:
        safety += 1
        r = svc.execute("dispatch_ready_runs", auth, {"task_id": task_id})
        created = list(r.data.get("created_runs") or [])
        if not created:
            break
        svc.drain_outbox()
        for rid in created:
            run_row = next(x for x in svc.list_runs(task_id) if x["run_id"] == rid)
            wu_id = str(run_row.get("work_unit_id") or "")
            node = node_by_id.get(wu_id)
            if node is None:
                # A REPAIR revision may add Work Units the boot topology does not know.
                node = {"work_unit_id": wu_id, "work_type": "VERIFY"}
            work_type = str(node.get("work_type") or "EXECUTE").upper()
            workspace_id = workspace_by_node.get(wu_id) or f"ws_{wu_id}"

            published = _publish_fake_outputs(svc, rid, node, workspace_id)
            artifact_hash = published.get("artifact_hash")
            if artifact_hash:
                record["publish_hashes"].append(str(artifact_hash))
            if work_type == "INTEGRATE" and artifact_hash:
                record["integrate_artifact_hash"] = artifact_hash

            target = _latest_delivery()
            result: dict[str, Any] = {"outcome": "COMPLETED", "verdict": "PASS"}
            if artifact_hash:
                result["artifact_refs"] = [artifact_hash]
                result["verified_artifact_refs"] = [artifact_hash]
            if target:
                result["acceptance_evidence"] = [
                    {
                        "criterion_id": "c1",
                        "artifact_hash": target,
                        "verdict": "PASS",
                        "check": "fake_harness",
                    }
                ]
            # Inject exactly one VERIFY FAIL against the real INTEGRATE digest so the
            # FAIL→REPAIR→new-VERIFY path actually executes (G2).
            if inject and not injected and work_type == "VERIFY":
                result["verdict"] = "FAIL"
                result["acceptance_evidence"] = [
                    {
                        "criterion_id": "c1",
                        "artifact_hash": target or artifact_hash or "",
                        "verdict": "FAIL",
                        "check": "fake_harness_inject",
                    }
                ]
                injected = True
                record["injected_fail_work_unit"] = wu_id
                record["injected_fail_artifact"] = target

            submit_result_and_exit(svc, auth, rid, result=result)
            record["runs"].append(
                {
                    "run_id": rid,
                    "work_unit_id": wu_id,
                    "work_type": work_type,
                    "publish": published,
                    "artifact_hash": artifact_hash,
                    "result": result,
                }
            )

        # Once the injected FAIL has landed, request a Repair revision.
        if injected and not repair_requested:
            repair_requested = True
            failed_wu = str(record.get("injected_fail_work_unit") or "")
            repair = svc.execute(
                "request_repair_plan",
                auth,
                {
                    "task_id": task_id,
                    "failed_verify_work_unit_id": failed_wu,
                    "artifact_hash": record.get("injected_fail_artifact") or "",
                    "keep_nodes": [n for n in nodes if n["work_unit_id"] != failed_wu],
                    "keep_edges": [e for e in edges if e.get("to_work_unit_id") != failed_wu],
                },
            )
            record["repair"] = {
                "ok": repair.ok,
                "error": repair.error_code,
                "plan_version": repair.data.get("plan_version") if repair.ok else None,
            }
            if repair.ok:
                # The revision's VERIFY/REPAIR ids are generated by the Core, so learn
                # them from the new active plan rather than assuming the boot ids.
                plan = svc.get_active_plan(task_id) or {}
                record["repair"]["plan_version"] = plan.get("plan_version")
                for raw_node in plan.get("nodes") or []:
                    node = dict(raw_node)
                    wid = str(node.get("work_unit_id") or "")
                    if not wid:
                        continue
                    if wid not in node_by_id:
                        node.setdefault("work_type", "VERIFY")
                        node_by_id[wid] = node
                    workspace_by_node.setdefault(
                        wid, str(node.get("workspace_id") or f"ws_{wid}")
                    )
                # The new VERDICT_PASS edge is pinned to the INTEGRATE deliverable, so
                # the REPAIR unit has to republish that exact output for
                # ``verified_artifact_hash`` to bind and the new VERIFY to become ready.
                integrate_node = next(
                    (
                        n
                        for n in node_by_id.values()
                        if str(n.get("work_type") or "").upper() == "INTEGRATE"
                    ),
                    None,
                )
                repair_node = next(
                    (
                        n
                        for n in node_by_id.values()
                        if str(n.get("work_type") or "").upper() == "REPAIR"
                    ),
                    None,
                )
                if repair_node is not None and integrate_node is not None:
                    # Reuse the boot INTEGRATE node's identity for content purposes: the
                    # VERDICT_PASS pin is a digest of that node's deterministic output,
                    # and the objective text feeds the body, so the REPAIR unit must
                    # reproduce it byte-for-byte for the new VERIFY to become ready.
                    repair_node["expected_outputs"] = list(
                        integrate_node.get("expected_outputs") or []
                    )
                    repair_node["_upstream_paths"] = list(
                        integrate_node.get("_upstream_paths") or []
                    )
                    repair_node["objective"] = integrate_node.get("objective")
                    repair_node["_content_node_id"] = integrate_node.get("work_unit_id")
                    repair_node["_content_work_type"] = integrate_node.get("work_type")

    return _assess_complex_success(svc, task_id, boot, record, require_artifacts=False)


def _g4_invariants(svc, task_id: str, record: dict[str, Any]) -> dict[str, Any]:
    """G4 invariants for one repeat: no duplicate Work Units, no PASS bypass.

    ``dispatch_ready_runs`` is idempotent per Work Unit, so a second immediate
    dispatch must create nothing; and the gate must be reached with a real VERIFY
    rather than a substituted DONE/PASS unit.
    """
    runs = svc.list_runs(task_id)
    wu_run_counts: dict[str, int] = {}
    for run in runs:
        wid = str(run.get("work_unit_id") or "")
        if wid:
            wu_run_counts[wid] = wu_run_counts.get(wid, 0) + 1
    # A Work Unit may legitimately retry, but never in the same attempt window; the
    # strict invariant is that no Work Unit id appears twice in one plan revision.
    return {
        "duplicate_work_units": sorted(
            wid for wid, count in wu_run_counts.items() if count > 1
        ),
        "verify_pass_work_units": record.get("verify_pass_work_units") or [],
        "pass_gate_bypassed": not bool(record.get("verify_pass_work_units")),
        "ok": bool(record.get("ok")),
    }


def _run_g4_repeat(
    task_def: dict[str, Any],
    *,
    data_dir: Path,
    iterations: int,
) -> dict[str, Any]:
    """G4: run the same Fake complex scenario N times and check the invariants."""
    results: list[dict[str, Any]] = []
    for index in range(iterations):
        iter_dir = data_dir / f"g4-{index:02d}"
        if iter_dir.exists():
            shutil.rmtree(iter_dir)
        iter_dir.mkdir(parents=True, exist_ok=True)
        planner = FakePlannerAdapter()
        svc, ctx = bootstrap_core(iter_dir, agent=planner, fake_time=False)
        try:
            planner.bind_core(svc)
            record = _run_fake_complex(svc, _human(), task_def)
            invariants = _g4_invariants(svc, record["task_id"], record)
            invariants["index"] = index
            invariants["task_id"] = record["task_id"]
            invariants["repair_executed"] = bool((record.get("repair") or {}).get("ok"))
            invariants["artifact_count"] = record.get("artifact_count")
            results.append(invariants)
        finally:
            ctx["lock"].release()

    duplicates = [r for r in results if r["duplicate_work_units"]]
    bypasses = [r for r in results if r["pass_gate_bypassed"]]
    failed = [r for r in results if not r["ok"]]
    return {
        "iterations": iterations,
        "ok": not duplicates and not bypasses and not failed,
        "passed": sum(1 for r in results if r["ok"]),
        "duplicate_runs": duplicates,
        "pass_gate_bypasses": bypasses,
        "failed_iterations": [r["index"] for r in failed],
        "repair_iterations": sum(1 for r in results if r["repair_executed"]),
        "results": results,
    }


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
    """Same topology as Fake, but EXECUTE Runs are completed by the real ApiAgentAdapter.

    Nothing is synthesized here: the delivery digest comes from what the model's Run
    actually published (``artifact_refs`` / the Work Unit's ``verified_artifact_hash``).
    A Repair revision is pinned to that same real digest, so the re-verify binds to
    evidence rather than to a placeholder.
    """
    boot = _bootstrap_complex_task(svc, auth, task_def, live=True)
    task_id = boot["task_id"]
    nodes = boot["nodes"]
    edges = boot["edges"]
    _uid = boot["uid"]
    # A Repair revision adds Work Units the boot topology does not know; they carry no
    # declared outputs, so seeding them with the standard files is enough to make the
    # Workspace usable.
    node_by_id = {str(n["work_unit_id"]): n for n in nodes}
    record: dict[str, Any] = {
        "task_def": task_def["task_id"],
        "task_id": task_id,
        "plan_version": boot["plan_version"],
        "workspaces": boot.get("workspaces"),
        "runs": [],
        "publish_hashes": [],
        "terminal": None,
        "ok": False,
        # G2 only counts when the FAIL→REPAIR→new-VERIFY path really ran.
        "inject_required": bool(task_def.get("inject_fail_repair")),
    }

    def _delivered_hash(result: dict[str, Any] | None, work_unit_id: Any) -> str | None:
        """The digest this Run actually delivered, preferring Core-confirmed evidence."""
        for ref in (result or {}).get("verified_artifact_refs") or []:
            digest = ref if isinstance(ref, str) else (ref or {}).get("hash")
            if digest:
                return str(digest)
        for ref in (result or {}).get("artifact_refs") or []:
            digest = ref if isinstance(ref, str) else (ref or {}).get("hash")
            if digest:
                return str(digest)
        if work_unit_id:
            unit = next(
                (
                    wu
                    for wu in svc.list_work_units(task_id)
                    if wu["work_unit_id"] == work_unit_id
                ),
                None,
            )
            if unit and unit.get("verified_artifact_hash"):
                return str(unit["verified_artifact_hash"])
        return None

    def _work_type_of(work_unit_id: str) -> str:
        node = next((n for n in nodes if n["work_unit_id"] == work_unit_id), None)
        if node is not None:
            return str(node.get("work_type") or "EXECUTE").upper()
        unit = next(
            (wu for wu in svc.list_work_units(task_id) if wu["work_unit_id"] == work_unit_id),
            None,
        )
        return str((unit or {}).get("work_type") or "EXECUTE").upper()

    def _note_inject(triggered: bool, detail: str = "") -> None:
        """Record honestly whether the injected FAIL prerequisite actually occurred."""
        record["inject_triggered"] = triggered
        if not triggered:
            record.setdefault("notes", []).append(
                "inject_fail_repair requested but the model returned PASS for VERIFY; "
                "the FAIL→REPAIR path was not exercised in this run"
                + (f" ({detail})" if detail else "")
            )

    safety = 0
    inject = bool(task_def.get("inject_fail_repair"))
    injected = False
    seeded: set[str] = set(boot.get("seeded") or [])
    seed_files: dict[str, str] = boot.get("seed_files") or {}
    delivered_keys: set[tuple[str, str, str]] = set()
    while safety < 20:
        safety += 1
        # A Repair revision introduces new Work Units whose Workspaces the boot pass
        # never provisioned. Live observed: the sandbox created them root-owned and
        # empty, so the REPAIR Run reported `workspace_missing` and had nothing to fix.
        # The active plan is the authority on which units exist now.
        for plan_node in (svc.get_active_plan(task_id) or {}).get("nodes") or []:
            wid = str(plan_node.get("work_unit_id") or "")
            if wid and wid not in node_by_id:
                node_by_id[wid] = dict(plan_node)
        _materialize_workspaces(
            svc, list(node_by_id.values()), seed_files, seeded=seeded
        )
        # Stage every finished prerequisite's deliverable into its dependents before
        # dispatching, so an INTEGRATE/VERIFY unit can actually read the artifacts its
        # objective names.
        deliveries = _deliver_dependency_artifacts(
            svc, task_id, nodes, delivered=delivered_keys
        )
        if deliveries:
            record.setdefault("dependency_deliveries", []).extend(deliveries)
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
            wu_id = row.get("work_unit_id")
            work_type = _work_type_of(str(wu_id or ""))
            delivered = _delivered_hash(result, wu_id)
            if delivered:
                record["publish_hashes"].append(delivered)
            if work_type == "INTEGRATE" and delivered:
                record["integrate_artifact_hash"] = delivered
            record["runs"].append(
                {
                    "run_id": row.get("run_id"),
                    "status": row.get("status"),
                    "result": result,
                    "work_unit_id": wu_id,
                    "work_type": work_type,
                    "artifact_hash": delivered,
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
                _note_inject(True)
                fail_wu = str(wu_id or _uid("wu_verify"))
                # Pin the revision to the real digest under verification.
                upstream = delivered or record.get("integrate_artifact_hash")
                payload: dict[str, Any] = {
                    "task_id": task_id,
                    "failed_verify_work_unit_id": fail_wu,
                    "keep_nodes": [n for n in nodes if n["work_unit_id"] != fail_wu],
                    "keep_edges": [
                        e for e in edges if e.get("to_work_unit_id") != fail_wu
                    ],
                }
                if upstream:
                    payload["artifact_hash"] = str(upstream)
                repair = svc.execute("request_repair_plan", auth, payload)
                record["repair"] = {
                    "ok": repair.ok,
                    "error": repair.error_code,
                    "plan_version": repair.data.get("plan_version") if repair.ok else None,
                    "pinned_artifact_hash": upstream,
                }
                injected = True
                inject = False
                if repair.ok:
                    plan = svc.get_active_plan(task_id) or {}
                    record["repair"]["plan_version"] = plan.get("plan_version")

    # Reconcile the integrate digest from the Core-bound Work Unit rather than the
    # per-Run value: a later run (VERIFY) that publishes nothing would otherwise clobber
    # a field the binding check reads.
    for unit in svc.list_work_units(task_id):
        if (
            str(unit.get("work_type") or "").upper() == "INTEGRATE"
            and unit.get("verified_artifact_hash")
        ):
            record["integrate_artifact_hash"] = str(unit["verified_artifact_hash"])
            break

    if record.get("inject_required") and not injected:
        _note_inject(False, "all VERIFY Runs reported PASS")

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
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="G4: run the same Fake complex scenario N times and check invariants",
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

    # Without this the Fake Planner never drains the Task message log: its
    # ``_maybe_consume_messages`` guard returns early and the checkpoint cursor
    # silently stays at 0, so reviewer-visible recovery would look wired while
    # consuming nothing.
    if hasattr(planner, "bind_core"):
        planner.bind_core(svc)

    auth = _human()
    tasks = _load_tasks(args.tasks)

    # G4: repeatability across N independent runs of the same scenario.
    g4: dict[str, Any] | None = None
    if args.repeat and args.repeat > 1:
        g4_task = next(
            (t for t in tasks if t.get("inject_fail_repair")),
            tasks[0] if tasks else None,
        )
        if g4_task is not None:
            g4 = _run_g4_repeat(
                g4_task, data_dir=args.data_dir / "g4", iterations=args.repeat
            )
            (args.out / "g4-summary.json").write_text(
                json.dumps(g4, indent=2, default=str), encoding="utf-8"
            )
            print(
                f"[{'PASS' if g4['ok'] else 'FAIL'}] G4 x{g4['iterations']}"
                f" passed={g4['passed']} repair_iterations={g4['repair_iterations']}"
                f" duplicate_runs={len(g4['duplicate_runs'])}"
                f" gate_bypasses={len(g4['pass_gate_bypasses'])}"
            )
        ctx["lock"].release()
        return 0 if (g4 and g4["ok"]) else 1

    records = []
    try:
        for t in tasks:
            rec = run_fn(svc, auth, t)
            rec["mode"] = "live" if live else "dry-run"
            path = args.out / f"{t['task_id']}.json"
            path.write_text(json.dumps(rec, indent=2, default=str), encoding="utf-8")
            records.append(rec)
            label = "PASS" if rec.get("ok") else "FAIL"
            detail = "" if rec.get("ok") else f" failures={rec.get('failures')}"
            print(
                f"[{label}] {t['task_id']} runs={len(rec.get('runs') or [])}"
                f" artifacts={rec.get('artifact_count')}{detail} -> {path}"
            )
    finally:
        ctx["lock"].release()

    passed = sum(1 for r in records if r.get("ok"))
    summary = {
        "mode": "live" if live else "dry-run",
        "model": model_name,
        "base_url": base_url,
        "tasks": len(records),
        # Gate result, not "a Run row exists" — the previous accounting reported
        # completed=3 while every Task was INCOMPLETE.
        "completed": passed,
        "failed": len(records) - passed,
        "ok": passed == len(records) and bool(records),
        "at": datetime.now(UTC).isoformat(),
        "records": [
            {
                "task_def": r["task_def"],
                "ok": bool(r.get("ok")),
                "plan_version": r.get("plan_version"),
                "failures": r.get("failures") or [],
                "artifact_count": r.get("artifact_count"),
                "unbacked_artifacts": r.get("unbacked_artifacts") or [],
            }
            for r in records
        ],
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    # A gate that cannot fail the build is not a gate.
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
