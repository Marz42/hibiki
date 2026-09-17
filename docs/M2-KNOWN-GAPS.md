# M2 known gaps (recorded 2026-09-17)

M2 is accepted for §24.5 G1–G6: Fake G1–G5 pass (evidence
`docs/acceptance/evidence/m2-2026-09-17-remediation/`), and the live gate completed
**3/3** complex tasks on `deepseek-flash` through the Docker sandbox (evidence
`docs/acceptance/evidence/m2-2026-09-17-live/`).

This file records what M2 **does not** do, so the gaps are not mistaken for solved work.
Each entry states the observation, the evidence, and whether it blocks M3.

## 1. Live REPAIR has no operator/task context — **open, does not block M3**

`Core.request_repair_plan` builds the revision as `repair after <verify_work_unit_id>`
(`src/hibiki/application/service.py`, `_request_repair_plan`). The new REPAIR unit
therefore:

- has an objective that does not say *what* failed or *what* to change, and
- receives no dependency edge, so nothing is staged into its Workspace.

Observed live: the REPAIR Run reported `blocked_by_model`; before the Workspace fix it
reported `workspace_missing` because the sandbox had created its (unseeded, root-owned)
directory. After seeding revision Workspaces the REPAIR unit does run and can publish
(e.g. `repair completed ... recreated the integrated deliverable`), but it is guessing at
the intended fix, not repairing a described defect.

The *revision mechanism* is proven: a real VERIFY FAIL produced Plan **v2** with a new
REPAIR + VERIFY, and the revision's `VERDICT_PASS` edge was pinned to the real digest
under verification (`docs/acceptance/evidence/m2-2026-09-17-live/m2-c2.json` in the runs
where the injected FAIL fired). The *live repair workflow* is what is missing.

Not a gate failure: §24.5 G2 requires a FAIL→REPAIR path, which the Fake gate exercises in
20/20 runs, and the live run demonstrates the revision is built from real evidence.

## 2. Live `VERDICT_PASS` edges cannot name an artifact hash — **open, does not block M3**

SPEC §6.1 requires a `VERDICT_PASS` edge to name the Artifact hash it verifies, and
`validate_dag` enforces it by default. A live Planner cannot satisfy that: no Run has
published bytes when the plan is proposed.

Current behaviour: the harness passes `require_verdict_artifact_hash=False` when proposing
a live topology; the Core then checks only `DONE` + `selected_verdict == PASS`. The gate
compensates by asserting the real binding itself — the INTEGRATE unit is `DONE`/`PASS`
with a content-backed `verified_artifact_hash` and a depending VERIFY unit is `DONE`/`PASS`
(`_assess_complex_success`).

What is lost: a live plan cannot rely on the Core's automatic hash-equality check, which is
what invalidates stale verify evidence when an upstream artifact changes. Any other live
consumer using `require_verdict_artifact_hash=False` gets the weaker predicate. A proper
fix needs a deferred/placeholder binding in the plan model, which is a design change, not
a harness tweak.

## 3. Planner recovery is not wired end-to-end on the live path — **open, does not block M3**

`FakePlannerAdapter.bind_core` + `_maybe_consume_messages` drains the Task message log into
the checkpoint cursor, and the M2 regression suite asserts it
(`test_fake_planner_consumes_task_messages_on_recovery`). In live mode the PLAN Run is
still driven by `FakePlannerAdapter` while EXECUTE runs use `ApiAgentAdapter`
(`m2_runner` builds `AssignmentRoutingAdapter(planner=FakePlannerAdapter(), ...)`), so live
G6 proves the *orchestration* path, not a real model resuming from a checkpoint.

Consequence: "Planner resumes from checkpoint + message cursor after a crash" is proven
only against the Fake planner. The SPEC §9.1 recovery semantics are implemented and
unit-tested; a live model acting as the Planner is M3 scope.

## 4. The orchestrator shell is unprivileged and `fs.write` cannot create directories — **environment property, documented**

- `shell.run` executes in the sandbox as `uid 65534:65534` (`src/hibiki/tools/sandbox.py`,
  `_CONTAINER_USER`); rootless Docker is unavailable on this host (no `uidmap`), so the
  container cannot escalate. The Workspace is therefore read-only to shell commands.
- On the Unix `dir_fd` path, `WorkspacePaths.atomic_write` walks with `create=False`
  (`src/hibiki/tools/paths.py`), so `fs.write` refuses a path whose parent does not exist.

Together these mean a nested deliverable (`edits/a.md`) is unreachable unless something
pre-creates the parent. The harness now pre-creates them from each node's
`expected_outputs` and states the boundary in the workspace README. Both behaviours are
deliberate sandbox hardening, not bugs, but they constrain task design: an objective may
not rely on the model creating a directory.

## 5. G6 evidence is one passing run — **recorded honestly**

Live runs vary. Across this session the same three tasks produced different failure modes
(`max_turns_exhausted`, `blocked_by_model`, `missing_acceptance_evidence`, a runner
timeout) before the harness fixes landed. The archived pack is the final run, where all
three tasks completed with `unbacked_artifacts == []` and `artifact_binding_ok == true`.
The raw log and `environment.txt` are stored with it. No claim is made that 3/3 is stable
across repeated runs; the Fake gate (×20) carries the repeatability requirement.

In the archived run the injected FAIL did not fire (the model verified PASS first time),
so `inject_triggered` is false and no repair revision was built. That path is evidenced by
earlier runs in the same session and by the Fake gate.

## 6. Carried from M0/M1 — **unchanged**

- In-process Core until M3; Worker↔Core stays in-process, REST/MCP is M3.
- Orchestrator remains Docker-group / host-root-equivalent; the Broker must not expose the
  socket.
- External business actions remain Fake.
- Windows uses path-mode `WorkspacePaths` (no directory FDs); Unix retains the dir_fd walk.
- `pending:<run_id>:<ns>` sandbox identity is not a Docker-queryable container id, so a
  crash between the pre-execution claim and a confirmed exit is settled by `reconcile`
  rather than by inspecting a real container.
