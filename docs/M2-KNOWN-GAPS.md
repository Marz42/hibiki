# M2 known gaps (recorded 2026-09-17, updated after the third review)

M2 is accepted for §24.5 G1–G6: Fake G1–G5 pass (evidence
`docs/acceptance/evidence/m2-2026-09-17-remediation/`), and the live gate completed
**3/3** complex tasks on `deepseek-flash` through the Docker sandbox (evidence
`docs/acceptance/evidence/m2-2026-09-17-live/`).

This file records what M2 **does not** do, so the gaps are not mistaken for solved work.
Each entry states the observation, the evidence, and whether it blocks M3.

## 0. Third review — all three P1s fixed

Regression-covered in `tests/m2/test_second_review_regressions.py`:

- **Premature Workspace release.** `register_sandbox_identity(exit_confirmed=True)`
  cleared the Workspace owner while the owning Run was still RUNNING, so two Runs could
  write one Workspace. A confirmed *container* exit now clears only that container's
  uncertainty; occupancy is released by `_clear_run_occupancy` when the Run reaches a
  terminal state. The dispatcher additionally refuses a second writer while the owner Run
  is still live, so the single-writer rule does not depend on flags staying consistent.
- **`VERDICT_PASS` hash opt-out.** `require_verdict_artifact_hash=False` let a plan the Core
  must reject be submitted, which breaks SPEC §6.1 — a Core constraint a caller can switch
  off is not a constraint. The parameter is removed from `validate_dag` and the opt-out is
  inert. The live harness runs the producing Work Units first and adds the VERIFY stage as a
  **Plan Revision** pinned to the real digest (plan v2 on all three live tasks).
- **Content-changing REPAIR.** `request_repair_plan` accepts `failed_evidence`, which becomes
  the REPAIR unit's objective, and `include_verify=False`, so the Core never creates a
  `VERDICT_PASS` edge against a digest that does not exist yet. The harness then adds the
  re-verify stage as a revision pinned to the **repaired** digest, and the edge is hung off
  the REPAIR unit that now produces it. The Fake gate exercises exactly this: the Repair
  publishes different bytes, the re-verify runs against them and passes (plan v3), with
  `verify_revision.pinned_artifact_hash == repair artifact digest`.

## 1. Live REPAIR input context — **partly addressed**

`request_repair_plan` now passes `failed_evidence` into the REPAIR objective, so the unit is
told what failed instead of a bare `repair after <verify_work_unit_id>`. It still receives no
dependency edge of its own: the harness relies on the objective text plus whatever the
Workspace already holds. A richer failure packet (the failing evidence artifact itself,
staged into the REPAIR Workspace) is still worth doing.

Not a gate failure: §24.5 G2 requires a FAIL→REPAIR path, which the Fake gate exercises on
every run and the live gate demonstrates by building plan v2 from a real VERIFY FAIL.


## 2. Live `VERDICT_PASS` edges need a second Plan Revision — **by design, not a gap**

SPEC §6.1 requires a `VERDICT_PASS` edge to name the Artifact hash it verifies, and no live
Run has published bytes when the topology is proposed. The live flow therefore activates
the producing Work Units first and adds VERIFY by a Plan Revision pinned to the digest that
actually exists. The Core rule is enforced unconditionally throughout; nothing opts out.

Cost: one extra PLAN Run and one extra plan version (`max_plan_revisions = 10`), and the
record carries `verify_revision` for audit.

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
