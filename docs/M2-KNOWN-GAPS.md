# M2 known gaps (recorded 2026-09-17, updated after the third review)

M2 is accepted for §24.5 G1–G6: Fake G1–G5 pass (evidence
`docs/acceptance/evidence/m2-2026-09-17-remediation/`), and the live gate completed
**3/3** complex tasks on `deepseek-flash` through the Docker sandbox (evidence
`docs/acceptance/evidence/m2-2026-09-17-live/`).

This file records what M2 **does not** do, so the gaps are not mistaken for solved work.
Each entry states the observation, the evidence, and whether it blocks M3.

## 0. Third review — two of three P1s fixed, one still open

Fixed in this round (regression-covered in `tests/m2/test_second_review_regressions.py`):

- **Premature Workspace release.** `register_sandbox_identity(exit_confirmed=True)`
  cleared the Workspace owner while the owning Run was still RUNNING, so two Runs could
  write one Workspace. A confirmed *container* exit now clears only that container's
  uncertainty; occupancy is released by `_clear_run_occupancy` when the Run reaches a
  terminal state. The dispatcher additionally refuses a second writer while the owner Run
  is still live, so the single-writer rule does not depend on flags staying consistent.
- **`VERDICT_PASS` hash opt-out.** `require_verdict_artifact_hash=False` let a plan the
  Core must reject be submitted, which breaks SPEC §6.1 — a Core constraint a caller can
  switch off is not a constraint. The parameter is removed from `validate_dag` and the
  opt-out is inert. The live harness now runs the producing Work Units first and adds the
  VERIFY stage as a **Plan Revision** pinned to the real digest (plan v2 on all three live
  tasks, verified end-to-end).

Still open:

- **A content-changing REPAIR cannot reach re-verification** — §2 below.

## 1. Live REPAIR has no operator/task context — **partly addressed, still open**

`request_repair_plan` now accepts `failed_evidence`, which becomes the REPAIR unit's
objective (previously `repair after <verify_work_unit_id>` with nothing actionable), and it
can defer the re-verify stage with `include_verify=False` so the Core never creates a
`VERDICT_PASS` edge against an unknown hash.

Remaining: REPAIR still receives no dependency edge, so its Workspace has no staged input
unless the harness delivers one, and the round-1 fake flow still has the REPAIR unit
re-emit the pinned bytes (see §2).

Not a gate failure: §24.5 G2 requires a FAIL→REPAIR path, which the Fake gate exercises, and
the live run demonstrates the revision is built from real evidence.

## 2. A content-changing REPAIR still cannot reach re-verification — **OPEN, not a gate failure**

Third-review P1 (partly fixed). `request_repair_plan` pins the new VERIFY edge to the
**pre-repair** digest:

```python
edges.append({
    "from_work_unit_id": repair_id,
    "to_work_unit_id": verify_id,
    "predicate": "VERDICT_PASS",
    "artifact_hash": upstream_hash,   # the artifact that FAILED
})
```

`_deps_satisfied` then compares that pin with the REPAIR unit's own
`verified_artifact_hash`. A Repair that changes content publishes a different digest, so the
dependency never matches and the re-verify stays `PENDING`. The Fake harness hides this by
having the REPAIR unit re-emit the pinned bytes (see `_run_fake_complex`), which proves the
revision plumbing but **not** H-038's intent.

What is in place:

- `request_repair_plan` accepts `failed_evidence`, which becomes the REPAIR objective;
- `request_repair_plan` accepts `include_verify=False`, so no `VERDICT_PASS` edge is created
  against an unknown hash;
- the Plan-Revision machinery needed to add the re-verify stage afterwards exists and is
  proven by the live VERIFY revision (`_propose_verification_revision`).

What is missing: driving that machinery for the Repair case, and delivering the failing
evidence plus the failing artifact into the REPAIR Workspace. Two harness attempts were made
and both produced a strictly worse state (dangling edges, a reused terminal VERIFY id), so
the round-1 flow was restored rather than left half-migrated. Doing this properly needs the
re-verify revision to be built from the Repair unit's published digest once it exists —
the same staged pattern the live VERIFY path already uses.

Not a §24.5 gate failure: G2 requires a FAIL→REPAIR path, which is exercised; it does not
require the repairing artifact to differ in content.

## 3. Live `VERDICT_PASS` edges need a second Plan Revision — **by design, not a gap**

SPEC §6.1 requires a `VERDICT_PASS` edge to name the Artifact hash it verifies, and no live
Run has published bytes when the topology is proposed. The live flow therefore activates
the producing Work Units first and adds VERIFY by a Plan Revision pinned to the digest that
actually exists. The Core rule is enforced unconditionally throughout; nothing opts out.

Cost: one extra PLAN Run and one extra plan version (`max_plan_revisions = 10`), and the
record carries `verify_revision` for audit.

## 4. Planner recovery is not wired end-to-end on the live path — **open, does not block M3**

`FakePlannerAdapter.bind_core` + `_maybe_consume_messages` drains the Task message log into
the checkpoint cursor, and the M2 regression suite asserts it
(`test_fake_planner_consumes_task_messages_on_recovery`). In live mode the PLAN Run is
still driven by `FakePlannerAdapter` while EXECUTE runs use `ApiAgentAdapter`
(`m2_runner` builds `AssignmentRoutingAdapter(planner=FakePlannerAdapter(), ...)`), so live
G6 proves the *orchestration* path, not a real model resuming from a checkpoint.

Consequence: "Planner resumes from checkpoint + message cursor after a crash" is proven
only against the Fake planner. The SPEC §9.1 recovery semantics are implemented and
unit-tested; a live model acting as the Planner is M3 scope.

## 5. The orchestrator shell is unprivileged and `fs.write` cannot create directories — **environment property, documented**

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

## 6. G6 evidence is one passing run — **recorded honestly**

Live runs vary. Across this session the same three tasks produced different failure modes
(`max_turns_exhausted`, `blocked_by_model`, `missing_acceptance_evidence`, a runner
timeout) before the harness fixes landed. The archived pack is the final run, where all
three tasks completed with `unbacked_artifacts == []` and `artifact_binding_ok == true`.
The raw log and `environment.txt` are stored with it. No claim is made that 3/3 is stable
across repeated runs; the Fake gate (×20) carries the repeatability requirement.

In the archived run the injected FAIL did not fire (the model verified PASS first time),
so `inject_triggered` is false and no repair revision was built. That path is evidenced by
earlier runs in the same session and by the Fake gate.

## 7. Carried from M0/M1 — **unchanged**

- In-process Core until M3; Worker↔Core stays in-process, REST/MCP is M3.
- Orchestrator remains Docker-group / host-root-equivalent; the Broker must not expose the
  socket.
- External business actions remain Fake.
- Windows uses path-mode `WorkspacePaths` (no directory FDs); Unix retains the dir_fd walk.
- `pending:<run_id>:<ns>` sandbox identity is not a Docker-queryable container id, so a
  crash between the pre-execution claim and a confirmed exit is settled by `reconcile`
  rather than by inspecting a real container.
