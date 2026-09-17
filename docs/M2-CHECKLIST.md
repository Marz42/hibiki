# M2 Execution Checklist — Planner / DAG / Integration (§24.5)

Status: **PENDING** (adversarial review of `1eb2477`; G6 “3/3” withdrawn — not frozen for M3)
Spec: `HIBIKI_MVP_SPEC_v0.2.1.md` §24.5, with §5–9 / §11 / §12.3 / §16–17 / §20 / §27
Prior milestones: M0 PASS (`docs/acceptance/M0-2026-09-13.md`), M1 PASS (`docs/acceptance/M1-2026-09-13.md`)
Design rules: `docs/adr/0001-tech-stack-m0.md`, `docs/adr/0002-known-limits-m0.md` (in-process Core carried forward)

## 0. Gate definition (copy of §24.5)

Scope: PlannerSession, public PLAN Run, checkpoints, Plan Revision / Contract Delta,
parallel scheduling, Integration, quality dependencies and rework.

Must complete:

| # | Requirement |
| --- | --- |
| G1 | H-033–H-038 all pass; H-011, H-013–H-016 re-accepted under a full multi-Agent Runtime |
| G2 | Fixed complex sample with at least two parallel Work Units, one INTEGRATE, one VERIFY, and one injected FAIL→REPAIR→re-VERIFY path |
| G3 | Barrier proves two conflict-free Workers actually overlap in their execution intervals; same-Workspace writers stay serial. Log-time proximity alone is not proof |
| G4 | Same Fake complex scenario run 20 times with out-of-order results and duplicate messages: zero duplicate Work Units, zero reuse of invalidated results, zero PASS-Gate bypass |
| G5 | Kill the Planner before/after Plan Proposal and before/after message consume; recovery must not drop Worker Results; old-generation proposals must not take effect |
| G6 | Three pre-fixed real complex tasks; at least two of three complete inside budget; collaboration and failure paths of all three are reconstructible |
| Gate | Complex Tasks can close the loop; no multi-Planner debate and no Worker-to-Worker chat |

## 1. Task list

Each task states: what to build, the SPEC clauses, the acceptance IDs, and the verification
evidence. Tasks A–E are Planner / schedule infrastructure; F–J are quality and revision
semantics; K–L are the gate runs.

### Task A — PlannerSession + public PLAN Run

- **Build**: Session lifecycle `ACTIVE / SUSPENDED / CLOSED`; at most one ACTIVE
  PlannerSession per Task; at most one valid PLAN Run per session; write and enforce
  `ActivePlanRunMarker`. Extend dispatch so `AssignmentKind.PLAN` creates an `AgentRun`
  bound to the session, enqueues `agent.start`, and clears the marker on exit. Add
  `FakePlannerAdapter` (same `AgentAdapter` surface) that can drive
  `submit_plan_proposal` with the session generation.
- **Spec**: §5, §9.1, §16.1.
- **Verify**: PLAN run creation + marker exclusivity; generation bump still rejects stale
  proposals (H-011 foundation); occupancy cleared on stop/result.

### Task B — Plan DAG validate / reject without destroying the old plan

- **Build**: Cycle, dangling edge, and cross-Task node proposals are refused; the prior
  ACTIVE Plan, Work Units, and markers remain intact (H-033). Enforce
  `max_plan_revisions=10` and `max_work_units_per_task=30`.
- **Spec**: §6.1, §27.
- **Verify**: Dedicated H-033 suite (not only `validate_dag` unit tests).

### Task C — Core-only spawn (H-034)

- **Build**: Planner and Worker cannot spawn peers or dispatch work directly. Any forged
  spawn / cross-Actor dispatch is refused and audited. Physical spawn depth stays 1;
  Message Store is the only Planner↔Worker path.
- **Spec**: §12.3; INV-04.
- **Verify**: H-034 refusal cases with audit rows.

### Task D — Dependency-ready parallel schedule + writer serial (H-035 / G3)

- **Build**: Ready independent Work Units dispatch concurrently up to per-Task /
  global caps; a live writer on a Workspace blocks a second writer. Barrier harness:
  Fake Workers pause after entering the execution interval so the test can assert two
  Runs are simultaneously `RUNNING`, then release.
- **Spec**: §11.1, §16.1–16.2.
- **Verify**: H-035 barrier proof; same-workspace serial negative control.

### Task E — Checkpoints and message cursors (H-036 / G5)

- **Build**: `checkpoint_ref`, `checkpoint_version`, and `last_consumed_message_seq`
  update in the **same transaction**. Messages may redeliver; proposals stay idempotent.
  Planner may end a PLAN Run while waiting for Workers; a new PLAN Run resumes from
  checkpoint + messages. Crash injection around proposal and message consume.
- **Spec**: §9.1.
- **Verify**: H-036 recovery cases; G5 kill points.

### Task F — VERIFY semantics and quality deps

- **Build**: `work_type=VERIFY` may finish Run `SUCCEEDED` / WU `DONE` with
  `verdict=FAIL`. Dependents that require `VERDICT_PASS` stay blocked. Emit Task
  `VERIFYING` / `verification.failed` where the transition table already allows it.
- **Spec**: §7, §8.
- **Verify**: H-013 re-acceptance under multi-Agent Runtime.

### Task G — INTEGRATE + conflict → Repair (H-037)

- **Build**: Parallel branches keep separate workspaces; a dedicated INTEGRATE Work Unit
  binds fixed input commit/hash. Conflicts do not silently overwrite — Integration FAIL
  or Repair; original Artifacts are retained.
- **Spec**: §11.1.
- **Verify**: H-037 conflict retention.

### Task H — Plan Revision vs Contract Delta

- **Build**: Authorization / acceptance changes require Contract Delta + Human (H-014);
  other topology edits are PLAN_REVISION. CAS on `expected_plan_version` +
  `expected_contract_version`; drain old PLAN/EXECUTE before applying a Delta.
- **Spec**: §6.2–6.3.
- **Verify**: H-014 re-acceptance under multi-Agent Runtime.

### Task I — Result reuse and invalidation (H-015 / H-016)

- **Build**: Unrelated plan bumps keep DONE results whose assignment is unchanged.
  Upstream Artifact hash changes invalidate Integration/Verify evidence along the
  dependency graph; a fresh Verify is required.
- **Spec**: §6.3–6.4.
- **Verify**: H-015 / H-016 re-acceptance under multi-Agent Runtime.

### Task J — FAIL → Repair → new Verify → acceptance (H-038 / G2)

- **Build**: Verify `verdict=FAIL` is not a network retry of the same Verify. Create
  Repair / Plan Revision (new WU + new Plan). New PASS binds the new hash; old
  PASS/FAIL must not substitute for new evidence.
- **Spec**: §17, §20.1.
- **Verify**: H-038; feeds the G2 complex sample path.

### Task K — Fixed complex Fake sample + ×20 (G2 / G4)

- **Build**: `docs/m2/tasks/` complex topology: two parallel EXECUTE → INTEGRATE →
  VERIFY, plus a FAIL→REPAIR→re-VERIFY path. Harness runs the same Fake scenario 20
  times with out-of-order results and duplicate messages. Re-run H-011 / H-013–H-016
  through Planner + Worker Fake adapters (not Core-command-only).
- **Gate**: G2 topology present; G4 invariants hold across 20 runs.
- **Verify**: junit + summary under `docs/acceptance/evidence/m2-<date>/`.

### Task L — Three live complex tasks + acceptance freeze (G6)

- **Build**: Three pre-fixed real complex tasks (real model + M1 sandbox); ≥2/3 inside
  budget; full collaboration/failure reconstructible. Publish
  `docs/acceptance/M2-<date>.md` + evidence; update README Acceptance status.
- **Gate**: G1–G6 reviewed together; **M3 only after this record passes**.

## 2. Dependency order

```
A ─┬─ B ─┬─ D ─┬─ F ─┬─ G ─┬─ J ─ K ─ L
   │     │     │     │     │
   ├─ C ─┘     │     ├─ H ─ I ─┘
   │           │
   └─ E ───────┘
```

Critical path: `A → B/C → D` and `A → E` join at `F → (G ∥ H→I) → J → K → L`.

Parallel windows: B∥C after A; G∥(H→I) after F; E may proceed beside D after A, but both
must land before K.

## 2.5 M0/M1 seams to build on

| Seam | Location | M2 gap |
| --- | --- | --- |
| Plan DAG validate/activate | `domain/plan.py`, `_activate_plan` | Refuse without destroying ACTIVE plan; enforce revision/WU caps |
| EXECUTE dispatch | `_dispatch_ready_runs` | Add PLAN branch; barrier-proven parallel EXECUTE |
| Planner generation / proposal | `replace_planner_generation`, `submit_plan_proposal` | Bind to PLAN AgentRun + marker; checkpoint/cursor |
| `ActivePlanRunMarker` | `persistence/models.py` | Never written today — enforce exclusivity |
| `AssignmentKind.PLAN` | `domain/enums.py` | Unused in service |
| VERDICT_PASS deps | `_deps_satisfied` | Wire VERIFYING transitions; Repair path |
| Contract Delta | `apply_contract_delta` | Drain PLAN+EXECUTE; multi-agent re-accept |
| Fake / API adapters | `runtime/fake_agent.py`, `api_agent.py` | FakePlannerAdapter; barrier subclass |
| Message Store | — | Minimal task message log for cursor (new table or event-backed watermark) |
| Concurrency defaults | `domain/defaults.py` | Already count all Runs; prove with barrier |
| Caps | `max_plan_revisions`, `max_work_units_per_task` | Declared, not enforced |

## 3. Decisions (locked 2026-09-17)

| Decision | Choice | Basis |
| --- | --- | --- |
| Adapter host | **In-process `ApplicationService` handle** (same as M1) | M1 ADR limitation; REST/MCP is M3 |
| Planner for G1–G5 | **`FakePlannerAdapter`** first | Deterministic gates before live spend |
| Live G6 | Real model via existing OpenAI-compatible env + M1 Docker sandbox | Reuse M1 harness patterns |
| H-035 barrier | Fake Worker pauses after entering execution; test asserts overlapping `RUNNING`, then releases | Spec forbids log-time proximity alone |
| Parallel merge | Dedicated **INTEGRATE** WU with fixed input hashes; conflict → Repair | §11.1; no Worker chat |
| Message cursor interim | Append-only `task_messages` (minimal) with per-Task sequence; planner cursor advances in the same txn as checkpoint | Spec §9.1; full inspector is M4 |
| Schema generation | Bump `m1 → m2` only if new tables/columns are required; known older generations still migrate | Same startup pre-check pattern as M1 |

### Limits carried forward (not M2 blockers)

1. Orchestrator remains Docker-group / host-root-equivalent; Broker must not expose the socket.
2. Worker↔Core stays in-process until M3.
3. External business actions remain Fake.

## 4. Progress log

| Date | Change |
| --- | --- |
| 2026-09-17 | Checklist created from Spec §24.5 and the M2 development-order plan; decisions locked (§3). |
| 2026-09-17 | **A–J landed**: PLAN Run + marker, DAG refuse-without-destroy, Core-only spawn, barrier parallel + shared workspace serial, checkpoint/cursor + task_messages (`0008` / schema `m2`), VERIFYING transitions, INTEGRATE conflict retention, repair revision, revision/WU caps. |
| 2026-09-17 | **K done (Fake)**: H-033–H-038 + multi-agent H-011/H-013–H-016 in `tests/m2` (29 passed); G4 ×20; harness dry-run 3/3 (`m2_runner`). |
| 2026-09-17 | **L partial**: acceptance record `docs/acceptance/M2-2026-09-17.md` — G1–G5 PASS; G6 live run recorded **0/3** PASS (`workspace_missing` + Docker Desktop down). |
| 2026-09-17 | **m2_runner**: load `.env` with M1 `HIBIKI_MODEL_*` names; live routes PLAN→FakePlanner / EXECUTE→ApiAgentAdapter. |
| 2026-09-17 | **G6 retry PASS 3/3**: Docker up; workspace seeding; Windows `WorkspacePaths` path-mode; clearer c2/c3 objectives. Live evidence refreshed. |
| 2026-09-17 | **Review of `1eb2477`**: acceptance reverted to **PENDING**. Blocking P1s: cross-principal orchestration, VERIFY FAIL→PASS rewrite, unconfirmed stop, unwired Planner recovery, discarded WorkUnit assignment, weak harness gate. |
| 2026-09-17 | **Remediation landed (code)**: orchestration Human+principal; PLAN generation bump revokes prior PLAN Run; VERIFY keeps COMPLETED+FAIL; unconfirmed stop/Docker query_failed; PLAN RunInput/messages; WorkUnit objective/input_refs frozen; harness DAG/artifact gate + VERDICT_PASS; truncated ContextAppend + seed parents. Regression: `tests/m2/test_review_regressions.py`. Full live G6 re-acceptance still required before PASS. |