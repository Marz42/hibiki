# M1 Execution Checklist — Real Worker & Execution Boundary (§24.4)

Status: **in progress** (started 2026-09-13, after M0 PASS / `75ab738`)
Spec: `HIBIKI_MVP_SPEC_v0.2.1.md` §24.4, with §9.2 / §10 / §11 / §13 / §14 / §17 / §18 / §19
Design rules for this milestone: `docs/adr/0001-tech-stack-m0.md`, `docs/adr/0002-known-limits-m0.md`

## 0. Gate definition (copy of §24.4)

Scope: one API-type Adapter, Tool Broker, Sandbox, Workspace, Artifact publishing,
ContextManifest/Append, real process stop and minimum recovery.

Must complete:

| # | Requirement |
| --- | --- |
| G1 | H-022–H-032 all pass; H-019's old-writer scenario re-verified with a real subprocess/container |
| G2 | Path traversal, symlink escape, cross-Task file/Artifact access, forbidden network, Human/vendor credential probing — all refused |
| G3 | Three pre-fixed simple tasks, each run twice: attachment summarization + structured result; small file conversion; controlled sample-repo code change + tests. ≥5 of 6 inside budget; all failures explainable; zero authorization violations |
| G4 | 100% record completeness per real Run: Profile, Manifest, actual inputs, tool requests, Result, Artifact hash |
| G5 | Pause/Cancel from command submission to executor exit ≤15 s for killable test processes; unconfirmable exits stay quarantined and are never reported as stopped |
| G6 | One repeatable recovery case each for: model request interrupt, worker crash, crash during artifact publish |
| Gate | Controlled real tasks allowed; no unverified host-shell Adapter; external business actions remain Fake/test endpoints |

## 1. Task list

Each task states: what to build, the SPEC clauses, the acceptance IDs, and the verification
evidence. Tasks A–F are infrastructure; G–J wire it into the kernel; K–L are the gate runs.

### Task A — Run execution contract (host ↔ worker ↔ Core)

- **Build**: `RunExecutionSpec` (the authoritative payload the runtime hands to a worker:
  run_id, task_id, work_unit_id, assignment_kind, profile ref, contract/plan versions,
  context manifest ref, workspace ref, granted tools, grant/fencing epochs, budgets) and the
  worker→Core write path (result, artifact publication, tool request, heartbeat, exit confirm)
  using run-bound Internal credentials (`bound_task_id` / `bound_run_id` / `bound_fencing_epoch` /
  `bound_grant_epoch`), shaped exactly like the existing test helper.
- **Spec**: §9.2, §12.1, §13.1, §15.
- **Decision pending**: adapter host = in-process `ApplicationService` handle (M1) vs HTTP client
  to a Core service. Default recommendation: in-process, recorded as an ADR limitation.
- **Verify**: unit tests that a forged/foreign/unbound credential is refused for every worker write
  (reuses H-002/H-004/R6 coverage patterns), and that a valid spec round-trips.

### Task B — OpenAI-compatible real adapter loop

- **Build**: `hibiki.runtime.api_agent.ApiAgentAdapter` implementing `AgentAdapter`
  (`start`/`send`/`stop`/`inspect`) plus a per-Run worker thread/process that runs the model loop:
  system+task prompt from the manifest, tool-call loop, streaming or non-streaming, retries with
  classified errors, usage accounting, bounded turns, and result submission through Task A.
- **Spec**: §9.2, §22.3, §17; INV-03/04/06.
- **Provider**: user-supplied `base_url` + `api_key` + `model` via env only (never in repo or DB).
- **Adapter contract tests**: `start` idempotent per run_id; revoke-before-register is atomic with
  `stop`; `inspect` reports identity/liveness/stop result (not a bare PID); `stop` reaps the whole
  process group; crash of the loop is observable as a failed/lost Run, never a false SUCCEEDED.
- **Verify**: adapter contract suite; one live smoke run.

### Task C — Tool Broker

- **Build**: `hibiki.tools.broker.ToolBroker` — pre-execution validation of authenticated Run,
  valid assignment, current `grant_epoch`, task control state, resource match, parameter digest,
  budget, and required approval; atomic claim of execution right in Core; actual I/O outside the DB
  transaction; every refusal audited; no silent widening on repeated requests.
- **Spec**: §13.1, §13.2, §16.1; INV-09.
- **Tools for M1**: `fs.read`, `fs.write`, `fs.list`, `shell.run` (sandboxed), `artifact.publish`.
- **Verify**: H-026 (unauthorized artifact/message/other-Task read refused), H-027 (traversal),
  H-028 (network/credential), plus a denied-tool audit test.

### Task D — Sandbox (per-Run container)

- **Build**: `hibiki.tools.sandbox.DockerSandboxAdapter` implementing `SandboxAdapter`:
  non-root, `no-new-privileges`, dropped capabilities, CPU/memory/pids/time limits, only the current
  Workspace writable, authorized inputs read-only, no host home / other Task / credentials /
  container socket, **no network by default**, model API key never injected into the container.
- **Spec**: §13.3, §11.1; INV-07.
- **Env**: Docker 28.5.1 healthy, cgroup v2 with cpu/memory/pids controllers (measured 2026-09-13).
- **Verify**: the H-028 battery (no external connection, no credential reachable, no host path
  escalation) and a resource-limit test (OOM/pids/time) that is not reported as success.

### Task E — Workspace management (real files, real writer)

- **Build**: on-disk Workspace per Work Unit (`data/workspaces/<ws_id>`), git worktree/branch or an
  isolated clone containing only the authorized baseline; staging area for outputs; baseline and
  post-Run diff recorded; no silent discard on retry; lock carries `owner_run_id` +
  `fencing_epoch`; actual writer exit required before release, otherwise QUARANTINED.
- **Spec**: §11.1, §11.3, §16.1(7), §17, §19; INV-07.
- **Verify**: H-029 (two writers, one admitted), H-019 rewrite with a real stubborn process
  (`setsid` + SIGKILL resistance), quarantine-then-release path.

### Task F — Artifact publishing with crash recovery

- **Build**: staging write → hash → fsync → atomic promote to immutable content path → DB
  registration in the same transaction as the Result/Event reference; scan for orphan files after a
  crash in the publish window; never register a file that is not readable, never leave a completed
  result without its file; GC only unreferenced staging content.
- **Spec**: §11.2, §11.3, §19.1.
- **Verify**: H-031 (crash between file publish and DB registration), orphan scan, hash mismatch
  blocks acceptance.

### Task G — ContextManifest materialization and ContextAppend

- **Build**: materialize the immutable initial manifest (Contract/Plan/dependency/artifact refs with
  fixed version+hash) per Run; record what was actually handed to the adapter; mandatory-context
  overflow → BLOCKED or an explicit preparation Work Unit, never silent truncation; append-only
  `ContextAppend` for dynamic inputs (feedback, user additions, tool reads) with reason,
  authorization ref, version/hash and materialized hash; sensitivity redaction before model/audit.
- **Spec**: §10.1–§10.4.
- **Verify**: H-023 (FRESH, no implicit transcript inheritance), H-024 (overflow), H-025 (append with
  provenance), H-012 (profile version frozen).

### Task H — Real process stop, heartbeat, lease, fencing

- **Build**: heartbeat from the authenticated run manager; lease expiry triggers reconciliation, not
  a death certificate; new assignment bumps the epoch and Core/Broker reject old-epoch control and
  tool requests; old Results become history only; stop = cooperative window then process-group
  SIGTERM→SIGKILL with exit verification (identity via `/proc` start time or pidfd, not a bare PID).
- **Spec**: §16.1, §18.1, §18.2, §19.2.
- **Verify**: G5's ≤15 s measurement, H-010 (late completion), H-019, stubborn-writer quarantine.

### Task I — Real crash recovery cases

- **Build**: the three §24.4 recovery cases as repeatable tests: model request interrupted,
  worker process crashes, crash during artifact publish. Recovery may leave the Run LOST/FAILED and
  the Work Unit retryable/BLOCKED; it must never double-write a Workspace or replay an external
  action.
- **Spec**: §17, §19.1, §19.2.
- **Verify**: each case green twice in a row from a clean data dir, with the recovery decision
  recorded as an Event.

### Task J — Permissions, grants and CLI/entry surface for real runs

- **Build**: Run permission = System ∩ Contract ceiling ∩ Profile ∩ assignment ∩ Grant, with the
  Profile driven selection and the grant epoch checked at tool time; CLI/entry path to launch a real
  run for local testing (still no host-shell adapter).
- **Spec**: §13.1, §22.1, §23.
- **Verify**: no-silent-escalation tests; Profile change only affects new Runs (H-012).

### Task K — Three fixed real tasks × 2 runs

Pre-fix the tasks and their contracts, then run each twice (6 runs total):

| Task | Deliverable | Evidence |
| --- | --- | --- |
| K1 | Attachment summarization + structured result | Result JSON + summarized Artifact with hash |
| K2 | Small file conversion | converted file + hash + conversion log |
| K3 | Controlled sample-repo change + tests | diff, test output, new commit/hash |

- **Gate**: ≥5/6 inside budget, every failure explained, zero authorization violations.
- **Record**: per Run — Profile, Manifest, actual inputs, tool requests, Result, Artifact hashes
  (G4: 100% completeness).

### Task L — Acceptance record and freeze

- **Build**: `docs/acceptance/M1-<date>.md` + `docs/acceptance/evidence/m1-<date>/` with the same
  evidence discipline as M0 (environment, source-tree hash, junit, per-gate measurements, failures
  kept), plus an updated ADR for the known limits (in-process host, sandbox scope, dynamic
  references).
- **Gate**: G1–G6 reviewed together; M2 only after this record passes.

## 2. Dependency order

```
A ─┬─ B ─┬─ C ─┬─ K ─ L
   │     │     │
   ├─ E ─┴─ D ─┘
   ├─ F
   ├─ G
   └─ H ─ I
J threads through B–D (grants per tool call)
```

Critical path: A → (B, E) → D → C → K → L. F, G, H, I can proceed in parallel once A lands.

## 2.5 M0 seams to build on (from the extension-point map)

| Seam | Location |
| --- | --- |
| Five ports to implement | `src/hibiki/domain/ports.py` (`AgentAdapter`, `SandboxAdapter`, `ExternalAdapter`, `ArtifactStore`, `Clock`) |
| Run creation + `agent.start` enqueue | `_dispatch_ready_runs` (`application/service.py:1609-1786`; payload `1751-1773`) |
| Outbox dispatch branches | `_dispatch_outbox_item` (`application/service.py:3664-3691`) |
| Run-bound write authorisation | `_assert_run_write_binding` (`application/service.py:1810-1854`) |
| Workspace lock state | `WorkspaceRow` (`persistence/models.py:275-286`), created in `_activate_plan` (`service.py:1522-1537`) |
| Manifest stub | `ContextManifestRow` (`models.py:288-298`), written at `service.py:1706-1717` |
| Artifacts | `ArtifactRow` (`models.py:429-441`), `_register_result_artifacts` (`service.py:2027-2105`) |
| Service collaborators | `ApplicationService.__init__` (`service.py:88-102`) |
| Schema version | `ensure_schema_version`/`bootstrap.py` expects `"m0"`; Alembic head is `0004_evidence_sequence` |

Blocking gaps (dependency order): (1) `agent.start` payload lacks tools/manifest/workspace/profile;
(2) ContextManifest is a stub with no append model; (3) Workspace has no filesystem backing;
(4) `ArtifactStore` is not a service dependency and artifact hashes are submitter-asserted;
(5) `SandboxAdapter` unwired; (6) ToolBroker unwired and `permission_ceiling` never enforced;
(7) stop has no kill timeout or 15 s budget (`cooperative_stop_window_seconds` unused);
(8) liveness is `inspect`-only with a `fake:{run_id}` identity; (9) new tables need migration `0005`
and a schema-version bump; (10) no test touches real adapters, broker, sandbox, workspace files,
manifest append, or artifact content.

## 3. Decisions (locked 2026-09-13)

| Decision | Choice | Basis |
| --- | --- | --- |
| Adapter host | **In-process `ApplicationService` handle** with run-bound Internal credentials; the model loop runs in a worker thread/process outside DB transactions | operator decision; limitation recorded in ADR |
| Model access | **`base_url` + `api_key` + `model` from environment only** (never in repo or DB); provider OpenAI-compatible, operator-supplied key | operator decision |
| HTTP client for the adapter | **Plain `httpx` against the OpenAI-compatible REST API** (no `openai` SDK): the SDK 3.x moved to `httpx2`, which the project does not use, and M1 needs only `chat.completions` | research report §1/§5 |
| Sandbox | **Docker per-Run container** as primary; `systemd-run --user` service as fallback driver | research report §2; Docker hardened profile verified on host |
| Path safety | **`dir_fd` component walk with `O_NOFOLLOW`/`O_DIRECTORY`** (not `startswith`, not `realpath`+`commonpath` alone) | research report §3; `openat2` is unavailable from CPython |
| Stop/exit proof | **pidfd + cgroup/container exit + `/proc` starttime identity**; never a bare PID | research report §4 |
| Target runtime | **Python 3.12** (`.python-version`, ADR-0001), despite spec §22.1 preferring 3.13 | host `.venv` is 3.12.12 |

### Host constraints and risks to carry into the M1 record

1. **Docker Desktop is rootful inside its VM**, and `ventus` is in the `docker` group, so the
   *orchestrator* is host-root-equivalent. The Broker must never expose the Docker socket to a
   sandbox. 2. **Rootless Docker cannot be satisfied here** (no `uidmap`/`newuidmap`, Desktop has no
   rootless mode) — spec §13.3's preference is not met in M1; this is an explicit limitation.
   3. **This session's own file sandbox (Landlock) makes userns sandboxes fail** (`unshare -Ur`
   cannot write `/proc/self/uid_map`), so sandbox tests must go through Docker or
   `systemd-run --user`; a failure there is not evidence about the real host. 4. Wall-clock limits
   are not a Docker flag — enforce orchestrator-side plus `--stop-timeout`. 5. `tarfile` on Python
   3.12 still defaults to `fully_trusted`; pass `filter="data"` explicitly for any extraction.

## 4. Progress log

| Date | Change |
| --- | --- |
| 2026-09-13 | Checklist created; environment recon (Docker 28.5.1 healthy, cgroup v2, no API credentials on host); M0 extension-point and adapter/sandbox research dispatched. |
| 2026-09-13 | Research complete: M0 extension-point map (gaps 1–10) and adapter/sandbox/path/stop report. Decisions locked (§3). Docker hardened sandbox profile verified on host: uid 65534, read-only root (`EROFS`), `CapEff=0`, `NoNewPrivs=1`, no network, empty `/home`. |
| 2026-09-13 | **Task A done**: `RunExecutionSpec` frozen at dispatch, persisted as `run_inputs`, referenced by the `agent.start` payload (spec hash + workspace path + granted tools); `get_run_input()` readable only through the run-bound credential; schema generation bumped `m0 → m1` and the startup pre-check now refuses *unknown* generations while still migrating a known older one (M0 database upgrades in place). 9 new tests. |
| 2026-09-13 | **Task A2 done** (delegated): migration `0005_m1_execution` + `RunInputRow` / `ToolInvocationRow` / `ContextAppendRow`, upgrade+downgrade verified on a scratch database. |
| 2026-09-13 | **Task D done** (delegated): `DockerSandboxAdapter` hardened profile; 11 tests executed with Docker, 0 skipped — read-only root, uid 65534, zero caps, `NoNewPrivs`, no network, ro inputs, OOM kill reported as `error` (not success), wall-clock timeout in ~2 s with no lingering container. |
| 2026-09-13 | Full suite after A + A2 + D: **123 passed**, 0 failed. |
| 2026-09-13 | **Task C done** (delegated): `WorkspacePaths` dir_fd walk + `ToolBroker` that audits every ALLOW/DENY into `tool_invocations`; denial cases (ceiling, foreign credential, not-RUNNING, frozen task, gate, epoch mismatch, path escape/symlink) all covered. |
| 2026-09-13 | **Task E done** (delegated): real on-disk workspaces with baseline/diff/archive manifest and quarantine markers. |
| 2026-09-13 | **Task F done**: `publish_artifact` (Internal-only, run-bound) hashes the file inside the Run's own workspace, stores bytes immutably *before* the DB transaction, keeps duplicates idempotent; `artifact_uri`/size/source columns via migration 0006 and `list_orphan_artifacts()` for the §11.3 crash window. 8 tests including the crash case. |
| 2026-09-13 | **Task B done** (delegated): `OpenAICompatibleClient` over httpx (no openai-SDK dependency) + `ApiAgentAdapter` real worker loop with idempotent start, honest stop, run-bound result submission and model-usage accounting; `bootstrap_core` now accepts any adapter. |
| 2026-09-13 | Full suite after B + C + E + F: **194 passed**, 0 failed. Commits `73dc42e`, `81a918e`, `2dc38ad`. |
| 2026-09-13 | Remaining before the gate: Task G (manifest materialization + ContextAppend), Task H/I (≤15 s stop proof, three recovery cases), Task J (permission/grant wiring into the broker), then Task K (3 fixed tasks × 2 live runs) and Task L (record). **Blocked on the operator's `base_url` + `api_key` + `model` for the live runs.** |
