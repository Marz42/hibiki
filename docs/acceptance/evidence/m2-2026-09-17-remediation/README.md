# M2 remediation evidence — Fake gate (2026-09-17)

Scope: second adversarial review of `8932533` (five blocking P1s). This pack re-runs the
Fake G1–G5 surface after the remediation below. **Live G6 is not in this pack** — it needs
fresh provider credentials and a working Docker/WSL integration (see `environment.txt`:
`docker: DOWN`, and the `.env` key returns 401).

## Commands (all exit 0)

```bash
uv run --no-sync python -m hibiki.interfaces.m2_runner --dry-run --clean \
  --data-dir /tmp/hibiki-ev-fake --out docs/acceptance/evidence/m2-2026-09-17-remediation/fake-complex

uv run --no-sync python -m hibiki.interfaces.m2_runner --dry-run --clean --repeat 20 \
  --data-dir /tmp/hibiki-ev-g4 --out docs/acceptance/evidence/m2-2026-09-17-remediation/g4-x20

uv run --no-sync pytest tests/m2 tests/m1 -q \
  --junitxml=docs/acceptance/evidence/m2-2026-09-17-remediation/junit-m1-m2.xml
```

## Results

| Check | Result | Evidence |
| --- | --- | --- |
| Fake complex c1/c2/c3 | **3/3 PASS** (`summary.ok=true`), exit 0 | `fake-complex/summary.json` |
| c2 FAIL→REPAIR→new-VERIFY | executed: plan v1 FAIL → plan **v2** with REPAIR + new VERIFY PASS | `fake-complex/m2-c2.json` |
| Artifact content backing | 0 `unbacked_artifacts`, 0 `unverified_artifact_content`, `artifact_binding_ok=true` | `fake-complex/m2-*.json` |
| G4 ×20 | 20/20 ok, **0 duplicate Work Unit runs**, **0 PASS-gate bypasses**, repair ran in 20/20 | `g4-x20/g4-summary.json` |
| M1+M2 suites | 102 passed, 1 skipped (docker unavailable) | `pytest-m1-m2.log`, `junit-m1-m2.xml` |

## What was broken before

The previous pack reported `completed: 3` while all three Tasks were `ok=false /
INCOMPLETE`: the `INTEGRATE` deliverable was declared as the synthetic string
`integ-m2-c1`, so `ArtifactRow.artifact_uri` stayed null, `verified_artifact_hash` never
bound, and the `VERDICT_PASS` edge could never become ready. VERIFY therefore never
dispatched and c2's injected FAIL→REPAIR path never ran. The summary counted "a Run row
exists" and the process always returned 0.

## Remediation in this pack

1. `_parse_explicit_result` parses the **complete** top-level JSON object (depth/string
   aware) instead of `rfind("{")`; truncated fragments no longer yield a scraped verdict,
   and nested `acceptance_evidence` survives.
2. PLAN manifest emits `mandatory_refs`/`dependency_result_refs` (the keys the reader
   materializes) with a real `mandatory_bytes`; the harness calls `FakePlannerAdapter.bind_core`.
3. `advance_planner_checkpoint` delegates to `_assert_planner_write_auth` (principal +
   active PLAN Run + generation), closing the revoked-run and cross-principal writes.
4. A pre-execution sandbox claim no longer quarantines the Workspace; a confirmed exit
   releases it, while an unconfirmed terminal Run still quarantines (`reconcile`).
5. Harness judges against the **final ACTIVE plan**, requires a typed `VERIFY` PASS, and
   returns non-zero when the gate fails.
6. Fake workers **publish real bytes**; the `VERDICT_PASS` edge is pinned to the real
   content digest; `list_unbacked_artifacts` + `verify_artifact_content` gate the result.

## Known limits (unchanged)

- In-process Core until M3; Docker-group orchestrator.
- `pending:<run_id>:<ns>` sandbox identity is not a Docker-queryable id, so a crash
  between claim and exit still relies on `reconcile` rather than a container inspect.
- Live G6 must be re-run before M2 is frozen.
