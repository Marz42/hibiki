# M2 live G6 acceptance evidence — 2026-09-17

Three pre-fixed complex tasks (`docs/m2/tasks/`) run against the real provider
(`deepseek-flash` @ `https://api.deepseek.com`) through the M1 Docker sandbox
(`hibiki-sandbox:py312`, Docker 28.5.1). Gate (§24.5 G6, spec line 915): **at least two of
three tasks complete inside budget, and all three collaboration/failure processes are
reconstructible.**

Verdict: **PASS — 3/3 completed**, harness exit code 0.

## Command

```bash
uv run --no-sync python -m hibiki.interfaces.m2_runner --live --clean \
  --data-dir /tmp/hibiki-m2-live12 --out docs/acceptance/evidence/m2-2026-09-17-live
```

## Per-task result

| Task | Result | Runs | Gate detail |
| --- | --- | --- | --- |
| m2-c1 parallel INTEGRATE + VERIFY | **COMPLETED** | 4 (2 parallel EXECUTE → INTEGRATE → VERIFY) | `unbacked_artifacts == []`, `artifact_binding_ok` |
| m2-c2 dual edit + integrate + verify | **COMPLETED** | 4 | same; injected FAIL did not fire (model verified PASS) |
| m2-c3 convert + catalog | **COMPLETED** | 4 | same |

`summary.json`: `completed: 3, failed: 0, ok: true`. Every task reports a typed
`work_type=VERIFY` unit in `verify_pass_work_units`, zero unbacked artifact references, and
`artifact_binding_ok == true`.

## What the passing run demonstrates

- **Parallel EXECUTE + INTEGRATE + VERIFY closes the loop** end to end against a real model
  and a real sandbox, with content-backed artifacts and no fabricated references.
- **Dependency delivery works**: upstream deliverables are staged into each dependent
  Workspace (INTEGRATE reads `<work_unit_id>.<ext>`; VERIFY reads `delivered/`), which is
  what lets an INTEGRATE unit actually merge its prerequisites.
- **The failure/revision path is exercised elsewhere in this session**: a real VERIFY FAIL
  produced Plan **v2** with a new REPAIR + VERIFY pinned to the digest under verification.
  In *this* run the model verified PASS first time, so `inject_triggered` is false and no
  revision was built. Both shapes are accepted by the gate.

## Harness defects this run depends on being fixed

1. the live path read a removed `boot["integ_hash"]` and would have raised;
2. `ApiAgentAdapter.max_turns` defaulted to 12, silently overriding the SPEC §27 ceiling of
   30 — it now defaults to the system limits;
3. a negated marker in prose ("[[HIBIKI:BLOCKED]] is not applicable") was read as a real
   block, failing a complete delivery;
4. upstream deliverables were never staged into dependent Workspaces;
5. a Repair revision's Workspaces were never provisioned, so the REPAIR Run reported
   `workspace_missing` against a root-owned empty directory;
6. task objectives described the whole pipeline per unit, so workers attempted later units'
   work and exhausted their turns.

## Verification at this tree

- `pytest-full.log` / `junit-full.xml`: **321 passed, 0 skipped** (Docker up, so the
  sandbox suite runs instead of skipping).
- `uv run ruff check .`: clean.
- `source-files.sha256` pins the tree that produced this evidence.

## Not claimed

A single passing run is not repeatability evidence. Live runs vary; the Fake gate (×20)
carries the repeatability requirement. See `docs/M2-KNOWN-GAPS.md` for the full list of
recorded limits, including the live REPAIR context gap and the hash-less live
`VERDICT_PASS` edge.
