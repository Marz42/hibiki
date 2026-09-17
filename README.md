# HIBIKI

Task-centered agent execution system. M0 is the deterministic kernel; M1 adds a
real Worker / sandbox boundary; M2 adds PlannerSession, DAG parallelism,
Integration, and Verify/Repair.

## Quick start

```bash
uv sync
uv run alembic upgrade head
uv run hibiki --help
uv run pytest
```

See `HIBIKI_MVP_SPEC_v0.2.1.md` and `docs/adr/`.

## Acceptance status

- **M0 §24.3**: [`docs/acceptance/M0-2026-09-13.md`](docs/acceptance/M0-2026-09-13.md) —
  **PASS** after the round-8 repairs (100 fixed seeds each dispatch a Run and submit a worker
  result; an unknown schema version refuses startup with a diagnostic). Evidence under
  `docs/acceptance/evidence/m0-2026-09-13/`.
- **M1 §24.4**: [`docs/acceptance/M1-2026-09-13.md`](docs/acceptance/M1-2026-09-13.md) —
  **PASS**: H-022–H-032 pass, the hardened per-Run Docker sandbox and the ≤15 s stop budget are
  measured, the three fixed tasks pass against a provider double and in the chaos rehearsal, and
  the live gate ran **6/6** (three fixed tasks × 2 runs, zero authorization violations, every
  artifact verified). Evidence under `docs/acceptance/evidence/m1-2026-09-13/`; the executable
  plan is [`docs/M1-CHECKLIST.md`](docs/M1-CHECKLIST.md).
- **M2 §24.5**: [`docs/acceptance/M2-2026-09-17.md`](docs/acceptance/M2-2026-09-17.md) —
  **PASS** for G1–G6. The Fake gate passes after two remediation rounds (c1/c2/c3 3/3 with
  a real FAIL→REPAIR→new-VERIFY, G4 ×20 with zero duplicate runs and zero PASS-gate
  bypasses; `docs/acceptance/evidence/m2-2026-09-17-remediation/`). The live gate ran three
  real complex tasks on `deepseek-flash` through the Docker sandbox and **all three
  completed** (`docs/acceptance/evidence/m2-2026-09-17-live/`). Remaining limits are
  recorded in [`docs/M2-KNOWN-GAPS.md`](docs/M2-KNOWN-GAPS.md); checklist:
  [`docs/M2-CHECKLIST.md`](docs/M2-CHECKLIST.md).

### Running the M1 live-model gate

Fill in `.env` (gitignored; see `.env.example`) with any OpenAI-compatible endpoint, then:

```bash
uv run --no-sync python -m hibiki.interfaces.m1_runner --check-credentials --out /tmp/check
uv run --no-sync python -m hibiki.interfaces.m1_runner \
  --data-dir /tmp/hibiki-m1 --out docs/acceptance/evidence/m1-<date>/live-runs --clean
```

The first command makes one real call and prints the model's reply (it never prints the key).
The second runs the three fixed tasks in `docs/m1/tasks/` twice each and writes one JSON
record per run plus a `summary.json`; the gate is at least 5 of 6 runs with a
COMPLETED/PASS result, verified artifacts and no fabricated artifact reference.
Without credentials, `--dry-run` exercises the same path and never reports a pass.

### Running the M2 complex Fake / live harness

```bash
# Fake G1-G5 gate (c1/c2/c3). Exits non-zero unless every Task's gate passes.
uv run --no-sync python -m hibiki.interfaces.m2_runner --dry-run --clean \
  --data-dir /tmp/hibiki-m2 --out docs/acceptance/evidence/m2-<date>/fake-complex

# G4: the same Fake complex scenario 20x, checking duplicate-run and PASS-bypass invariants.
uv run --no-sync python -m hibiki.interfaces.m2_runner --dry-run --clean --repeat 20 \
  --data-dir /tmp/hibiki-m2-g4 --out docs/acceptance/evidence/m2-<date>/g4-x20

# Live G6 (requires a provider key and the Docker sandbox).
uv run --no-sync python -m hibiki.interfaces.m2_runner --live --clean \
  --data-dir /tmp/hibiki-m2 --out docs/acceptance/evidence/m2-<date>/live-runs
```

The Fake path publishes real, content-backed artifacts, so the `VERDICT_PASS` edge binds a
genuine digest and c2's FAIL→REPAIR→new-VERIFY path executes. `summary.json` counts gate
passes (not Run rows) and the process exits non-zero when any Task fails its gate. The suite
runs offline on Fake adapters only; set `UV_CACHE_DIR` to a writable path when the default uv
cache is not accessible:

```bash
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv sync
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv run --no-sync pytest -q
```
