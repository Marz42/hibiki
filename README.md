# HIBIKI

Task-centered agent execution system. M0 implements the deterministic kernel
(no LLM, no Docker, no WebUI): Contract → Plan → Run → Decision with Fake adapters.

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
  **PENDING**: H-022–H-032 pass, the hardened per-Run Docker sandbox and the ≤15 s stop budget are
  measured, and the three fixed tasks run 6/6 against a local provider double. The live-model gate
  (six runs with operator credentials) is the only open item. Evidence under
  `docs/acceptance/evidence/m1-2026-09-13/`; the executable plan is
  [`docs/M1-CHECKLIST.md`](docs/M1-CHECKLIST.md).

The suite runs offline on Fake adapters only; set `UV_CACHE_DIR` to a writable path when the
default uv cache is not accessible:

```bash
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv sync
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv run --no-sync pytest -q
```
