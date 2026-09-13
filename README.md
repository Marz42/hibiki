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

Latest M0 §24.3 record: [`docs/acceptance/M0-2026-09-13.md`](docs/acceptance/M0-2026-09-13.md) —
**PASS** after the round-8 repairs: 102 tests pass, the 100 fixed seeds each dispatch a Run and
submit a worker result, and an unknown schema version refuses startup with a diagnostic. The M0
schema is frozen at that commit; M1 (single real worker) may start. Raw evidence is under
`docs/acceptance/evidence/m0-2026-09-13/`.

The suite runs offline on Fake adapters only; set `UV_CACHE_DIR` to a writable path when the
default uv cache is not accessible:

```bash
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv sync
UV_CACHE_DIR=/tmp/hibiki-uv-cache uv run --no-sync pytest -q
```
