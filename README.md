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
