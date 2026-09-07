# ADR-0001: M0 tech stack lock

## Status

Accepted — 2026-09-07

## Context

SPEC v0.2.1 recommends Python 3.13. The bootstrap host provides CPython 3.12.9
(uv resolved 3.12.13). M0 must freeze a reproducible stack without changing
system invariants.

## Decision

- Language: Python **3.12** (bugfix-supported; upgrade path to 3.13 via later ADR).
- Validation: Pydantic v2
- Persistence: SQLAlchemy 2.0 sync Session + Alembic + SQLite WAL
- Scheduling: serial command queue in-process; Inbox/Outbox in SQLite
- Adapters: FakeAgentAdapter, FakeClock, FakeExternalAdapter
- Tooling: uv lock, Ruff, pytest, Hypothesis
- Interfaces in M0: CLI only (Application Service shared)

## Consequences

- `requires-python = ">=3.12"` in pyproject.toml
- No FastAPI/WebUI/MCP/Docker in M0
- Schema and protocol remain SPEC-aligned; runtime version is an environment detail
